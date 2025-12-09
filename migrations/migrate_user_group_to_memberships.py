#!/usr/bin/env python3
"""
Migration: Copy user_group from User table to workspace_id in Memberships table

This script:
1. Scans all users with user_group attribute
2. For each user, gets their memberships
3. Updates each membership with workspace_id = user_group
4. Optionally removes user_group from user record (Phase 4)

Usage:
    python migrations/migrate_user_group_to_memberships.py            # Dry-run (preview changes)
    python migrations/migrate_user_group_to_memberships.py --execute  # Execute migration
"""

import os
import sys
import argparse
from typing import Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from boto3.dynamodb.conditions import Key


class UserGroupMigration:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        
        # Get table names from environment
        self.user_table_name = os.getenv("USER_TABLE_NAME")
        self.membership_table_name = os.getenv("MEMBERSHIPS_TABLE_NAME")
        
        if not self.user_table_name or not self.membership_table_name:
            raise ValueError("USER_TABLE_NAME and MEMBERSHIPS_TABLE_NAME must be set in .env")
        
        self.user_table = self.dynamodb.Table(self.user_table_name)
        self.membership_table = self.dynamodb.Table(self.membership_table_name)
        
        # Statistics
        self.stats = {
            "users_scanned": 0,
            "users_with_group": 0,
            "memberships_updated": 0,
            "errors": 0
        }
    
    def scan_users_with_group(self):
        """Scan all users and find those with user_group attribute"""
        users_with_group = []
        
        print(f"📊 Scanning {self.user_table_name} for users with user_group...")
        
        try:
            response = self.user_table.scan()
            items = response.get("Items", [])
            
            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.user_table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))
            
            for user in items:
                self.stats["users_scanned"] += 1
                
                if "user_group" in user and user["user_group"]:
                    users_with_group.append({
                        "id": user["id"],
                        "user_name": user.get("user_name", "Unknown"),
                        "user_group": user["user_group"]
                    })
                    self.stats["users_with_group"] += 1
            
            print(f"✅ Found {len(users_with_group)} users with user_group out of {self.stats['users_scanned']} total users")
            return users_with_group
            
        except Exception as e:
            print(f"❌ Error scanning users: {e}")
            self.stats["errors"] += 1
            raise
    
    def get_user_memberships(self, user_id: str):
        """Get all memberships for a user"""
        try:
            response = self.membership_table.query(
                KeyConditionExpression=Key("PK").eq(f"USER#{user_id}")
            )
            
            memberships = response.get("Items", [])
            
            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.membership_table.query(
                    KeyConditionExpression=Key("PK").eq(f"USER#{user_id}"),
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                memberships.extend(response.get("Items", []))
            
            return memberships
            
        except Exception as e:
            print(f"❌ Error getting memberships for user {user_id}: {e}")
            self.stats["errors"] += 1
            return []
    
    def update_membership_workspace(self, pk: str, sk: str, workspace_id: str):
        """Update a membership with workspace_id"""
        if self.dry_run:
            print(f"   [DRY RUN] Would update {pk}/{sk} with workspace_id={workspace_id}")
            return True
        
        try:
            self.membership_table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression="SET workspace_id = :wid",
                ExpressionAttributeValues={":wid": workspace_id}
            )
            return True
        except Exception as e:
            print(f"   ❌ Error updating membership {pk}/{sk}: {e}")
            self.stats["errors"] += 1
            return False
    
    def migrate_user(self, user: dict[str, Any]):
        """Migrate a single user's group to memberships"""
        user_id = user["id"]
        user_name = user["user_name"]
        user_group = user["user_group"]
        
        print(f"\n👤 Processing user: {user_name} (ID: {user_id}, group: {user_group})")
        
        # Get user's memberships
        memberships = self.get_user_memberships(user_id)
        
        if not memberships:
            print(f"   ⚠️  No memberships found for user {user_id}")
            return
        
        print(f"   📋 Found {len(memberships)} membership(s)")
        
        # Update each membership with workspace_id
        for membership in memberships:
            pk = membership["PK"]
            sk = membership["SK"]
            account_id = sk.split("#")[1] if "#" in sk else sk
            
            # Check if workspace_id already exists
            if "workspace_id" in membership:
                print(f"   ℹ️  Membership {account_id} already has workspace_id={membership['workspace_id']}, skipping")
                continue
            
            # Update membership
            if self.update_membership_workspace(pk, sk, user_group):
                self.stats["memberships_updated"] += 1
                print(f"   ✅ Updated membership for account {account_id}")
    
    def run(self):
        """Run the migration"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🚀 Starting User Group Migration ({mode})")
        print(f"{'='*60}\n")
        
        # Scan for users with user_group
        users_with_group = self.scan_users_with_group()
        
        if not users_with_group:
            print("\n✅ No users with user_group found. Migration not needed.")
            return
        
        # Confirm execution
        if not self.dry_run:
            print(f"\n⚠️  WARNING: About to update {len(users_with_group)} users' memberships")
            confirm = input("Type 'yes' to continue: ")
            if confirm.lower() != "yes":
                print("❌ Migration cancelled")
                return
        
        # Migrate each user
        for user in users_with_group:
            self.migrate_user(user)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Migration Summary ({mode})")
        print(f"{'='*60}")
        print(f"Users scanned: {self.stats['users_scanned']}")
        print(f"Users with user_group: {self.stats['users_with_group']}")
        print(f"Memberships updated: {self.stats['memberships_updated']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"{'='*60}\n")
        
        if self.dry_run:
            print("ℹ️  This was a DRY RUN. No changes were made.")
            print("   Run with --execute to apply changes.\n")
        else:
            print("✅ Migration complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Migrate user_group to workspace_id in memberships")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the migration (default is dry-run)"
    )
    
    args = parser.parse_args()
    
    migration = UserGroupMigration(dry_run=not args.execute)
    migration.run()


if __name__ == "__main__":
    main()
