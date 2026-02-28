#!/usr/bin/env python3
"""
Migration script to update membership SK format to include workspace_id

Old format: SK = ACCOUNT#{account_id}
New format: SK = ACCOUNT#{account_id}#WORKSPACE#{workspace_id}

This enables users to have multiple memberships per account (one per workspace).

Usage:
    python migrations/migrate_memberships_add_workspace_to_sk.py            # Dry-run (preview changes)
    python migrations/migrate_memberships_add_workspace_to_sk.py --execute  # Execute migration
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


class MembershipSKMigration:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        
        # Get table name from environment
        self.membership_table_name = os.getenv("MEMBERSHIPS_TABLE_NAME")
        
        if not self.membership_table_name:
            raise ValueError("MEMBERSHIPS_TABLE_NAME must be set in .env")
        
        self.membership_table = self.dynamodb.Table(self.membership_table_name)
        
        # Statistics
        self.stats = {
            "total_scanned": 0,
            "migrated": 0,
            "skipped": 0,
            "errors": 0
        }
    
    def scan_memberships(self):
        """Scan all memberships from the table"""
        items = []
        
        print(f"📊 Scanning {self.membership_table_name}...")
        
        try:
            response = self.membership_table.scan()
            items = response.get("Items", [])
            
            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.membership_table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))
            
            self.stats["total_scanned"] = len(items)
            print(f"✅ Found {len(items)} memberships\n")
            return items
            
        except Exception as e:
            print(f"❌ Error scanning memberships: {e}")
            self.stats["errors"] += 1
            raise
    
    def migrate_membership(self, item: dict[str, Any]):
        """Migrate a single membership to the new SK format"""
        old_pk = item.get("PK")  # USER#{user_id}
        old_sk = item.get("SK")  # ACCOUNT#{account_id} or ACCOUNT#{account_id}#WORKSPACE#{workspace_id}
        
        if not old_pk or not old_sk:
            print(f"❌ ERROR: Invalid item (missing PK or SK): {item}")
            self.stats["errors"] += 1
            return
        
        # Check if already migrated (has WORKSPACE in SK)
        if "#WORKSPACE#" in old_sk:
            # Check if it still has lowercase workspace_id that needs cleanup
            if "workspace_id" in item:
                if self.dry_run:
                    print(f"[DRY RUN] Would clean up lowercase workspace_id from: {old_pk} / {old_sk}")
                else:
                    # Remove lowercase workspace_id from already-migrated record
                    try:
                        self.membership_table.update_item(
                            Key={"PK": old_pk, "SK": old_sk},
                            UpdateExpression="REMOVE workspace_id"
                        )
                        print(f"🧹 Cleaned up lowercase workspace_id from: {old_pk} / {old_sk}")
                    except Exception as e:
                        print(f"⚠️  Error cleaning up {old_pk}/{old_sk}: {e}")
                        self.stats["errors"] += 1
            else:
                print(f"✓ Already migrated: {old_pk} / {old_sk}")
            self.stats["skipped"] += 1
            return
        
        # Get workspace_id - check both lowercase and uppercase
        workspace_id = item.get("WORKSPACE_ID") or item.get("workspace_id")
        
        if not workspace_id:
            print(f"⚠️  WARNING: Membership {old_pk}/{old_sk} has no workspace_id, skipping")
            self.stats["skipped"] += 1
            return
        
        # Create new SK with workspace
        new_sk = f"{old_sk}#WORKSPACE#{workspace_id}"
        
        if self.dry_run:
            print(f"[DRY RUN] Would migrate: {old_pk} / {old_sk} -> {new_sk}")
            self.stats["migrated"] += 1
            return
        
        # Create new item with workspace in SK
        # Remove lowercase workspace_id if it exists to avoid duplicates
        new_item = {**item}
        new_item["SK"] = new_sk
        new_item["WORKSPACE_ID"] = workspace_id  # Use uppercase for consistency
        
        # Remove lowercase workspace_id if it exists
        if "workspace_id" in new_item:
            del new_item["workspace_id"]
        
        try:
            # Write new item
            self.membership_table.put_item(Item=new_item)
            
            # Delete old item
            self.membership_table.delete_item(Key={"PK": old_pk, "SK": old_sk})
            
            print(f"✅ Migrated: {old_pk} / {old_sk} -> {new_sk}")
            self.stats["migrated"] += 1
            
        except Exception as e:
            print(f"❌ ERROR migrating {old_pk}/{old_sk}: {e}")
            self.stats["errors"] += 1
    
    def run(self):
        """Run the migration"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🚀 Starting Membership SK Migration ({mode})")
        print(f"{'='*60}")
        print("This will migrate memberships to the new SK format:")
        print("  Old: ACCOUNT#{account_id}")
        print("  New: ACCOUNT#{account_id}#WORKSPACE#{workspace_id}")
        print(f"{'='*60}\n")
        
        # Scan all memberships
        memberships = self.scan_memberships()
        
        if not memberships:
            print("✅ No memberships found. Migration not needed.")
            return
        
        # Confirm execution
        if not self.dry_run:
            print(f"\n⚠️  WARNING: About to migrate {len(memberships)} memberships")
            confirm = input("Type 'yes' to continue: ")
            if confirm.lower() != "yes":
                print("❌ Migration cancelled")
                return
            print()
        
        # Migrate each membership
        for membership in memberships:
            self.migrate_membership(membership)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Migration Summary ({mode})")
        print(f"{'='*60}")
        print(f"Total scanned:  {self.stats['total_scanned']}")
        print(f"Migrated:       {self.stats['migrated']}")
        print(f"Skipped:        {self.stats['skipped']}")
        print(f"Errors:         {self.stats['errors']}")
        print(f"{'='*60}\n")
        
        if self.dry_run:
            print("ℹ️  This was a DRY RUN. No changes were made.")
            print("   Run with --execute to apply changes.\n")
        else:
            print("✅ Migration complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Migrate membership SK to include workspace_id")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the migration (default is dry-run)"
    )
    
    args = parser.parse_args()
    
    migration = MembershipSKMigration(dry_run=not args.execute)
    migration.run()


if __name__ == "__main__":
    main()
