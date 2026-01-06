#!/usr/bin/env python3
"""
Script: Disable Memberships

This script allows disabling memberships for a specific user or an entire account.

Usage:
    python migrations/disable_memberships.py --user-id USER_ID                                     # Disable all for user
    python migrations/disable_memberships.py --account-id ACCOUNT_ID                               # Disable all for account
    python migrations/disable_memberships.py --account-id ACCOUNT_ID --except-user-ids ID1,ID2     # Exclude users
    
    Add --execute to apply changes.
"""

import os
import sys
import argparse
from typing import Any, List
from dotenv import load_dotenv
import boto3
from boto3.dynamodb.conditions import Key

# Load environment variables
load_dotenv()

# Add parent directory to path for imports if needed in future
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class MembershipDisabler:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        
        self.membership_table_name = os.getenv("MEMBERSHIPS_TABLE_NAME")
        if not self.membership_table_name:
            raise ValueError("MEMBERSHIPS_TABLE_NAME must be set in .env")
        
        self.membership_table = self.dynamodb.Table(self.membership_table_name)
        
        self.stats = {
            "scanned": 0,
            "disabled": 0,
            "skipped": 0,
            "errors": 0
        }

    def get_user_memberships(self, user_id: str) -> List[Any]:
        """Get all memberships for a user"""
        try:
            response = self.membership_table.query(
                KeyConditionExpression=Key("PK").eq(f"USER#{user_id}")
            )
            return response.get("Items", [])
        except Exception as e:
            print(f"❌ Error getting user memberships: {e}")
            self.stats["errors"] += 1
            return []

    def get_account_memberships(self, account_id: str) -> List[Any]:
        """Get all memberships for an account using GSI"""
        try:
            response = self.membership_table.query(
                IndexName="byAccount",
                KeyConditionExpression=Key("ACCOUNT_ID").eq(account_id)
            )
            items = response.get("Items", [])
            
            # Pagination
            while "LastEvaluatedKey" in response:
                response = self.membership_table.query(
                    IndexName="byAccount",
                    KeyConditionExpression=Key("ACCOUNT_ID").eq(account_id),
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))
                
            return items
        except Exception as e:
            print(f"❌ Error getting account memberships: {e}")
            self.stats["errors"] += 1
            return []

    def disable_membership(self, pk: str, sk: str):
        """Set membership status to disabled"""
        if self.dry_run:
            print(f"   [DRY RUN] Would disable membership {pk} / {sk}")
            return True
            
        try:
            self.membership_table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression="SET #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": "disabled"}
            )
            print(f"   ✅ Disabled membership {pk} / {sk}")
            return True
        except Exception as e:
            print(f"   ❌ Error disabling membership {pk} / {sk}: {e}")
            self.stats["errors"] += 1
            return False

    def run(self, user_id: str = None, account_id: str = None, except_user_ids: List[str] = None):
        """Run the disable process"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🚫 Disable Memberships ({mode})")
        print(f"{'='*60}\n")
        
        if not user_id and not account_id:
            print("❌ Error: Must provide either --user-id or --account-id")
            return

        if except_user_ids:
            print(f"🛡️  Excluding Users: {', '.join(except_user_ids)}")

        target_memberships = []

        # Strategy Selection
        if user_id and not account_id:
            print(f"👤 Target: All memberships for User {user_id}")
            target_memberships = self.get_user_memberships(user_id)
        
        elif account_id and not user_id:
            print(f"🏢 Target: All memberships for Account {account_id}")
            target_memberships = self.get_account_memberships(account_id)

        elif user_id and account_id:
            print(f"🎯 Target: User {user_id} in Account {account_id}")
            # Get user memberships and filter
            all_user_memberships = self.get_user_memberships(user_id)
            target_memberships = [
                m for m in all_user_memberships 
                if m.get("ACCOUNT_ID") == account_id or (m["SK"] == f"ACCOUNT#{account_id}")
            ]

        if not target_memberships:
            print(f"\n⚠️  No memberships found matching criteria.")
            return

        # Filter out already disabled AND excluded users
        to_process = []
        for m in target_memberships:
            acc = m.get("ACCOUNT_ID", "unknown")
            usr = m.get("USER_ID", "unknown")
            
            # Check exclusion
            if except_user_ids and usr in except_user_ids:
                print(f"   🛡️  Skipping excluded user {usr} @ {acc}")
                self.stats["skipped"] += 1
                continue

            if m.get("status") == "disabled":
                print(f"   ℹ️  Membership {usr} @ {acc} is already disabled")
                continue
            
            to_process.append(m)

        if not to_process:
            print("\n✅ All matching memberships are either already disabled or skipped.")
            return

        print(f"\nFound {len(to_process)} membership(s) to disable:")
        for m in to_process:
             print(f" - User: {m.get('USER_ID')} | Account: {m.get('ACCOUNT_ID')}")

        # Confirmation
        if not self.dry_run:
            print(f"\n⚠️  WARNING: About to disable {len(to_process)} memberships")
            confirm = input("Type 'yes' to continue: ")
            if confirm.lower() != "yes":
                print("❌ Operation cancelled")
                return

        # Execute
        print("\nProcessing...")
        for m in to_process:
            if self.disable_membership(m["PK"], m["SK"]):
                self.stats["disabled"] += 1
        
        # Summary
        print(f"\n{'='*60}")
        print(f"📊 Summary")
        print(f"{'='*60}")
        print(f"Memberships disabled: {self.stats['disabled']}")
        print(f"Users skipped: {self.stats['skipped']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"{'='*60}\n")

        if self.dry_run:
             print("ℹ️  This was a DRY RUN. No changes were made.")
             print("   Run with --execute to apply changes.\n")


def main():
    parser = argparse.ArgumentParser(description="Disable user memberships")
    parser.add_argument("--user-id", help="User ID to disable memberships for")
    parser.add_argument("--account-id", help="Account ID to disable memberships for")
    parser.add_argument("--except-user-ids", help="Comma-separated list of User IDs to exclude")
    parser.add_argument("--execute", action="store_true", help="Execute changes")
    
    args = parser.parse_args()
    
    if not args.user_id and not args.account_id:
        parser.error("At least one of --user-id or --account-id is required.")
    
    except_ids = []
    if args.except_user_ids:
        except_ids = [uid.strip() for uid in args.except_user_ids.split(",")]

    disabler = MembershipDisabler(dry_run=not args.execute)
    disabler.run(args.user_id, args.account_id, except_ids)

if __name__ == "__main__":
    main()
