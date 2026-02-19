#!/usr/bin/env python3
"""
Migration: Remove 'group' and 'user_group' attributes from User table

This script:
1. Scans all users in the User table
2. Removes 'group' and 'user_group' attributes from each user record
   (these fields are now managed via workspace memberships)

Usage:
    python migrations/remove_group_from_users.py            # Dry-run (preview changes)
    python migrations/remove_group_from_users.py --execute  # Execute migration
"""

import os
import sys
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3


class RemoveGroupMigration:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")

        # Get table name from environment
        self.user_table_name = os.getenv("USER_TABLE_NAME")

        if not self.user_table_name:
            raise ValueError("USER_TABLE_NAME must be set in .env")

        self.user_table = self.dynamodb.Table(self.user_table_name)

        # Statistics
        self.stats = {
            "users_scanned": 0,
            "users_with_group": 0,
            "users_with_user_group": 0,
            "users_updated": 0,
            "errors": 0,
        }

    def scan_all_users(self):
        """Scan all users in the User table"""
        print(f"📊 Scanning {self.user_table_name} for users with 'group' or 'user_group'...")

        items = []
        try:
            response = self.user_table.scan()
            items = response.get("Items", [])

            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.user_table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))

            print(f"✅ Scanned {len(items)} total users")
            return items

        except Exception as e:
            print(f"❌ Error scanning users: {e}")
            self.stats["errors"] += 1
            raise

    def remove_group_attrs(self, user: dict):
        """Remove 'group' and 'user_group' attributes from a user record"""
        user_id = user.get("id")
        user_name = user.get("user_name", "Unknown")

        has_group = "group" in user
        has_user_group = "user_group" in user

        if not has_group and not has_user_group:
            return  # Nothing to remove

        attrs_to_remove = []
        if has_group:
            attrs_to_remove.append("group")
            self.stats["users_with_group"] += 1
        if has_user_group:
            attrs_to_remove.append("user_group")
            self.stats["users_with_user_group"] += 1

        remove_expr = "REMOVE " + ", ".join(f"#{a}" for a in attrs_to_remove)
        attr_names = {f"#{a}": a for a in attrs_to_remove}

        if self.dry_run:
            group_val = user.get("group", "N/A")
            user_group_val = user.get("user_group", "N/A")
            print(f"   [DRY RUN] Would remove {attrs_to_remove} from {user_name} (ID: {user_id}) "
                  f"[group={group_val}, user_group={user_group_val}]")
            self.stats["users_updated"] += 1
            return

        try:
            self.user_table.update_item(
                Key={"id": user_id},
                UpdateExpression=remove_expr,
                ExpressionAttributeNames=attr_names,
            )
            self.stats["users_updated"] += 1
            print(f"   ✅ Removed {attrs_to_remove} from {user_name} (ID: {user_id})")
        except Exception as e:
            print(f"   ❌ Error updating user {user_id}: {e}")
            self.stats["errors"] += 1

    def run(self):
        """Run the migration"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🚀 Remove 'group'/'user_group' from User Table ({mode})")
        print(f"{'='*60}\n")

        # Scan all users
        users = self.scan_all_users()

        if not users:
            print("\n✅ No users found. Migration not needed.")
            return

        # Filter to users that have at least one attribute
        users_to_update = [u for u in users if "group" in u or "user_group" in u]
        self.stats["users_scanned"] = len(users)

        if not users_to_update:
            print("\n✅ No users have 'group' or 'user_group'. Migration not needed.")
            return

        print(f"\n📋 Found {len(users_to_update)} users with 'group' or 'user_group' to clean up\n")

        # Confirm execution
        if not self.dry_run:
            print(f"⚠️  WARNING: About to remove attributes from {len(users_to_update)} users")
            confirm = input("Type 'yes' to continue: ")
            if confirm.lower() != "yes":
                print("❌ Migration cancelled")
                return

        # Process each user
        for user in users_to_update:
            self.remove_group_attrs(user)

        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Migration Summary ({mode})")
        print(f"{'='*60}")
        print(f"Users scanned:          {self.stats['users_scanned']}")
        print(f"Users with 'group':     {self.stats['users_with_group']}")
        print(f"Users with 'user_group':{self.stats['users_with_user_group']}")
        print(f"Users updated:          {self.stats['users_updated']}")
        print(f"Errors:                 {self.stats['errors']}")
        print(f"{'='*60}\n")

        if self.dry_run:
            print("ℹ️  This was a DRY RUN. No changes were made.")
            print("   Run with --execute to apply changes.\n")
        else:
            print("✅ Migration complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Remove 'group' and 'user_group' from User table")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the migration (default is dry-run)",
    )

    args = parser.parse_args()

    migration = RemoveGroupMigration(dry_run=not args.execute)
    migration.run()


if __name__ == "__main__":
    main()
