#!/usr/bin/env python3
"""
Migration Script: Remove account_id from User Table

This script removes the account_id attribute from all user records in DynamoDB.
Users are now global entities, and account relationships are managed through 
the Memberships table.

Usage:
    # Dry run (default) - shows what would be changed
    python migrations/remove_account_id_from_users.py
    
    # Execute the migration
    python migrations/remove_account_id_from_users.py --execute
    
    # Specify custom table name
    python migrations/remove_account_id_from_users.py --table-name MyUserTable --execute

Requirements:
    - boto3
    - AWS credentials configured
    - Appropriate DynamoDB permissions
"""

import argparse
import boto3
from boto3.dynamodb.conditions import Attr
from typing import List, Dict, Any
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class UserMigration:
    def __init__(self, table_name: str, dry_run: bool = True):
        self.table_name = table_name
        self.dry_run = dry_run
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
        
        self.stats = {
            'total_users': 0,
            'users_with_account_id': 0,
            'users_updated': 0,
            'errors': 0
        }
    
    def scan_all_users(self) -> List[Dict[str, Any]]:
        """Scan all users from the table"""
        print(f"Scanning all users from table: {self.table_name}")
        
        users = []
        scan_kwargs = {}
        
        while True:
            response = self.table.scan(**scan_kwargs)
            users.extend(response.get('Items', []))
            
            # Check if there are more items to scan
            if 'LastEvaluatedKey' not in response:
                break
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        
        self.stats['total_users'] = len(users)
        print(f"Found {len(users)} total users")
        return users
    
    def filter_users_with_account_id(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter users that have account_id attribute"""
        users_with_account_id = [
            user for user in users 
            if 'account_id' in user
        ]
        
        self.stats['users_with_account_id'] = len(users_with_account_id)
        print(f"Found {len(users_with_account_id)} users with account_id attribute")
        return users_with_account_id
    
    def remove_account_id(self, user_id: str) -> bool:
        """Remove account_id attribute from a user"""
        try:
            if self.dry_run:
                print(f"  [DRY RUN] Would remove account_id from user: {user_id}")
                return True
            
            # Use UpdateItem with REMOVE action
            self.table.update_item(
                Key={'id': user_id},
                UpdateExpression='REMOVE account_id',
                ReturnValues='NONE'
            )
            
            print(f"  ✓ Removed account_id from user: {user_id}")
            return True
            
        except Exception as e:
            print(f"  ✗ Error removing account_id from user {user_id}: {str(e)}")
            self.stats['errors'] += 1
            return False
    
    def run(self):
        """Execute the migration"""
        print("\n" + "="*70)
        print("User Migration: Remove account_id attribute")
        print("="*70)
        print(f"Table: {self.table_name}")
        print(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTE'}")
        print("="*70 + "\n")
        
        # Step 1: Scan all users
        users = self.scan_all_users()
        
        if not users:
            print("No users found. Exiting.")
            return
        
        # Step 2: Filter users with account_id
        users_to_update = self.filter_users_with_account_id(users)
        
        if not users_to_update:
            print("\n✓ No users have account_id attribute. Migration not needed.")
            return
        
        # Step 3: Show sample of users to be updated
        print(f"\nSample of users to be updated (showing first 5):")
        for user in users_to_update[:5]:
            print(f"  - ID: {user['id']}, account_id: {user.get('account_id')}, email: {user.get('email', 'N/A')}")
        
        if len(users_to_update) > 5:
            print(f"  ... and {len(users_to_update) - 5} more")
        
        # Step 4: Confirm if not dry run
        if not self.dry_run:
            print(f"\n⚠️  WARNING: About to remove account_id from {len(users_to_update)} users")
            response = input("Are you sure you want to proceed? (yes/no): ")
            if response.lower() != 'yes':
                print("Migration cancelled.")
                return
        
        # Step 5: Remove account_id from each user
        print(f"\n{'[DRY RUN] ' if self.dry_run else ''}Removing account_id from users...")
        
        for user in users_to_update:
            if self.remove_account_id(user['id']):
                self.stats['users_updated'] += 1
        
        # Step 6: Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print migration summary"""
        print("\n" + "="*70)
        print("Migration Summary")
        print("="*70)
        print(f"Total users scanned:           {self.stats['total_users']}")
        print(f"Users with account_id:         {self.stats['users_with_account_id']}")
        print(f"Users updated:                 {self.stats['users_updated']}")
        print(f"Errors:                        {self.stats['errors']}")
        print("="*70)
        
        if self.dry_run:
            print("\n💡 This was a DRY RUN. No changes were made.")
            print("   Run with --execute flag to apply changes.")
        else:
            if self.stats['errors'] == 0:
                print("\n✓ Migration completed successfully!")
            else:
                print(f"\n⚠️  Migration completed with {self.stats['errors']} errors.")
                print("   Please review the errors above.")


def main():
    parser = argparse.ArgumentParser(
        description='Remove account_id attribute from User table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--table-name',
        default=os.getenv('USER_TABLE_NAME', 'User'),
        help='DynamoDB table name (default: from USER_TABLE_NAME env var or "User")'
    )
    
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute the migration (default is dry-run)'
    )
    
    args = parser.parse_args()
    
    # Run migration
    migration = UserMigration(
        table_name=args.table_name,
        dry_run=not args.execute
    )
    
    try:
        migration.run()
    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
