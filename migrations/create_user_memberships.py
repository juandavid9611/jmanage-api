#!/usr/bin/env python3
"""
Migration script to create user membership records from existing users in DynamoDB.

This script:
1. Scans the Users table for all users with status != 'disabled'
2. Creates membership records in the MEMBERSHIPS table for a specified account
3. Sets role to 'user' by default (admins can be set manually later)

The membership record structure:
{
    "PK": "USER#<user_id>",
    "SK": "ACCOUNT#<account_id>",
    "ACCOUNT_ID": "<account_id>",
    "USER_ID": "<user_id>",
    "role": "user",
    "status": "active"
}

Usage:
    # Dry run (recommended first)
    python create_user_memberships.py --account-id vittoriacd --dry-run
    
    # Actually create memberships
    python create_user_memberships.py --account-id vittoriacd
    
    # Skip users that already have memberships
    python create_user_memberships.py --account-id vittoriacd --skip-existing
"""

import argparse
import boto3
import os
import sys
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UserMembershipMigration:
    """Create membership records for existing users"""
    
    def __init__(self, users_table_name: str, memberships_table_name: str):
        self.dynamodb = boto3.resource('dynamodb')
        self.users_table = self.dynamodb.Table(users_table_name)
        self.memberships_table = self.dynamodb.Table(memberships_table_name)
        self.users_table_name = users_table_name
        self.memberships_table_name = memberships_table_name
    
    def _user_pk(self, user_id: str) -> str:
        """Generate PK for user"""
        return f"USER#{user_id}"
    
    def _account_sk(self, account_id: str) -> str:
        """Generate SK for account"""
        return f"ACCOUNT#{account_id}"
    
    def scan_all_users(self) -> List[Dict[str, Any]]:
        """Scan Users table for all users"""
        logger.info(f"Scanning {self.users_table_name} for all users...")
        
        users = []
        last_evaluated_key = None
        
        while True:
            scan_kwargs = {}
            if last_evaluated_key:
                scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
            try:
                response = self.users_table.scan(**scan_kwargs)
                items = response.get('Items', [])
                users.extend(items)
                
                last_evaluated_key = response.get('LastEvaluatedKey')
                
                if not last_evaluated_key:
                    break
                    
            except Exception as e:
                logger.error(f"Error scanning users table: {e}")
                raise
        
        logger.info(f"Found {len(users)} users")
        return users
    
    def membership_exists(self, user_id: str, account_id: str) -> bool:
        """Check if membership already exists"""
        try:
            response = self.memberships_table.get_item(
                Key={
                    "PK": self._user_pk(user_id),
                    "SK": self._account_sk(account_id)
                }
            )
            return 'Item' in response
        except Exception as e:
            logger.error(f"Error checking membership existence: {e}")
            return False
    
    def create_membership(self, user_id: str, account_id: str, role: str = "user", status: str = "active") -> bool:
        """Create a membership record"""
        item = {
            "PK": self._user_pk(user_id),
            "SK": self._account_sk(account_id),
            "ACCOUNT_ID": account_id,
            "USER_ID": user_id,
            "role": role,
            "status": status
        }
        
        try:
            self.memberships_table.put_item(Item=item)
            return True
        except Exception as e:
            logger.error(f"Error creating membership for user {user_id}: {e}")
            return False
    
    def migrate(self, account_id: str, dry_run: bool = False, skip_existing: bool = False) -> None:
        """Migrate all users to the specified account"""
        logger.info(f"\n{'='*80}")
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Creating memberships for account: {account_id}")
        logger.info(f"{'='*80}\n")
        
        # Scan for all users
        users = self.scan_all_users()
        
        if not users:
            logger.warning("No users found!")
            return
        
        success_count = 0
        skipped_count = 0
        error_count = 0
        disabled_count = 0
        
        for i, user in enumerate(users, 1):
            user_id = user.get('id')
            if not user_id:
                logger.warning(f"User {i}/{len(users)}: Skipping user without 'id' field")
                error_count += 1
                continue
            
            # Get user status and map to membership status
            user_status = user.get('status', 'active')
            membership_status = 'disabled' if user_status == 'disabled' else 'active'
            
            # Check if membership already exists
            if skip_existing and self.membership_exists(user_id, account_id):
                logger.info(f"User {i}/{len(users)}: Skipping {user_id} - membership already exists")
                skipped_count += 1
                continue
            
            if dry_run:
                status_label = f"[{membership_status}]"
                logger.info(f"[DRY RUN] User {i}/{len(users)}: Would create membership for {user_id} {status_label}")
                success_count += 1
                if membership_status == 'disabled':
                    disabled_count += 1
            else:
                if self.create_membership(user_id, account_id, role="user", status=membership_status):
                    status_label = f"[{membership_status}]"
                    logger.info(f"✓ User {i}/{len(users)}: Created membership for {user_id} {status_label}")
                    success_count += 1
                    if membership_status == 'disabled':
                        disabled_count += 1
                else:
                    logger.error(f"✗ User {i}/{len(users)}: Failed to create membership for {user_id}")
                    error_count += 1
        
        # Summary
        logger.info(f"\n{'='*80}")
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Migration Summary")
        logger.info(f"{'='*80}")
        logger.info(f"Total users scanned: {len(users)}")
        logger.info(f"✓ Memberships created: {success_count}")
        logger.info(f"  - Active: {success_count - disabled_count}")
        logger.info(f"  - Disabled: {disabled_count}")
        if skip_existing:
            logger.info(f"⊘ Skipped (already exist): {skipped_count}")
        logger.info(f"✗ Errors: {error_count}")
        logger.info(f"{'='*80}\n")
        
        if dry_run:
            logger.info("This was a DRY RUN. No changes were made.")
            logger.info("Run without --dry-run to actually create memberships.\n")


def main():
    parser = argparse.ArgumentParser(
        description='Create user memberships from existing users in DynamoDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--account-id',
        required=True,
        help='Account ID to assign all users to'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test without making changes'
    )
    
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip users that already have memberships for this account'
    )
    
    args = parser.parse_args()
    
    # Get table names from environment
    users_table = os.getenv('USER_TABLE_NAME')
    memberships_table = os.getenv('MEMBERSHIPS_TABLE_NAME')
    
    if not users_table:
        logger.error("USER_TABLE_NAME not found in environment variables")
        sys.exit(1)
    
    if not memberships_table:
        logger.error("MEMBERSHIPS_TABLE_NAME not found in environment variables")
        sys.exit(1)
    
    logger.info(f"Users table: {users_table}")
    logger.info(f"Memberships table: {memberships_table}\n")
    
    # Confirm before proceeding (unless dry-run)
    if not args.dry_run:
        logger.warning("⚠️  WARNING: This will create membership records in the database!")
        logger.warning(f"⚠️  Account ID: {args.account_id}")
        logger.warning(f"⚠️  Default role: user")
        response = input("\nDo you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Migration cancelled.")
            return
    
    # Create migration instance
    migration = UserMembershipMigration(users_table, memberships_table)
    
    # Run migration
    migration.migrate(args.account_id, args.dry_run, args.skip_existing)


if __name__ == '__main__':
    main()
