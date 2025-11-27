#!/usr/bin/env python3
"""
S3 Migration Script for Account-Scoped Paths

Migrates existing S3 files to new account-scoped structure:
  OLD: {env}/tours/{tour_id}/{filename}
  NEW: {env}/accounts/{account_id}/tours/{tour_id}/{filename}

Usage:
    python migrate_s3_to_account_scoped.py --account-id <default_account_id> --bucket <bucket_name> [--env <env>] [--dry-run]

Examples:
    # Dry run
    python migrate_s3_to_account_scoped.py --account-id acc_default --bucket jmanage-bucket --dry-run
    
    # Migrate production
    python migrate_s3_to_account_scoped.py --account-id acc_default --bucket jmanage-bucket --env prod
"""

import argparse
import boto3
import logging
import sys
from datetime import datetime
from typing import List, Dict, Tuple
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f's3_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Path patterns to migrate
PATH_PATTERNS = {
    'tours': r'^{env}/tours/([^/]+)/(.+)$',
    'products': r'^{env}/products/([^/]+)/(.+)$',
    'users': r'^{env}/users/([^/]+)/(.+)$',
}

class S3MigrationStats:
    """Track migration statistics"""
    def __init__(self):
        self.total_scanned = 0
        self.total_migrated = 0
        self.total_skipped = 0
        self.total_errors = 0
        self.errors: List[Dict] = []
    
    def log_summary(self):
        logger.info("=" * 80)
        logger.info("S3 MIGRATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total files scanned: {self.total_scanned}")
        logger.info(f"Total files migrated: {self.total_migrated}")
        logger.info(f"Total files skipped: {self.total_skipped}")
        logger.info(f"Total errors: {self.total_errors}")
        if self.errors:
            logger.error("\nErrors encountered:")
            for error in self.errors:
                logger.error(f"  - {error}")
        logger.info("=" * 80)


class S3Migration:
    """Handle S3 file migration to account-scoped paths"""
    
    def __init__(self, bucket_name: str, account_id: str, env: str = 'dev', dry_run: bool = False):
        self.bucket_name = bucket_name
        self.account_id = account_id
        self.env = env
        self.dry_run = dry_run
        self.s3 = boto3.client('s3')
        self.stats = S3MigrationStats()
        
        logger.info(f"Initialized S3 migration")
        logger.info(f"Bucket: {bucket_name}")
        logger.info(f"Default account_id: {account_id}")
        logger.info(f"Environment: {env}")
        logger.info(f"Dry run mode: {dry_run}")
    
    def get_new_path(self, old_key: str) -> str | None:
        """
        Convert old path to new account-scoped path
        
        Examples:
            dev/tours/tour_123/image.jpg -> dev/accounts/acc_default/tours/tour_123/image.jpg
            dev/products/prod_456/photo.jpg -> dev/accounts/acc_default/products/prod_456/photo.jpg
            dev/users/user_789/profile_photos/avatar.jpg -> dev/accounts/acc_default/users/user_789/profile_photos/avatar.jpg
            dev/users/user_789/invoices/pay_001/receipt.pdf -> dev/accounts/acc_default/users/user_789/invoices/pay_001/receipt.pdf
        """
        import re
        
        # Tours: {env}/tours/{tour_id}/{filename}
        match = re.match(f'^{self.env}/tours/([^/]+)/(.+)$', old_key)
        if match:
            tour_id, filename = match.groups()
            return f'{self.env}/accounts/{self.account_id}/tours/{tour_id}/{filename}'
        
        # Products: {env}/products/{product_id}/{filename}
        match = re.match(f'^{self.env}/products/([^/]+)/(.+)$', old_key)
        if match:
            product_id, filename = match.groups()
            return f'{self.env}/accounts/{self.account_id}/products/{product_id}/{filename}'
        
        # Users - profile photos: {env}/users/{user_id}/profile_photos/{filename}
        match = re.match(f'^{self.env}/users/([^/]+)/profile_photos/(.+)$', old_key)
        if match:
            user_id, filename = match.groups()
            return f'{self.env}/accounts/{self.account_id}/users/{user_id}/profile_photos/{filename}'
        
        # Users - invoices: {env}/users/{user_id}/invoices/{payment_id}/{filename}
        match = re.match(f'^{self.env}/users/([^/]+)/invoices/([^/]+)/(.+)$', old_key)
        if match:
            user_id, payment_id, filename = match.groups()
            return f'{self.env}/accounts/{self.account_id}/users/{user_id}/invoices/{payment_id}/{filename}'
        
        return None
    
    def needs_migration(self, key: str) -> bool:
        """Check if file needs migration (not already in account-scoped path)"""
        return f'/accounts/{self.account_id}/' not in key
    
    def copy_object(self, source_key: str, dest_key: str) -> bool:
        """Copy S3 object to new location"""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would copy: {source_key} -> {dest_key}")
            return True
        
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            self.s3.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            logger.info(f"Copied: {source_key} -> {dest_key}")
            return True
        except ClientError as e:
            logger.error(f"Error copying {source_key}: {e}")
            self.stats.total_errors += 1
            self.stats.errors.append({
                'source': source_key,
                'dest': dest_key,
                'error': str(e)
            })
            return False
    
    def delete_object(self, key: str) -> bool:
        """Delete old S3 object after successful migration"""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would delete: {key}")
            return True
        
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=key)
            logger.debug(f"Deleted old file: {key}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting {key}: {e}")
            return False
    
    def list_files(self, prefix: str = '') -> List[str]:
        """List all files in bucket with optional prefix"""
        files = []
        paginator = self.s3.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append(obj['Key'])
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            self.stats.total_errors += 1
        
        return files
    
    def migrate_files(self, delete_old: bool = False):
        """Migrate all files to account-scoped paths"""
        logger.info(f"\nStarting S3 migration for environment: {self.env}")
        logger.info(f"Scanning bucket: {self.bucket_name}")
        
        # List all files in the environment prefix
        all_files = self.list_files(prefix=self.env)
        self.stats.total_scanned = len(all_files)
        
        logger.info(f"Found {len(all_files)} files with prefix '{self.env}/'")
        
        # Filter files that need migration
        files_to_migrate = []
        for file_key in all_files:
            if self.needs_migration(file_key) and self.get_new_path(file_key):
                files_to_migrate.append(file_key)
            else:
                self.stats.total_skipped += 1
        
        logger.info(f"{len(files_to_migrate)} files need migration")
        logger.info(f"{self.stats.total_skipped} files already migrated or don't match patterns")
        
        # Migrate files
        for i, old_key in enumerate(files_to_migrate, 1):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(files_to_migrate)} files processed")
            
            new_key = self.get_new_path(old_key)
            if not new_key:
                logger.warning(f"Could not determine new path for: {old_key}")
                self.stats.total_skipped += 1
                continue
            
            # Copy to new location
            if self.copy_object(old_key, new_key):
                self.stats.total_migrated += 1
                
                # Optionally delete old file
                if delete_old:
                    self.delete_object(old_key)
        
        # Log summary
        self.stats.log_summary()
    
    def verify_migration(self) -> Dict[str, int]:
        """Verify migration by checking old vs new structure"""
        logger.info("\nVerifying migration...")
        
        old_pattern_count = 0
        new_pattern_count = 0
        
        all_files = self.list_files(prefix=self.env)
        
        for file_key in all_files:
            if f'/accounts/{self.account_id}/' in file_key:
                new_pattern_count += 1
            elif any(pattern in file_key for pattern in ['/tours/', '/products/', '/users/']):
                old_pattern_count += 1
        
        logger.info(f"Files in old structure: {old_pattern_count}")
        logger.info(f"Files in new structure: {new_pattern_count}")
        
        return {
            'old_structure': old_pattern_count,
            'new_structure': new_pattern_count
        }


def main():
    parser = argparse.ArgumentParser(
        description='Migrate S3 files to account-scoped paths',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--account-id',
        required=True,
        help='Default account_id for existing files'
    )
    
    parser.add_argument(
        '--bucket',
        required=True,
        help='S3 bucket name'
    )
    
    parser.add_argument(
        '--env',
        default='dev',
        help='Environment prefix (default: dev)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no actual changes)'
    )
    
    parser.add_argument(
        '--delete-old',
        action='store_true',
        help='Delete old files after successful migration (CAUTION!)'
    )
    
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify migration status, do not migrate'
    )
    
    args = parser.parse_args()
    
    # Warning for destructive operations
    if args.delete_old and not args.dry_run:
        print("\n" + "=" * 80)
        print("WARNING: --delete-old will PERMANENTLY DELETE old files!")
        print("=" * 80)
        print(f"Bucket: {args.bucket}")
        print(f"Environment: {args.env}")
        print("=" * 80)
        
        response = input("\nAre you SURE you want to delete old files? (type 'DELETE' to confirm): ")
        if response != 'DELETE':
            print("Migration cancelled.")
            return
    
    # Run migration
    migration = S3Migration(
        bucket_name=args.bucket,
        account_id=args.account_id,
        env=args.env,
        dry_run=args.dry_run
    )
    
    if args.verify_only:
        migration.verify_migration()
    else:
        migration.migrate_files(delete_old=args.delete_old)
        migration.verify_migration()


if __name__ == '__main__':
    main()
