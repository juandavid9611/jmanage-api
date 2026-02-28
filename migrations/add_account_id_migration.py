#!/usr/bin/env python3
"""
Migration script to add account_id to all existing DynamoDB records.

This script will:
1. Scan all tables
2. Add a default account_id to each record
3. Update records in batches
4. Log progress and errors

Usage:
    python add_account_id_migration.py --account-id <default_account_id> [--dry-run] [--table <table_name>]

Examples:
    # Dry run to see what would be updated
    python add_account_id_migration.py --account-id acc_default --dry-run
    
    # Migrate all tables
    python add_account_id_migration.py --account-id acc_default
    
    # Migrate specific table
    python add_account_id_migration.py --account-id acc_default --table users
"""

import argparse
import boto3
import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from decimal import Decimal
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Table configurations
TABLES_CONFIG = {
    'users': {
        'env_var': 'USER_TABLE_NAME',
        'primary_key': {'id': 'S'},
        'gsi_name': 'account_id_index',  # GSI to be created in infrastructure
    },
    'tours': {
        'env_var': 'TOUR_TABLE_NAME',
        'primary_key': {'id': 'S'},
        'gsi_name': 'account_id_index',
    },
    'workspaces': {
        'env_var': 'WORKSPACE_TABLE_NAME',
        'primary_key': {'id': 'S'},
        'gsi_name': 'account_id_index',
    },
    'products': {
        'env_var': 'PRODUCT_TABLE_NAME',
        'primary_key': {'pk': 'S', 'sk': 'S'},
        'gsi_name': 'account_id_index',
    },
    'orders': {
        'env_var': 'ORDER_TABLE_NAME',
        'primary_key': {'id': 'S'},
        'gsi_name': 'account_id_index',
    },
    'payment_requests': {
        'env_var': 'PAYMENT_REQUEST_TABLE_NAME',
        'primary_key': {'id': 'S'},
        'gsi_name': 'account_id_index',
    },
    'calendar': {
        'env_var': 'CALENDAR_TABLE_NAME',
        'primary_key': {'id': 'S'},
        'gsi_name': 'account_id_index',
    },
}

class MigrationStats:
    """Track migration statistics"""
    def __init__(self):
        self.total_scanned = 0
        self.total_updated = 0
        self.total_skipped = 0
        self.total_errors = 0
        self.errors: List[Dict[str, Any]] = []
    
    def log_summary(self):
        logger.info("=" * 80)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total records scanned: {self.total_scanned}")
        logger.info(f"Total records updated: {self.total_updated}")
        logger.info(f"Total records skipped: {self.total_skipped}")
        logger.info(f"Total errors: {self.total_errors}")
        if self.errors:
            logger.error("\nErrors encountered:")
            for error in self.errors:
                logger.error(f"  - {error}")
        logger.info("=" * 80)


class AccountIdMigration:
    """Handle migration of account_id to DynamoDB tables"""
    
    def __init__(self, default_account_id: str, dry_run: bool = False):
        self.default_account_id = default_account_id
        self.dry_run = dry_run
        self.dynamodb = boto3.resource('dynamodb')
        self.stats = MigrationStats()
        
        logger.info(f"Initialized migration with account_id: {default_account_id}")
        logger.info(f"Dry run mode: {dry_run}")
    
    def get_table_name(self, table_key: str) -> Optional[str]:
        """Get table name from environment variable"""
        env_var = TABLES_CONFIG[table_key]['env_var']
        table_name = os.environ.get(env_var)
        if not table_name:
            logger.warning(f"Table {table_key} not found in environment ({env_var})")
        return table_name
    
    def scan_table(self, table) -> List[Dict[str, Any]]:
        """Scan entire table with pagination"""
        items = []
        last_evaluated_key = None
        
        while True:
            scan_kwargs = {}
            if last_evaluated_key:
                scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
            try:
                response = table.scan(**scan_kwargs)
                items.extend(response.get('Items', []))
                last_evaluated_key = response.get('LastEvaluatedKey')
                
                logger.info(f"Scanned {len(response.get('Items', []))} items from {table.name}")
                
                if not last_evaluated_key:
                    break
            except Exception as e:
                logger.error(f"Error scanning table {table.name}: {e}")
                self.stats.total_errors += 1
                self.stats.errors.append({
                    'table': table.name,
                    'operation': 'scan',
                    'error': str(e)
                })
                break
        
        return items
    
    def needs_migration(self, item: Dict[str, Any]) -> bool:
        """Check if item needs account_id added"""
        return 'account_id' not in item
    
    def update_item(self, table, item: Dict[str, Any], primary_key: Dict[str, str]) -> bool:
        """Update a single item with account_id"""
        # Build the key from primary key definition
        key = {}
        for key_name, key_type in primary_key.items():
            if key_name in item:
                key[key_name] = item[key_name]
            else:
                logger.error(f"Primary key {key_name} not found in item: {item}")
                return False
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update item with key: {key}")
            return True
        
        try:
            table.update_item(
                Key=key,
                UpdateExpression='SET account_id = :account_id',
                ExpressionAttributeValues={
                    ':account_id': self.default_account_id
                },
                ConditionExpression='attribute_not_exists(account_id)'
            )
            logger.debug(f"Updated item with key: {key}")
            return True
        except table.meta.client.exceptions.ConditionalCheckFailedException:
            # Item already has account_id
            logger.debug(f"Item already has account_id, skipping: {key}")
            self.stats.total_skipped += 1
            return False
        except Exception as e:
            logger.error(f"Error updating item {key}: {e}")
            self.stats.total_errors += 1
            self.stats.errors.append({
                'table': table.name,
                'key': key,
                'operation': 'update',
                'error': str(e)
            })
            return False
    
    def migrate_table(self, table_key: str) -> bool:
        """Migrate a single table"""
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Migrating table: {table_key}")
        logger.info(f"{'=' * 80}")
        
        table_name = self.get_table_name(table_key)
        if not table_name:
            logger.error(f"Skipping {table_key} - table name not found")
            return False
        
        try:
            table = self.dynamodb.Table(table_name)
            config = TABLES_CONFIG[table_key]
            
            # Scan table
            logger.info(f"Scanning table {table_name}...")
            items = self.scan_table(table)
            self.stats.total_scanned += len(items)
            
            logger.info(f"Found {len(items)} items in {table_name}")
            
            # Filter items that need migration
            items_to_migrate = [item for item in items if self.needs_migration(item)]
            logger.info(f"{len(items_to_migrate)} items need account_id added")
            
            # Update items
            updated_count = 0
            for i, item in enumerate(items_to_migrate, 1):
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(items_to_migrate)} items processed")
                
                if self.update_item(table, item, config['primary_key']):
                    updated_count += 1
            
            self.stats.total_updated += updated_count
            logger.info(f"Successfully updated {updated_count} items in {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error migrating table {table_key}: {e}")
            self.stats.total_errors += 1
            self.stats.errors.append({
                'table': table_key,
                'operation': 'migrate_table',
                'error': str(e)
            })
            return False
    
    def migrate_all_tables(self, specific_table: Optional[str] = None):
        """Migrate all tables or a specific table"""
        if specific_table:
            if specific_table not in TABLES_CONFIG:
                logger.error(f"Unknown table: {specific_table}")
                logger.info(f"Available tables: {', '.join(TABLES_CONFIG.keys())}")
                return
            tables_to_migrate = [specific_table]
        else:
            tables_to_migrate = list(TABLES_CONFIG.keys())
        
        logger.info(f"\nStarting migration for {len(tables_to_migrate)} table(s)")
        logger.info(f"Default account_id: {self.default_account_id}")
        logger.info(f"Dry run: {self.dry_run}\n")
        
        for table_key in tables_to_migrate:
            self.migrate_table(table_key)
        
        # Log summary
        self.stats.log_summary()


def main():
    parser = argparse.ArgumentParser(
        description='Migrate DynamoDB tables to add account_id field',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--account-id',
        required=True,
        help='Default account_id to assign to existing records'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no actual updates)'
    )
    
    parser.add_argument(
        '--table',
        choices=list(TABLES_CONFIG.keys()),
        help='Migrate only a specific table'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level'
    )
    
    args = parser.parse_args()
    
    # Set log level
    logger.setLevel(getattr(logging, args.log_level))
    
    # Confirm before proceeding (unless dry-run)
    if not args.dry_run:
        print("\n" + "=" * 80)
        print("WARNING: This will modify production data!")
        print("=" * 80)
        print(f"Default account_id: {args.account_id}")
        print(f"Tables to migrate: {args.table if args.table else 'ALL'}")
        print("=" * 80)
        
        response = input("\nDo you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Migration cancelled.")
            return
    
    # Run migration
    migration = AccountIdMigration(
        default_account_id=args.account_id,
        dry_run=args.dry_run
    )
    
    migration.migrate_all_tables(specific_table=args.table)


if __name__ == '__main__':
    main()
