#!/usr/bin/env python3
"""
Database Migration Script for S3 Paths

Updates S3 keys in DynamoDB to match the new account-scoped structure:
  OLD: {env}/tours/{tour_id}/{filename}
  NEW: {env}/accounts/{account_id}/tours/{tour_id}/{filename}

Tables and attributes to update:
- PaymentRequest: images (List[str])
- Tour: images (List[str])
- Product: images (List[str])
- User: avatar_url (String)

Usage:
    python migrate_db_s3_paths.py [--env <env>] [--dry-run]

Examples:
    # Dry run
    python migrate_db_s3_paths.py --env dev --dry-run
    
    # Migrate production
    python migrate_db_s3_paths.py --env prod
"""

import argparse
import boto3
import logging
import sys
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'db_s3_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Table configurations
TABLES_CONFIG = {
    'payment_requests': {
        'env_var': 'PAYMENT_REQUEST_TABLE_NAME',
        'attributes': ['images'],
        'type': 'list'
    },
    'tours': {
        'env_var': 'TOUR_TABLE_NAME',
        'attributes': ['images'],
        'type': 'list'
    },
    'products': {
        'env_var': 'PRODUCT_TABLE_NAME',
        'attributes': ['images'],
        'type': 'list'
    },
    'users': {
        'env_var': 'USER_TABLE_NAME',
        'attributes': ['avatar_url', 'avatarUrl'], # Handle both just in case
        'type': 'string'
    }
}

class DBS3Migration:
    def __init__(self, env: str = 'dev', dry_run: bool = False):
        self.env = env
        self.dry_run = dry_run
        self.dynamodb = boto3.resource('dynamodb')
        self.stats = {
            'scanned': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        logger.info(f"Initialized DB S3 Path Migration")
        logger.info(f"Environment: {env}")
        logger.info(f"Dry run: {dry_run}")

    def get_new_key(self, old_key: str, account_id: str) -> str | None:
        """
        Convert old key to new account-scoped key using the record's account_id
        """
        if not old_key or not isinstance(old_key, str):
            return None
            
        # Check if already migrated
        if f'/accounts/{account_id}/' in old_key:
            return None

        # Check if it matches expected patterns
        # We look for {env}/... and insert /accounts/{account_id}/ after {env}
        
        # Pattern: {env}/(tours|products|users)/...
        # We want to replace "{env}/" with "{env}/accounts/{account_id}/"
        
        prefix = f"{self.env}/"
        if old_key.startswith(prefix):
            # Ensure we don't double migrate if account_id is somehow already there but regex missed it
            rest = old_key[len(prefix):]
            if rest.startswith(f"accounts/{account_id}/"):
                return None
                
            return f"{self.env}/accounts/{account_id}/{rest}"
            
        return None

    def process_item(self, item: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Process a single item and return updates if needed
        """
        account_id = item.get('account_id')
        if not account_id:
            return None

        updates = {}
        has_updates = False

        for attr in config['attributes']:
            if attr not in item:
                continue
                
            value = item[attr]
            
            if config['type'] == 'list':
                if not isinstance(value, list):
                    continue
                    
                new_list = []
                list_changed = False
                
                for key in value:
                    new_key = self.get_new_key(key, account_id)
                    if new_key:
                        new_list.append(new_key)
                        list_changed = True
                    else:
                        new_list.append(key)
                
                if list_changed:
                    updates[attr] = new_list
                    has_updates = True
                    
            elif config['type'] == 'string':
                if not isinstance(value, str):
                    continue
                    
                new_key = self.get_new_key(value, account_id)
                if new_key:
                    updates[attr] = new_key
                    has_updates = True

        return updates if has_updates else None

    def update_item(self, table, key: Dict[str, Any], updates: Dict[str, Any]):
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update item {key} with: {updates}")
            return True

        try:
            update_expr_parts = []
            expr_attr_vals = {}
            expr_attr_names = {}

            for attr, value in updates.items():
                safe_attr = f"#{attr}"
                safe_val = f":{attr}"
                update_expr_parts.append(f"{safe_attr} = {safe_val}")
                expr_attr_names[safe_attr] = attr
                expr_attr_vals[safe_val] = value

            update_expr = "SET " + ", ".join(update_expr_parts)

            table.update_item(
                Key=key,
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_vals
            )
            logger.info(f"Updated item {key}")
            return True
        except ClientError as e:
            logger.error(f"Error updating item {key}: {e}")
            self.stats['errors'] += 1
            return False

    def migrate_table(self, table_key: str):
        config = TABLES_CONFIG[table_key]
        table_name = os.environ.get(config['env_var'])
        
        if not table_name:
            logger.warning(f"Table {table_key} not found in environment")
            return

        logger.info(f"Scanning table {table_name}...")
        table = self.dynamodb.Table(table_name)
        
        try:
            scan_kwargs = {}
            done = False
            start_key = None
            
            while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                    
                response = table.scan(**scan_kwargs)
                items = response.get('Items', [])
                start_key = response.get('LastEvaluatedKey')
                done = start_key is None
                
                self.stats['scanned'] += len(items)
                
                for item in items:
                    updates = self.process_item(item, config)
                    
                    if updates:
                        # Construct primary key for update
                        # Most tables use 'id', products use 'pk' and 'sk'
                        key = {}
                        if 'pk' in item and 'sk' in item:
                            key = {'pk': item['pk'], 'sk': item['sk']}
                        elif 'id' in item:
                            key = {'id': item['id']}
                        else:
                            logger.error(f"Could not determine primary key for item: {item}")
                            continue
                            
                        if self.update_item(table, key, updates):
                            self.stats['updated'] += 1
                    else:
                        self.stats['skipped'] += 1
                        # Debug why it was skipped
                        if self.stats['skipped'] <= 10:  # Log first 10 skips
                            reason = "Unknown"
                            if not item.get('account_id'):
                                reason = "Missing account_id"
                            else:
                                reason = "No matching S3 keys found (already migrated or pattern mismatch)"
                                # Log the values being checked
                                for attr in config['attributes']:
                                    if attr in item:
                                        logger.info(f"  [SKIP DEBUG] Item {item.get('id', 'unknown')} attr '{attr}' value: {item[attr]}")
                            
                            logger.info(f"Skipped item {item.get('id', 'unknown')}: {reason}")
                        
        except Exception as e:
            logger.error(f"Error scanning table {table_name}: {e}")
            self.stats['errors'] += 1

    def run(self):
        for table_key in TABLES_CONFIG:
            self.migrate_table(table_key)
            
        logger.info("=" * 80)
        logger.info("DB MIGRATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total scanned: {self.stats['scanned']}")
        logger.info(f"Total updated: {self.stats['updated']}")
        logger.info(f"Total skipped: {self.stats['skipped']}")
        logger.info(f"Total errors: {self.stats['errors']}")

def main():
    parser = argparse.ArgumentParser(description='Migrate DB S3 paths')
    parser.add_argument('--env', default='dev', help='Environment (dev/prod)')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode')
    
    args = parser.parse_args()
    
    migration = DBS3Migration(env=args.env, dry_run=args.dry_run)
    migration.run()

if __name__ == '__main__':
    main()
