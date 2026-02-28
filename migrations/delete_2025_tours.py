#!/usr/bin/env python3
"""
Migration: Delete all tours from 2025

This script:
1. Scans all tours for the specified account
2. Filters for tours from 2025 based on created_at timestamp
3. Deletes those tours

Usage:
    python migrations/delete_2025_tours.py                    # Dry-run (preview changes)
    python migrations/delete_2025_tours.py --execute          # Execute deletion
    python migrations/delete_2025_tours.py --account vittoriacd --execute  # Specify account
"""

import os
import sys
import argparse
from datetime import datetime
from typing import Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from boto3.dynamodb.conditions import Attr


class Delete2025Tours:
    def __init__(self, account_id: str, dry_run: bool = True):
        self.account_id = account_id
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        
        # Get table name from environment
        self.tour_table_name = os.getenv("TOUR_TABLE_NAME", "jmanage_tour")
        self.tour_table = self.dynamodb.Table(self.tour_table_name)
        
        # Statistics
        self.stats = {
            "tours_scanned": 0,
            "tours_2025": 0,
            "tours_deleted": 0,
            "errors": 0
        }
    
    def scan_all_tours(self):
        """Scan all tours for the account with pagination"""
        tours = []
        
        print(f"📊 Scanning {self.tour_table_name} for tours in account '{self.account_id}'...")
        
        try:
            filter_expr = Attr("account_id").eq(self.account_id)
            response = self.tour_table.scan(FilterExpression=filter_expr)
            tours = response.get("Items", [])
            
            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.tour_table.scan(
                    FilterExpression=filter_expr,
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                tours.extend(response.get("Items", []))
            
            self.stats["tours_scanned"] = len(tours)
            print(f"✅ Found {len(tours)} total tours")
            return tours
            
        except Exception as e:
            print(f"❌ Error scanning tours: {e}")
            self.stats["errors"] += 1
            raise
    
    def is_2025_tour(self, tour: dict[str, Any]) -> bool:
        """Check if a tour is from 2025 based on available start dates"""
        # Check available field for startDate values (millisecond timestamps)
        if "available" in tour and tour["available"]:
            try:
                available = tour["available"]
                timestamp_seconds = float(available["startDate"]) / 1000.0
                start_date = datetime.fromtimestamp(timestamp_seconds)
                if start_date.year == 2024:
                    return True
            except (ValueError, TypeError, OSError):
                pass
        
        return False
    
    def delete_tour(self, tour_id: str) -> bool:
        """Delete a tour"""
        if self.dry_run:
            return True
        
        try:
            self.tour_table.delete_item(Key={"id": tour_id})
            return True
        except Exception as e:
            print(f"   ❌ Error deleting tour {tour_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    def run(self):
        """Run the deletion"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🗑️  Delete 2025 Tours ({mode})")
        print(f"{'='*60}\n")
        print(f"Account: {self.account_id}\n")
        
        # Scan all tours
        all_tours = self.scan_all_tours()
        
        if not all_tours:
            print("\n✅ No tours found.")
            return
        
        # Filter for 2025 tours
        tours_2025 = [tour for tour in all_tours if self.is_2025_tour(tour)]
        self.stats["tours_2025"] = len(tours_2025)
        
        if not tours_2025:
            print("\n✅ No tours from 2025 found.")
            return
        
        print(f"\n📋 Found {len(tours_2025)} tours from 2025:\n")
        
        # Display tours to be deleted
        for tour in tours_2025:
            tour_id = tour.get("id", "unknown")
            name = tour.get("tour_name", "No name")
            user_group = tour.get("user_group", "No group")
            event_type = tour.get("event_type", "No type")
            
            # Extract 2025 dates from available
            dates_2025 = []
            if "available" in tour and tour["available"]:
                available = tour["available"]
                timestamp_seconds = float(available["startDate"]) / 1000.0
                start_date = datetime.fromtimestamp(timestamp_seconds)
                if start_date.year == 2024:
                    dates_2025.append(start_date.strftime("%Y-%m-%d"))
            
            print(f"   • {name}")
            print(f"     ID: {tour_id}")
            print(f"     2025 Dates: {', '.join(dates_2025) if dates_2025 else 'Unknown'}")
            print(f"     Group: {user_group}")
            print(f"     Type: {event_type}\n")
        
        # Confirm execution
        if not self.dry_run:
            print(f"\n⚠️  WARNING: About to delete {len(tours_2025)} tours from 2025")
            confirm = input("Type 'DELETE' to continue: ")
            if confirm != "DELETE":
                print("❌ Deletion cancelled")
                return
        
        # Delete tours
        print(f"\n{'Simulating deletion of' if self.dry_run else 'Deleting'} {len(tours_2025)} tours...\n")
        
        for tour in tours_2025:
            tour_id = tour.get("id")
            name = tour.get("tour_name", "No name")
            
            if not tour_id:
                print(f"   ⚠️  Skipping tour without ID: {name}")
                self.stats["errors"] += 1
                continue
            
            if self.delete_tour(tour_id):
                self.stats["tours_deleted"] += 1
                if self.dry_run:
                    print(f"   [DRY RUN] Would delete: {name} ({tour_id})")
                else:
                    print(f"   ✅ Deleted: {name} ({tour_id})")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Deletion Summary ({mode})")
        print(f"{'='*60}")
        print(f"Tours scanned: {self.stats['tours_scanned']}")
        print(f"Tours from 2025: {self.stats['tours_2025']}")
        print(f"Tours deleted: {self.stats['tours_deleted']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"{'='*60}\n")
        
        if self.dry_run:
            print("ℹ️  This was a DRY RUN. No changes were made.")
            print("   Run with --execute to apply changes.\n")
        else:
            print("✅ Deletion complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Delete all tours from 2025")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the deletion (default is dry-run)"
    )
    parser.add_argument(
        "--account",
        default="vittoriacd",
        help="Account ID to process (default: vittoriacd)"
    )
    
    args = parser.parse_args()
    
    migration = Delete2025Tours(
        account_id=args.account,
        dry_run=not args.execute
    )
    migration.run()


if __name__ == "__main__":
    main()
