#!/usr/bin/env python3
"""
Migration: Check for orphaned calendar events and tours

This script checks for:
1. Tours with calendar_event_id that points to non-existent calendar events
2. Calendar events with tour_id that points to non-existent tours

Usage:
    python migrations/check_orphan_calendars_tours.py                    # Check for orphans
    python migrations/check_orphan_calendars_tours.py --account vittoriacd  # Specify account
    python migrations/check_orphan_calendars_tours.py --fix             # Delete orphaned references (sets to None)
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
from boto3.dynamodb.conditions import Attr


class OrphanChecker:
    def __init__(self, account_id: str, fix: bool = False):
        self.account_id = account_id
        self.fix = fix
        self.dynamodb = boto3.resource("dynamodb")
        
        # Get table names from environment
        self.tour_table_name = os.getenv("TOUR_TABLE_NAME", "jmanage_tour")
        self.calendar_table_name = os.getenv("CALENDAR_TABLE_NAME", "jmanage_calendar")
        
        self.tour_table = self.dynamodb.Table(self.tour_table_name)
        self.calendar_table = self.dynamodb.Table(self.calendar_table_name)
        
        # Statistics
        self.stats = {
            "tours_scanned": 0,
            "calendars_scanned": 0,
            "orphan_tours": 0,
            "orphan_calendars": 0,
            "fixed": 0,
            "errors": 0
        }
        
        # Store IDs for quick lookup
        self.tour_ids = set()
        self.calendar_ids = set()
    
    def scan_all(self, table, filter_expr):
        """Scan all items from a table with pagination"""
        items = []
        
        try:
            response = table.scan(FilterExpression=filter_expr)
            items = response.get("Items", [])
            
            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = table.scan(
                    FilterExpression=filter_expr,
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                items.extend(response.get("Items", []))
            
            return items
            
        except Exception as e:
            print(f"❌ Error scanning table: {e}")
            self.stats["errors"] += 1
            return []
    
    def load_tours(self):
        """Load all tours for the account"""
        print(f"📊 Loading tours from {self.tour_table_name}...")
        
        filter_expr = Attr("account_id").eq(self.account_id)
        tours = self.scan_all(self.tour_table, filter_expr)
        
        self.stats["tours_scanned"] = len(tours)
        print(f"✅ Found {len(tours)} tours")
        
        # Store tour IDs
        for tour in tours:
            if "id" in tour:
                self.tour_ids.add(tour["id"])
        
        return tours
    
    def load_calendars(self):
        """Load all calendar events for the account"""
        print(f"📊 Loading calendar events from {self.calendar_table_name}...")
        
        filter_expr = Attr("account_id").eq(self.account_id)
        calendars = self.scan_all(self.calendar_table, filter_expr)
        
        self.stats["calendars_scanned"] = len(calendars)
        print(f"✅ Found {len(calendars)} calendar events")
        
        # Store calendar IDs
        for calendar in calendars:
            if "id" in calendar:
                self.calendar_ids.add(calendar["id"])
        
        return calendars
    
    def check_orphan_tours(self, tours):
        """Check for tours with invalid calendar_event_id"""
        orphan_tours = []
        
        print(f"\n🔍 Checking tours for orphaned calendar_event_id references...")
        
        for tour in tours:
            calendar_event_id = tour.get("calendar_event_id")
            
            # Skip if no calendar_event_id
            if not calendar_event_id:
                continue
            
            # Check if calendar event exists
            if calendar_event_id not in self.calendar_ids:
                orphan_tours.append({
                    "id": tour.get("id"),
                    "name": tour.get("tour_name", "Unknown"),
                    "calendar_event_id": calendar_event_id,
                    "group": tour.get("user_group", "Unknown")
                })
                self.stats["orphan_tours"] += 1
        
        return orphan_tours
    
    def check_orphan_calendars(self, calendars):
        """Check for calendar events with invalid tour_id"""
        orphan_calendars = []
        
        print(f"🔍 Checking calendar events for orphaned tour_id references...")
        
        for calendar in calendars:
            tour_id = calendar.get("tour_id")
            
            # Skip if no tour_id
            if not tour_id:
                continue
            
            # Check if tour exists
            if tour_id not in self.tour_ids:
                orphan_calendars.append({
                    "id": calendar.get("id"),
                    "title": calendar.get("title", "Unknown"),
                    "tour_id": tour_id,
                    "group": calendar.get("user_group", "Unknown")
                })
                self.stats["orphan_calendars"] += 1
        
        return orphan_calendars
    
    def fix_orphan_tour(self, tour_id: str) -> bool:
        """Remove orphaned calendar_event_id from tour"""
        try:
            self.tour_table.update_item(
                Key={"id": tour_id},
                UpdateExpression="REMOVE calendar_event_id",
                ReturnValues="UPDATED_NEW"
            )
            self.stats["fixed"] += 1
            return True
        except Exception as e:
            print(f"   ❌ Error fixing tour {tour_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    def fix_orphan_calendar(self, calendar_id: str) -> bool:
        """Remove orphaned tour_id from calendar event"""
        try:
            self.calendar_table.update_item(
                Key={"id": calendar_id},
                UpdateExpression="REMOVE tour_id",
                ReturnValues="UPDATED_NEW"
            )
            self.stats["fixed"] += 1
            return True
        except Exception as e:
            print(f"   ❌ Error fixing calendar {calendar_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    def run(self):
        """Run the orphan check"""
        mode = "FIX MODE" if self.fix else "CHECK MODE"
        print(f"\n{'='*60}")
        print(f"🔍 Orphan Calendar/Tour Checker ({mode})")
        print(f"{'='*60}\n")
        print(f"Account: {self.account_id}\n")
        
        # Load all tours and calendars
        tours = self.load_tours()
        calendars = self.load_calendars()
        
        if not tours and not calendars:
            print("\n✅ No data found.")
            return
        
        # Check for orphans
        orphan_tours = self.check_orphan_tours(tours)
        orphan_calendars = self.check_orphan_calendars(calendars)
        
        # Report orphaned tours
        if orphan_tours:
            print(f"\n⚠️  Found {len(orphan_tours)} tours with orphaned calendar_event_id:\n")
            for tour in orphan_tours:
                print(f"   • Tour: {tour['name']}")
                print(f"     ID: {tour['id']}")
                print(f"     Group: {tour['group']}")
                print(f"     Missing Calendar ID: {tour['calendar_event_id']}\n")
        else:
            print(f"\n✅ No orphaned tours found.")
        
        # Report orphaned calendar events
        if orphan_calendars:
            print(f"\n⚠️  Found {len(orphan_calendars)} calendar events with orphaned tour_id:\n")
            for calendar in orphan_calendars:
                print(f"   • Calendar: {calendar['title']}")
                print(f"     ID: {calendar['id']}")
                print(f"     Group: {calendar['group']}")
                print(f"     Missing Tour ID: {calendar['tour_id']}\n")
        else:
            print(f"\n✅ No orphaned calendar events found.")
        
        # Fix orphans if requested
        if self.fix and (orphan_tours or orphan_calendars):
            print(f"\n⚠️  About to remove orphaned references")
            print(f"   Tours to fix: {len(orphan_tours)}")
            print(f"   Calendar events to fix: {len(orphan_calendars)}")
            confirm = input("Type 'FIX' to continue: ")
            
            if confirm != "FIX":
                print("❌ Fix cancelled")
                return
            
            # Fix tours
            if orphan_tours:
                print(f"\n🔧 Fixing {len(orphan_tours)} orphaned tours...")
                for tour in orphan_tours:
                    if self.fix_orphan_tour(tour['id']):
                        print(f"   ✅ Fixed tour: {tour['name']} ({tour['id']})")
                    else:
                        print(f"   ❌ Failed to fix tour: {tour['name']} ({tour['id']})")
            
            # Fix calendar events
            if orphan_calendars:
                print(f"\n🔧 Fixing {len(orphan_calendars)} orphaned calendar events...")
                for calendar in orphan_calendars:
                    if self.fix_orphan_calendar(calendar['id']):
                        print(f"   ✅ Fixed calendar: {calendar['title']} ({calendar['id']})")
                    else:
                        print(f"   ❌ Failed to fix calendar: {calendar['title']} ({calendar['id']})")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Summary ({mode})")
        print(f"{'='*60}")
        print(f"Tours scanned: {self.stats['tours_scanned']}")
        print(f"Calendar events scanned: {self.stats['calendars_scanned']}")
        print(f"Orphaned tours: {self.stats['orphan_tours']}")
        print(f"Orphaned calendar events: {self.stats['orphan_calendars']}")
        if self.fix:
            print(f"Fixed: {self.stats['fixed']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"{'='*60}\n")
        
        if not self.fix and (orphan_tours or orphan_calendars):
            print("ℹ️  Run with --fix to remove orphaned references.\n")
        elif self.fix:
            print("✅ Fix complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Check for orphaned calendar events and tours")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Fix orphaned references (removes invalid IDs)"
    )
    parser.add_argument(
        "--account",
        default="vittoriacd",
        help="Account ID to process (default: vittoriacd)"
    )
    
    args = parser.parse_args()
    
    checker = OrphanChecker(
        account_id=args.account,
        fix=args.fix
    )
    checker.run()


if __name__ == "__main__":
    main()
