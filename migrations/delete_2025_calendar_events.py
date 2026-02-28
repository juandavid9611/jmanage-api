#!/usr/bin/env python3
"""
Migration: Delete all calendar events from 2025

This script:
1. Scans all calendar events for the specified account
2. Filters for events from 2025 based on start_date
3. Deletes those events

Usage:
    python migrations/delete_2025_calendar_events.py                    # Dry-run (preview changes)
    python migrations/delete_2025_calendar_events.py --execute          # Execute deletion
    python migrations/delete_2025_calendar_events.py --account vittoriacd --execute  # Specify account
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


class Delete2025CalendarEvents:
    def __init__(self, account_id: str, dry_run: bool = True):
        self.account_id = account_id
        self.dry_run = dry_run
        self.dynamodb = boto3.resource("dynamodb")
        
        # Get table name from environment
        self.calendar_table_name = os.getenv("CALENDAR_TABLE_NAME", "jmanage_calendar")
        self.calendar_table = self.dynamodb.Table(self.calendar_table_name)
        
        # Statistics
        self.stats = {
            "events_scanned": 0,
            "events_2025": 0,
            "events_deleted": 0,
            "errors": 0
        }
    
    def scan_all_events(self):
        """Scan all calendar events for the account with pagination"""
        events = []
        
        print(f"📊 Scanning {self.calendar_table_name} for events in account '{self.account_id}'...")
        
        try:
            filter_expr = Attr("account_id").eq(self.account_id)
            response = self.calendar_table.scan(FilterExpression=filter_expr)
            events = response.get("Items", [])
            
            # Handle pagination
            while "LastEvaluatedKey" in response:
                response = self.calendar_table.scan(
                    FilterExpression=filter_expr,
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                events.extend(response.get("Items", []))
            
            self.stats["events_scanned"] = len(events)
            print(f"✅ Found {len(events)} total calendar events")
            return events
            
        except Exception as e:
            print(f"❌ Error scanning calendar events: {e}")
            self.stats["errors"] += 1
            raise
    
    def is_2025_event(self, event: dict[str, Any]) -> bool:
        """Check if a calendar event is from 2025"""
        # Check event_start field (timestamp in milliseconds)
        if "event_start" in event:
            try:
                # Convert milliseconds to seconds (DynamoDB returns Decimal, convert to float)
                timestamp_seconds = float(event["event_start"]) / 1000.0
                event_date = datetime.fromtimestamp(timestamp_seconds)
                return event_date.year == 2025
            except (ValueError, TypeError, OSError):
                pass
        
        # Fallback: check start_date field (ISO format string)
        if "start_date" in event:
            try:
                start_date = datetime.fromisoformat(event["start_date"].replace("Z", "+00:00"))
                return start_date.year == 2025
            except (ValueError, AttributeError):
                pass
        
        # Fallback: check created_time if available (timestamp in seconds)
        if "created_time" in event:
            try:
                created_date = datetime.fromtimestamp(event["created_time"])
                return created_date.year == 2025
            except (ValueError, TypeError, OSError):
                pass
        
        return False
    
    def delete_event(self, event_id: str) -> bool:
        """Delete a calendar event"""
        if self.dry_run:
            return True
        
        try:
            self.calendar_table.delete_item(Key={"id": event_id})
            return True
        except Exception as e:
            print(f"   ❌ Error deleting event {event_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    def run(self):
        """Run the deletion"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🗑️  Delete 2025 Calendar Events ({mode})")
        print(f"{'='*60}\n")
        print(f"Account: {self.account_id}\n")
        
        # Scan all events
        all_events = self.scan_all_events()
        
        if not all_events:
            print("\n✅ No calendar events found.")
            return
        
        # Filter for 2025 events
        events_2025 = [event for event in all_events if self.is_2025_event(event)]
        self.stats["events_2025"] = len(events_2025)
        
        if not events_2025:
            print("\n✅ No calendar events from 2025 found.")
            return
        
        print(f"\n📋 Found {len(events_2025)} calendar events from 2025:\n")
        
        # Display events to be deleted
        for event in events_2025:
            event_id = event.get("id", "unknown")
            title = event.get("title", "No title")
            start_date = event.get("start_date", "No date")
            user_group = event.get("user_group", "No group")
            
            print(f"   • {title}")
            print(f"     ID: {event_id}")
            print(f"     Date: {start_date}")
            print(f"     Group: {user_group}\n")
        
        # Confirm execution
        if not self.dry_run:
            print(f"\n⚠️  WARNING: About to delete {len(events_2025)} calendar events from 2025")
            confirm = input("Type 'DELETE' to continue: ")
            if confirm != "DELETE":
                print("❌ Deletion cancelled")
                return
        
        # Delete events
        print(f"\n{'Simulating deletion of' if self.dry_run else 'Deleting'} {len(events_2025)} events...\n")
        
        for event in events_2025:
            event_id = event.get("id")
            title = event.get("title", "No title")
            
            if not event_id:
                print(f"   ⚠️  Skipping event without ID: {title}")
                self.stats["errors"] += 1
                continue
            
            if self.delete_event(event_id):
                self.stats["events_deleted"] += 1
                if self.dry_run:
                    print(f"   [DRY RUN] Would delete: {title} ({event_id})")
                else:
                    print(f"   ✅ Deleted: {title} ({event_id})")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Deletion Summary ({mode})")
        print(f"{'='*60}")
        print(f"Events scanned: {self.stats['events_scanned']}")
        print(f"Events from 2025: {self.stats['events_2025']}")
        print(f"Events deleted: {self.stats['events_deleted']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"{'='*60}\n")
        
        if self.dry_run:
            print("ℹ️  This was a DRY RUN. No changes were made.")
            print("   Run with --execute to apply changes.\n")
        else:
            print("✅ Deletion complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Delete all calendar events from 2025")
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
    
    migration = Delete2025CalendarEvents(
        account_id=args.account,
        dry_run=not args.execute
    )
    migration.run()


if __name__ == "__main__":
    main()
