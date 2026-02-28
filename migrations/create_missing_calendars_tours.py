#!/usr/bin/env python3
"""
Migration: Create missing calendar events and tours based on their relationships

This script:
1. Finds tours with calendar_event_id but the calendar event doesn't exist -> creates calendar event
2. Finds calendar events with tour_id but the tour doesn't exist -> creates tour

The script recreates the missing entity based on the existing one, copying relevant fields:
- For tours -> calendar: title, location, start/end times, category, group, participants
- For calendars -> tours: name, location, available dates, services, bookers, event type, group

Usage:
    python migrations/create_missing_calendars_tours.py                    # Dry-run
    python migrations/create_missing_calendars_tours.py --execute          # Execute creation
    python migrations/create_missing_calendars_tours.py --account vittoriacd --execute
"""

import os
import sys
import argparse
from datetime import datetime
from typing import Any
from uuid import uuid4
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from boto3.dynamodb.conditions import Attr


class MissingEntityCreator:
    def __init__(self, account_id: str, dry_run: bool = True):
        self.account_id = account_id
        self.dry_run = dry_run
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
            "missing_calendars": 0,
            "missing_tours": 0,
            "calendars_created": 0,
            "tours_created": 0,
            "errors": 0
        }
        
        # Store IDs and data for quick lookup
        self.tour_ids = set()
        self.calendar_ids = set()
        self.tours = {}
        self.calendars = {}
    
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
    
    def load_data(self):
        """Load all tours and calendar events"""
        print(f"📊 Loading data from {self.tour_table_name} and {self.calendar_table_name}...")
        
        filter_expr = Attr("account_id").eq(self.account_id)
        
        # Load tours
        tours = self.scan_all(self.tour_table, filter_expr)
        self.stats["tours_scanned"] = len(tours)
        for tour in tours:
            if "id" in tour:
                self.tour_ids.add(tour["id"])
                self.tours[tour["id"]] = tour
        
        # Load calendar events
        calendars = self.scan_all(self.calendar_table, filter_expr)
        self.stats["calendars_scanned"] = len(calendars)
        for calendar in calendars:
            if "id" in calendar:
                self.calendar_ids.add(calendar["id"])
                self.calendars[calendar["id"]] = calendar
        
        print(f"✅ Loaded {len(tours)} tours and {len(calendars)} calendar events\n")
    
    def build_calendar_from_tour(self, tour: dict[str, Any]) -> dict[str, Any]:
        """Build a calendar event from tour data"""
        # Extract available dates
        available = tour.get("available", {})
        start_date = available.get("startDate", int(datetime.now().timestamp() * 1000))
        end_date = available.get("endDate", start_date)
        
        # Build title from tour name
        title = tour.get("tour_name", "Evento")
        
        return {
            "id": tour.get("calendar_event_id"),  # Use the referenced ID
            "account_id": self.account_id,
            "all_day": False,
            "color": "#3788d8",  # Default color
            "description": tour.get("content", ""),
            "event_location": tour.get("event_location", ""),
            "event_start": start_date,
            "event_end": end_date,
            "title": title,
            "category": tour.get("event_type", "other"),
            "participants": {},  # Will be populated by user participations
            "user_group": tour.get("user_group", ""),
            "create_tour": True,
            "tour_id": tour.get("id"),
        }
    
    def build_tour_from_calendar(self, calendar: dict[str, Any]) -> dict[str, Any]:
        """Build a tour from calendar event data"""
        # Compute services based on group and category (matching tour_builder.py logic)
        services = []
        group = calendar.get("user_group", "")
        category = calendar.get("category", "")
        
        # SERVICE_BY_GROUP mapping from tour_builder.py
        SERVICE_BY_GROUP = {
            "male": "Vittoria Masculino",
            "female": "Vittoria Femenino",
        }
        
        # Add service from group mapping
        group_service = SERVICE_BY_GROUP.get(group)
        if group_service:
            services.append(group_service)
        
        # Add category as service
        if category:
            services.append(category)
        
        # Deduplicate while preserving order
        services = list(dict.fromkeys(services))
        
        # Convert participants (user_id -> user_name) to bookers (user_id -> booker_object)
        participants = calendar.get("participants", {})
        bookers = {}
        
        for user_id, user_name in participants.items():
            # Match the exact structure from CalendarService.participate
            bookers[user_id] = {
                "id": user_id,
                "name": user_name,
                "avatarUrl": None,
                "guests": 1,
                "approved": True,  # Default to approved for migrated participants
                "late": False,
                "yellowCard": False,
                "redCard": False,
                "mvp": False,
                "goals": 0,
                "assists": 0,
            }
        
        return {
            "id": calendar.get("tour_id"),  # Use the referenced ID
            "account_id": self.account_id,
            "tour_name": calendar.get("title", "Tour"),
            "images": [],
            "publish": "draft",
            "services": services,
            "available": {
                "startDate": calendar.get("event_start"),
                "endDate": calendar.get("event_end")
            },
            "tour_guides": [],
            "bookers": bookers,  # Properly structured bookers migrated from participants
            "content": calendar.get("description", ""),
            "tags": [],
            "event_location": calendar.get("event_location", ""),
            "scores": {"home": 0, "away": 0},
            "created_at": datetime.now().isoformat(),
            "calendar_event_id": calendar.get("id"),
            "event_type": calendar.get("category", "other"),
            "user_group": group,
        }
    
    def create_calendar_event(self, calendar: dict[str, Any]) -> bool:
        """Create a calendar event in DynamoDB"""
        if self.dry_run:
            return True
        
        try:
            self.calendar_table.put_item(Item=calendar)
            self.stats["calendars_created"] += 1
            return True
        except Exception as e:
            print(f"   ❌ Error creating calendar {calendar.get('id')}: {e}")
            self.stats["errors"] += 1
            return False
    
    def create_tour(self, tour: dict[str, Any]) -> bool:
        """Create a tour in DynamoDB"""
        if self.dry_run:
            return True
        
        try:
            self.tour_table.put_item(Item=tour)
            self.stats["tours_created"] += 1
            return True
        except Exception as e:
            print(f"   ❌ Error creating tour {tour.get('id')}: {e}")
            self.stats["errors"] += 1
            return False
    
    def find_missing_calendars(self):
        """Find tours with missing calendar events"""
        missing = []
        
        for tour_id, tour in self.tours.items():
            calendar_event_id = tour.get("calendar_event_id")
            
            if not calendar_event_id:
                continue
            
            if calendar_event_id not in self.calendar_ids:
                missing.append({
                    "tour": tour,
                    "calendar_event_id": calendar_event_id
                })
                self.stats["missing_calendars"] += 1
        
        return missing
    
    def find_missing_tours(self):
        """Find calendar events with missing tours"""
        missing = []
        
        for calendar_id, calendar in self.calendars.items():
            tour_id = calendar.get("tour_id")
            
            if not tour_id:
                continue
            
            if tour_id not in self.tour_ids:
                missing.append({
                    "calendar": calendar,
                    "tour_id": tour_id
                })
                self.stats["missing_tours"] += 1
        
        return missing
    
    def run(self):
        """Run the migration"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        print(f"\n{'='*60}")
        print(f"🔧 Create Missing Calendar Events and Tours ({mode})")
        print(f"{'='*60}\n")
        print(f"Account: {self.account_id}\n")
        
        # Load all data
        self.load_data()
        
        # Find missing entities
        missing_calendars = self.find_missing_calendars()
        missing_tours = self.find_missing_tours()
        
        # Report missing calendars
        if missing_calendars:
            print(f"⚠️  Found {len(missing_calendars)} tours with missing calendar events:\n")
            for item in missing_calendars:
                tour = item["tour"]
                print(f"   • Tour: {tour.get('tour_name', 'Unknown')}")
                print(f"     Tour ID: {tour.get('id')}")
                print(f"     Missing Calendar ID: {item['calendar_event_id']}")
                print(f"     Group: {tour.get('user_group', 'Unknown')}\n")
        else:
            print("✅ No missing calendar events found.\n")
        
        # Report missing tours
        if missing_tours:
            print(f"⚠️  Found {len(missing_tours)} calendar events with missing tours:\n")
            for item in missing_tours:
                calendar = item["calendar"]
                print(f"   • Calendar: {calendar.get('title', 'Unknown')}")
                print(f"     Calendar ID: {calendar.get('id')}")
                print(f"     Missing Tour ID: {item['tour_id']}")
                print(f"     Group: {calendar.get('user_group', 'Unknown')}\n")
        else:
            print("✅ No missing tours found.\n")
        
        # Execute creation if requested
        if not self.dry_run and (missing_calendars or missing_tours):
            print(f"\n⚠️  About to create missing entities:")
            print(f"   Calendar events to create: {len(missing_calendars)}")
            print(f"   Tours to create: {len(missing_tours)}")
            confirm = input("Type 'CREATE' to continue: ")
            
            if confirm != "CREATE":
                print("❌ Creation cancelled")
                return
            
            # Create missing calendar events
            if missing_calendars:
                print(f"\n🔧 Creating {len(missing_calendars)} calendar events...")
                for item in missing_calendars:
                    tour = item["tour"]
                    calendar = self.build_calendar_from_tour(tour)
                    
                    if self.create_calendar_event(calendar):
                        print(f"   ✅ Created calendar: {calendar['title']} ({calendar['id']})")
                    else:
                        print(f"   ❌ Failed to create calendar for tour: {tour.get('tour_name')}")
            
            # Create missing tours
            if missing_tours:
                print(f"\n🔧 Creating {len(missing_tours)} tours...")
                for item in missing_tours:
                    calendar = item["calendar"]
                    tour = self.build_tour_from_calendar(calendar)
                    
                    if self.create_tour(tour):
                        print(f"   ✅ Created tour: {tour['tour_name']} ({tour['id']})")
                    else:
                        print(f"   ❌ Failed to create tour for calendar: {calendar.get('title')}")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Summary ({mode})")
        print(f"{'='*60}")
        print(f"Tours scanned: {self.stats['tours_scanned']}")
        print(f"Calendar events scanned: {self.stats['calendars_scanned']}")
        print(f"Missing calendar events: {self.stats['missing_calendars']}")
        print(f"Missing tours: {self.stats['missing_tours']}")
        if not self.dry_run:
            print(f"Calendar events created: {self.stats['calendars_created']}")
            print(f"Tours created: {self.stats['tours_created']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"{'='*60}\n")
        
        if self.dry_run and (missing_calendars or missing_tours):
            print("ℹ️  This was a DRY RUN. No entities were created.")
            print("   Run with --execute to create missing entities.\n")
        elif not self.dry_run:
            print("✅ Creation complete!\n")


def main():
    parser = argparse.ArgumentParser(description="Create missing calendar events and tours")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the creation (default is dry-run)"
    )
    parser.add_argument(
        "--account",
        default="vittoriacd",
        help="Account ID to process (default: vittoriacd)"
    )
    
    args = parser.parse_args()
    
    creator = MissingEntityCreator(
        account_id=args.account,
        dry_run=not args.execute
    )
    creator.run()


if __name__ == "__main__":
    main()
