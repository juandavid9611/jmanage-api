#!/usr/bin/env python3
"""
Application Usage Report

Shows:
  1. Users created this month
  2. Users who participated in at least one calendar event
  3. Users who have at least one payment request
  4. Union of users with event participation and payment requests

Usage:
    python migrations/usage_report.py
"""

import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3


def _scan_all(table, **kwargs):
    items = []
    start_key = None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return items


class UsageReport:
    def __init__(self):
        dynamodb = boto3.resource("dynamodb")

        user_tn = os.getenv("USER_TABLE_NAME")
        calendar_tn = os.getenv("CALENDAR_TABLE_NAME")
        payment_tn = os.getenv("PAYMENT_REQUEST_TABLE_NAME")

        if not all([user_tn, calendar_tn, payment_tn]):
            raise ValueError("USER_TABLE_NAME, CALENDAR_TABLE_NAME, and PAYMENT_REQUEST_TABLE_NAME must be set")

        self.user_table = dynamodb.Table(user_tn)
        self.calendar_table = dynamodb.Table(calendar_tn)
        self.payment_table = dynamodb.Table(payment_tn)

    def get_all_users(self):
        return _scan_all(self.user_table)

    def get_all_calendar_events(self):
        return _scan_all(self.calendar_table)

    def get_all_payment_requests(self):
        return _scan_all(self.payment_table)

    def run(self):
        now = datetime.now(timezone.utc)
        jan_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        jan_start_epoch = int(jan_start.timestamp())
        feb_end = now.replace(month=3, day=1, hour=0, minute=0, second=0, microsecond=0)
        feb_end_epoch = int(feb_end.timestamp())

        print(f"\n{'='*60}")
        print(f"📊 Application Usage Report")
        print(f"   Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"   Period: January – February {now.year}")
        print(f"{'='*60}\n")

        # ── 1. Users ──
        print("👥 Loading users...")
        users = self.get_all_users()
        total_users = len(users)

        # Users created in January–February (created_time is epoch seconds)
        new_users = []
        for u in users:
            created = u.get("created_time")
            if created and jan_start_epoch <= int(created) < feb_end_epoch:
                new_users.append(u)

        print(f"\n{'─'*60}")
        print(f"1️⃣  USERS CREATED IN JANUARY–FEBRUARY {now.year}")
        print(f"{'─'*60}")
        print(f"   Total users in system: {total_users}")
        print(f"   Created in Jan–Feb: {len(new_users)}")
        if new_users:
            print()
            for u in new_users:
                name = u.get("user_name", "Unknown")
                email = u.get("email", "N/A")
                created_dt = datetime.fromtimestamp(int(u["created_time"]), tz=timezone.utc)
                print(f"   • {name} ({email}) — {created_dt.strftime('%Y-%m-%d')}")

        # ── 2. Calendar participation ──
        print(f"\n{'─'*60}")
        print(f"2️⃣  USERS WITH CALENDAR EVENT PARTICIPATION")
        print(f"{'─'*60}")
        print("📅 Loading calendar events...")
        events = self.get_all_calendar_events()
        total_events = len(events)

        # Collect unique user IDs from all event participants
        participating_users = {}  # user_id -> user_name
        events_with_participants = 0
        for event in events:
            participants = event.get("participants", {})
            if participants:
                events_with_participants += 1
                for uid, uname in participants.items():
                    if uid not in participating_users:
                        participating_users[uid] = uname

        print(f"   Total calendar events:         {total_events}")
        print(f"   Events with participants:      {events_with_participants}")
        print(f"   Unique users who participated: {len(participating_users)}")
        if participating_users:
            print()
            for uid, uname in sorted(participating_users.items(), key=lambda x: x[1]):
                print(f"   • {uname} (ID: {uid})")

        # ── 3. Payment requests (Jan–Feb only) ──
        print(f"\n{'─'*60}")
        print(f"3️⃣  USERS WITH PAYMENT REQUESTS (JAN–FEB {now.year})")
        print(f"{'─'*60}")
        print("💰 Loading payment requests...")
        payments = self.get_all_payment_requests()

        # Filter to payments created in January–February
        payments = [p for p in payments if jan_start_epoch <= int(p.get("created_time", 0)) < feb_end_epoch]
        total_payments = len(payments)

        # Collect unique user IDs from payment requests
        users_with_payments = {}  # user_id -> count
        for pr in payments:
            uid = pr.get("user_id")
            if uid:
                users_with_payments[uid] = users_with_payments.get(uid, 0) + 1

        # Resolve user names
        user_name_map = {u["id"]: u.get("user_name", "Unknown") for u in users}

        print(f"   Total payment requests:                {total_payments}")
        print(f"   Unique users with payment requests:    {len(users_with_payments)}")
        if users_with_payments:
            print()
            for uid, count in sorted(users_with_payments.items(), key=lambda x: x[1], reverse=True):
                name = user_name_map.get(uid, uid)
                print(f"   • {name} — {count} request{'s' if count > 1 else ''}")

        # ── 4. Union: event participation ∪ payment requests ──
        print(f"\n{'─'*60}")
        print(f"4️⃣  UNION: USERS WITH EVENTS OR PAYMENT REQUESTS")
        print(f"{'─'*60}")

        union_user_ids = set(participating_users.keys()) | set(users_with_payments.keys())

        print(f"   Users with event participation only:   {len(set(participating_users.keys()) - set(users_with_payments.keys()))}")
        print(f"   Users with payment requests only:      {len(set(users_with_payments.keys()) - set(participating_users.keys()))}")
        print(f"   Users with both:                       {len(set(participating_users.keys()) & set(users_with_payments.keys()))}")
        print(f"   Total unique users (union):            {len(union_user_ids)}")
        if union_user_ids:
            print()
            for uid in sorted(union_user_ids, key=lambda u: user_name_map.get(u, u)):
                name = user_name_map.get(uid, uid)
                tags = []
                if uid in participating_users:
                    tags.append("events")
                if uid in users_with_payments:
                    tags.append("payments")
                print(f"   • {name} — {', '.join(tags)}")

        # ── Summary ──
        print(f"\n{'='*60}")
        print(f"📋 SUMMARY")
        print(f"{'='*60}")
        print(f"   Total users:                        {total_users}")
        print(f"   New users (Jan–Feb):                {len(new_users)}")
        print(f"   Users with event participation:     {len(participating_users)}")
        print(f"   Users with payment requests:        {len(users_with_payments)}")
        print(f"   Active users (events ∪ payments):   {len(union_user_ids)}")
        print(f"{'='*60}\n")


def main():
    report = UsageReport()
    report.run()


if __name__ == "__main__":
    main()
