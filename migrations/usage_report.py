#!/usr/bin/env python3
"""
Application Usage Report

For a given month, generates:
  • a .log file with the human-readable report
  • a .html invoice file based on usage_report_template.html

Sections:
  1. Users created in the month
  2. Users who participated in at least one calendar event
  3. Users who have at least one payment request in the month
  4. Union of users with event participation and payment requests

Usage:
    python migrations/usage_report.py                    # current month
    python migrations/usage_report.py --month 4          # April of current year
    python migrations/usage_report.py --month 4 --year 2026
"""

import argparse
import calendar
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3


MIGRATIONS_DIR = Path(__file__).resolve().parent
TEMPLATE_PATH = MIGRATIONS_DIR / "usage_report_template.html"

ADMIN_DISCOUNT_USERS = 5
RATE_PER_USER_COP = 10000

SPANISH_MONTHS = [
    "", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
SPANISH_MONTHS_SHORT = [
    "", "Ene", "Feb", "Mar", "Abr", "May", "Jun",
    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
]


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


def _format_cop(amount: int) -> str:
    return f"${amount:,.0f}".replace(",", ".")


class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, s):
        for stream in self._streams:
            stream.write(s)

    def flush(self):
        for stream in self._streams:
            stream.flush()


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

    def run(self, year: int, month: int) -> dict:
        period_start = datetime(year, month, 1, tzinfo=timezone.utc)
        next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
        period_end = datetime(next_year, next_month, 1, tzinfo=timezone.utc)
        start_epoch = int(period_start.timestamp())
        end_epoch = int(period_end.timestamp())
        month_name = calendar.month_name[month]
        now = datetime.now(timezone.utc)

        print(f"\n{'='*60}")
        print(f"📊 Application Usage Report")
        print(f"   Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"   Period: {month_name} {year}")
        print(f"{'='*60}\n")

        # ── 1. Users ──
        print("👥 Loading users...")
        users = self.get_all_users()
        total_users = len(users)

        new_users = []
        for u in users:
            created = u.get("created_time")
            if created and start_epoch <= int(created) < end_epoch:
                new_users.append(u)

        print(f"\n{'─'*60}")
        print(f"1️⃣  USERS CREATED IN {month_name.upper()} {year}")
        print(f"{'─'*60}")
        print(f"   Total users in system: {total_users}")
        print(f"   Created in {month_name}: {len(new_users)}")
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

        participating_users = {}
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

        # ── 3. Payment requests ──
        print(f"\n{'─'*60}")
        print(f"3️⃣  USERS WITH PAYMENT REQUESTS ({month_name.upper()} {year})")
        print(f"{'─'*60}")
        print("💰 Loading payment requests...")
        payments = self.get_all_payment_requests()

        payments = [p for p in payments if start_epoch <= int(p.get("created_time", 0)) < end_epoch]
        total_payments = len(payments)

        users_with_payments = {}
        for pr in payments:
            uid = pr.get("user_id")
            if uid:
                users_with_payments[uid] = users_with_payments.get(uid, 0) + 1

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
        print(f"📋 SUMMARY — {month_name} {year}")
        print(f"{'='*60}")
        print(f"   Total users:                        {total_users}")
        print(f"   New users ({month_name}):              {len(new_users)}")
        print(f"   Users with event participation:     {len(participating_users)}")
        print(f"   Users with payment requests:        {len(users_with_payments)}")
        print(f"   Active users (events ∪ payments):   {len(union_user_ids)}")
        print(f"{'='*60}\n")

        return {
            "total_users": total_users,
            "new_users": len(new_users),
            "events_count": len(participating_users),
            "payments_count": len(users_with_payments),
            "active_count": len(union_user_ids),
            "now": now,
        }


def render_invoice_html(metrics: dict, year: int, month: int) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    active = metrics["active_count"]
    admin_count = min(ADMIN_DISCOUNT_USERS, active)
    chargeable = active - admin_count

    active_subtotal = active * RATE_PER_USER_COP
    admin_discount_amount = admin_count * RATE_PER_USER_COP
    total_amount = active_subtotal - admin_discount_amount

    spanish_month = SPANISH_MONTHS[month]
    spanish_month_short = SPANISH_MONTHS_SHORT[month]
    now = metrics["now"]
    generated_date = f"{now.day} de {SPANISH_MONTHS[now.month]}, {now.year}"

    substitutions = {
        "{{PERIOD}}": f"{spanish_month} {year}",
        "{{INVOICE_NUMBER}}": f"INV-{year}-{month:02d}",
        "{{GENERATED_DATE}}": generated_date,
        "{{EVENTS_COUNT}}": str(metrics["events_count"]),
        "{{PAYMENTS_COUNT}}": str(metrics["payments_count"]),
        "{{ACTIVE_COUNT}}": str(active),
        "{{TOTAL_USERS}}": str(metrics["total_users"]),
        "{{MONTH_SHORT}}": spanish_month_short,
        "{{NEW_USERS}}": str(metrics["new_users"]),
        "{{CHARGEABLE_COUNT}}": str(chargeable),
        "{{ADMIN_COUNT}}": str(admin_count),
        "{{RATE}}": _format_cop(RATE_PER_USER_COP),
        "{{ACTIVE_SUBTOTAL}}": _format_cop(active_subtotal),
        "{{ADMIN_DISCOUNT_AMOUNT}}": _format_cop(admin_discount_amount),
        "{{TOTAL_AMOUNT}}": _format_cop(total_amount),
    }

    out = template
    for key, value in substitutions.items():
        out = out.replace(key, value)
    return out


def main():
    now = datetime.now(timezone.utc)
    parser = argparse.ArgumentParser(description="Application usage report for a given month.")
    parser.add_argument("--month", type=int, default=now.month, help="Month (1-12). Defaults to current month.")
    parser.add_argument("--year", type=int, default=now.year, help="Year (e.g. 2026). Defaults to current year.")
    args = parser.parse_args()

    if not 1 <= args.month <= 12:
        parser.error("--month must be between 1 and 12")

    month_slug = SPANISH_MONTHS[args.month].lower()
    log_path = MIGRATIONS_DIR / f"usage_report_{month_slug}_{args.year}.log"
    html_path = MIGRATIONS_DIR / f"reporte_vittoria_{month_slug}_{args.year}.html"

    report = UsageReport()

    original_stdout = sys.stdout
    with open(log_path, "w", encoding="utf-8") as log_file:
        sys.stdout = _Tee(original_stdout, log_file)
        try:
            metrics = report.run(args.year, args.month)
        finally:
            sys.stdout = original_stdout

    html = render_invoice_html(metrics, args.year, args.month)
    html_path.write_text(html, encoding="utf-8")

    print(f"📝 Log written to:  {log_path}")
    print(f"🧾 HTML written to: {html_path}")


if __name__ == "__main__":
    main()
