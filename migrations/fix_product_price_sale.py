#!/usr/bin/env python3
"""
Migration: clean up pre-existing Product items for the shop fixes (manual run).

For every product item (sk == "PRODUCT") in PRODUCT_TABLE_NAME:
  * price_sale  -> REMOVED when it is 0 or >= price (legacy "no discount" encodings;
                   contract: priceSale is optional and 0 <= priceSale < price).
  * id          -> backfilled from pk ("PRODUCT#<id>") when missing. The account_id_index
                   GSI (PK account_id, SK id) only indexes items that carry BOTH top-level
                   attributes; items missing account_id cannot be inferred and are only reported.
  * publish     -> "published" when missing.
  * available   -> copied from quantity when missing.
  * gsi1/2/3/5 + neg_total_sold -> (re)computed with the same builder the API uses
                   (gsi3_sk now holds the LIVE price), only when missing or stale.

Dry-run by default: prints counts and product ids only (no product data).

Usage:
    source .venv/bin/activate        # AWS credentials/region + PRODUCT_TABLE_NAME in env or .env
    python migrations/fix_product_price_sale.py             # dry-run
    python migrations/fix_product_price_sale.py --confirm   # write
"""

import argparse
import os
import sys
from decimal import Decimal

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from boto3.dynamodb.conditions import Attr  # noqa: E402

from repositories.ddb_session import product_table  # noqa: E402
from repositories.product_repo_ddb import _build_gsi_attrs, to_decimal  # noqa: E402

GSI_KEYS = ("gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk", "gsi3_pk", "gsi3_sk", "gsi5_pk", "gsi5_sk", "neg_total_sold")


def scan_products(table) -> list[dict]:
    items, kwargs = [], {"FilterExpression": Attr("sk").eq("PRODUCT")}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def plan_changes(item: dict) -> tuple[dict, list[str], list[str]]:
    """Pure: returns (attributes to SET, attributes to REMOVE, problems)."""
    sets: dict = {}
    removes: list[str] = []
    problems: list[str] = []

    if not item.get("account_id"):
        problems.append("missing account_id (cannot infer; not in account_id_index)")
    if not item.get("id"):
        sets["id"] = str(item["pk"]).split("#", 1)[1]
    if not item.get("publish"):
        sets["publish"] = "published"
    if item.get("available") is None:
        sets["available"] = int(item.get("quantity", 0))

    sale = item.get("price_sale")
    sale_changed = False
    if sale is not None and (to_decimal(sale) <= 0 or to_decimal(sale) >= to_decimal(item.get("price", 0))):
        removes.append("price_sale")
        sale_changed = True

    merged = {**item, **sets}
    if sale_changed:
        merged.pop("price_sale", None)
    expected = _build_gsi_attrs(merged)
    for k in GSI_KEYS:
        if sale_changed and k == "gsi3_sk" or item.get(k) != expected[k] and not (
            isinstance(expected[k], Decimal) and item.get(k) is not None and to_decimal(item[k]) == expected[k]
        ):
            sets[k] = expected[k]
    return sets, removes, problems


def apply(table, item: dict, sets: dict, removes: list[str]) -> None:
    names, values, set_parts = {}, {}, []
    for i, (k, v) in enumerate(sets.items()):
        names[f"#s{i}"], values[f":s{i}"] = k, v
        set_parts.append(f"#s{i} = :s{i}")
    remove_parts = []
    for i, k in enumerate(removes):
        names[f"#r{i}"] = k
        remove_parts.append(f"#r{i}")
    expr = ""
    if set_parts:
        expr += "SET " + ", ".join(set_parts)
    if remove_parts:
        expr += " REMOVE " + ", ".join(remove_parts)
    kwargs = {"ExpressionAttributeNames": names}
    if values:
        kwargs["ExpressionAttributeValues"] = values
    table.update_item(
        Key={"pk": item["pk"], "sk": item["sk"]},
        UpdateExpression=expr.strip(),
        ConditionExpression="attribute_exists(pk)",
        **kwargs,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--confirm", action="store_true", help="write changes (default is dry-run)")
    args = parser.parse_args()

    table = product_table()
    items = scan_products(table)
    print(f"Table: {table.name} | products scanned: {len(items)} | mode: {'WRITE' if args.confirm else 'DRY-RUN'}")

    changed = sale_fixed = unfixable = 0
    for item in items:
        sets, removes, problems = plan_changes(item)
        pid = item.get("id") or item.get("pk")
        for p in problems:
            unfixable += 1
            print(f"  [WARN] {pid}: {p}")
        if not sets and not removes:
            continue
        changed += 1
        sale_fixed += "price_sale" in removes
        print(f"  {pid}: set {sorted(sets)} remove {removes}")
        if args.confirm:
            apply(table, item, sets, removes)

    print(f"Products to change: {changed} | price_sale cleared: {sale_fixed} | needing manual attention: {unfixable}")
    if not args.confirm:
        print("Dry-run only. Re-run with --confirm to write.")


if __name__ == "__main__":
    main()
