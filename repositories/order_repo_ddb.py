import logging
import os
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Attr, Key

from decimal import Decimal
from .ddb_session import order_table, product_table

logger = logging.getLogger(__name__)


class StockConflictError(Exception):
    """A product no longer has enough stock (or is gone/unpublished) at write time."""


class StatusConflictError(Exception):
    """The order status changed concurrently; the transition was not applied."""


def to_decimal(value: Any) -> Decimal:
    """Convierte int/float/str/None a Decimal de forma segura."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


# ---- pure transaction item builders (unit-tested; no AWS calls) ---------------

def build_stock_decrement(product_table_name: str, product_id: str, account_id: str, qty: int) -> Dict[str, Any]:
    """Atomically take `qty` units. Fails the whole transaction unless the product belongs to
    the account, is published and has available >= qty."""
    return {
        "Update": {
            "TableName": product_table_name,
            "Key": {"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"},
            "UpdateExpression": "ADD available :negq, total_sold :q, gsi2_sk :negq, neg_total_sold :negq",
            "ConditionExpression": "attribute_exists(pk) AND account_id = :acc AND publish = :pub AND available >= :q",
            "ExpressionAttributeValues": {
                ":q": int(qty),
                ":negq": -int(qty),
                ":acc": account_id,
                ":pub": "published",
            },
        }
    }


def build_stock_restore(product_table_name: str, product_id: str, account_id: str, qty: int) -> Dict[str, Any]:
    """Give `qty` units back (cancel / refund)."""
    return {
        "Update": {
            "TableName": product_table_name,
            "Key": {"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"},
            "UpdateExpression": "ADD available :q, total_sold :negq, gsi2_sk :q, neg_total_sold :q",
            "ConditionExpression": "attribute_exists(pk) AND account_id = :acc",
            "ExpressionAttributeValues": {":q": int(qty), ":negq": -int(qty), ":acc": account_id},
        }
    }


def build_order_put(order_table_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "Put": {
            "TableName": order_table_name,
            "Item": item,
            "ConditionExpression": "attribute_not_exists(id)",
        }
    }


def build_status_update(
    order_table_name: str, order_id: str, account_id: str, old: str, new: str, event: Dict[str, Any]
) -> Dict[str, Any]:
    """Conditional status change + history append in one expression (no whole-item put)."""
    return {
        "TableName": order_table_name,
        "Key": {"id": order_id},
        "UpdateExpression": "SET #st = :new, history = list_append(if_not_exists(history, :empty), :evt)",
        "ConditionExpression": "#st = :old AND account_id = :acc",
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {
            ":new": new, ":old": old, ":acc": account_id, ":empty": [], ":evt": [event],
        },
    }


def build_create_transaction(
    order_table_name: str,
    product_table_name: str,
    order_item: Dict[str, Any],
    account_id: str,
    quantities: Dict[str, int],
) -> List[Dict[str, Any]]:
    items = [build_order_put(order_table_name, order_item)]
    items += [build_stock_decrement(product_table_name, pid, account_id, q) for pid, q in quantities.items()]
    return items


def build_status_transaction(
    order_table_name: str,
    product_table_name: str,
    order_id: str,
    account_id: str,
    old: str,
    new: str,
    event: Dict[str, Any],
    restock: Dict[str, int],
) -> List[Dict[str, Any]]:
    items = [{"Update": build_status_update(order_table_name, order_id, account_id, old, new, event)}]
    items += [build_stock_restore(product_table_name, pid, account_id, q) for pid, q in restock.items()]
    return items


class OrderRepo:
    def __init__(self):
        self.table = order_table()
        self._product_table = product_table()
        self._account_gsi = os.getenv("ORDER_ACCOUNT_GSI", "account_id_index")

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def new_order_number() -> str:
        """Human-friendly and collision-resistant: #YYMMDD-XXXXXXXX (32 random bits per day)."""
        return f"#{datetime.now(timezone.utc).strftime('%y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    def _transact(self, items: List[Dict[str, Any]]) -> None:
        self.table.meta.client.transact_write_items(TransactItems=items)

    def _is_cancel(self, exc: Exception) -> bool:
        return getattr(exc, "response", {}).get("Error", {}).get("Code") == "TransactionCanceledException"

    def create_with_stock(
        self, order_item: Dict[str, Any], account_id: str, quantities: Dict[str, int]
    ) -> Dict[str, Any]:
        """Write the order and decrement stock atomically. Raises StockConflictError."""
        tx = build_create_transaction(
            self.table.name, self._product_table.name, order_item, account_id, quantities
        )
        try:
            self._transact(tx)
        except Exception as e:
            if self._is_cancel(e):
                raise StockConflictError(str(e)) from e
            raise
        return order_item

    def transition_status(
        self,
        order_id: str,
        account_id: str,
        old: str,
        new: str,
        event: Dict[str, Any],
        restock: Optional[Dict[str, int]] = None,
    ) -> None:
        """Move old -> new only if the order is still in `old`; restores stock in the same
        transaction when `restock` is given. Raises StatusConflictError."""
        try:
            if restock:
                self._transact(build_status_transaction(
                    self.table.name, self._product_table.name, order_id, account_id, old, new, event, restock
                ))
            else:
                upd = build_status_update(self.table.name, order_id, account_id, old, new, event)
                self.table.update_item(
                    Key=upd["Key"],
                    UpdateExpression=upd["UpdateExpression"],
                    ConditionExpression=upd["ConditionExpression"],
                    ExpressionAttributeNames=upd["ExpressionAttributeNames"],
                    ExpressionAttributeValues=upd["ExpressionAttributeValues"],
                )
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code == "ConditionalCheckFailedException" or self._is_cancel(e):
                raise StatusConflictError(str(e)) from e
            raise

    def existing_product_ids(self, account_id: str, product_ids: List[str]) -> set:
        """Products that still exist in the account (restock skips deleted ones)."""
        found = set()
        for pid in product_ids:
            item = self._product_table.get_item(Key={"pk": f"PRODUCT#{pid}", "sk": "PRODUCT"}).get("Item")
            if item and item.get("account_id") == account_id:
                found.add(pid)
        return found

    def list_all(
        self,
        account_id: str,
        workspace_id: Optional[str] = None,
        customer_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Orders of the account, optionally narrowed by workspace and/or customer.
        Queries the account GSI; falls back to a scan (with a warning) if the query raises."""
        flt = None
        if workspace_id:
            flt = Attr("workspace_id").eq(workspace_id)
        if customer_id:
            c = Attr("customer.id").eq(customer_id)
            flt = c if flt is None else flt & c
        try:
            return self._paginate(
                self.table.query,
                IndexName=self._account_gsi,
                KeyConditionExpression=Key("account_id").eq(account_id),
                **({"FilterExpression": flt} if flt is not None else {}),
            )
        except Exception:
            logger.warning("Order account GSI query failed; falling back to scan", exc_info=True)
            account_filter = Attr("account_id").eq(account_id)
            return self._paginate(
                self.table.scan,
                FilterExpression=account_filter if flt is None else account_filter & flt,
            )

    @staticmethod
    def _paginate(fn, **kwargs) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        while True:
            resp = fn(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                return items
            kwargs["ExclusiveStartKey"] = last

    def get_by_id(self, order_id: str, account_id: str) -> Optional[Dict[str, Any]]:
        """Get order by ID, validating it belongs to the account"""
        response = self.table.get_item(Key={"id": order_id})
        item = response.get("Item")

        if item and item.get("account_id") != account_id:
            return None

        return item

    def set_delivery(self, order_id: str, account_id: str, delivery: Dict[str, Any]) -> None:
        """Targeted update of the delivery attribute (never a whole-item put)."""
        self.table.update_item(
            Key={"id": order_id},
            UpdateExpression="SET delivery = :d",
            ConditionExpression=Attr("account_id").eq(account_id),
            ExpressionAttributeValues={":d": delivery},
        )

    def set_payment_request_id(self, order_id: str, account_id: str, payment_request_id: str) -> None:
        """Link a payment request to an existing order."""
        self.table.update_item(
            Key={"id": order_id},
            UpdateExpression="SET payment_request_id = :prid",
            ConditionExpression=Attr("account_id").eq(account_id),
            ExpressionAttributeValues={":prid": payment_request_id},
        )

    def set_check(
        self,
        order_id: str,
        account_id: str,
        field: str,
        value: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Set an admin check field (provider_check / delivery_check) atomically."""
        if field not in ("provider_check", "delivery_check"):
            raise ValueError(f"Unsupported check field: {field}")
        current = self.get_by_id(order_id, account_id)
        if not current:
            return None
        self.table.update_item(
            Key={"id": order_id},
            UpdateExpression=f"SET {field} = :val",
            ConditionExpression=Attr("account_id").eq(account_id),
            ExpressionAttributeValues={":val": value},
        )
        current[field] = value
        return current

    def append_event(
        self,
        order_id: str,
        account_id: str,
        event: Dict[str, Any],
    ) -> None:
        """Atomically append an event to order.history using DDB list_append."""
        self.table.update_item(
            Key={"id": order_id},
            UpdateExpression="SET history = list_append(if_not_exists(history, :empty), :evt)",
            ConditionExpression=Attr("account_id").eq(account_id),
            ExpressionAttributeValues={":empty": [], ":evt": [event]},
        )
