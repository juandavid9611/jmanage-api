import logging
import os
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone

from repositories.ddb_session import product_table
from services.product_rules import effective_price

logger = logging.getLogger(__name__)

# ---- helpers -------------------------------------------------

def to_decimal(value: Any) -> Decimal:
    """Convierte int/float/str/None a Decimal de forma segura."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _neg(n: int | float | Decimal | None) -> Decimal:
    return -to_decimal(n or 0)

def _build_gsi_attrs(item: Dict[str, Any]) -> Dict[str, Any]:
    # GSI1: account + category by created_at
    account_id = item.get("account_id") or "_NO_ACCOUNT"
    category = item.get("category") or "_UNCAT"
    created_at = item.get("created_at") or _now_iso()
    # GSI2: account + featured by total_sold desc -> usamos neg_total_sold
    neg_total_sold = _neg(item.get("total_sold", 0))
    # GSI3: account + price
    price = effective_price(item.get("price", 0), item.get("price_sale"))
    # GSI5: account + tag/name search
    first_tag = (item.get("tags") or item.get("genders") or ["_NO_TAG"])[0]
    gsi = {
        "gsi1_pk": f"ACCOUNT#{account_id}#CAT#{category}",
        "gsi1_sk": created_at,
        "gsi2_pk": f"ACCOUNT#{account_id}#FEATURED",
        "gsi2_sk": neg_total_sold,
        "gsi3_pk": f"ACCOUNT#{account_id}#PRICE",
        "gsi3_sk": price,
        "gsi5_pk": f"ACCOUNT#{account_id}#TAG#{str(first_tag).lower()}",
        "gsi5_sk": created_at,
        "neg_total_sold": neg_total_sold,
    }
    return gsi

# ---- repository ---------------------------------------------

class ProductRepo:
    def __init__(self):
        self.table = product_table()
        self._account_gsi = os.getenv("PRODUCT_ACCOUNT_GSI", "account_id_index")

    # CREATE
    def create(self, payload: Dict[str, Any], account_id: str) -> Dict[str, Any]:
        """Create a new product for the specified account"""
        product_id = str(uuid.uuid4())
        now = _now_iso()
        taxes = payload.get("taxes")
        price_sale = payload.get("price_sale")

        item = {
            "pk": f"PRODUCT#{product_id}",
            "sk": "PRODUCT",
            "id": product_id,
            "account_id": account_id,
            "created_at": now,
            "name": payload["name"],
            "category": payload["category"],
            "price": to_decimal(payload["price"]),
            "publish": payload.get("publish", "published"),
            "available": int(payload["available"] if payload.get("available") is not None else payload.get("quantity", 0)),
            "quantity": int(payload.get("quantity", 0)),
            "taxes": to_decimal(taxes) if taxes is not None else None,
            "price_sale": to_decimal(price_sale) if price_sale is not None else None,
            "inventory_type": payload.get("inventory_type"),
            "code": payload.get("code"),
            "sku": payload.get("sku"),
            "description_html": payload.get("description"),
            "sub_description": payload.get("sub_description"),
            "cover_url": payload.get("cover_url"),
            "genders": payload.get("gender", []),
            "tags": payload.get("tags", []),
            "images": payload.get("images") or [],
            "colors": payload.get("colors", []),
            "sizes": payload.get("sizes", []),
            "ratings_buckets": [],
            "reviews": [],
            "total_ratings": Decimal("0"),
            "total_sold": 0,
            "total_reviews": 0,
            "new_label": {"enabled": True, "content": "NEW"},
        }
        item.update(_build_gsi_attrs(item))
        self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
        return item

    # READ
    def get_by_id(self, product_id: str, account_id: str) -> Optional[Dict[str, Any]]:
        """Get product by ID, validating it belongs to the account"""
        resp = self.table.get_item(Key={"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"})
        item = resp.get("Item")
        
        # Validate account ownership
        if item and item.get("account_id") != account_id:
            return None
            
        return item
    
    # LIST ALL
    def list_all(self, account_id: str) -> List[Dict[str, Any]]:
        """All products of the account, via the account GSI (falls back to a scan with a
        warning if the query raises, e.g. while the GSI is still being created)."""
        try:
            return self._query_account(account_id)
        except Exception:
            logger.warning(
                "Product account GSI query failed for %s; falling back to scan", self._account_gsi, exc_info=True
            )
            return self._scan_account(account_id)

    def _query_account(self, account_id: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        kwargs: Dict[str, Any] = {
            "IndexName": self._account_gsi,
            "KeyConditionExpression": Key("account_id").eq(account_id),
        }
        while True:
            resp = self.table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                return items
            kwargs["ExclusiveStartKey"] = last

    def _scan_account(self, account_id: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        kwargs: Dict[str, Any] = {
            "FilterExpression": Attr("sk").eq("PRODUCT") & Attr("account_id").eq(account_id)
        }
        while True:
            resp = self.table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                return items
            kwargs["ExclusiveStartKey"] = last

    # UPDATE (partial PUT: only the keys present in `payload`; a None value REMOVEs the attribute)
    UPDATE_MAPPING = {
        "name": "name", "category": "category", "price": "price", "publish": "publish",
        "available": "available", "quantity": "quantity", "taxes": "taxes",
        "price_sale": "price_sale", "inventory_type": "inventory_type", "code": "code",
        "sku": "sku", "description": "description_html", "sub_description": "sub_description",
        "cover_url": "cover_url", "gender": "genders", "tags": "tags", "images": "images",
        "colors": "colors", "sizes": "sizes",
    }
    NUMERIC_ATTRS = {"price", "price_sale", "taxes", "available", "quantity"}
    GSI_TRIGGERS = {"category", "price", "price_sale", "genders", "tags"}

    @classmethod
    def build_update(cls, current: Dict[str, Any], payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Pure: build UpdateExpression kwargs from a partial payload (None = remove)."""
        sets: Dict[str, Any] = {}
        removes: List[str] = []
        for k_in, v in payload.items():
            attr = cls.UPDATE_MAPPING.get(k_in)
            if not attr:
                continue
            if v is None:
                removes.append(attr)
                continue
            sets[attr] = to_decimal(v) if attr in cls.NUMERIC_ATTRS else v

        if cls.GSI_TRIGGERS & (set(sets) | set(removes)):
            merged = {**current, **sets}
            for r in removes:
                merged.pop(r, None)
            sets.update(_build_gsi_attrs(merged))

        if not sets and not removes:
            return None
        names: Dict[str, str] = {}
        values: Dict[str, Any] = {}
        set_parts = []
        for attr, v in sets.items():
            names[f"#_{attr}"] = attr
            values[f":{attr}"] = v
            set_parts.append(f"#_{attr} = :{attr}")
        remove_parts = []
        for attr in removes:
            names[f"#_{attr}"] = attr
            remove_parts.append(f"#_{attr}")
        expr = ""
        if set_parts:
            expr += "SET " + ", ".join(set_parts)
        if remove_parts:
            expr += " REMOVE " + ", ".join(remove_parts)
        return {
            "UpdateExpression": expr.strip(),
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
        }

    def update(self, product_id: str, account_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update product, validating it belongs to the account"""
        current = self.get_by_id(product_id, account_id)
        if not current:
            return None
        built = self.build_update(current, payload)
        if built is None:
            return current
        if not built["ExpressionAttributeValues"]:
            built.pop("ExpressionAttributeValues")
        built["ExpressionAttributeValues"] = {**built.get("ExpressionAttributeValues", {}), ":_acc": account_id}
        built["ExpressionAttributeNames"]["#_account_id"] = "account_id"
        try:
            resp = self.table.update_item(
                Key={"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"},
                ConditionExpression="attribute_exists(pk) AND #_account_id = :_acc",
                ReturnValues="ALL_NEW",
                **built,
            )
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            return None
        return resp.get("Attributes")

    def append_images(self, product_id: str, account_id: str, keys: List[str]) -> bool:
        """Atomically append image keys (DynamoDB list_append) without a read-modify-write."""
        try:
            self.table.update_item(
                Key={"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"},
                UpdateExpression="SET images = list_append(if_not_exists(images, :empty), :new)",
                ConditionExpression="attribute_exists(pk) AND account_id = :acc",
                ExpressionAttributeValues={":empty": [], ":new": list(keys), ":acc": account_id},
            )
            return True
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            return False

    # DELETE
    def delete(self, product_id: str, account_id: str) -> bool:
        """Delete product, validating it belongs to the account"""
        # Verify ownership before deleting
        current = self.get_by_id(product_id, account_id)
        if not current:
            return False
        self.table.delete_item(Key={"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"})
        return True
