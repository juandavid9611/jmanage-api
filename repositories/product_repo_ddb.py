from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from boto3.dynamodb.conditions import Key, Attr
import uuid
from datetime import datetime, timezone

from repositories.ddb_session import product_table

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

def _encode_pagination_key(key: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return key or None

def _neg(n: int | float | Decimal | None) -> Decimal:
    return -to_decimal(n or 0)

def _tokens_from_name(name: str) -> List[str]:
    # tokens simples para GSI5 (puedes mejorarlo con normalización)
    return [name.lower()] if name else []

def _build_gsi_attrs(item: Dict[str, Any]) -> Dict[str, Any]:
    # GSI1: account + category by created_at
    account_id = item.get("account_id") or "_NO_ACCOUNT"
    category = item.get("category") or "_UNCAT"
    created_at = item.get("created_at") or _now_iso()
    # GSI2: account + featured by total_sold desc -> usamos neg_total_sold
    neg_total_sold = _neg(item.get("total_sold", 0))
    # GSI3: account + price
    price = to_decimal(item.get("price", 0))
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

def _choose_query_plan(account_id: str, query: Optional[str], filters: Dict[str, Any], sort_by: Optional[str]) -> Dict[str, Any]:
    category = filters.get("category")
    min_price = filters.get("min_price")
    max_price = filters.get("max_price")

    if sort_by == "featured":
        return {
            "index": "GSI2_Featured",
            "key": Key("gsi2_pk").eq(f"ACCOUNT#{account_id}#FEATURED"),
            "range": None,
            "forward": False,
        }

    if sort_by == "newest":
        if category:
            return {
                "index": "GSI1_CategoryNewest",
                "key": Key("gsi1_pk").eq(f"ACCOUNT#{account_id}#CAT#{category}"),
                "range": None,
                "forward": False,
            }
        return {
            "index": "GSI1_CategoryNewest",
            "key": Key("gsi1_pk").eq(f"ACCOUNT#{account_id}#CAT#_ALL"),
            "range": None,
            "forward": False,
        }

    if sort_by in ("priceAsc", "priceDesc"):
        base: Dict[str, Any] = {
            "index": "GSI3_Price",
            "key": Key("gsi3_pk").eq(f"ACCOUNT#{account_id}#PRICE"),
            "range": None,
        }
        if min_price is not None and max_price is not None:
            base["range"] = Key("gsi3_sk").between(
                to_decimal(min_price),
                to_decimal(max_price),
            )
        elif min_price is not None:
            base["range"] = Key("gsi3_sk").gte(to_decimal(min_price))
        elif max_price is not None:
            base["range"] = Key("gsi3_sk").lte(to_decimal(max_price))

        return {**base, "forward": (sort_by == "priceAsc")}

    if query:
        return {
            "index": "GSI5_TagSearch",
            "key": Key("gsi5_pk").eq(f"ACCOUNT#{account_id}#TAG#{query.lower()}"),
            "range": None,
            "forward": False,
        }

    if category:
        return {
            "index": "GSI1_CategoryNewest",
            "key": Key("gsi1_pk").eq(f"ACCOUNT#{account_id}#CAT#{category}"),
            "range": None,
            "forward": False,
        }

    return {
        "index": "GSI2_Featured",
        "key": Key("gsi2_pk").eq(f"ACCOUNT#{account_id}#FEATURED"),
        "range": None,
        "forward": False,
    }


def _map_out(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": item["id"],
        "gender": item.get("genders", []),
        "images": item.get("images", []),
        "reviews": item.get("reviews", []),
        "publish": item.get("publish", "published"),
        "ratings": item.get("ratings_buckets", []),
        "category": item["category"],
        "available": int(item.get("available", 0)),
        "price_sale": item.get("price_sale"),
        "taxes": item.get("taxes"),
        "quantity": int(item.get("quantity", 0)),
        "inventory_type": item.get("inventory_type"),
        "tags": item.get("tags", []),
        "code": item.get("code"),
        "description": item.get("description_html"),
        "sku": item.get("sku"),
        "created_at": item.get("created_at"),
        "name": item["name"],
        "price": float(item["price"]),
        "cover_url": item.get("cover_url"),
        "colors": item.get("colors", []),
        "total_ratings": float(item.get("total_ratings", 0.0)),
        "total_sold": int(item.get("total_sold", 0)),
        "total_reviews": int(item.get("total_reviews", 0)),
        "new_label": item.get("new_label", {"enabled": True, "content": "NEW"}),
        "sale_label": {"enabled": bool(item.get("price_sale") is not None), "content": "SALE"},
        "sizes": item.get("sizes", []),
        "sub_description": item.get("sub_description"),
    }

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
            "available": int(payload.get("available", 0)),
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
            "images": payload.get("images", []),
            "colors": payload.get("colors", []),
            "sizes": payload.get("sizes", []),
            "ratings_buckets": [rb.model_dump(by_alias=False) if hasattr(rb, "model_dump") else rb for rb in payload.get("ratings", [])],
            "reviews": [rv.model_dump(by_alias=False) if hasattr(rv, "model_dump") else rv for rv in payload.get("reviews", [])],
            "total_ratings": to_decimal(payload.get("total_ratings", 0.0)),
            "total_sold": int(payload.get("total_sold", 0)),
            "total_reviews": int(payload.get("total_reviews", 0)),
            "new_label": payload.get("new_label", {"enabled": True, "content": "NEW"}),
        }
        item.update(_build_gsi_attrs(item))
        self.table.put_item(Item=item)
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
        """List all products for the specified account"""
        items = []
        exclusive_key = None
        while True:
            kwargs = {
                "FilterExpression": Attr("sk").eq("PRODUCT") & Attr("account_id").eq(account_id)
            }
            if exclusive_key:
                kwargs["ExclusiveStartKey"] = exclusive_key
            resp = self.table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            exclusive_key = resp.get("LastEvaluatedKey")
            if not exclusive_key:
                break
        return items

    # SEARCH/LIST
    def search(
        self,
        account_id: str,
        query: Optional[str],
        filters: Dict[str, Any],
        sort_by: Optional[str],
        limit: int,
        next_token: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Search products within the specified account"""
        plan = _choose_query_plan(account_id, query, filters, sort_by)
        key_condition = plan["key"]
        if plan.get("range") is not None:
            key_condition = key_condition & plan["range"]

        kwargs = dict(
            IndexName=plan["index"],
            KeyConditionExpression=key_condition,
            ScanIndexForward=plan.get("forward", False),
            Limit=limit,
        )

        fe = None
        if filters.get("genders"):
            f = Attr("genders").contains(filters["genders"][0])
            for g in filters["genders"][1:]:
                f = f | Attr("genders").contains(g)
            fe = f if fe is None else fe & f

        if filters.get("colors"):
            f = Attr("colors").contains(filters["colors"][0])
            for c in filters["colors"][1:]:
                f = f | Attr("colors").contains(c)
            fe = f if fe is None else fe & f

        if (mr := filters.get("min_rating")) is not None:
            f = Attr("total_ratings").gte(to_decimal(mr))
            fe = f if fe is None else fe & f

        if fe is not None:
            kwargs["FilterExpression"] = fe
        if next_token:
            kwargs["ExclusiveStartKey"] = next_token

        resp = self.table.query(**kwargs)
        return resp.get("Items", []), resp.get("LastEvaluatedKey")

    # UPDATE (PUT parcial: solo campos presentes)
    def update(self, product_id: str, account_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update product, validating it belongs to the account"""
        if not payload:
            return self.get_by_id(product_id, account_id)

        # Verify ownership
        current = self.get_by_id(product_id, account_id)
        if not current:
            return None

        # Map camel->snake atributos internos
        mapping = {
            "name": "name", "category": "category", "price": "price", "publish": "publish",
            "available": "available", "quantity": "quantity", "taxes": "taxes",
            "price_sale": "price_sale", "inventory_type": "inventory_type", "code": "code",
            "sku": "sku", "description": "description_html", "sub_description": "sub_description",
            "cover_url": "cover_url", "gender": "genders", "tags": "tags", "images": "images",
            "colors": "colors", "sizes": "sizes", "ratings": "ratings_buckets", "reviews": "reviews",
            "total_ratings": "total_ratings", "total_sold": "total_sold", "total_reviews": "total_reviews",
        }

        numeric_attrs = {
            "price", "price_sale", "taxes", "available", "quantity", "total_ratings", "total_sold", 
            "total_reviews", "gsi2_sk", "gsi3_sk", "neg_total_sold",
        }

        expr_set = []
        names = {}
        values = {}

        for k_in, v in payload.items():
            if v is None:
                continue
            if k_in in ("ratings", "reviews") and hasattr(v, "model_dump"):
                v = v.model_dump(by_alias=False)

            attr = mapping.get(k_in)
            if not attr:
                continue

            # Si el atributo es numérico, lo convertimos a Decimal
            if attr in numeric_attrs:
                v = to_decimal(v)

            names[f"#_{attr}"] = attr
            values[f":{attr}"] = v
            expr_set.append(f"#_{attr} = :{attr}")

        # recomputar GSIs si cambian category/price/total_sold/tags/genders
        if any(n in names for n in ["#_category", "#_price", "#_total_sold", "#_genders", "#_tags"]):
            # Mezclamos el estado actual con los nuevos valores
            merged: Dict[str, Any] = {
                **current,
                **{n.replace("#_", ""): values[f":{n.replace('#_', '')}"] for n in names},
            }

            gsi_attrs = _build_gsi_attrs(merged)  # gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk, gsi3_pk, gsi3_sk, ...

            for gk, gv in gsi_attrs.items():
                # Forzamos Decimal en atributos numéricos del GSI
                if gk in numeric_attrs:
                    gv = to_decimal(gv)

                names[f"#_{gk}"] = gk
                values[f":{gk}"] = gv
                expr_set.append(f"#_{gk} = :{gk}")

        if not expr_set:
            return self.get_by_id(product_id, account_id)

        update_expr = "SET " + ", ".join(expr_set)

        resp = self.table.update_item(
            Key={"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")

    # DELETE
    def delete(self, product_id: str, account_id: str) -> bool:
        """Delete product, validating it belongs to the account"""
        # Verify ownership before deleting
        current = self.get_by_id(product_id, account_id)
        if not current:
            return False
        self.table.delete_item(Key={"pk": f"PRODUCT#{product_id}", "sk": "PRODUCT"})
        return True
