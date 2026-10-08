import logging
import uuid
from decimal import Decimal
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from repositories.order_repo_ddb import OrderRepo, StatusConflictError, StockConflictError
from repositories.product_repo_ddb import ProductRepo
from repositories.s3_adapter import S3Adapter
from services.membership_service import MembershipService
from services.notification_orchestator import Notifications
from services.order_rules import (
    RESTOCK_STATUSES, ShopError, aggregate_quantities, can_transition, compute_totals, money,
)
from services.payment_request_service import PaymentRequestService
from services.product_rules import effective_price
from api.schemas.orders import Order, OrderCreate, OrderUpdate, OrderCheckUpdate
from api.schemas.payments import BulkPutPaymentRequest

logger = logging.getLogger(__name__)

PAYMENT_DUE_DAYS_DEFAULT = 7


ORDER_EVENT_TITLES = {
    "order_created": "Orden creada",
    "payment_created": "Pago creado",
    "payment_approval_pending": "Aprobando pago",
    "payment_paid": "Pago confirmado",
    "payment_overdue": "Pago vencido",
    "payment_canceled": "Pago cancelado",
    "payment_pending": "Pago pendiente",
    "order_status_changed": "Orden actualizada",
    "provider_check_on": "Pedido al proveedor confirmado",
    "provider_check_off": "Pedido al proveedor revertido",
    "delivery_check_on": "Entrega confirmada",
    "delivery_check_off": "Entrega revertida",
}


CHECK_FIELDS = {
    "provider": "provider_check",
    "delivery": "delivery_check",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event(event_type: str, title: Optional[str] = None, meta: Optional[dict] = None) -> dict:
    return {
        "type": event_type,
        "title": title or ORDER_EVENT_TITLES.get(event_type, event_type),
        "time": _now_iso(),
        **({"meta": meta} if meta else {}),
    }




class OrderService:
    def __init__(
        self,
        repo: OrderRepo,
        payment_request_svc: PaymentRequestService,
        notifier: Notifications,
        product_repo: ProductRepo,
        membership_svc: MembershipService,
        s3: S3Adapter,
    ):
        self.repo = repo
        self.payment_request_svc = payment_request_svc
        self.notifier = notifier
        self.product_repo = product_repo
        self.membership_svc = membership_svc
        self.s3 = s3

    # ---- reads (visibility: admin sees the account, everyone else only their own) ----

    def list_orders(
        self, account_id: str, workspace_id: Optional[str] = None,
        user_id: Optional[str] = None, is_admin: bool = True,
    ) -> List[Order]:
        items = self.repo.list_all(
            account_id, workspace_id=workspace_id, customer_id=None if is_admin else user_id
        )
        return [Order.model_validate(item) for item in items]

    def get_order(
        self, order_id: str, account_id: str, user_id: Optional[str] = None, is_admin: bool = True
    ) -> Optional[Order]:
        item = self.repo.get_by_id(order_id, account_id)
        if not item:
            return None
        if not is_admin and (item.get("customer") or {}).get("id") != user_id:
            return None
        return Order.model_validate(item)

    # ---- create ----

    def create_order(
        self, payload: OrderCreate, account_id: str, user_id: str, token_email: Optional[str] = None
    ) -> Order:
        if payload.workspace_id not in self.membership_svc.get_user_workspaces(user_id, account_id):
            raise ShopError("forbidden", "No tienes acceso a este workspace")

        lines = [l.model_dump() for l in payload.items]
        quantities = aggregate_quantities(lines)
        products = {}
        for pid in quantities:
            p = self.product_repo.get_by_id(pid, account_id)
            if not p or p.get("publish", "published") != "published":
                raise ShopError("not_found", f"Producto no disponible: {pid}")
            products[pid] = p
        for pid, qty in quantities.items():
            if qty > int(products[pid].get("available", 0)):
                raise ShopError("out_of_stock", f"Stock insuficiente para {products[pid]['name']}")

        order_items, priced = self._build_items(lines, products)
        totals = compute_totals(priced, payload.shipping, payload.discount)

        customer = payload.customer
        email = token_email or customer.email
        if not email:
            raise ShopError("invalid", "Se requiere un correo para la orden")
        now = _now_iso()
        order_id = str(uuid.uuid4())
        item = {
            "id": order_id,
            "account_id": account_id,
            "workspace_id": payload.workspace_id,
            "order_number": self.repo.new_order_number(),
            "created_at": now,
            "taxes": Decimal("0"),
            "items": order_items,
            "history": [{"type": "order_created", "title": ORDER_EVENT_TITLES["order_created"], "time": now}],
            **totals,
            "customer": {
                "id": user_id,
                "name": customer.name or email,
                "email": email,
                "phone_number": customer.phone_number or "",
                "avatar_url": customer.avatar_url,
            },
            "delivery": {
                "shipment_amount": totals["shipping"],
                "delivery_type": payload.delivery.delivery_type or "",
            },
            "total_quantity": sum(quantities.values()),
            "shipping_address": payload.shipping_address.model_dump(),
            "payment": payload.payment.model_dump(),
            "status": "pending",
            "payment_request_id": None,
            "provider_check": None,
            "delivery_check": None,
        }
        try:
            self.repo.create_with_stock(item, account_id, dict(quantities))
        except StockConflictError:
            raise ShopError("out_of_stock", "Stock insuficiente: otro pedido se llevo las ultimas unidades")

        try:
            payment_request_id = self._create_payment_request_for_order(item, account_id)
        except Exception:
            logger.exception("Payment request creation failed for order %s; cancelling", order_id)
            self._cancel_after_failure(item, account_id)
            raise ShopError("payment_failed", "No se pudo crear la solicitud de pago; la orden fue cancelada")
        if payment_request_id:
            self.repo.set_payment_request_id(order_id, account_id, payment_request_id)
            self.repo.append_event(
                order_id, account_id,
                build_event("payment_created", meta={"payment_request_id": payment_request_id}),
            )

        self._notify(
            lambda: self.notifier.order_created(
                email=email,
                user_name=item["customer"]["name"],
                order_number=item["order_number"],
                total_amount=float(item["total_amount"]),
            ),
            "order_created",
        )
        refreshed = self.repo.get_by_id(order_id, account_id) or item
        return Order.model_validate(refreshed)

    def _build_items(self, lines: list, products: dict):
        order_items, priced = [], []
        for line in lines:
            p = products[line["product_id"]]
            color, size = line.get("color"), line.get("size")
            if p.get("colors") and color not in p["colors"]:
                raise ShopError("invalid", f"Color invalido para {p['name']}")
            if p.get("sizes") and size not in p["sizes"]:
                raise ShopError("invalid", f"Talla invalida para {p['name']}")
            unit = effective_price(p["price"], p.get("price_sale"))
            cover = p.get("cover_url") or ((p.get("images") or [None])[0])
            order_items.append({
                "id": p["id"],
                "sku": p.get("sku") or p["id"],
                "quantity": line["quantity"],
                "name": p["name"],
                "cover_url": self.s3.get_s3_public_url(cover) if cover else "",
                "price": money(unit),
                "available": int(p.get("available", 0)),
                "colors": [color] if color else [],
                "size": size or "",
            })
            priced.append((unit, line["quantity"]))
        return order_items, priced

    def _notify(self, fn, label: str) -> None:
        try:
            fn()
        except Exception:
            logger.exception("Notification %s failed (order already written)", label)

    def _restock_map(self, order: dict, account_id: str) -> dict:
        qty = aggregate_quantities(
            {"product_id": i["id"], "quantity": i["quantity"]} for i in (order.get("items") or [])
        )
        alive = self.repo.existing_product_ids(account_id, list(qty))
        return {pid: q for pid, q in qty.items() if pid in alive}

    def _cancel_after_failure(self, order: dict, account_id: str) -> None:
        try:
            self._transition(order, account_id, "cancelled", reason="payment_request_failed")
        except Exception:
            logger.exception("CRITICAL: could not cancel/restock order %s after payment failure", order["id"])

    # ---- status ----

    def _transition(self, order: dict, account_id: str, new: str, reason: Optional[str] = None) -> None:
        old = order.get("status")
        if not can_transition(old, new):
            raise ShopError("conflict", f"Transicion invalida: {old} -> {new}")
        meta = {"from": old, "to": new}
        if reason:
            meta["reason"] = reason
        event = build_event("order_status_changed", title=f"Orden: {new}", meta=meta)
        restock = self._restock_map(order, account_id) if new in RESTOCK_STATUSES else None
        try:
            self.repo.transition_status(order["id"], account_id, old, new, event, restock)
        except StatusConflictError:
            raise ShopError("conflict", "La orden cambio de estado; recarga e intenta de nuevo")

    def update_order(self, order_id: str, account_id: str, payload: OrderUpdate) -> Optional[Order]:
        existing = self.repo.get_by_id(order_id, account_id)
        if not existing:
            return None

        if payload.status and payload.status != existing.get("status"):
            if not can_transition(existing.get("status"), payload.status):
                raise ShopError("conflict", f"Transicion invalida: {existing.get('status')} -> {payload.status}")
            if payload.status == "completed":
                provider = existing.get("provider_check") or {}
                delivery = existing.get("delivery_check") or {}
                if not (provider.get("checked") and delivery.get("checked")):
                    raise ShopError(
                        "invalid",
                        "Cannot mark order as completed: both provider order and delivery checks must be confirmed.",
                    )
        if payload.delivery is not None:
            self.repo.set_delivery(order_id, account_id, payload.delivery.model_dump())
        if payload.status and payload.status != existing.get("status"):
            self._transition(existing, account_id, payload.status)
            customer = existing.get("customer") or {}
            if customer.get("email"):
                self._notify(
                    lambda: self.notifier.order_status_changed(
                        email=customer["email"],
                        user_name=customer.get("name") or customer["email"],
                        order_number=existing.get("order_number", ""),
                        status=payload.status,
                    ),
                    "order_status_changed",
                )
        refreshed = self.repo.get_by_id(order_id, account_id)
        return Order.model_validate(refreshed) if refreshed else None

    def delete_order(self, order_id: str, account_id: str) -> bool:
        """Soft delete: cancel the order, restore stock and cancel the linked payment request."""
        existing = self.repo.get_by_id(order_id, account_id)
        if not existing:
            return False
        if existing.get("status") == "cancelled":
            return True
        self._transition(existing, account_id, "cancelled", reason="deleted")
        pr_id = existing.get("payment_request_id")
        if pr_id:
            try:
                self.payment_request_svc.cancel_for_order(pr_id, account_id)
            except Exception:
                logger.exception("Order %s cancelled but its payment request %s was not", order_id, pr_id)
        return True

    def set_provider_check(
        self, order_id: str, account_id: str, user_id: str, payload: OrderCheckUpdate
    ) -> Optional[Order]:
        return self._set_check(order_id, account_id, user_id, "provider", payload)

    def set_delivery_check(
        self, order_id: str, account_id: str, user_id: str, payload: OrderCheckUpdate
    ) -> Optional[Order]:
        return self._set_check(order_id, account_id, user_id, "delivery", payload)

    def _set_check(
        self,
        order_id: str,
        account_id: str,
        user_id: str,
        kind: str,
        payload: OrderCheckUpdate,
    ) -> Optional[Order]:
        field = CHECK_FIELDS[kind]
        existing = self.repo.get_by_id(order_id, account_id)
        if not existing:
            return None

        previous = (existing.get(field) or {}).get("checked", False)
        if previous == payload.checked:
            # Idempotent: no state change, no event, return current state.
            return Order.model_validate(existing)

        check_value = {
            "checked": payload.checked,
            "checked_at": _now_iso(),
            "checked_by": user_id,
            "note": payload.note,
        }
        self.repo.set_check(order_id, account_id, field, check_value)

        event_type = f"{kind}_check_{'on' if payload.checked else 'off'}"
        meta = {"by": user_id}
        if payload.note:
            meta["note"] = payload.note
        self.repo.append_event(order_id, account_id, build_event(event_type, meta=meta))

        refreshed = self.repo.get_by_id(order_id, account_id) or existing
        return Order.model_validate(refreshed)

    def _create_payment_request_for_order(self, order_item: dict, account_id: str) -> Optional[str]:
        customer = order_item.get("customer") or {}
        if not customer.get("email") or not customer.get("id"):
            return None
        workspace_id = order_item.get("workspace_id") or order_item.get("workspaceId")
        if not workspace_id:
            return None

        order_number = order_item.get("order_number") or order_item.get("orderNumber", "")
        total_amount = Decimal(str(order_item.get("total_amount") or 0))
        create_date = order_item.get("created_at") or order_item.get("createdAt") or _now_iso()
        try:
            due_dt = datetime.fromisoformat(create_date) + timedelta(days=PAYMENT_DUE_DAYS_DEFAULT)
        except ValueError:
            due_dt = datetime.now(timezone.utc) + timedelta(days=PAYMENT_DUE_DAYS_DEFAULT)
        due_date = due_dt.isoformat()

        description_items = ", ".join(
            f"{i.get('name', '')} x{i.get('quantity', 1)}" for i in (order_item.get("items") or [])
        )

        bulk = BulkPutPaymentRequest(
            createDate=create_date,
            dueDate=due_date,
            concept=f"Orden {order_number}",
            description=description_items or f"Orden {order_number}",
            category="order",
            group=workspace_id,
            paymentRequestTo=[{
                "id": customer["id"],
                "name": customer.get("name") or customer["email"],
                "email": customer["email"],
            }],
            userPrice=total_amount,
            orderId=order_item["id"],
        )
        created = self.payment_request_svc.bulk_create(bulk, account_id)
        if not created:
            raise RuntimeError("payment request was not created")
        return created[0].get("id")
