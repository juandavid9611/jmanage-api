from typing import List, Optional
from datetime import datetime, timedelta, timezone
from repositories.order_repo_ddb import OrderRepo
from services.notification_orchestator import Notifications
from services.payment_request_service import PaymentRequestService
from api.schemas.orders import Order, OrderCreate, OrderUpdate
from api.schemas.payments import BulkPutPaymentRequest


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
    ):
        self.repo = repo
        self.payment_request_svc = payment_request_svc
        self.notifier = notifier

    def list_orders(self, account_id: str, workspace_id: Optional[str] = None) -> List[Order]:
        items = self.repo.list_all(account_id, workspace_id=workspace_id)
        return [Order.model_validate(item) for item in items]

    def get_order(self, order_id: str, account_id: str) -> Optional[Order]:
        item = self.repo.get_by_id(order_id, account_id)
        if item:
            return Order.model_validate(item)
        return None

    def create_order(self, payload: OrderCreate, account_id: str) -> Order:
        data = payload.model_dump()
        item = self.repo.create(data, account_id)

        payment_request_id = self._create_payment_request_for_order(item, account_id)
        if payment_request_id:
            self.repo.set_payment_request_id(item["id"], account_id, payment_request_id)
            self.repo.append_event(
                item["id"],
                account_id,
                build_event("payment_created", meta={"payment_request_id": payment_request_id}),
            )

        customer = item.get("customer") or {}
        if customer.get("email"):
            self.notifier.order_created(
                email=customer["email"],
                user_name=customer.get("name") or customer["email"],
                order_number=item.get("order_number") or item.get("orderNumber", ""),
                total_amount=float(item.get("total_amount") or item.get("totalAmount", 0)),
            )
        # Re-read to return the latest state (with the appended events).
        refreshed = self.repo.get_by_id(item["id"], account_id) or item
        return Order.model_validate(refreshed)

    def update_order(self, order_id: str, account_id: str, payload: OrderUpdate) -> Optional[Order]:
        existing = self.repo.get_by_id(order_id, account_id)
        if not existing:
            return None
        data = payload.model_dump(exclude_unset=True)
        item = self.repo.update(order_id, account_id, data)
        if not item:
            return None

        old_status = existing.get("status")
        new_status = item.get("status")
        if payload.status and old_status != new_status:
            self.repo.append_event(
                order_id,
                account_id,
                build_event(
                    "order_status_changed",
                    title=f"Orden: {new_status}",
                    meta={"from": old_status, "to": new_status},
                ),
            )
            customer = item.get("customer") or {}
            if customer.get("email"):
                self.notifier.order_status_changed(
                    email=customer["email"],
                    user_name=customer.get("name") or customer["email"],
                    order_number=item.get("order_number") or item.get("orderNumber", ""),
                    status=new_status,
                )
        refreshed = self.repo.get_by_id(order_id, account_id) or item
        return Order.model_validate(refreshed)

    def delete_order(self, order_id: str, account_id: str) -> bool:
        return self.repo.delete(order_id, account_id)

    def _create_payment_request_for_order(self, order_item: dict, account_id: str) -> Optional[str]:
        customer = order_item.get("customer") or {}
        if not customer.get("email") or not customer.get("id"):
            return None
        workspace_id = order_item.get("workspace_id") or order_item.get("workspaceId")
        if not workspace_id:
            return None

        order_number = order_item.get("order_number") or order_item.get("orderNumber", "")
        total_amount = int(float(order_item.get("total_amount") or order_item.get("totalAmount", 0)))
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
        try:
            created = self.payment_request_svc.bulk_create(bulk, account_id)
            if created:
                return created[0].get("id")
        except Exception as e:
            print(f"Failed to create payment request for order {order_item.get('id')}: {e}")
        return None
