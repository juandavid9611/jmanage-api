from time import time
from uuid import uuid4
from typing import Any
from datetime import datetime
from api.schemas.files import FileSpec
from repositories.s3_adapter import S3Adapter
from services.notification_orchestator import Notifications
from repositories.payment_requests_repo_ddb import PaymentRequestsRepo
from api.schemas.payments import PaymentRequestStatus, BulkPutPaymentRequest


class PaymentRequestService:
    def __init__(self, repo: PaymentRequestsRepo, s3: S3Adapter, notifier: Notifications):
        self.repo = repo
        self.notifier = notifier
        self.s3 = s3
        #TODO Important! Fix mapping userPrice, user_price, totalAmount
        #TODO Switch to excluded and custom mapping like in TourService
        self._updateable_fields = {
            "createDate", "dueDate", "concept", "description", "category",
            "userGroup", "userPrice", "overduePrice", "status"
        }
        self._relevant_notification_fields = {
            "dueDate", "totalAmount", "concept", "status"
        }
        self._payments_username = "Vittoria CD Pagos"

    def get(self, payment_request_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(payment_request_id, account_id)
        if item:
            return self._map_payment_request(item)
        return None

    def list_payment_requests(self, account_id: str, *, user_id: str | None = None, group: str | None = None) -> list[dict[str, Any]]:
        if user_id:
            items = self.repo.list_by_user(user_id, account_id)
        elif group:
            items = self.repo.list_by_group(group, account_id)
        else:
            items = self.repo.list_all(account_id)
        return [self._map_payment_request(i, get_presigned_url=False) for i in items]

    def bulk_create(self, bulk_item: BulkPutPaymentRequest, account_id: str) -> list[dict[str, Any]]:
        if bulk_item is None or not hasattr(bulk_item, "paymentRequestTo") or not bulk_item.paymentRequestTo:
            raise ValueError("No users provided for payment request creation.")

        new_payment_requests = []
        created_time = int(time())
        for user in bulk_item.paymentRequestTo:
            #TODO User URL is create with GET presigned url, should be populated later with a GET user with other attributes
            new_payment_request = self._get_new_payment_request(bulk_item, user, created_time, account_id)
            self.repo.put(new_payment_request)
            self.notifier.payment_created(
                email=user["email"],
                user_name=user["name"],
                concept=bulk_item.concept,
                amount=bulk_item.userPrice,
                due_date=bulk_item.dueDate
            )
            new_payment_requests.append(self._map_payment_request(new_payment_request, get_presigned_url=False))
        return new_payment_requests

    def update(self, payment_request_id: str, account_id: str, item: BulkPutPaymentRequest) -> dict[str, Any] | None:
        existing = self.get(payment_request_id, account_id)
        if not existing:
            return None
        updates = self._get_needed_updates(item)
        if not updates:
            return existing
        self.repo.update(payment_request_id, account_id, updates)
        user = item.paymentRequestTo[0]
        new_item = self.get(payment_request_id, account_id)
        if not new_item:
            raise ValueError(f"Payment Request {payment_request_id} not found after update.")
        notification_fields_changed = self._get_notification_fields_changed(existing, new_item)
        if notification_fields_changed:
            self.notifier.payment_updated(
                email=user["email"], 
                user_name=user["name"], 
                concept=new_item['concept'], 
                changes=notification_fields_changed
            )
        return new_item

    def delete(self, payment_request_id: str, account_id: str) -> None:
        self.repo.delete(payment_request_id, account_id)

    def generate_put_presigned_urls(self, payment_request_id: str, account_id: str, files: list[FileSpec]) -> dict[str, dict[str, str]]:
        presigned_urls = {}
        payment_request = self.get(payment_request_id, account_id)
        if not payment_request:
            raise ValueError(f"Payment Request {payment_request_id} not found")
        user_id = payment_request["userId"]
        for file in files:
            if not isinstance(file, FileSpec):
                raise TypeError("Each file must be an instance of FileSpec.")

            file_name = file.file_name
            file_content_type = file.content_type
            if not file_name or not file_content_type:
                raise ValueError("File 'file_name' and 'content_type' cannot be empty.")

            result = self.s3.presign_invoice_put(
                account_id=account_id,
                user_id=user_id, 
                payment_request_id=payment_request_id, 
                filename=file_name, 
                content_type=file_content_type
            )
            presigned_urls[file_name] = result["url"]
        return presigned_urls

    def request_payment_request_approval(self, payment_request_id: str, account_id: str, file_names: list[str]) -> str:
        existing = self.get(payment_request_id, account_id)
        if not existing:
            raise ValueError(f"Payment Request {payment_request_id} not found")
        images = []
        for file_name in file_names:
            key = self.s3._kb.invoice_file(account_id, existing["userId"], payment_request_id, file_name)
            images.append(key)
        self.repo.update(
            payment_request_id, 
            account_id,
            {
                "payment_status": PaymentRequestStatus.APPROVAL_PENDING,
                "images": images
            }
        )
        
        new_item = self.get(payment_request_id, account_id)
        if not new_item:
            raise ValueError(f"Payment Request {payment_request_id} not found after update.")
        user = existing["paymentRequestTo"]
        notification_fields_changed = self._get_notification_fields_changed(existing, new_item)

        if notification_fields_changed:
            self.notifier.payment_updated(
                email=user["email"], 
                user_name=user["name"], 
                concept=new_item['concept'], 
                changes=notification_fields_changed,
                notify_admins=True
            )
        return payment_request_id

    def process_overdue_payments(self) -> list[dict[str, Any]]:
        accounts = ['vittoriacd']
        overdue_payments = []
        for account_id in accounts:
            pending_items = self.repo.list_by_status(PaymentRequestStatus.PENDING, account_id)
            datetime_now = datetime.now()
            account_overdue_payments = []
            for item in pending_items:
                due_datetime = datetime.fromisoformat(item["due_date"])
                due_datetime = due_datetime.replace(tzinfo=None)
                if due_datetime < datetime_now:
                    overdue_price = item.get("overdue_price", 0)
                    new_price = item["user_price"] if overdue_price == 0 else overdue_price
                    self.repo.update(
                        item["id"],
                        {
                            "payment_status": PaymentRequestStatus.OVERDUE,
                            "user_price": new_price
                        }
                    )
                    account_overdue_payments.append({
                        "id": item["id"], 
                        "concept": item["concept"],
                        "user_price": new_price,
                        "to_name": item["payment_request_to"]["name"],
                        "to_email": item["payment_request_to"]["email"]
                    })
            overdue_payments.extend(account_overdue_payments)
            self.notifier.overdue_payments_processed(
                account_id=account_id,
                user_name=self._payments_username,
                pending_count=len(list(pending_items)),
                overdue_payments=account_overdue_payments,
            )
        return overdue_payments

    def _map_payment_request(self, item: dict[str, Any], get_presigned_url=True) -> dict[str, Any]:
        item["createDate"] = item.pop("create_date", None)
        item["dueDate"] = item.pop("due_date", None)
        item["status"] = item.pop("payment_status", None)
        item["paymentRequestTo"] = item.pop("payment_request_to", None)
        item["totalAmount"] = item.pop("user_price", None)
        item["overduePrice"] = item.pop("overdue_price", None)
        item["group"] = item.pop("user_group", None)
        item["userId"] = item.pop("user_id", None)
        item["createdTime"] = item.pop("created_time", None)
        if get_presigned_url:
            item["images"] = [self.s3.presign_get_from_explicit_key(key=image) for image in item.get("images", [])]
        return item

    def _get_needed_updates(self, item: BulkPutPaymentRequest) -> dict[str, Any]:
        updates = {}
        for field in self._updateable_fields:
            value = getattr(item, field, None)
            if value is not None:
                updates[self._mapped_field_name(field)] = value
        return updates
    
    def _mapped_field_name(self, field: str) -> str:
        mapping = {
            "createDate": "create_date",
            "dueDate": "due_date",
            "concept": "concept",
            "description": "description",
            "category": "category",
            "userGroup": "user_group",
            "userPrice": "user_price",
            "overduePrice": "overdue_price",
            "status": "payment_status"
        }
        return mapping.get(field, field)

    def _get_notification_fields_changed(self, old_payment_request, new_payment_request):
        fields_changed = []
        for field in self._relevant_notification_fields:
            if old_payment_request[field] != new_payment_request[field]:
                fields_changed.append({"name": field, "old_value": old_payment_request[field], "new_value": new_payment_request[field]})
        return fields_changed

    def _get_new_payment_request(self, bulk_item: BulkPutPaymentRequest, user: dict[str, Any], created_time: int, account_id: str) -> dict[str, Any]:
        # TODO Clean user dict from unneeded attributes
        user.pop("user_metrics", None)
        return {
            "id": f"{uuid4().hex}",
            "account_id": account_id,
            "create_date": bulk_item.createDate,
            "due_date": bulk_item.dueDate,
            "concept": bulk_item.concept,
            "description": bulk_item.description,
            "category": bulk_item.category,
            "payment_request_to": user,
            "user_id": user["id"],
            "user_group": bulk_item.group,
            "user_price": bulk_item.userPrice,
            "overdue_price": bulk_item.overduePrice,
            "payment_status": PaymentRequestStatus.PENDING,
            "created_time": created_time,
        }