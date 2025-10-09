import locale
from typing import Dict, Union
from typing import Dict, Optional, Mapping, Any
from api.schemas.calendar import PutCalendarEvent
from repositories.notifications.ports import EmailSender
from repositories.notifications.ports import InAppSender
from utils.datetime_utils import format_datetime_pretty_es, parse_timestamp_to_datetime, try_parsing_date


class Notifications:
    """
    Domain-friendly facade. Keeps template IDs and payload shapes in one place.
    Other services call these methods instead of talking Courier directly.
    """

    # Centralize template IDs here (override via DI/env if you prefer):
    COURIER_TEMPLATE_PAYMENT_CREATED = "AW5D9440CF4MZAH1CHWVA2D0DP4D"
    COURIER_TEMPLATE_PAYMENT_UPDATED = "B12CHAN5364VKVJMHZATM74CD4DE"
    COURIER_TEMPLATE_PAYMENT_OVERDUE = "40AZX1PQRGM3D0QD0R3AZG1P6AAE"
    COURIER_TEMPLATE_USER_WELCOME = "H9MDTT27FTMKH7K3HCM1M4MDR23T"
    COURIER_TEMPLATE_CHRISTMAS_GREETING = "070Z3SZX8V4YAXMTAEWKCXW2NVEV"

    def __init__(self, email_sender: EmailSender, in_app_sender: InAppSender) -> None:
        self._email_sender = email_sender
        self._in_app_sender = in_app_sender
        self._admin_emails = ["loga9822@hotmail.com", "clubdeportivovittoria+pagos@gmail.com"]
        # locale.setlocale(locale.LC_ALL, 'es_CO.UTF-8')  # Set to Colombian Spanish locale

    # Generic escape hatch (rarely needed if you stick to domain methods):
    def _send_email(
        self,
        *,
        template_id: str,
        to_email: str,
        data: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return self._email_sender.send_template(
            template_id=template_id,
            to_email=to_email,
            data=data,
        )
    
    def _send_in_app_notification(
        self,
        *,
        user_email: str,
        title: str,
        content: str,
        category: Optional[str] = None,
        action_url_path: str = "dashboard/"
    ) -> str:
        return self._in_app_sender.publish(
            user_email=user_email,
            title=title,
            content=content,
            category=category,
            action_url_path=action_url_path
        )
    
    def _send_bulk_in_app_notification(
        self,
        *,
        user_emails: list[str],
        title: str,
        content: str,
        category: Optional[str] = None,
        action_url_path: str = "dashboard/"
    ) -> str:

        return self._in_app_sender.publish_bulk(
            user_ids=user_emails,
            title=title,
            content=content,
            category=category,
            action_url_path=action_url_path
        )

    # --- Domain methods ---

    def send_user_welcome(self, *, email: str, name: str) -> str:
        return self._send_email(
            template_id=self.COURIER_TEMPLATE_USER_WELCOME,
            to_email=email,
            data={"userName": name} ,
        )

    def send_christmas_greeting(self, *, email: str, name: str) -> str:
        return self._send_email(
            template_id=self.COURIER_TEMPLATE_CHRISTMAS_GREETING,
            to_email=email,
            data={"userName": name} ,
        )

    def payment_created(
        self,
        *,
        email: str,
        user_name: str,
        concept: str,
        amount: int,
        due_date: str
    ) -> Dict[str, Union[str, Exception]]:
        results: Dict[str, Union[str, Exception]] = {}
        try:
            results["email"] = self._send_email(
                template_id=self.COURIER_TEMPLATE_PAYMENT_CREATED,
                to_email=email,
                data={
                    "userName": user_name,
                    "concept": concept,
                    "value": amount,
                    "dueDate": due_date,
                }
            )
        except Exception as e:
            results["email"] = e
        try:
            # TODO: locale is not working ValueError: Currency formatting is not possible using the 'C' locale.
            results["in_app"] = self._send_in_app_notification(
                user_email=email,
                title="Nuevo requerimiento de pago",
                content=f"Se ha creado un nuevo requerimiento de pago por {locale.currency(amount, grouping=True)} con concepto '{concept}' y fecha de vencimiento {due_date}.",
                category="payment",
                action_url_path="dashboard/invoice/user-list"
            )
        except Exception as e:
            results["in_app"] = e
        return results

    def payment_updated(
        self,
        *,
        email: str,
        user_name: str,
        concept: str,
        changes: list[dict[str, Any]], # TODO define a proper type for this,
        notify_admins: bool = False
    ) -> Dict[str, Union[str, Exception]]:
        results: Dict[str, Union[str, Exception]] = {}
        try:
            data = {
                "userName": user_name,
                "concept": concept,
                "changes": [self._get_formatted_notification_field(field) for field in changes],
            }
            results["email"] = self._send_email(
                template_id=self.COURIER_TEMPLATE_PAYMENT_UPDATED,
                to_email=email,
                data=data,
            )
            if notify_admins:
                for admin_email in self._admin_emails:
                    self._send_email(
                        template_id=self.COURIER_TEMPLATE_PAYMENT_UPDATED,
                        to_email=admin_email,
                        data=data,
                    )
        except Exception as e:
            results["email"] = e
        try:
            results["in_app"] = self._send_in_app_notification(
                user_email=email,
                title="Actualización en cobro",
                content=f"Se ha actualizado el cobro: '{concept}'.",
                category="payment_request_updated",
                action_url_path="dashboard/invoice/user-list"
            )
        except Exception as e:
            results["in_app"] = e
        return results

    def overdue_payments_processed(
        self,
        *,
        user_name: str,
        overdue_payments: list[dict[str, Any]],
    ) -> Dict[str, Union[str, Exception]]:
        results: Dict[str, Union[str, Exception]] = {}
        for payment in overdue_payments:
            email = payment["payment_request_to"]["email"]
            name = payment["payment_request_to"]["name"]
            try:
                results[f"in_app_{email}"] = self._send_in_app_notification(
                    user_email=email,
                    title="Recordatorio de pago vencido",
                    content=f"Tienes un pago vencido: '{payment['concept']}",
                    category="payment_overdue",
                    action_url_path="dashboard/invoice/user-list"
                )
            except Exception as e:
                results[f"in_app_{email}"] = e
            try:
                results[f"email_{email}"] = self._send_email(
                    template_id=self.COURIER_TEMPLATE_PAYMENT_OVERDUE,
                    to_email=email,
                    data={
                        "userName": name,
                        "overduePaymentRequests": [payment],
                    },
                )
            except Exception as e:
                results[f"email_{email}"] = e
        for email in self._admin_emails:
            try:
                self._send_email(
                    template_id=self.COURIER_TEMPLATE_PAYMENT_OVERDUE,
                    to_email=email,
                    data={
                        "userName": user_name,
                        "overduePaymentRequests": overdue_payments,
                    },
                )
            except Exception as e:
                results[email] = e
        return results

    def calendar_event_created(self, *, user_emails: list[str], calendar_event: PutCalendarEvent) -> str:
        title = "Nuevo evento: " + calendar_event.title
        event_datetime = parse_timestamp_to_datetime(calendar_event.start)
        event_day = format_datetime_pretty_es(event_datetime)
        content = f"Inscribete ya! {event_day} en {calendar_event.location}. {calendar_event.description}"
        category = "calendar_event_created"

        return self._send_bulk_in_app_notification(
            user_emails=user_emails,
            title=title,
            content=content,
            category=category,
            action_url_path="dashboard/calendar"
        )
    
    def _get_formatted_notification_field(self, field):
        if field["name"] == "due_date":
            old_value = try_parsing_date(field["old_value"])
            old_value = old_value.strftime("%d/%m/%Y")
            new_value = try_parsing_date(field["new_value"])
            new_value = new_value.strftime("%d/%m/%Y")
            return {"name": "Fecha de vencimiento", "old_value": old_value, "new_value": new_value}
        if field["name"] == "user_price":
            return {"name": "Valor", "old_value": locale.currency(field["old_value"], grouping=True), "new_value": locale.currency(field["new_value"], grouping=True)}
        if field["name"] == "concept":
            return {"name": "Concepto", "old_value": field["old_value"], "new_value": field["new_value"]}
        if field["name"] == "payment_status":
            english_to_spanish_status = {
                "paid": "Pagado",
                "pending": "Pendiente",
                "overdue": "Vencido",
                "canceled": "Cancelado",
                "approval_pending": "Pendiente de aprobación"
            }
            return {"name": "Estado", "old_value": english_to_spanish_status[field["old_value"]], "new_value": english_to_spanish_status[field["new_value"]]}
        
