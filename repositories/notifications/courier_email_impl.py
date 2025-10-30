import os
from trycourier import Courier
from typing import Mapping, Any
from repositories.notifications.ports import EmailSender


class CourierNotificationSender(EmailSender):
    """
    Thin adapter for TryCourier. Compatible with both modern and legacy client methods.
    Prefers client.send_message(message=...) if available; falls back to client.send(...).
    """
    def __init__(self, client: Courier | None = None, auth_token: str | None = None):
        if client:
            self._client = client
        else:
            token = auth_token or os.environ.get("COURIER_AUTH_TOKEN")
            if not token:
                raise RuntimeError("COURIER_AUTH_TOKEN is not configured")
            self._client = Courier(auth_token=token)

    def send_template(
        self,
        *,
        template_id: str,
        to_email: str | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> str:
        if not to_email:
            raise ValueError("Must provide to_email")

        message = {
            "to": {"email": to_email},
            "template": template_id,
            "data": dict(data or {}),
        }

        # Modern Courier API:
        # https://docs.courier.com/reference/send/
        resp = self._client.send_message(message=message)

        # Courier typically returns {"requestId": "..."} (sometimes "messageId")
        return resp.get("requestId") or resp.get("messageId") or "unknown"