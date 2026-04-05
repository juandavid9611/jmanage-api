import os
import requests
from repositories.notifications.ports import InAppSender


class OneSignalNotificationSender(InAppSender):
    """
    Thin adapter for OneSignal. Implements the InAppSender protocol.
    Targets users by external_user_id (assumed to be their email).
    """

    def __init__(self):
        self.app_id = os.environ.get("ONESIGNAL_APP_ID")
        self.rest_api_key = os.environ.get("ONESIGNAL_REST_API_KEY")
        self.base_action_url = os.environ.get("BASE_ACTION_URL")
        self.url = "https://onesignal.com/api/v1/notifications"
        self.headers = {
            "Authorization": f"Key {self.rest_api_key}",
            "Content-Type": "application/json",
        }
        if not self.app_id or not self.rest_api_key:
            print("WARNING: ONESIGNAL_APP_ID or ONESIGNAL_REST_API_KEY not configured")

    def publish(self, *, user_email: str, title: str, content: str, category: str | None = None, action_url_path: str = "dashboard/") -> str:
        payload = self._build_payload(
            external_user_ids=[user_email],
            title=title,
            content=content,
            category=category,
            action_url_path=action_url_path,
        )
        response = requests.post(self.url, json=payload, headers=self.headers)
        print(response.json())
        return response.json().get("id", "")

    def publish_bulk(self, *, user_emails: list[str], title: str, content: str, category: str | None = None, action_url_path: str = "dashboard/") -> str:
        payload = self._build_payload(
            external_user_ids=user_emails,
            title=title,
            content=content,
            category=category,
            action_url_path=action_url_path,
        )
        response = requests.post(self.url, json=payload, headers=self.headers)
        print(response.json())
        return response.json().get("id", "")

    def _build_payload(self, *, external_user_ids: list[str], title: str, content: str, category: str | None, action_url_path: str) -> dict:
        payload = {
            "app_id": self.app_id,
            "headings": {"en": title},
            "contents": {"en": content},
            "include_aliases": {"external_id": external_user_ids},
            "target_channel": "push",
            "url": f"{self.base_action_url.rstrip('/')}/{action_url_path}",
        }
        if category:
            payload["data"] = {"category": category}
        return payload
