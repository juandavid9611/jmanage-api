import os

from repositories.notifications.ports import InAppSender
from repositories.notification_repo_ddb import NotificationRepo
from repositories.notifications.onesignal_impl import OneSignalNotificationSender


class DdbInAppSender(InAppSender):
    def __init__(self, repo: NotificationRepo, onesignal: OneSignalNotificationSender):
        self._repo = repo
        self._onesignal = onesignal
        self._base_action_url = os.environ.get("BASE_ACTION_URL", "")

    def publish(self, *, user_email: str, title: str, content: str, category: str | None = None, action_url_path: str = "dashboard/") -> str:
        action_url = f"{self._base_action_url.rstrip('/')}/{action_url_path}"
        self._repo.put(user_email, title, content, category, action_url)
        return self._onesignal.publish(
            user_email=user_email,
            title=title,
            content=content,
            category=category,
            action_url_path=action_url_path,
        )

    def publish_bulk(self, *, user_emails: list[str], title: str, content: str, category: str | None = None, action_url_path: str = "dashboard/") -> str:
        action_url = f"{self._base_action_url.rstrip('/')}/{action_url_path}"
        for email in user_emails:
            self._repo.put(email, title, content, category, action_url)
        return self._onesignal.publish_bulk(
            user_emails=user_emails,
            title=title,
            content=content,
            category=category,
            action_url_path=action_url_path,
        )
