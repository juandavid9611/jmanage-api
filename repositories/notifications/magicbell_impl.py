import os
import requests
from repositories.notifications.ports import InAppSender


class MagicBellNotificationSender(InAppSender):
    """
    Thin adapter for MagicBell. Implements the InAppSender protocol.
    """

    def __init__(self):
        self.api_key = os.environ.get("MAGICBELL_API_KEY")
        self.api_secret = os.environ.get("MAGICBELL_API_SECRET")
        self.headers = {
            "X-MAGICBELL-API-KEY": os.environ.get("MAGICBELL_API_KEY"),
            "X-MAGICBELL-API-SECRET": os.environ.get("MAGICBELL_API_SECRET")
        }
        self.base_action_url = os.environ.get("BASE_ACTION_URL")
        self.url = "https://api.magicbell.com/broadcasts"
        if not self.api_key or not self.api_secret:
            print("WARNING: MAGICBELL_API_KEY or MAGICBELL_API_SECRET not configured")


    def publish(self, user_email, title, content, category, action_url_path="dashboard/"):
        payload = {
            "broadcast": {
                "title": title, 
                "content": content,
                "category": category,
                "action_url": f"{self.base_action_url}/{action_url_path}",
                "recipients": [
                    {
                        "email": user_email
                    },
                ]
            }
        }

        response = requests.post(self.url, json=payload, headers=self.headers)
        print(response.json())

    def publish_bulk(self, user_emails, title, content, category, action_url_path="dashboard/"):
        payload = {
            "broadcast": {
                "title": title, 
                "content": content,
                "category": category,
                "action_url": f"{self.base_action_url}/{action_url_path}",
                "recipients": [
                    {
                        "email": email
                    } for email in user_emails
                ]
            }
        }

        response = requests.post(self.url, json=payload, headers=self.headers)
        print(response.json())