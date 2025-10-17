from typing import Protocol, Mapping, Any, Optional

class EmailSender(Protocol):
    def send_template(
        self, *, template_id: str, to_email: str, data: Optional[Mapping[str, Any]] = None
    ) -> str: ...

class InAppSender(Protocol):
    def publish(
            self, *, user_email: str, title: str, content: str, category: Optional[str] = None, action_url_path: str = "dashboard/"
    ) -> str: ...

    def publish_bulk(
        self, *, user_emails: list[str], title: str, content: str, category: Optional[str] = None, action_url_path: str = "dashboard/"
    ) -> str: ...