from typing import Protocol, Mapping, Any

class EmailSender(Protocol):
    def send_template(
        self, *, template_id: str, to_email: str, data: Mapping[str, Any] | None = None
    ) -> str: ...

class InAppSender(Protocol):
    def publish(
            self, *, user_email: str, title: str, content: str, category: str | None = None, action_url_path: str = "dashboard/"
    ) -> str: ...

    def publish_bulk(
        self, *, user_emails: list[str], title: str, content: str, category: str | None = None, action_url_path: str = "dashboard/"
    ) -> str: ...