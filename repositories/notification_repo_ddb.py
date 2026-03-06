import time
import uuid

from boto3.dynamodb.conditions import Attr, Key

from repositories.ddb_session import notification_table


class NotificationRepo:
    def put(self, user_email: str, title: str, content: str, category: str | None, action_url: str) -> dict:
        now_ms = int(time.time() * 1000)
        item = {
            "id": str(uuid.uuid4()),
            "user_email": user_email,
            "title": title,
            "content": content,
            "category": category or "",
            "action_url": action_url,
            "sent_at": now_ms,
        }
        notification_table().put_item(Item=item)
        return item

    def list_by_user(self, user_email: str) -> list[dict]:
        table = notification_table()
        items = []
        kwargs = {
            "IndexName": "user_email_index",
            "KeyConditionExpression": Key("user_email").eq(user_email),
            "ScanIndexForward": False,
        }
        while True:
            resp = table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        return items

    def mark_read(self, notification_id: str, user_email: str) -> None:
        now_ms = int(time.time() * 1000)
        notification_table().update_item(
            Key={"id": notification_id},
            UpdateExpression="SET read_at = :now",
            ConditionExpression=Attr("user_email").eq(user_email),
            ExpressionAttributeValues={":now": now_ms},
        )

    def mark_all_read(self, user_email: str) -> None:
        now_ms = int(time.time() * 1000)
        table = notification_table()
        items = []
        kwargs = {
            "IndexName": "user_email_index",
            "KeyConditionExpression": Key("user_email").eq(user_email),
            "FilterExpression": Attr("read_at").not_exists(),
            "ScanIndexForward": False,
        }
        while True:
            resp = table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        for item in items:
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET read_at = :now",
                ExpressionAttributeValues={":now": now_ms},
            )
