from typing import Any
from uuid import uuid4
from datetime import datetime
from repositories.ddb_session import file_table


class FileRepo:
    """Repository for file metadata stored in DynamoDB"""
    
    def __init__(self):
        self.table = file_table()
    
    def create(self, data: dict[str, Any], account_id: str) -> dict[str, Any]:
        """Create a new file record"""
        now = datetime.now().isoformat()
        file_id = f"{uuid4().hex}"
        
        item = {
            "id": file_id,
            "account_id": account_id,
            "name": data.get("name"),
            "url": None,  # Will be set after upload
            "tags": data.get("tags", []),
            "size": data.get("size"),
            "type": data.get("type"),
            "is_favorited": data.get("is_favorited", False),
            "created_at": now,
            "modified_at": now,
        }
        
        self.table.put_item(Item=item)
        return item
    
    def get_by_id(self, file_id: str, account_id: str) -> dict[str, Any] | None:
        """Get a file by ID"""
        resp = self.table.get_item(Key={"id": file_id})
        item = resp.get("Item")
        
        # Verify account ownership
        if item and item.get("account_id") == account_id:
            return item
        return None
    
    def list_all(self, account_id: str) -> list[dict[str, Any]]:
        """List all files for an account"""
        resp = self.table.query(
            IndexName="account_id_index",
            KeyConditionExpression="account_id = :aid",
            ExpressionAttributeValues={":aid": account_id},
            ScanIndexForward=False,  # Most recent first
        )
        return resp.get("Items", [])
    
    def update(self, file_id: str, account_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        """Update file metadata"""
        # Verify ownership first
        existing = self.get_by_id(file_id, account_id)
        if not existing:
            return None
        
        # Build update expression
        update_expr_parts = []
        expr_attr_values = {}
        expr_attr_names = {}
        
        updates["modified_at"] = datetime.now().isoformat()
        
        for key, value in updates.items():
            # Handle reserved keywords
            attr_name = f"#{key}"
            attr_value = f":{key}"
            update_expr_parts.append(f"{attr_name} = {attr_value}")
            expr_attr_names[attr_name] = key
            expr_attr_values[attr_value] = value
        
        update_expression = "SET " + ", ".join(update_expr_parts)
        
        resp = self.table.update_item(
            Key={"id": file_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
            ReturnValues="ALL_NEW",
        )
        
        return resp.get("Attributes")
    
    def delete(self, file_id: str, account_id: str) -> bool:
        """Delete a file record"""
        # Verify ownership first
        existing = self.get_by_id(file_id, account_id)
        if not existing:
            return False
        
        self.table.delete_item(Key={"id": file_id})
        return True
