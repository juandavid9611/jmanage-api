from .ddb_session import membership_table
from boto3.dynamodb.conditions import Key
from typing import List, Dict, Any, Optional

class MembershipRepo:
    def __init__(self):
        self._table = membership_table()

    def _user_pk(self, user_id: str) -> str:
        return f"USER#{user_id}"
    
    def _account_sk(self, account_id: str) -> str:
        return f"ACCOUNT#{account_id}"

    def _parse_account_id(self, sk: str) -> str:
        # SK is "ACCOUNT#{id}"
        return sk.split("#", 1)[1] if "#" in sk else sk

    def get_active_memberships(self, user_id: str) -> Dict[str, Any]:
        """Query DynamoDB for active user memberships
        
        Returns:
            dict with keys:
                - account_ids: list of account IDs
                - accounts_roles: dict mapping account_id to role
        """
        accounts = []
        roles = {}

        try:
            resp = self._table.query(KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)))
            items = resp.get("Items", [])

            while "LastEvaluatedKey" in resp:
                resp = self._table.query(
                    KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)),
                    ExclusiveStartKey=resp["LastEvaluatedKey"],
                )
                items.extend(resp.get("Items", []))

            for it in items:
                if it.get("status", "active") != "active":
                    continue
                
                acc_id = self._parse_account_id(it.get("SK", ""))
                if not acc_id:
                    continue
                
                role = it.get("role", "user")
                accounts.append(acc_id)
                roles[acc_id] = role
                
            # de-dupe
            accounts = list(dict.fromkeys(accounts))
            
            return {
                'account_ids': accounts,
                'accounts_roles': roles
            }
        except Exception as e:
            print(f"Error querying memberships: {e}")
            raise e
    
    def create(self, user_id: str, account_id: str, role: str = "user", status: str = "active") -> None:
        """Create a membership record"""
        item = {
            "PK": self._user_pk(user_id),
            "SK": self._account_sk(account_id),
            "ACCOUNT_ID": account_id,
            "USER_ID": user_id,
            "role": role,
            "status": status
        }
        
        try:
            self._table.put_item(Item=item)
        except Exception as e:
            print(f"Error creating membership: {e}")
            raise e
    
    def delete(self, user_id: str, account_id: str) -> None:
        """Delete a specific membership"""
        try:
            self._table.delete_item(
                Key={
                    "PK": self._user_pk(user_id),
                    "SK": self._account_sk(account_id)
                }
            )
        except Exception as e:
            print(f"Error deleting membership: {e}")
            raise e
    
    def update_status(self, user_id: str, account_id: str, status: str) -> None:
        """Update membership status"""
        try:
            self._table.update_item(
                Key={
                    "PK": self._user_pk(user_id),
                    "SK": self._account_sk(account_id)
                },
                UpdateExpression="SET #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": status}
            )
        except Exception as e:
            print(f"Error updating membership status: {e}")
            raise e
    
    def delete_all_for_user(self, user_id: str) -> None:
        """Delete all memberships for a user"""
        try:
            # Query all memberships
            resp = self._table.query(KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)))
            items = resp.get("Items", [])
            
            while "LastEvaluatedKey" in resp:
                resp = self._table.query(
                    KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)),
                    ExclusiveStartKey=resp["LastEvaluatedKey"],
                )
                items.extend(resp.get("Items", []))
            
            # Delete each
            for item in items:
                self._table.delete_item(
                    Key={
                        "PK": item["PK"],
                        "SK": item["SK"]
                    }
                )
        except Exception as e:
            print(f"Error deleting all memberships: {e}")
            raise e
