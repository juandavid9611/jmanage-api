import os
import boto3
from boto3.dynamodb.conditions import Key
from typing import List, Dict, Any, Optional

class MembershipRepo:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.table_name = os.environ.get('MEMBERSHIPS_TABLE_NAME')
        self.table = self.dynamodb.Table(self.table_name) if self.table_name else None

    def _user_pk(self, user_id: str) -> str:
        return f"USER#{user_id}"

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
        if not self.table:
            print("WARNING: MEMBERSHIPS_TABLE_NAME not set")
            return {'account_ids': [], 'accounts_roles': {}}

        accounts = []
        roles = {}

        try:
            resp = self.table.query(KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)))
            items = resp.get("Items", [])

            while "LastEvaluatedKey" in resp:
                resp = self.table.query(
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
                
                role = it.get("role", "member")
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
