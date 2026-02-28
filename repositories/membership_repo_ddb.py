from .ddb_session import membership_table
from boto3.dynamodb.conditions import Key
from typing import List, Dict, Any, Optional

class MembershipRepo:
    def __init__(self):
        self._table = membership_table()

    def _user_pk(self, user_id: str) -> str:
        return f"USER#{user_id}"
    
    def _account_workspace_sk(self, account_id: str, workspace_id: str) -> str:
        """SK format including workspace"""
        return f"ACCOUNT#{account_id}#WORKSPACE#{workspace_id}"

    def _parse_account_id(self, sk: str) -> str:
        # SK is "ACCOUNT#{id}" or "ACCOUNT#{id}#WORKSPACE#{workspace_id}"
        parts = sk.split("#")
        if len(parts) >= 2:
            return parts[1]
        return sk
    
    def _parse_workspace_id(self, sk: str) -> str | None:
        """Extract workspace_id from SK"""
        # SK format: ACCOUNT#{account_id}#WORKSPACE#{workspace_id}
        parts = sk.split("#")
        if len(parts) >= 4 and parts[2] == "WORKSPACE":
            return parts[3]
        return None

    def list_by_account(self, account_id: str) -> List[Dict[str, Any]]:
        """List all memberships for an account using GSI"""
        try:
            resp = self._table.query(
                IndexName="byAccount",
                KeyConditionExpression=Key("ACCOUNT_ID").eq(account_id)
            )
            items = resp.get("Items", [])
            
            while "LastEvaluatedKey" in resp:
                resp = self._table.query(
                    IndexName="byAccount",
                    KeyConditionExpression=Key("ACCOUNT_ID").eq(account_id),
                    ExclusiveStartKey=resp["LastEvaluatedKey"]
                )
                items.extend(resp.get("Items", []))
            
            memberships = []
            for it in items:
                workspace_id = self._parse_workspace_id(it.get("SK", ""))
                if not workspace_id:
                    print(f"WARNING: Membership has no workspace in SK: {it.get('PK')}/{it.get('SK')}")
                    continue
                memberships.append({
                    "user_id": it.get("USER_ID"),
                    "account_id": it.get("ACCOUNT_ID"),
                    "workspace_id": workspace_id,
                    "role": it.get("role", "user"),
                    "status": it.get("status", "active")
                })
            return memberships
        except Exception as e:
            print(f"Error listing account memberships: {e}")
            raise e


    def get_active_memberships(self, user_id: str) -> List[Dict[str, Any]]:
        """Query DynamoDB for active user memberships
        
        Returns:
            list of membership dicts with account_id, role, status, workspace_id
        """
        memberships = []

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
                
                workspace_id = self._parse_workspace_id(it.get("SK", ""))
                if not workspace_id:
                    print(f"WARNING: Membership has no workspace in SK: {it.get('PK')}/{it.get('SK')}")
                    continue
                
                memberships.append({
                    "account_id": acc_id,
                    "workspace_id": workspace_id,
                    "role": it.get("role", "user"),
                    "status": it.get("status", "active")
                })
            
            return memberships
        except Exception as e:
            print(f"Error querying memberships: {e}")
            raise e
    
    def get_user_account_memberships(self, user_id: str, account_id: str) -> List[Dict[str, Any]]:
        """Get ALL memberships for a user in a specific account"""
        try:
            resp = self._table.query(
                KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)) & 
                                      Key("SK").begins_with(f"ACCOUNT#{account_id}#")
            )
            items = resp.get("Items", [])
            
            while "LastEvaluatedKey" in resp:
                resp = self._table.query(
                    KeyConditionExpression=Key("PK").eq(self._user_pk(user_id)) & 
                                          Key("SK").begins_with(f"ACCOUNT#{account_id}#"),
                    ExclusiveStartKey=resp["LastEvaluatedKey"]
                )
                items.extend(resp.get("Items", []))
            
            memberships = []
            for it in items:
                workspace_id = self._parse_workspace_id(it.get("SK", ""))
                if not workspace_id:
                    print(f"WARNING: Membership has no workspace in SK: {it.get('PK')}/{it.get('SK')}")
                    continue
                memberships.append({
                    "user_id": it.get("USER_ID"),
                    "account_id": it.get("ACCOUNT_ID"),
                    "workspace_id": workspace_id,
                    "role": it.get("role", "user"),
                    "status": it.get("status", "active")
                })
            return memberships
        except Exception as e:
            print(f"Error getting user account memberships: {e}")
            raise e
    
    def get_workspace_memberships(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all memberships for a workspace using GSI"""
        try:
            resp = self._table.query(
                IndexName="byWorkspace",
                KeyConditionExpression=Key("WORKSPACE_ID").eq(workspace_id)
            )
            items = resp.get("Items", [])
            
            while "LastEvaluatedKey" in resp:
                resp = self._table.query(
                    IndexName="byWorkspace",
                    KeyConditionExpression=Key("WORKSPACE_ID").eq(workspace_id),
                    ExclusiveStartKey=resp["LastEvaluatedKey"]
                )
                items.extend(resp.get("Items", []))
            
            memberships = []
            for it in items:
                memberships.append({
                    "user_id": it.get("USER_ID"),
                    "account_id": it.get("ACCOUNT_ID"),
                    "workspace_id": it.get("WORKSPACE_ID"),
                    "role": it.get("role", "user"),
                    "status": it.get("status", "active")
                })
            return memberships
        except Exception as e:
            print(f"Error getting workspace memberships: {e}")
            raise e
    
    def create(self, user_id: str, account_id: str, workspace_id: str, role: str = "user", status: str = "active") -> None:
        """Create a membership record - workspace_id is now REQUIRED"""
        item = {
            "PK": self._user_pk(user_id),
            "SK": self._account_workspace_sk(account_id, workspace_id),  # NEW FORMAT
            "ACCOUNT_ID": account_id,
            "WORKSPACE_ID": workspace_id,  # Store as attribute for GSI
            "USER_ID": user_id,
            "role": role,
            "status": status
        }
        
        try:
            self._table.put_item(Item=item)
        except Exception as e:
            print(f"Error creating membership: {e}")
            raise e
    
    def delete(self, user_id: str, account_id: str, workspace_id: str) -> None:
        """Delete a specific membership - now requires workspace_id"""
        try:
            self._table.delete_item(
                Key={
                    "PK": self._user_pk(user_id),
                    "SK": self._account_workspace_sk(account_id, workspace_id)
                }
            )
        except Exception as e:
            print(f"Error deleting membership: {e}")
            raise e
    
    def update_status(self, user_id: str, account_id: str, workspace_id: str, status: str) -> None:
        """Update membership status - now requires workspace_id"""
        try:
            self._table.update_item(
                Key={
                    "PK": self._user_pk(user_id),
                    "SK": self._account_workspace_sk(account_id, workspace_id)
                },
                UpdateExpression="SET #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": status}
            )
        except Exception as e:
            print(f"Error updating membership status: {e}")
            raise e
    
    # update_workspace() removed - workspace is now part of the key, not an attribute
    
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
