from typing import List, Optional, Dict, Any
from repositories.order_repo_ddb import OrderRepo
from api.schemas.orders import Order, OrderCreate, OrderUpdate

class OrderService:
    def __init__(self, repo: OrderRepo):
        self.repo = repo

    def list_orders(self, account_id: str) -> List[Order]:
        items = self.repo.list_all(account_id)
        return [Order(**item) for item in items]

    def get_order(self, order_id: str, account_id: str) -> Optional[Order]:
        item = self.repo.get_by_id(order_id, account_id)
        if item:
            return Order(**item)
        return None

    def create_order(self, payload: OrderCreate, account_id: str) -> Order:
        # Convert Pydantic model to dict
        data = payload.model_dump()
        item = self.repo.create(data, account_id)
        return Order(**item)

    def update_order(self, order_id: str, account_id: str, payload: OrderUpdate) -> Optional[Order]:
        # Convert Pydantic model to dict, excluding None values
        data = payload.model_dump(exclude_unset=True)
        item = self.repo.update(order_id, account_id, data)
        if item:
            return Order(**item)
        return None

    def delete_order(self, order_id: str, account_id: str) -> bool:
        return self.repo.delete(order_id, account_id)
