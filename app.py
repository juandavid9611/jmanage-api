from enum import Enum
import os
import time
from typing import Optional
from uuid import uuid4
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
from boto3.dynamodb.conditions import Key
from mangum import Mangum

app = FastAPI()

IS_LOCAL = True

if not IS_LOCAL:
    handler = Mangum(app)

class PaymentRequestStatus(str, Enum):
    PAID = "paid"
    PENDING = "pending"
    CANCELED = "canceled"

class PutPaymentRequest(BaseModel):
    description: str
    user_id: Optional[str] = None
    payment_request_id: Optional[str] = None
    payment_status: Optional[PaymentRequestStatus] = None
    

@app.get("/")
async def root():
    return {
        "statusCode": 200,
        "body": "Hello from Fast API Lambda!"
    }

@app.post("/payment_request")
async def create_payment_request(put_payment_request: PutPaymentRequest):
    created_time = int(time.time())
    item = {
        "user_id": put_payment_request.user_id,
        "description": put_payment_request.description,
        "payment_status": PaymentRequestStatus.PENDING,
        "created_time": created_time,
        "payment_request_id": f"payment_request_{uuid4().hex}",
        "ttl": int(created_time + 86400),
    }

    table = _get_table()
    table.put_item(Item=item)
    return item


@app.get("/payment_request/{payment_request_id}")
async def get_payment_request(payment_request_id: str):
    table = _get_table()
    response = table.get_item(Key={"payment_request_id": payment_request_id})
    item = response.get("Item")

    if not item:
        raise HTTPException(status_code=404, detail=f"Request payment {payment_request_id} not found")
    return item

@app.get("/user_payment_requests/{user_id}")
async def get_user_payment_requests(user_id: str):
    table = _get_table()
    response = table.query(
        IndexName="user_index",
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
    )
    payment_requests = response.get("Items")
    return payment_requests

@app.put("/payment_request/{payment_request_id}")
async def update_payment_request(put_payment_request: PutPaymentRequest):
    table = _get_table()
    table.update_item(
        Key={"payment_request_id": put_payment_request.payment_request_id},
        UpdateExpression="SET description = :description, payment_status = :payment_status",
        ExpressionAttributeValues={
            ":description": put_payment_request.description, 
            ":payment_status": put_payment_request.payment_status
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_payment_request_id": put_payment_request.payment_request_id}

@app.delete("/payment_request/{payment_reuqest_id}")
async def delete_payment_request(payment_request_id: str):
    table = _get_table()
    table.delete_item(Key={"payment_request_id": payment_request_id})
    return {"deleted_payment_request_id": payment_request_id}

def _get_table():
    table_name = "JmanageInfraStack-PaymentRequest928AFAA0-1SQCUZRN22DBD"
    if not IS_LOCAL:
        table_name = os.environ.get("PAYMENT_REQUEST_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)