from datetime import datetime
from enum import Enum
import os
import random
import time
from typing import Optional
from uuid import uuid4
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
from boto3.dynamodb.conditions import Key
from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware
from cognito_idp_actions import CognitoIdentityProviderWrapper


app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3030",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

IS_LOCAL = True

if not IS_LOCAL:
    handler = Mangum(app)

def _get_cognito_wrapper():
    if not IS_LOCAL:
        return CognitoIdentityProviderWrapper(
            boto3.client("cognito-idp"), os.environ.get("USER_POOL_ID"), os.environ.get("USER_POOL_API_CLIENT_ID")
        )
    return CognitoIdentityProviderWrapper(
        boto3.client("cognito-idp"), "us-west-2_CTvMrsxtC", "7t6shka96h6eb4iucj53nrpcc0"
    )

cog_wrapper = _get_cognito_wrapper()

class PaymentRequestStatus(str, Enum):
    PAID = "paid"
    PENDING = "pending"
    OVERDUE = "overdue"
    CANCELED = "canceled"

class PutPaymentRequest(BaseModel):
    id: Optional[str] = None
    status: Optional[PaymentRequestStatus] = None
    createDate: str
    dueDate: str
    concept: str
    description: str
    category: str
    group: str
    paymentRequestTo: Optional[list] = None
    isVerified: Optional[bool] = None
    userPrice: int
    sponsorPrice: int
    sponsorPercentage: int

class PutUserMetrics(BaseModel):
    asistencia_entrenos: int
    asistencia_partidos: int
    puntualidad_pagos: int
    llegadas_tarde: int
    deuda_acumulada: int
    total: int
    last_update: Optional[str] = None

class UserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

class UserConfirmationStatus(str, Enum):
    CONFIRMED = "confirmed"
    PENDING = "pending"

class PutUser(BaseModel):
    id: Optional[str] = None
    name: str
    email: str
    phoneNumber: str
    address: str
    city: str
    state: str
    group: Optional[str] = None
    role: str
    country: str
    zipCode: str
    company: str
    avatarUrl: Optional[str] = None
    status: str
    

@app.get("/")
async def root():
    return {
        "statusCode": 200,
        "body": "Hello from Fast API Lambda!"
    }

@app.get("/users")
async def get_users():
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    cog_users = cog_wrapper.list_users()
    users_mapped = map_users(items, cog_users)
    return {'users': users_mapped}

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    table = _get_user_table()
    response = table.get_item(Key={"id": user_id})
    item = response.get("Item")

    if not item:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    cog_user = cog_wrapper.get_user(item["email"])
    item["confirmation_status"] = UserConfirmationStatus.CONFIRMED if cog_user["UserStatus"] == "CONFIRMED" else UserConfirmationStatus.PENDING
    return _map_user(item)

@app.post("/users")
async def create_user(put_user: PutUser):
    created_time = int(time.time())
    user_response = cog_wrapper.sign_up_user(put_user.email, "vittoria2024", put_user.email, put_user.name)

    item = {
        "id": user_response["UserSub"],
        "user_name": put_user.name,
        "email": put_user.email,
        "phone_number": put_user.phoneNumber,
        "address": put_user.address,
        "city": put_user.city,
        "city_state": put_user.state,
        "country": put_user.country,
        "user_group": put_user.group,
        "zip_code": put_user.zipCode,
        "company": put_user.company,
        "user_role": put_user.role,
        "avatar_url": put_user.avatarUrl,
        "user_status": UserStatus.ACTIVE,
        "created_time": created_time,
        "user_metrics": {
            "asistencia_entrenos": 0,
            "asistencia_partidos": 0,
            "puntualidad_pagos": 0,
            "llegadas_tarde": 0,
            "deuda_acumulada": 0,
            "total": 0,
            "last_update": datetime.now().isoformat()
        }
    }

    table = _get_user_table()
    table.put_item(Item=item)
    item["confirmation_status"] = UserConfirmationStatus.PENDING
    return _map_user(item)

@app.put("/users/{user_id}")
async def update_user(user_id:str, put_user: PutUser):
    cog_user = cog_wrapper.get_user(put_user.email)
    if not cog_user:
        raise HTTPException(status_code=404, detail=f"Cog User {put_user.email} not found")
    if cog_user.get("name") != put_user.name:
       cog_wrapper.update_user_field(put_user.email, "name", put_user.name)

    table = _get_user_table()
    table.update_item(
        Key={"id": put_user.id},
        UpdateExpression="SET user_name = :user_name, phone_number = :phone_number, address = :address, city = :city, city_state = :city_state, country = :country, user_group = :user_group, zip_code = :zip_code, company = :company, user_role = :user_role, avatar_url = :avatar_url, user_status = :user_status",
        ExpressionAttributeValues={
            ":user_name": put_user.name, 
            ":phone_number": put_user.phoneNumber,
            ":address": put_user.address,
            ":city": put_user.city,
            ":city_state": put_user.state,
            ":country": put_user.country,
            ":user_group": put_user.group,
            ":zip_code": put_user.zipCode,
            ":company": put_user.company,
            ":user_role": put_user.role,
            ":avatar_url": put_user.avatarUrl,
            ":user_status": put_user.status
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_user_id": put_user.id}

@app.delete("/users/{user_id}")
async def delete_user(user_id: str):
    # TODO: Delete user from cognito, soft delete
    table = _get_user_table()
    table.delete_item(Key={"id": user_id})
    return {"deleted_user_id": user_id}

@app.get("/payment_requests")
async def get_payment_requests(user_id: Optional[str] = None):
    items_mapped = []
    if user_id:
        table = _get_payment_request_table()
        response = table.query(
            IndexName="user_index",
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        items = response.get("Items")
        items_mapped = [_map_payment_request(item) for item in items]
    else :
        table = _get_payment_request_table()
        response = table.scan()
        items = response.get("Items")
        items_mapped = [_map_payment_request(item) for item in items]
    return items_mapped


@app.get("/payment_requests/{payment_request_id}")
async def get_payment_request(payment_request_id: str):
    table = _get_payment_request_table()
    response = table.get_item(Key={"id": payment_request_id})
    item = response.get("Item")

    if not item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    return _map_payment_request(item)

@app.post("/payment_requests")
async def create_payment_requests(put_payment_request: PutPaymentRequest):
    created_time = int(time.time())
    table = _get_payment_request_table()
    response = []
    for user in put_payment_request.paymentRequestTo:
        item = {
            "id": f"{uuid4().hex}",
            "create_date": put_payment_request.createDate,
            "due_date": put_payment_request.dueDate,
            "concept": put_payment_request.concept,
            "description": put_payment_request.description,
            "category": put_payment_request.category,
            "payment_request_to": user,
            "user_id": user["id"],
            "user_group": put_payment_request.group,
            "user_price": put_payment_request.userPrice,
            "sponsor_price": put_payment_request.sponsorPrice,
            "sponsor_percentage": put_payment_request.sponsorPercentage,
            "payment_status": PaymentRequestStatus.PENDING,
            "created_time": created_time,
        }
        table.put_item(Item=item)
        response.append(_map_payment_request(item))
    return response

@app.put("/payment_requests/{payment_request_id}")
async def update_payment_request(payment_request_id: str, put_payment_request: PutPaymentRequest):
    table = _get_payment_request_table()
    table.update_item(
        Key={"id": put_payment_request.id},
        UpdateExpression="SET create_date = :create_date, due_date = :due_date, concept = :concept, description = :description, category = :category, user_group = :user_group, user_price = :user_price, sponsor_price = :sponsor_price, sponsor_percentage = :sponsor_percentage, payment_status = :payment_status",
        ExpressionAttributeValues={
            ":create_date": put_payment_request.createDate, 
            ":due_date": put_payment_request.dueDate,
            ":concept": put_payment_request.concept,
            ":description": put_payment_request.description,
            ":category": put_payment_request.category,
            ":user_group": put_payment_request.group,
            ":user_price": put_payment_request.userPrice,
            ":sponsor_price": put_payment_request.sponsorPrice,
            ":sponsor_percentage": put_payment_request.sponsorPercentage,
            ":payment_status": put_payment_request.status
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_payment_request_id": put_payment_request.id}

@app.delete("/payment_requests/{payment_request_id}")
async def delete_payment_request(payment_request_id: str):
    table = _get_payment_request_table()
    table.delete_item(Key={"id": payment_request_id})
    return {"deleted_payment_request_id": payment_request_id}

@app.get("/users/{user_id}/metrics")
async def get_user_metrics(user_id: str):
    table = _get_user_table()
    response = table.get_item(Key={"id": user_id})
    item = response.get("Item")
    if not item.get("user_metrics"):
        initial_user_metrics = {
            "asistencia_entrenos": 0,
            "asistencia_partidos": 0,
            "puntualidad_pagos": 0,
            "llegadas_tarde": 0,
            "deuda_acumulada": 0,
            "total": 0,
            "last_update": datetime.now().isoformat()
        }
        table.update_item(
        Key={"id": user_id},
        UpdateExpression="SET user_metrics = :user_metrics",
        ExpressionAttributeValues={
            ":user_metrics": initial_user_metrics,
            },
        ReturnValues="ALL_NEW",
        )
        return initial_user_metrics
    return item.get("user_metrics")

@app.put("/users/{user_id}/metrics")
async def update_user_metrics(user_id: str, put_user_metrics: PutUserMetrics):
    table = _get_user_table()
    last_update = datetime.now().isoformat()
    print(last_update)
    put_user_metrics.last_update = last_update
    table.update_item(
        Key={"id": user_id},
        UpdateExpression="SET user_metrics = :user_metrics",
        ExpressionAttributeValues={
            ":user_metrics": put_user_metrics.dict(),
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_metrics": put_user_metrics.dict()}

@app.get("/calendar")
async def get_calendar():
    return _get_events()

def _get_payment_request_table():
    table_name = "JmanageInfraStack-dev-PaymentRequest928AFAA0-94UPR2HVAWNB"
    if not IS_LOCAL:
        table_name = os.environ.get("PAYMENT_REQUEST_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_user_table():
    table_name = "JmanageInfraStack-dev-User00B015A1-130A0CR764IXY"
    if not IS_LOCAL:
        table_name = os.environ.get("USER_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def map_users(db_users, cog_users):
    cog_users_map = {user["Username"]: user for user in cog_users}
    users = []
    for db_user in db_users:
        cog_user = cog_users_map[db_user["id"]]
        db_user["confirmation_status"] =  UserConfirmationStatus.CONFIRMED if cog_user["UserStatus"] == "CONFIRMED" else UserConfirmationStatus.PENDING
        user = _map_user(db_user)
        users.append(user)
    return users

def _map_user(item):
    item["name"] = item.pop("user_name", None)
    item["phoneNumber"] = item.pop("phone_number", None)
    item["state"] = item.pop("city_state", None)
    item["zipCode"] = item.pop("zip_code", None)
    item["avatarUrl"] = item.pop("avatar_url", None)
    item["status"] = item.pop("user_status", None)
    item["role"] = item.pop("user_role", None)
    item["state"] = item.pop("city_state", None)
    item["group"] = item.pop("user_group", None)
    item["confirmationStatus"] = item.pop("confirmation_status", None)
    return item

def _map_payment_request(item):
    item["createDate"] = item.pop("create_date", None)
    item["dueDate"] = item.pop("due_date", None)
    item["status"] = item.pop("payment_status", None)
    item["paymentRequestTo"] = item.pop("payment_request_to", None)
    item["totalAmount"] = item.pop("user_price", None)
    item["sponsorPrice"] = item.pop("sponsor_price", None)
    item["sponsorPercentage"] = item.pop("sponsor_percentage", None)
    return item

def _get_events():
    response = {
        "events": [
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b2",
                "allDay": True,
                "color": "#00A76F",
                "description": "Atque eaque ducimus minima distinctio velit. Laborum et veniam officiis. Delectus ex saepe hic id laboriosam officia. Odit nostrum qui illum saepe debitis ullam. Laudantium beatae modi fugit ut. Dolores consequatur beatae nihil voluptates rem maiores.",
                "start": 1707309011395,
                "end": 1707314411395,
                "title": "The Ultimate Guide to Productivity Hacks"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b3",
                "allDay": False,
                "color": "#00B8D9",
                "description": "Rerum eius velit dolores. Explicabo ad nemo quibusdam. Voluptatem eum suscipit et ipsum et consequatur aperiam quia. Rerum nulla sequi recusandae illum velit quia quas. Et error laborum maiores cupiditate occaecati.",
                "start": 1707807611395,
                "end": 1707820211395,
                "title": "Partido Amistoso sede Escuela de Ingenieros"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b4",
                "allDay": False,
                "color": "#22C55E",
                "description": "Et non omnis qui. Qui sunt deserunt dolorem aut velit cumque adipisci aut enim. Nihil quis quisquam nesciunt dicta nobis ab aperiam dolorem repellat. Voluptates non blanditiis. Error et tenetur iste soluta cupiditate ratione perspiciatis et. Quibusdam aliquid nam sunt et quisquam non esse.",
                "start": 1708606811395,
                "end": 1708621211395,
                "title": "How to Master the Art of Public Speaking"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b5",
                "allDay": False,
                "color": "#8E33FF",
                "description": "Nihil ea sunt facilis praesentium atque. Ab animi alias sequi molestias aut velit ea. Sed possimus eos. Et est aliquid est voluptatem.",
                "start": 1708347611395,
                "end": 1708362011395,
                "title": "Entrega de Uniformes - Anuncio de Patrocinadores"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b6",
                "allDay": False,
                "color": "#FFAB00",
                "description": "Non rerum modi. Accusamus voluptatem odit nihil in. Quidem et iusto numquam veniam culpa aperiam odio aut enim. Quae vel dolores. Pariatur est culpa veritatis aut dolorem.",
                "start": 1708596911395,
                "end": 1708597811395,
                "title": "Entrenamiento Vittoria Femenino"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b7",
                "allDay": False,
                "color": "#FF5630",
                "description": "Est enim et sit non impedit aperiam cumque animi. Aut eius impedit saepe blanditiis. Totam molestias magnam minima fugiat.",
                "start": 1708034399999,
                "end": 1708034400000,
                "title": "Partido Amistoso sede Creativo. Hungaros"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b8",
                "allDay": False,
                "color": "#003768",
                "description": "Unde a inventore et. Sed esse ut. Atque ducimus quibusdam fuga quas id qui fuga.",
                "start": 1708605911395,
                "end": 1708606211395,
                "title": "Entrenamiento Vittoria Masculino"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b9",
                "allDay": False,
                "color": "#00B8D9",
                "description": "Eaque natus adipisci soluta nostrum dolorem. Nesciunt ipsum molestias ut aliquid natus ut omnis qui fugiat. Dolor et rem. Ut neque voluptatem blanditiis quasi ullam deleniti.",
                "start": 1708609811395,
                "end": 1708610111395,
                "title": "10 Must-Visit Destinations for Adventure Travelers"
            },
            {
                "id": "e99f09a7-dd88-49d5-b1c8-1daf80c2d7b10",
                "allDay": False,
                "color": "#7A0916",
                "description": "Nam et error exercitationem qui voluptate optio. Officia omnis qui accusantium ipsam qui. Quia sequi nulla perspiciatis optio vero omnis maxime omnis ipsum. Perspiciatis consequuntur asperiores veniam dolores.",
                "start": 1708863131395,
                "end": 1709036411395,
                "title": "JManage - Vittoria despliegue"
            }
        ]
    }
    return response
