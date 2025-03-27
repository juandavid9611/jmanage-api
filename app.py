from datetime import datetime, timezone
import decimal
from enum import Enum
import os
import time
from typing import Optional, Union
from uuid import uuid4
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel
import boto3
from boto3.dynamodb.conditions import Key
from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware
from JWTBearer import JWTBearer
from auth import PermissionChecker, get_current_user, jwks
from cognito_idp_actions import CognitoIdentityProviderWrapper
from trycourier import Courier
import locale
from typing import List
import re
import requests
import uvicorn
import locale
from babel.dates import format_datetime
import pytz


app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3031",
    "http://localhost:3030",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

auth = JWTBearer(jwks)
locale.setlocale(locale.LC_ALL, 'en_US.utf-8') 

USE_MANGUM = os.environ.get("USE_MANGUM", "false")

courier_client = Courier(auth_token=os.environ.get("COURIER_AUTH_TOKEN"))

if USE_MANGUM:
    print('Using Mangum')
    handler = Mangum(app)

def _get_cognito_wrapper():
    return CognitoIdentityProviderWrapper(
        boto3.client("cognito-idp"), os.environ.get("USER_POOL_ID"), os.environ.get("USER_POOL_API_CLIENT_ID")
    )

cog_wrapper = _get_cognito_wrapper()

class PaymentRequestStatus(str, Enum):
    PAID = "paid"
    PENDING = "pending"
    OVERDUE = "overdue"
    CANCELED = "canceled"
    APPROVAL_PENDING = "approval_pending"

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
    overduePrice: Optional[int] = None

class PutUserMetrics(BaseModel):
    asistencia_entrenos: int
    asistencia_partidos: int
    puntualidad_pagos: int
    llegadas_tarde: int
    deuda_acumulada: int
    total: int
    puntaje_asistencia: decimal.Decimal
    puntaje_asistencia_description: str
    last_update: Optional[str] = None

class PutUserAvatar(BaseModel):
    avatar_url: str

class UserStatus(str, Enum):
    ACTIVE = "active"
    DISABLED = "disabled"

class UserConfirmationStatus(str, Enum):
    CONFIRMED = "confirmed"
    PENDING = "pending"

class CreateUser(BaseModel):
    id: str
    name: str
    email: str

class PutUser(BaseModel):
    id: str
    name: str
    identityCardNumber: str
    email: str
    phoneNumber: str
    country: str
    city: str
    address: str
    group: str
    rh: str
    eps: str
    emergencyContactName: str
    emergencyContactPhoneNumber: str
    emergencyContactRelationship: str
    status: str
    shirtNumber: str

class PutCalendarEvent(BaseModel):
    id: Optional[str] = None
    allDay: Optional[bool] = None
    color: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str]
    start: Optional[int]
    end: Union[int, str]
    title: Optional[str]
    category: Optional[str]
    createTour: Optional[bool] = False
    group: Optional[str] = None
    tourId: Optional[str] = None

class PutTour(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    images: Optional[list] = None
    publish: Optional[str] = None
    services: Optional[list] = None
    available: Optional[dict] = None
    tourGuides: Optional[list] = None
    bookers: Optional[dict] = None
    content: Optional[str] = None
    tags: Optional[list] = None
    location: Optional[str] = None
    scores: Optional[dict] = None
    createdAt: Optional[str] = None
    calendarEventId: Optional[str] = None
    group: Optional[str] = None

class PutWorkspace(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    logo: Optional[str] = None
    plan: Optional[str] = None

class PatchProperty(BaseModel):
    name: str
    value: str

@app.get("/")
async def root():
    return {
        "statusCode": 200,
        "body": "Hello from Fast API Lambda!"
    }

@app.get("/users", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_users(workspace_id: str = Query(None), include_disabled: bool = False):
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    if workspace_id:
        items = [item for item in items if item.get("user_group") == workspace_id]
    if not include_disabled:
        items = [item for item in items if item.get("user_status") == UserStatus.ACTIVE]
    cog_users = cog_wrapper.list_users()
    users_mapped = map_users(items, cog_users)
    return {"users": users_mapped}

@app.get("/users/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
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
async def create_user(create_user: CreateUser):
    created_time = int(time.time())

    item = {
        "id": create_user.id,
        "user_name": create_user.name,
        "email": create_user.email,
        "user_status": UserStatus.ACTIVE,
        "created_time": created_time,
        "user_group": "male",
        "user_metrics": {
            "asistencia_entrenos": 0,
            "asistencia_partidos": 0,
            "puntualidad_pagos": 0,
            "llegadas_tarde": 0,
            "deuda_acumulada": 0,
            "total": 0,
            "puntaje_asistencia_description": "",
            "puntaje_asistencia": 3 ,
            "last_update": datetime.now().isoformat()
        },
        "shirt_number": "0",
    }

    table = _get_user_table()
    table.put_item(Item=item)
    send_welcome_notification(create_user.email, create_user.name)
    return _map_user(item)

@app.put("/users/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def update_user(user_id:str, put_user: PutUser):
    cog_user = cog_wrapper.get_user(put_user.email)
    if not cog_user:
        raise HTTPException(status_code=404, detail=f"Cog User {put_user.email} not found")
    if cog_user.get("name") != put_user.name:
       cog_wrapper.update_user_field(put_user.email, "name", put_user.name)

    table = _get_user_table()
    table.update_item(
        Key={"id": put_user.id},
        UpdateExpression="SET user_name = :user_name, identity_card_number = :identity_card_number, phone_number = :phone_number, country = :country, city = :city, address = :address, user_group = :user_group, rh = :rh, eps = :eps, emergency_contact_name = :emergency_contact_name, emergency_contact_phone_number = :emergency_contact_phone_number, emergency_contact_relationship = :emergency_contact_relationship, user_status = :user_status, shirt_number = :shirt_number",
        ExpressionAttributeValues={
            ":user_name": put_user.name, 
            ":identity_card_number": put_user.identityCardNumber,
            ":phone_number": put_user.phoneNumber,
            ":country": put_user.country,
            ":city": put_user.city,
            ":address": put_user.address,
            ":user_group": put_user.group,
            ":rh": put_user.rh,
            ":eps": put_user.eps,
            ":emergency_contact_name": put_user.emergencyContactName,
            ":emergency_contact_phone_number": put_user.emergencyContactPhoneNumber,
            ":emergency_contact_relationship": put_user.emergencyContactRelationship,
            ":user_status": put_user.status,
            ":shirt_number": put_user.shirtNumber,
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_user_id": put_user.id}

@app.put("/users/{user_id}/disable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def disable_user(user_id: str):
    table = _get_user_table()
    cog_wrapper.disable_user(user_id)
    table.update_item(
        Key={"id": user_id},
        UpdateExpression="SET user_status = :user_status",
        ExpressionAttributeValues={
            ":user_status": UserStatus.DISABLED,
            },
        ReturnValues="ALL_NEW",
    )
    return {"disabled_user_id": user_id}

@app.put("/users/{user_id}/enable", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def enable_user(user_id: str):
    table = _get_user_table()
    cog_wrapper.enable_user(user_id)
    table.update_item(
        Key={"id": user_id},
        UpdateExpression="SET user_status = :user_status",
        ExpressionAttributeValues={
            ":user_status": UserStatus.ACTIVE,
            },
        ReturnValues="ALL_NEW",
    )
    return {"enabled_user_id": user_id}


@app.put("/users/{user_id}/avatar", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def update_user_avatar_url(user_id: str, userAvatar: PutUserAvatar):
    table = _get_user_table()
    table.update_item(
        Key={"id": user_id},
        UpdateExpression="SET avatar_url = :avatar_url",
        ExpressionAttributeValues={
            ":avatar_url": userAvatar.avatar_url,
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_avatar_url": userAvatar.avatar_url}

@app.post("/users/{user_id}/generate-presigned-url", dependencies=[Depends(PermissionChecker(required_permissions=['user', 'admin']))])
async def generate_user_presigned_url(user_id: str, files:list, user = Depends(get_current_user)):
    s3 = boto3.client('s3')
    presigned_urls = {}
    auth_user_id = user["sub"]
    if auth_user_id != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    env = _get_env()
    folder_path = f"{env}/users/{user_id}/profile_photos/"
    bucket_name = _get_s3_bucket_name()
    file_name = files[0]['file_name']
    content_type = files[0]['content_type']
    try:
        full_key = f"{folder_path}{file_name}"
        presigned_url = s3.generate_presigned_url(
            ClientMethod = 'put_object',
            Params={'Bucket': bucket_name, 'Key': full_key, 'ContentType': content_type},
            ExpiresIn=3600
        )
        presigned_urls['get_presigned_url'] = _generate_get_presigned_url(bucket_name, full_key)
        presigned_urls['put_presigned_url'] = presigned_url
        presigned_urls['key'] = full_key
    except Exception as e:
        raise HTTPException(status_code=500, detail=e)

    return {"urls": presigned_urls}

@app.delete("/users/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_user(user_id: str):
    cog_wrapper.delete_user(user_id)
    table = _get_user_table()
    table.delete_item(Key={"id": user_id})
    return {"deleted_user_id": user_id}

@app.get("/payment_requests", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_payment_requests(user_id: Optional[str] = None, workspace_id: Optional[str] = None):
    items_mapped = []
    if user_id:
        table = _get_payment_request_table()
        response = table.query(
            IndexName="user_index",
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        items = response.get("Items")
        items_mapped = [_map_payment_request(item, False) for item in items]
        return items_mapped
    
    if workspace_id:
        table = _get_payment_request_table()
        response = table.scan()
        items = response.get("Items")
        items_mapped = [_map_payment_request(item, False) for item in items if item.get("user_group") == workspace_id]
        return items_mapped
    
    table = _get_payment_request_table()
    response = table.scan()
    items = response.get("Items")
    items_mapped = [_map_payment_request(item, False) for item in items]
    return items_mapped


@app.get("/payment_requests/{payment_request_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_payment_request(payment_request_id: str):
    table = _get_payment_request_table()
    response = table.get_item(Key={"id": payment_request_id})
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    return _map_payment_request(item)

@app.post("/payment_requests", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_payment_requests(put_payment_request: PutPaymentRequest):
    created_time = int(time.time())
    table = _get_payment_request_table()
    response = []
    if put_payment_request.paymentRequestTo is not None:
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
                "overdue_price": put_payment_request.overduePrice,
                "payment_status": PaymentRequestStatus.PENDING,
                "created_time": created_time,
            }
            if type(item['payment_request_to']['user_metrics']['puntaje_asistencia']) == float:
                item['payment_request_to']['user_metrics']['puntaje_asistencia'] = decimal.Decimal(item['payment_request_to']['user_metrics']['puntaje_asistencia'])
            table.put_item(Item=item)
            response.append(_map_payment_request(item, False))
            send_payment_request_creation_notification(user["email"], user["name"], put_payment_request.concept, put_payment_request.userPrice, put_payment_request.dueDate)
            send_payment_request_creation_magic_bell_notification(user["email"], put_payment_request.concept, put_payment_request.userPrice, put_payment_request.dueDate)
    return response

@app.put("/payment_requests/{payment_request_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_payment_request(payment_request_id: str, put_payment_request: PutPaymentRequest):
    table = _get_payment_request_table()
    response = table.get_item(Key={"id": put_payment_request.id})
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    
    table.update_item(
        Key={"id": put_payment_request.id},
        UpdateExpression="SET create_date = :create_date, due_date = :due_date, concept = :concept, description = :description, category = :category, user_group = :user_group, user_price = :user_price, overdue_price = :overdue_price, payment_status = :payment_status",
        ExpressionAttributeValues={
            ":create_date": put_payment_request.createDate, 
            ":due_date": put_payment_request.dueDate,
            ":concept": put_payment_request.concept,
            ":description": put_payment_request.description,
            ":category": put_payment_request.category,
            ":user_group": put_payment_request.group,
            ":user_price": put_payment_request.userPrice,
            ":overdue_price": put_payment_request.overduePrice,
            ":payment_status": put_payment_request.status
            },
        ReturnValues="ALL_NEW",
    )
    user = item["payment_request_to"]
    new_item = table.get_item(Key={"id": put_payment_request.id}).get("Item")
    notification_fields_changed = get_notification_fields_changed(item, new_item)
    if notification_fields_changed:
        send_payment_request_update_notification(user["email"], user["name"], new_item['concept'], notification_fields_changed)
        send_payment_request_update_magic_bell_notification(user["email"], new_item['concept'])

    return {"updated_payment_request_id": put_payment_request.id}

@app.delete("/payment_requests/{payment_request_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_payment_request(payment_request_id: str):
    table = _get_payment_request_table()
    table.delete_item(Key={"id": payment_request_id})
    return {"deleted_payment_request_id": payment_request_id}

@app.get("/users/{user_id}/metrics", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
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
            "last_update": datetime.now().isoformat(),
            "puntaje_asistencia_description": "",
            "puntaje_asistencia": 3 
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

@app.put("/users/{user_id}/metrics", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_user_metrics(user_id: str, put_user_metrics: PutUserMetrics):
    table = _get_user_table()
    last_update = datetime.now().isoformat()
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

@app.get("/users/{user_id}/late_arrives", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_late_arrives(user_id: str):
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    items = [item for item in items if item['user_status'] == UserStatus.ACTIVE]
    cog_users = cog_wrapper.list_users()
    users_mapped = map_users(items, cog_users)
    late_arrives = []
    for user in users_mapped:
        late_arrive = {
            "id": user["id"],
            "avatarUrl": user["avatarUrl"], 
            "name": user["name"], 
            "description": user["user_metrics"]["puntaje_asistencia_description"], 
            "rating": user["user_metrics"]["puntaje_asistencia"], 
            "postedAt": user["user_metrics"]["last_update"],
            "group": user["group"],
        }
        if user["id"] == user_id:
            late_arrives.insert(0, late_arrive)
        elif user["user_metrics"]["puntaje_asistencia"] < 3:
            late_arrives.append(late_arrive)
    return late_arrives

@app.post("/set_default_late_arrives", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def set_default_late_arrives():
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    for item in items:
        user_metrics = item.get("user_metrics")
        if user_metrics:
            user_metrics["puntaje_asistencia"] = 3
            user_metrics["puntaje_asistencia_description"] = "No tienes faltas ni llegadas tarde"
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET user_metrics = :user_metrics",
                ExpressionAttributeValues={
                    ":user_metrics": user_metrics,
                    },
                ReturnValues="ALL_NEW",
            )
    return {"message": "Default late arrives set"}

@app.get("/calendar")
async def get_calendar(workspace_id: str = Query(None)):
    table = _get_calendar_table()
    response = table.scan()
    items = response.get("Items")
    if workspace_id:
        items = [item for item in items if item.get("user_group") == workspace_id]
    mapped_events = [_map_calendar_event(item) for item in items]
    return mapped_events

@app.post("/calendar", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_calendar_event(put_calendar_event: PutCalendarEvent):
    table = _get_calendar_table()
    calendar_event_id = f"{uuid4().hex}"
    calendar_item = {
        "id": calendar_event_id,
        "all_day": put_calendar_event.allDay,
        "color": put_calendar_event.color,
        "description": put_calendar_event.description,
        "event_location": put_calendar_event.location,
        "event_start": put_calendar_event.start,
        "event_end": put_calendar_event.end,
        "title": put_calendar_event.title,
        "category": put_calendar_event.category,
        "participants": {},
        "user_group": put_calendar_event.group,
        "create_tour": put_calendar_event.createTour,
        "tour_id": None,
    }
    if put_calendar_event.createTour:
        put_tour = _get_tour_from_calendar_event(put_calendar_event)
        put_tour.calendarEventId = calendar_event_id
        tour = await create_tour(put_tour)
        calendar_item["tour_id"] = tour["id"]
    table.put_item(Item=calendar_item)
    users_from_group = _get_users_from_group(put_calendar_event.group)
    user_emails = [user["email"] for user in users_from_group]
    _send_event_creation_magic_bell_notification(user_emails, put_calendar_event)
    return _map_calendar_event(calendar_item)

@app.put("/calendar", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_calendar_event(put_calendar_event: PutCalendarEvent):
    table = _get_calendar_table()
    old_event_item = table.get_item(Key={"id": put_calendar_event.id}).get("Item")
    if not old_event_item:
        raise HTTPException(status_code=404, detail=f"Event {put_calendar_event.id} not found")
    
    update_expression, values = _get_calendar_update_expression_and_values(put_calendar_event)
    table.update_item(
        Key={"id": put_calendar_event.id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=values,
        ReturnValues="ALL_NEW",
    )
    new_event_item = table.get_item(Key={"id": put_calendar_event.id}).get("Item")
    if put_calendar_event.createTour and not old_event_item.get("create_tour", False): # Create tour
        put_tour = _get_tour_from_calendar_event(put_calendar_event)
        participants = new_event_item.get("participants", {})
        bookers = {}
        if participants:
            for participant in participants:
                bookers[participant] = {
                    "id": participant,
                    "name": participants[participant],
                    "avatarUrl": None,
                    "approved": True,
                    "late": False,
                    "yellowCard": False,
                    "redCard": False,
                    "mvp": False,
                    "goals": 0,
                    "assists": 0,
                }
        put_tour.bookers = bookers
        put_tour.calendarEventId = put_calendar_event.id
        tour = await create_tour(put_tour)
        new_event_item["tour_id"] = tour["id"]
        table.update_item(
            Key={"id": put_calendar_event.id},
            UpdateExpression="SET tour_id = :tour_id",
            ExpressionAttributeValues={":tour_id": tour["id"]},
            ReturnValues="ALL_NEW",
        )
    if put_calendar_event.createTour and old_event_item.get("create_tour", False): # Update tour
        relevant_fields = ["title", "event_start", "event_end", "event_location"]
        fields_changed = _get_fields_changed(old_event_item, new_event_item, relevant_fields)
        if fields_changed:
            update_expression, values = _get_partial_tour_update_expression_and_values(put_calendar_event)
            table = _get_tour_table()
            table.update_item(
                Key={"id": old_event_item["tour_id"]},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=values,
                ReturnValues="ALL_NEW",
            )
    return {"updated_event_id": put_calendar_event.id}

@app.delete("/calendar/{event_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_calendar_event(event_id: str):
    table = _get_calendar_table()
    table.delete_item(Key={"id": event_id})
    return {"deleted_event_id": event_id}

@app.post("/calendar/{event_id}/participate", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def participate_calendar_event(event_id: str, user = Depends(get_current_user), participate_data: dict = {}) -> dict:
    table = _get_calendar_table()
    user_id = user["sub"]
    calendar_item = table.get_item(Key={"id": event_id}).get("Item")
    if not calendar_item:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    if participate_data.get("value") == True and user_id not in calendar_item["participants"]:
        calendar_item["participants"][user_id] = user["name"]
    if participate_data.get("value") == False and user_id in calendar_item["participants"]:
        del calendar_item["participants"][user_id]
    table.update_item(
        Key={"id": event_id},
        UpdateExpression="SET participants = :participants",
        ExpressionAttributeValues={
            ":participants": calendar_item["participants"],
            },
        ReturnValues="ALL_NEW",
    )
    if calendar_item.get("create_tour", False):
        table = _get_tour_table()
        tour = table.get_item(Key={"id": calendar_item["tour_id"]}).get("Item")
        if not tour:
            print(f"Tour {calendar_item['tour_id']} not found")
        if participate_data.get("value") == True and user_id not in tour["bookers"]:
            user_booked = {
                "id": user_id,
                "name": user["name"],
                "avatarUrl": None,
                "guests": 1,
                "approved": True,
                "late": False,
                "yellowCard": False,
                "redCard": False,
                "mvp": False,
                "goals": 0,
                "assists": 0,

            }
            tour["bookers"][user_id] = user_booked
        if participate_data.get("value") == False and user_id in tour["bookers"]:
            del tour["bookers"][user_id]
        table.update_item(
            Key={"id": calendar_item["tour_id"]},
            UpdateExpression="SET bookers = :bookers",
            ExpressionAttributeValues={
                ":bookers": tour["bookers"],
                },
            ReturnValues="ALL_NEW",
        )
    return {"participated_event_id": event_id}

@app.post("/process_overdue_request_payments", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def process_overdue_request_payments():
    print('Processing overdue request payments')
    table = _get_payment_request_table()
    response = table.query(
        IndexName="status_index",
        KeyConditionExpression=Key("payment_status").eq(PaymentRequestStatus.PENDING)
    )
    items = response.get("Items")
    datetime_now = datetime.now()
    processed_request_payments = []
    for item in items:
        due_datetime = try_parsing_date(item["due_date"])
        due_datetime = due_datetime.replace(tzinfo=None)
        if due_datetime < datetime_now and item["payment_status"] == PaymentRequestStatus.PENDING:
            overdue_price = item.get("overdue_price", 0)
            new_price = item["user_price"] if overdue_price == 0 else overdue_price
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET payment_status = :payment_status, user_price = :user_price",
                ExpressionAttributeValues={
                    ":payment_status": PaymentRequestStatus.OVERDUE,
                    ":user_price": new_price
                    },
                ReturnValues="ALL_NEW",
            )
            processed_request_payments.append(
                {"id": item["id"], "concept": item["concept"], "to_name": item["payment_request_to"]["name"], "to_email": item["payment_request_to"]["email"]}
            )
    send_overdue_payment_magic_bell_notification(processed_request_payments)
    send_overdue_payment_notification('loga9822@hotmail.com', "Luis Garcia", processed_request_payments)
    send_overdue_payment_notification('clubdeportivovittoria+pagos@gmail.com', "Vittoria CD", processed_request_payments)
    return {"processed_request_payments": processed_request_payments}

@app.post("/send_christmas_notification", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def send_massive_christmas_notification():
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    for item in items:
        user_email = item["email"]
        user_name = item["user_name"]
        send_christmas_notification(user_email, user_name)

@app.get("/bioimpedance", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def get_bioimpedance():
    response = [
        {
            "date": "2024-01-01T00:00:00.000Z",
            "user_id": "a1b2c3",
            "user_name": "Juan Perez",
            "peso": 70,
            "grasa_corporal": 20,
            "masa_muscular": 50,
            "grasa_visceral": 10,
            "agua": 60,
            "edad_metabolica": 30,
            "resistencia_1": 500,
            "resistencia_2": 1000,
        },
        {
            "date": "2024-01-01T00:00:00.000Z",
            "user_id": "a1b2c3",
            "user_name": "Isela Creyo",
            "peso": 70,
            "grasa_corporal": 20,
            "masa_muscular": 50,
            "grasa_visceral": 10,
            "agua": 60,
            "edad_metabolica": 30,
            "resistencia_1": 500,
            "resistencia_2": 1000,
        },
    ]
    return response

@app.post("/payment_requests/{payment_request_id}/generate-presigned-urls", dependencies=[Depends(PermissionChecker(required_permissions=['user', 'admin']))])
async def generate_payment_request_presigned_urls(payment_request_id: str, files: list, user = Depends(get_current_user)):
    s3 = boto3.client('s3')
    presigned_urls = {}
    user_id = user["sub"]
    env = _get_env()
    folder_path = f"{env}/users/{user_id}/invoices/{payment_request_id}/"
    bucket_name = _get_s3_bucket_name()
    for file in files:
        try:
            full_key = f"{folder_path}{file['file_name']}"
            presigned_url = s3.generate_presigned_url(
                ClientMethod = 'put_object',
                Params={'Bucket': bucket_name, 'Key': full_key, 'ContentType': file['content_type']},
                ExpiresIn=3600
            )
            presigned_urls[file['file_name']] = presigned_url
        except Exception as e:
            return {"error": str(e)}

    return {"urls": presigned_urls}

@app.post("/payment_requests/{payment_request_id}/request_approval", dependencies=[Depends(PermissionChecker(required_permissions=['user', 'admin']))])
async def request_payment_request_approval(payment_request_id: str, file_names: list, user = Depends(get_current_user)):
    table = _get_payment_request_table()
    env = _get_env()
    response = table.get_item(Key={"id": payment_request_id})
    user_id = user["sub"]
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Payment Request {payment_request_id} not found")
    if item["payment_status"] != PaymentRequestStatus.PENDING and item["payment_status"] != PaymentRequestStatus.OVERDUE:
        raise HTTPException(status_code=400, detail=f"Payment Request {payment_request_id} is not in pending or overdue status")
    if not file_names:
        raise HTTPException(status_code=400, detail="No files were uploaded")
    images = []
    for file_name in file_names:
        folder_path = f"{env}/users/{user_id}/invoices/{payment_request_id}/"
        full_key = f"{folder_path}{file_name}"
        images.append(full_key)
    table.update_item(
        Key={"id": payment_request_id},
        UpdateExpression="SET payment_status = :payment_status, images = :images",
        ExpressionAttributeValues={
            ":payment_status": PaymentRequestStatus.APPROVAL_PENDING,
            ":images": images
            },
        ReturnValues="ALL_NEW",
    )
    user = item["payment_request_to"]
    new_item = table.get_item(Key={"id": payment_request_id}).get("Item")
    notification_fields_changed = get_notification_fields_changed(item, new_item)
    print(notification_fields_changed)
    if notification_fields_changed:
        send_payment_request_update_notification(user["email"], user["name"], new_item['concept'], notification_fields_changed)
        send_payment_request_update_notification('loga9822@hotmail.com', user["name"], new_item['concept'], notification_fields_changed)
        send_payment_request_update_notification('clubdeportivovittoria+pagos@gmail.com', user["name"], new_item['concept'], notification_fields_changed)
    return {"requested_payment_request_approval_id": payment_request_id}

@app.post("/tours", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_tour(put_tour: PutTour):
    table = _get_tour_table()
    put_tour.images = []
    put_tour.publish = "draft"
    put_tour.tourGuides = []
    put_tour.tags = []
    put_tour.scores = { "home": 0, "away": 0 }
    put_tour.createdAt = datetime.now().isoformat()
    item = {
        "id": f"{uuid4().hex}",
        "tour_name": put_tour.name,
        "images": put_tour.images,
        "publish": put_tour.publish,
        "services": put_tour.services,
        "available": put_tour.available,
        "tour_guides": put_tour.tourGuides,
        "bookers": put_tour.bookers,
        "content": put_tour.content,
        "tags": put_tour.tags,
        "event_location": put_tour.location,
        "scores": put_tour.scores,
        "created_at": datetime.now().isoformat(),
        "calendar_event_id": put_tour.calendarEventId,
        "user_group": put_tour.group,
    }
    table.put_item(Item=item)
    return _map_tour(item)

@app.get("/tours", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_tours(workspace_id: Optional[str] = None):
    table = _get_tour_table()
    response = table.scan()
    items = response.get("Items")
    if workspace_id:
        items = [item for item in items if item.get("user_group") == workspace_id]
    mapped_tours = [_map_tour(item) for item in items]
    return mapped_tours

@app.get("/tours/{tour_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_tour(tour_id: str):
    table = _get_tour_table()
    response = table.get_item(Key={"id": tour_id})
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    return _map_tour(item)

@app.put("/tours/{tour_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_tour(tour_id: str, put_tour: PutTour):
    table = _get_tour_table()
    update_expression, values = _get_tour_update_expression_and_values(put_tour)
    table.update_item(
        Key={"id": put_tour.id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=values,
        ReturnValues="ALL_NEW",
    )
    return {"updated_tour_id": put_tour.id}

@app.delete("/tours/{tour_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_tour(tour_id: str):
    table = _get_tour_table()
    table.delete_item(Key={"id": tour_id})
    return {"deleted_tour_id": tour_id}

@app.post("/tours/{tour_id}/generate-presigned-urls", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def generate_tour_presigned_urls(tour_id: str, files: list):
    s3 = boto3.client('s3')
    presigned_urls = {}
    env = _get_env()
    folder_path = f"{env}/tours/{tour_id}/"
    bucket_name = _get_s3_bucket_name()
    for file in files:
        try:
            full_key = f"{folder_path}{file['file_name']}"
            presigned_url = s3.generate_presigned_url(
                ClientMethod = 'put_object',
                Params={'Bucket': bucket_name, 'Key': full_key, 'ContentType': file['content_type']},
                ExpiresIn=3600
            )
            presigned_urls[file['file_name']] = presigned_url
        except Exception as e:
            return {"error": str(e)}

    return {"urls": presigned_urls}

@app.post("/tours/{tour_id}/add_images", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def add_tour_images(tour_id: str, file_names: list):
    table = _get_tour_table()
    response = table.get_item(Key={"id": tour_id})
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    if not file_names:
        raise HTTPException(status_code=400, detail="No images were uploaded")
    images = []
    env = _get_env()
    for file in file_names:
        folder_path = f"{env}/tours/{tour_id}/"
        full_key = f"{folder_path}{file}"
        images.append(full_key)
    table.update_item(
        Key={"id": tour_id},
        UpdateExpression="SET images = :images",
        ExpressionAttributeValues={
            ":images": images,
            },
        ReturnValues="ALL_NEW",
    )
    return {"added_images": images}

@app.patch("/tours/{tour_id}/bookers/{booker_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_tour_booker_property(tour_id: str, booker_id, patch_property: PatchProperty):
    table = _get_tour_table()
    response = table.get_item(Key={"id": tour_id})
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Tour {tour_id} not found")
    bookers = item.get("bookers", {})
    if booker_id not in bookers:
        raise HTTPException(status_code=404, detail=f"Booker {booker_id} not found in tour {tour_id}")
    booker = bookers[booker_id]
    if patch_property.name == "approved":
        booker["approved"] = patch_property.value == "True"
    elif patch_property.name == "late":
        booker["late"] = patch_property.value == "True"
    elif patch_property.name == "yellowCard":
        booker["yellowCard"] = patch_property.value == "True"
    elif patch_property.name == "redCard":
        booker["redCard"] = patch_property.value == "True"
    elif patch_property.name == "mvp":
        booker["mvp"] = patch_property.value == "True"
    elif patch_property.name == "goals":
        booker["goals"] = int(patch_property.value)
    elif patch_property.name == "assists":
        booker["assists"] = int(patch_property.value)
    else:
        raise HTTPException(status_code=400, detail=f"Property {patch_property.name} not found")
    table.update_item(
        Key={"id": tour_id},
        UpdateExpression="SET bookers = :bookers",
        ExpressionAttributeValues={
            ":bookers": bookers,
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_booker_properties": bookers}

@app.post("/update_bookers_model", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
def update_bookers_model():
    table = _get_tour_table()
    response = table.scan()
    items = response.get("Items")
    for item in items:
        bookers = item.get("bookers", {})
        for booker_id in bookers:
            booker = bookers[booker_id]
            booker["approved"] = booker.get("approved", True)
            booker["late"] = booker.get("late", False)
            booker["yellowCard"] = booker.get("yellowCard", False)
            booker["redCard"] = booker.get("redCard", False)
            booker["mvp"] = booker.get("mvp", False)
            booker["goals"] = booker.get("goals", 0)
            booker["assists"] = booker.get("assists", 0)
        table.update_item(
            Key={"id": item["id"]},
            UpdateExpression="SET bookers = :bookers",
            ExpressionAttributeValues={
                ":bookers": bookers,
                },
            ReturnValues="ALL_NEW",
        )
    return {"message": "Bookers model updated"}

@app.get("/workspaces", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
def get_workspaces(user = Depends(get_current_user)):
    table = _get_workspace_table()
    response = table.scan()
    items = response.get("Items")
    if user["custom:role"] == "admin":
        return items
    user_db = _get_user_table().get_item(Key={"id": user["sub"]}).get("Item")
    if not user_db:
        raise HTTPException(status_code=404, detail=f"User {user['sub']} not found")
    return [item for item in items if item["id"] == user_db["user_group"]]

@app.post("/workspaces", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
def create_workspace(put_workspace: PutWorkspace):
    table = _get_workspace_table()
    item = {
        "id": put_workspace.id,
        "name": put_workspace.name,
        "logo": put_workspace.logo,
        "plan": put_workspace.plan,
    }
    table.put_item(Item=item)
    return item

@app.put("/workspaces/{workspace_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
def update_workspace(workspace_id: str, put_workspace: PutWorkspace):
    table = _get_workspace_table()
    table.update_item(
        Key={"id": workspace_id},
        UpdateExpression="SET name = :name, logo = :logo, plan = :plan",
        ExpressionAttributeValues={
            ":name": put_workspace.name,
            ":logo": put_workspace.logo,
            ":plan": put_workspace.plan,
            },
        ReturnValues="ALL_NEW",
    )
    return {"updated_workspace_id": workspace_id}

@app.delete("/workspaces/{workspace_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
def delete_workspace(workspace_id: str):
    table = _get_workspace_table()
    table.delete_item(Key={"id": workspace_id})
    return {"deleted_workspace_id": workspace_id}

@app.post("/fix_payment_requests_group", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
def fix_payment_requests_group():
    table = _get_payment_request_table()
    response = table.scan()
    items = response.get("Items")
    fixed_payment_requests = []
    for item in items:
        user = item["payment_request_to"]
        if item.get("user_group") != user["group"]:
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET user_group = :user_group",
                ExpressionAttributeValues={
                    ":user_group": user["group"],
                    },
                ReturnValues="ALL_NEW",
            )
            fixed_payment_requests.append({"id": item["id"], "concept": item["concept"], "user_group": user["group"], "payment_group": item["user_group"]})
    return {"fixed_payment_requests": fixed_payment_requests}

def get_notification_fields_changed(old_payment_request, new_payment_request):
    fields_changed = []
    if old_payment_request["due_date"] != new_payment_request["due_date"]:
        fields_changed.append({"name": "due_date", "old_value": old_payment_request["due_date"], "new_value": new_payment_request["due_date"]})
    if old_payment_request["user_price"] != new_payment_request["user_price"]:
        fields_changed.append({"name": "user_price", "old_value": old_payment_request["user_price"], "new_value": new_payment_request["user_price"]})
    if old_payment_request["concept"] != new_payment_request["concept"]:
        fields_changed.append({"name": "concept", "old_value": old_payment_request["concept"], "new_value": new_payment_request["concept"]})
    if old_payment_request["payment_status"] != new_payment_request["payment_status"]:
        fields_changed.append({"name": "payment_status", "old_value": old_payment_request["payment_status"], "new_value": new_payment_request["payment_status"]})
    return fields_changed

def _get_fields_changed(old_item, new_item, fields_to_check):
    fields_changed = []
    for field in fields_to_check:
        if old_item[field] != new_item[field]:
            fields_changed.append({"name": field, "old_value": old_item[field], "new_value": new_item[field]})
    return fields_changed

def send_payment_request_creation_notification(user_email, user_name, concept, value, due_date):
    if isinstance(value, str):
        value = int(value)

    due_date_datetime = try_parsing_date(due_date)
    formatted_due_date = due_date_datetime.strftime("%d/%m/%Y")
    response = courier_client.send_message(
        message={
            'to': {'email': user_email},
            'template': 'AW5D9440CF4MZAH1CHWVA2D0DP4D', 
            'data': {
                "userName": user_name,
                "concept": concept,
                "value": locale.currency(value, grouping=True),
                "dueDate": formatted_due_date,
            }
        }
    )
    return response

def send_payment_request_creation_magic_bell_notification(user_email, concept, value, due_date):
    formatted_due_date = try_parsing_date(due_date).strftime("%d/%m/%Y")
    title = "Nuevo pago pendiente"
    concept = f"Se ha generado un nuevo pago por {locale.currency(value, grouping=True)} con fecha de vencimiento el {formatted_due_date}"
    category = "payment_request_created"
    _send_magicbell_notification(user_email, title, concept, category)

def send_payment_request_update_magic_bell_notification(user_email, concept):
    title = "Actualización en cobro"
    content = f"Se han registrado cambios en el cobro: {concept}"
    category = "payment_request_updated"
    _send_magicbell_notification(user_email, title, content, category)

def send_overdue_payment_magic_bell_notification(overdue_payment_requests):
    for payment_request in overdue_payment_requests:
        user_email = payment_request["to_email"]
        title = "Pago vencido"
        content = f"El pago por {payment_request['concept']} se encuentra vencido"
        category = "payment_request_overdue"
        _send_magicbell_notification(user_email, title, content, category)


@app.post("/send_payment_request_creation_push_notification", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
def send_payment_request_creation_push_notification(user_id):

    url = "https://api.magicbell.com/broadcasts"

    user = _get_user_table().get_item(Key={"id": user_id}).get("Item")

    payload = {
        "broadcast": {
            "title": "Bienvenido a tu club Vittoria CD", 
            "content": "Ahora contaremos con notificaciones para mantenerte informado en tu dia a dia",
            "category": "order_created",
            "topic": "order:33098",
            "recipients": [
                {
                    "email": user['email']
                },
            ]
        }
    }

    headers = {
        "X-MAGICBELL-API-KEY": os.environ.get("MAGICBELL_API_KEY"),
        "X-MAGICBELL-API-SECRET": os.environ.get("MAGICBELL_API_SECRET"),
    }

    response = requests.post(url, json=payload, headers=headers)
    print(response.json())

@app.get("/top_goals_and_assists")
def get_top_goals_and_assists(workspace_id: str = Query(None)):
    table = _get_tour_table()
    response = table.scan()
    items = response.get("Items")
    items = [item for item in items if item.get("user_group") == workspace_id]
    top_goals_and_assists = {}
    for item in items:
        bookers = item.get("bookers", {})
        for booker_id in bookers:
            booker = bookers[booker_id]
            if booker["goals"] > 0 or booker["assists"] > 0:
                if booker_id not in top_goals_and_assists:
                    top_goals_and_assists[booker_id] = {
                        "id": booker_id,
                        "name": booker["name"],
                        "avatarUrl": booker["avatarUrl"],
                        "goals": 0,
                        "assists": 0,
                    }
                top_goals_and_assists[booker_id]["goals"] += booker["goals"]
                top_goals_and_assists[booker_id]["assists"] += booker["assists"]
    return list(top_goals_and_assists.values())

def send_payment_request_update_notification(user_email, user_name, concept, fields_changed):
    response = courier_client.send_message(
        message={
            'to': {'email': user_email},
            'template': 'B12CHAN5364VKVJMHZATM74CD4DE', 
            'data': {
                "userName": user_name,
                "concept": concept,
                "changes": [get_formatted_notification_field(field) for field in fields_changed]
            }
        }
    )
    return response

def send_welcome_notification(user_email, user_name):
    response = courier_client.send_message(
        message={
            'to': {'email': user_email},
            'template': 'H9MDTT27FTMKH7K3HCM1M4MDR23T', 
            'data': {
                "userName": user_name,
            }
        }
    )
    return response

def send_christmas_notification(user_email, user_name):
    response = courier_client.send_message(
        message={
            'to': {'email': user_email},
            'template': '070Z3SZX8V4YAXMTAEWKCXW2NVEV',
            'data': {
                "userName": user_name,
            }
        }
    )
    return response

def send_overdue_payment_notification(email, user_name,  overdue_payment_requests):
    response = courier_client.send_message(
        message={
            'to': {'email': email},
            'template': '40AZX1PQRGM3D0QD0R3AZG1P6AAE', 
            'data': {
                "userName": user_name,
                "overduePaymentRequests": overdue_payment_requests
            }
        }
    )
    return response

def _send_event_creation_magic_bell_notification(user_emails, calendar_event):
    title = "Nuevo evento: " + calendar_event.title
    location = calendar_event.location
    description = calendar_event.description
    event_datetime = parse_timestamp_to_datetime(calendar_event.start)
    event_day = parse_datetime_to_pretty_es(event_datetime)
    content = f"Inscribete ya! {event_day} en {location}. {description}"
    category = "tour_created"
    _send_bulk_magicbell_notification(user_emails, title, content, category)


def _send_magicbell_notification(user_email, title, content, category):
    url = "https://api.magicbell.com/broadcasts"

    payload = {
        "broadcast": {
            "title": title, 
            "content": content,
            "category": category,
            "action_url": "https://portal.sportsmanage.app/dashboard/invoice/user-list",
            "recipients": [
                {
                    "email": user_email
                },
            ]
        }
    }

    headers = {
        "X-MAGICBELL-API-KEY": os.environ.get("MAGICBELL_API_KEY"),
        "X-MAGICBELL-API-SECRET": os.environ.get("MAGICBELL_API_SECRET"),
    }

    response = requests.post(url, json=payload, headers=headers)
    print(response.json())

def _send_bulk_magicbell_notification(user_emails, title, content, category):
    url = "https://api.magicbell.com/broadcasts"

    payload = {
        "broadcast": {
            "title": title, 
            "content": content,
            "category": category,
            "action_url": "https://portal.sportsmanage.app/dashboard/invoice/user-list",
            "recipients": [
                {
                    "email": email
                } for email in user_emails
            ]
        }
    }

    headers = {
        "X-MAGICBELL-API-KEY": os.environ.get("MAGICBELL_API_KEY"),
        "X-MAGICBELL-API-SECRET": os.environ.get("MAGICBELL_API_SECRET"),
    }

    response = requests.post(url, json=payload, headers=headers)
    print(response.json())

def get_formatted_notification_field(field):
    if field["name"] == "due_date":
        old_value = try_parsing_date(field["old_value"])
        old_value = old_value.strftime("%d/%m/%Y")
        new_value = try_parsing_date(field["new_value"])
        new_value = new_value.strftime("%d/%m/%Y")
        return {"name": "Fecha de vencimiento", "old_value": old_value, "new_value": new_value}
    if field["name"] == "user_price":
        return {"name": "Valor", "old_value": locale.currency(field["old_value"], grouping=True), "new_value": locale.currency(field["new_value"], grouping=True)}
    if field["name"] == "concept":
        return {"name": "Concepto", "old_value": field["old_value"], "new_value": field["new_value"]}
    if field["name"] == "payment_status":
        english_to_spanish_status = {
            "paid": "Pagado",
            "pending": "Pendiente",
            "overdue": "Vencido",
            "canceled": "Cancelado",
            "approval_pending": "Pendiente de aprobación"
        }
        return {"name": "Estado", "old_value": english_to_spanish_status[field["old_value"]], "new_value": english_to_spanish_status[field["new_value"]]}
    
def _get_tour_from_calendar_event(put_calendar_event: PutCalendarEvent):
    services = []
    if put_calendar_event.group == "male":
        services.append("Vittoria Masculino")
    if put_calendar_event.group == "female":
        services.append("Vittoria Femenino")
    put_tour = PutTour(
        name=put_calendar_event.title,
        images=[],
        publish="draft",
        services=services,
        available={
            "startDate": put_calendar_event.start,
            "endDate": put_calendar_event.end
        },
        tourGuides=[],
        bookers={},
        content="",
        tags=[],
        location=put_calendar_event.location,
        scores={"home": 0, "away": 0},
        calendarEventId=put_calendar_event.id,
        group=put_calendar_event.group
    )
    return put_tour
    
def _generate_get_presigned_url(bucket_name, full_key):
    if not full_key:
        return None
    try:
        s3 = boto3.client('s3')
        presigned_url = s3.generate_presigned_url(
            ClientMethod = 'get_object',
            Params={'Bucket': bucket_name, 'Key': full_key},
            ExpiresIn=3600
        )
        return presigned_url
    except Exception as e:
        print(f"Error fetching {full_key} {str(e)}")
        return None
    
def _get_s3_public_url(bucket_name, full_key):
    if not full_key:
        return None
    return f"https://{bucket_name}.s3.amazonaws.com/{full_key}"

def _get_user_table():
    table_name = os.environ.get("USER_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_payment_request_table():
    table_name = os.environ.get("PAYMENT_REQUEST_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_calendar_table():
    table_name = os.environ.get("CALENDAR_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_tour_table():
    table_name = os.environ.get("TOUR_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_workspace_table():
    table_name = os.environ.get("WORKSPACE_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_s3_bucket_name():
    return os.environ.get("BUCKET_NAME")

def _get_env():
    return os.environ.get("ENV")

def map_users(db_users, cog_users):
    cog_users_map = {user["Username"]: user for user in cog_users}
    users = []
    for db_user in db_users:
        cog_user = cog_users_map[db_user["id"]]
        db_user["confirmation_status"] = UserConfirmationStatus.CONFIRMED if cog_user["UserStatus"] == "CONFIRMED" else UserConfirmationStatus.PENDING
        user = _map_user(db_user)
        users.append(user)
    return users

def _map_user(item):
    item["name"] = item.pop("user_name", None)
    item["phoneNumber"] = item.pop("phone_number", None)
    item["avatarUrl"] = _generate_get_presigned_url(_get_s3_bucket_name(),item.pop("avatar_url", None))
    item["status"] = item.pop("user_status", None)
    item["group"] = item.pop("user_group", None)
    item["confirmationStatus"] = item.pop("confirmation_status", None)
    item["identityCardNumber"] = item.pop("identity_card_number", None)
    item["emergencyContactName"] = item.pop("emergency_contact_name", None)
    item["emergencyContactPhoneNumber"] = item.pop("emergency_contact_phone_number", None)
    item["emergencyContactRelationship"] = item.pop("emergency_contact_relationship", None)
    item["shirtNumber"] = item.pop("shirt_number", None)
    return item

def _map_payment_request(item, get_presigned_url = True):
    item["createDate"] = item.pop("create_date", None)
    item["dueDate"] = item.pop("due_date", None)
    item["status"] = item.pop("payment_status", None)
    item["paymentRequestTo"] = item.pop("payment_request_to", None)
    item["totalAmount"] = item.pop("user_price", None)
    item["overduePrice"] = item.pop("overdue_price", None)
    item["group"] = item.pop("user_group", None)
    item["userId"] = item.pop("user_id", None)
    item["createdTime"] = item.pop("created_time", None)
    if get_presigned_url:
        item["images"] = [_generate_get_presigned_url(_get_s3_bucket_name(), image) for image in item.get("images", [])]
    return item

def _map_calendar_event(item):
    item["allDay"] = item.pop("all_day", None)
    item["start"] = item.pop("event_start", None)
    item["end"] = item.pop("event_end", None)
    item["group"] = item.pop("user_group", None)
    item["tourId"] = item.pop("tour_id", None)
    item["location"] = item.pop("event_location", None)
    item["createTour"] = item.pop("create_tour", None)
    return item

def _map_tour(item):
    item["name"] = item.pop("tour_name", None)
    item["createdAt"] = item.pop("created_at", None)
    item["tourGuides"] = item.pop("tour_guides", None)
    item["location"] = item.pop("event_location", None)
    item["calendarEventId"] = item.pop("calendar_event_id", None)
    item["group"] = item.pop("user_group", None)
    item["images"] = [_get_s3_public_url(_get_s3_bucket_name(), image) for image in item.get("images", [])]
    return item

def _get_partial_tour_update_expression_and_values(put_calendar_event):
    update_expression = "SET tour_name = :tour_name, available = :available, event_location = :event_location"
    values = {
        ":tour_name": put_calendar_event.title,
        ":available": {
            "startDate": put_calendar_event.start,
            "endDate": put_calendar_event.end
        },
        ":event_location": put_calendar_event.location
    }
    return update_expression, values

def _get_tour_update_expression_and_values(put_tour):
    excluded_keys = ["id"]
    custom_mapping_keys = {"name": "tour_name", "location": "event_location"}
    return _get_update_expression_and_values(put_tour, excluded_keys, custom_mapping_keys)

def _get_calendar_update_expression_and_values(put_calendar_event: PutCalendarEvent):
    excluded_keys = ["id", "participants"]
    custom_mapping_keys = {"start": "event_start", "end": "event_end", "group": "user_group", "location": "event_location"}
    return _get_update_expression_and_values(put_calendar_event, excluded_keys, custom_mapping_keys)

def _get_update_expression_and_values(put_item, excluded_keys, custom_mapping_keys={}):
    update_expression = "SET "
    values = {}
    for key, value in put_item.dict().items():
        if key not in excluded_keys and value is not None:
            update_expression += f"{map_attribute_key(key, custom_mapping_keys)} = :{map_attribute_key(key, custom_mapping_keys)}, "
            values[f":{map_attribute_key(key, custom_mapping_keys)}"] = value
    update_expression = update_expression[:-2]
    return update_expression, values

def _get_users_from_group(group_id):
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    items = [item for item in items if item.get("user_group") == group_id]
    active_users = [item for item in items if item.get("user_status") == UserStatus.ACTIVE]
    return active_users

def map_attribute_key(key, custom_mapping_keys={}):
    if key in custom_mapping_keys:
        return custom_mapping_keys[key]
    return re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()

def try_parsing_date(text):
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try :
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    raise ValueError('no valid date format found')

def parse_timestamp_to_datetime(timestamp):
    if timestamp > 10**10:  # If the timestamp is too large, assume it's in milliseconds
        timestamp /= 1000  
    bogota_tz = pytz.timezone("America/Bogota")
    return datetime.fromtimestamp(timestamp, tz=bogota_tz)

def parse_datetime_to_pretty_es(dt):
    formatted = format_datetime(dt, "EEEE d 'de' MMMM 'de' yyyy '-' h:mm a", locale="es_CO")

    # Capitalize the first letter of the day and month
    words = formatted.split()
    if len(words) >= 4:
        words[0] = words[0].capitalize()  # Capitalize day
        words[3] = words[3].capitalize()  # Capitalize month
    formatted = " ".join(words)

    # Make AM/PM prettier
    formatted = formatted.replace("a. m.", "AM").replace("p. m.", "PM")  

    return formatted
    
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8085, reload=True)