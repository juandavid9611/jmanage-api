from datetime import datetime
import decimal
from enum import Enum
import os
import time
from typing import Optional, Union
from uuid import uuid4
from fastapi import Depends, FastAPI, HTTPException
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
    sponsorPrice: int
    sponsorPercentage: int

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

class PutCalendarEvent(BaseModel):
    id: Optional[str] = None
    allDay: Optional[bool] = None
    color: Optional[str] = None
    description: Optional[str]
    start: Optional[int]
    end: Union[int, str]
    title: Optional[str]
    category: Optional[str]
    

@app.get("/")
async def root():
    return {
        "statusCode": 200,
        "body": "Hello from Fast API Lambda!"
    }

@app.get("/users", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_users():
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
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

@app.post("/users", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_user(put_user: PutUser):
    created_time = int(time.time())
    user_response = cog_wrapper.sign_up_user("vittoria2024", put_user.email, put_user.name)

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
            "puntaje_asistencia_description": "",
            "puntaje_asistencia": 3 ,
            "last_update": datetime.now().isoformat()
        }
    }

    table = _get_user_table()
    table.put_item(Item=item)
    item["confirmation_status"] = UserConfirmationStatus.PENDING
    send_welcome_notification(put_user.email, put_user.name)
    return _map_user(item)

@app.put("/users/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
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

@app.delete("/users/{user_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_user(user_id: str):
    cog_wrapper.delete_user(user_id)
    table = _get_user_table()
    table.delete_item(Key={"id": user_id})
    return {"deleted_user_id": user_id}

@app.get("/payment_requests", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
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
                "sponsor_price": put_payment_request.sponsorPrice,
                "sponsor_percentage": put_payment_request.sponsorPercentage,
                "payment_status": PaymentRequestStatus.PENDING,
                "created_time": created_time,
            }
            if type(item['payment_request_to']['user_metrics']['puntaje_asistencia']) == float:
                item['payment_request_to']['user_metrics']['puntaje_asistencia'] = decimal.Decimal(item['payment_request_to']['user_metrics']['puntaje_asistencia'])
            table.put_item(Item=item)
            response.append(_map_payment_request(item))
            send_payment_request_creation_notification(user["email"], user["name"], put_payment_request.concept, put_payment_request.userPrice, put_payment_request.dueDate)
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
    user = item["payment_request_to"]
    new_item = table.get_item(Key={"id": put_payment_request.id}).get("Item")
    notification_fields_changed = get_notification_fields_changed(item, new_item)
    if notification_fields_changed:
        send_payment_request_update_notification(user["email"], user["name"], new_item['concept'], notification_fields_changed)
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
async def get_calendar():
    table = _get_calendar_table()
    response = table.scan()
    items = response.get("Items")
    mapped_events = [_map_calendar_event(item) for item in items]
    return mapped_events

@app.post("/calendar", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_calendar_event(put_calendar_event: PutCalendarEvent):
    table = _get_calendar_table()
    item = {
        "id": f"{uuid4().hex}",
        "all_day": put_calendar_event.allDay,
        "color": put_calendar_event.color,
        "description": put_calendar_event.description,
        "event_start": put_calendar_event.start,
        "event_end": put_calendar_event.end,
        "title": put_calendar_event.title,
        "category": put_calendar_event.category,
        "participants": {}
    }
    table.put_item(Item=item)
    return _map_calendar_event(item)

@app.put("/calendar", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_calendar_event(put_calendar_event: PutCalendarEvent):
    table = _get_calendar_table()
    update_expression, values = _get_update_expression_and_values(put_calendar_event)
    table.update_item(
        Key={"id": put_calendar_event.id},
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
    response = table.get_item(Key={"id": event_id})
    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    if participate_data.get("value") == True and user_id not in item["participants"]:
        item["participants"][user_id] = user["name"]
    if participate_data.get("value") == False and user_id in item["participants"]:
        del item["participants"][user_id]
    table.update_item(
        Key={"id": event_id},
        UpdateExpression="SET participants = :participants",
        ExpressionAttributeValues={
            ":participants": item["participants"],
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
    time_format_example = "%Y-%m-%dT%H:%M:%S.%fZ"
    processed_request_payments = []
    for item in items:
        due_datetime = datetime.strptime(item["due_date"], time_format_example)
        due_datetime = due_datetime.replace(tzinfo=None)
        if due_datetime < datetime_now and item["payment_status"] == PaymentRequestStatus.PENDING:
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET payment_status = :payment_status",
                ExpressionAttributeValues={
                    ":payment_status": PaymentRequestStatus.OVERDUE
                    },
                ReturnValues="ALL_NEW",
            )
            processed_request_payments.append(
                {"id": item["id"], "concept": item["concept"], "to_name": item["payment_request_to"]["name"], "to_email": item["payment_request_to"]["email"]}
            )
    send_overdue_payment_notification('loga9822@hotmail.com', "Luis Garcia", processed_request_payments)
    send_overdue_payment_notification('clubdeportivovittoria+pagos@gmail.com', "Vittoria CD", processed_request_payments)
    return {"processed_request_payments": processed_request_payments}

@app.post("/send_welcome_notification", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def send_massive_welcome_notification():
    table = _get_user_table()
    response = table.scan()
    items = response.get("Items")
    for item in items:
        user_email = item["email"]
        user_name = item["user_name"]
        send_welcome_notification(user_email, user_name)

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

@app.post("/generate-presigned-urls", dependencies=[Depends(PermissionChecker(required_permissions=['user', 'admin']))])
async def generate_presigned_urls(files: list, user = Depends(get_current_user)):
    s3 = boto3.client('s3')
    presigned_urls = {}
    user_id = user["sub"]
    folder_path = f"invoices/{user_id}/"
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
    bucket_name = _get_s3_bucket_name()
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
        images.append(f"https://{bucket_name}.s3.amazonaws.com/invoices/{user_id}/{file_name}")
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

def _get_payment_request_table():
    table_name = os.environ.get("PAYMENT_REQUEST_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_user_table():
    table_name = os.environ.get("USER_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_calendar_table():
    table_name = os.environ.get("CALENDAR_TABLE_NAME")
    return boto3.resource("dynamodb").Table(table_name)

def _get_s3_bucket_name():
    return os.environ.get("BUCKET_NAME")

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

def _map_calendar_event(item):
    item["allDay"] = item.pop("all_day", None)
    item["start"] = item.pop("event_start", None)
    item["end"] = item.pop("event_end", None)
    return item

def _get_update_expression_and_values(put_calendar_event):
    update_expression = "SET "
    values = {}
    excluded_keys = ["id", "participants"]
    for key, value in put_calendar_event.dict().items():
        if key not in excluded_keys and value:
            update_expression += f"{map_attribute_key(key)} = :{map_attribute_key(key)}, "
            values[f":{map_attribute_key(key)}"] = value
    update_expression = update_expression[:-2]
    print(update_expression)
    return update_expression, values

def map_attribute_key(item):
    if item == "allDay":
        return "all_day"
    if item == "start":
        return "event_start"
    if item == "end":
        return "event_end"
    return item

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
    