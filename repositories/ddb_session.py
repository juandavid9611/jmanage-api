import os
import boto3

def ddb_resource():
    return boto3.resource("dynamodb")

def user_table():
    return ddb_resource().Table(os.environ.get("USER_TABLE_NAME"))

def payment_request_table():
    return ddb_resource().Table(os.environ.get("PAYMENT_REQUEST_TABLE_NAME"))

def calendar_table():
    return ddb_resource().Table(os.environ.get("CALENDAR_TABLE_NAME"))

def tour_table():
    return ddb_resource().Table(os.environ.get("TOUR_TABLE_NAME"))

def workspace_table():
    return ddb_resource().Table(os.environ.get("WORKSPACE_TABLE_NAME"))

def product_table():
    return ddb_resource().Table(os.environ.get("PRODUCT_TABLE_NAME"))
