import os
import boto3


dynamodb = boto3.resource("dynamodb")


def payment_request_table():
    return dynamodb.Table(os.getenv("PAYMENT_REQUEST_TABLE_NAME"))


def user_table():
    return dynamodb.Table(os.getenv("USER_TABLE_NAME"))


def calendar_table():
    return dynamodb.Table(os.getenv("CALENDAR_TABLE_NAME"))


def tour_table():
    return dynamodb.Table(os.getenv("TOUR_TABLE_NAME"))


def workspace_table():
    return dynamodb.Table(os.getenv("WORKSPACE_TABLE_NAME"))


def membership_table():
    return dynamodb.Table(os.getenv("MEMBERSHIPS_TABLE_NAME"))


def product_table():
    return dynamodb.Table(os.getenv("PRODUCT_TABLE_NAME"))


def order_table():
    return dynamodb.Table(os.getenv("ORDER_TABLE_NAME"))


def account_table():
    return dynamodb.Table(os.getenv("ACCOUNT_TABLE_NAME"))


def file_table():
    return dynamodb.Table(os.getenv("FILE_TABLE_NAME"))
