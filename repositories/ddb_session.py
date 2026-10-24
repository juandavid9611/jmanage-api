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


def tournament_table():
    return dynamodb.Table(os.getenv("TOURNAMENT_TABLE_NAME"))


def tournament_team_table():
    return dynamodb.Table(os.getenv("TOURNAMENT_TEAM_TABLE_NAME"))


def tournament_player_table():
    return dynamodb.Table(os.getenv("TOURNAMENT_PLAYER_TABLE_NAME"))


def tournament_match_table():
    return dynamodb.Table(os.getenv("TOURNAMENT_MATCH_TABLE_NAME"))


def tournament_match_event_table():
    return dynamodb.Table(os.getenv("TOURNAMENT_MATCH_EVENT_TABLE_NAME"))


def notification_table():
    return dynamodb.Table(os.getenv("NOTIFICATION_TABLE_NAME"))


def votation_table():
    return dynamodb.Table(os.getenv("VOTATION_TABLE_NAME"))


def tournament_invitation_table():
    return dynamodb.Table(os.getenv("TOURNAMENT_INVITATION_TABLE_NAME"))


def donation_table():
    return dynamodb.Table(os.getenv("DONATION_TABLE_NAME"))


def training_session_table():
    return dynamodb.Table(os.getenv("TRAINING_SESSION_TABLE_NAME"))


def club_tournament_table():
    return dynamodb.Table(os.getenv("CLUB_TOURNAMENT_TABLE_NAME"))


def club_roster_table():
    return dynamodb.Table(os.getenv("CLUB_ROSTER_TABLE_NAME"))


def club_match_table():
    return dynamodb.Table(os.getenv("CLUB_MATCH_TABLE_NAME"))


# ── Finance module (bank accounts, expenses, budget, payment submissions) ─────

def bank_account_table():
    return dynamodb.Table(os.getenv("BANK_ACCOUNT_TABLE_NAME"))


def bank_adjustment_table():
    return dynamodb.Table(os.getenv("BANK_ADJUSTMENT_TABLE_NAME"))


def expense_table():
    return dynamodb.Table(os.getenv("EXPENSE_TABLE_NAME"))


def expense_payment_table():
    return dynamodb.Table(os.getenv("EXPENSE_PAYMENT_TABLE_NAME"))


def budget_item_table():
    return dynamodb.Table(os.getenv("BUDGET_ITEM_TABLE_NAME"))


def budget_quantity_table():
    return dynamodb.Table(os.getenv("BUDGET_QUANTITY_TABLE_NAME"))


def payment_submission_table():
    return dynamodb.Table(os.getenv("PAYMENT_SUBMISSION_TABLE_NAME"))
