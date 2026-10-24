"""Shared fixtures for the finance tests: a moto DynamoDB with every table and GSI the module uses.

The schemas mirror jmanage-infra (`PaymentRequest` as deployed, the new finance tables as defined
in plans/finance-backend.md section 5), so conditions, transactions and index queries behave like
production. Use the `finance_db` fixture; tables are created fresh for every test.
"""
import os

import boto3
import pytest
from moto import mock_aws

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("AWS_REGION", "us-west-2")

# (construct name, env var, [(index name, hash key, range key | None, range type)])
S, N = "S", "N"
TABLES = {
    "PaymentRequest": ("PAYMENT_REQUEST_TABLE_NAME", [
        ("user_index", "user_id", "created_time", N),
        ("status_index", "payment_status", "created_time", N),
        ("account_id_index", "account_id", "id", S),
    ]),
    "BankAccount": ("BANK_ACCOUNT_TABLE_NAME", [("account_id_index", "account_id", "id", S)]),
    "BankAccountAdjustment": ("BANK_ADJUSTMENT_TABLE_NAME", [
        ("bank_account_id_index", "bank_account_id", "id", S),
        ("account_id_index", "account_id", "id", S),
    ]),
    "Expense": ("EXPENSE_TABLE_NAME", [("account_id_index", "account_id", "id", S)]),
    "ExpensePayment": ("EXPENSE_PAYMENT_TABLE_NAME", [
        ("expense_id_index", "expense_id", "id", S),
        ("bank_account_id_index", "bank_account_id", "id", S),
    ]),
    "BudgetItem": ("BUDGET_ITEM_TABLE_NAME", [("account_id_index", "account_id", "id", S)]),
    "BudgetQuantity": ("BUDGET_QUANTITY_TABLE_NAME", [("account_month_index", "account_id", "month", S)]),
    "PaymentSubmission": ("PAYMENT_SUBMISSION_TABLE_NAME", [("payment_request_id_index", "payment_request_id", "id", S)]),
}


def _create_table(client, name, indexes):
    attrs = {"id": S}
    for _, hash_key, range_key, range_type in indexes:
        attrs[hash_key] = attrs.get(hash_key, S if hash_key not in ("created_time",) else N)
        if range_key:
            attrs[range_key] = range_type
    client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": k, "AttributeType": v} for k, v in attrs.items()],
        GlobalSecondaryIndexes=[
            {
                "IndexName": index_name,
                "KeySchema": [{"AttributeName": hash_key, "KeyType": "HASH"}]
                + ([{"AttributeName": range_key, "KeyType": "RANGE"}] if range_key else []),
                "Projection": {"ProjectionType": "ALL"},
            }
            for index_name, hash_key, range_key, _ in indexes
        ],
    )


@pytest.fixture
def finance_db(monkeypatch):
    """Fake AWS with all finance tables + the bucket; yields the boto3 resource."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("BUCKET_NAME", "jmanage-test-bucket")
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-west-2")
        for construct, (env_var, indexes) in TABLES.items():
            table_name = f"test-{construct}"
            _create_table(client, table_name, indexes)
            monkeypatch.setenv(env_var, table_name)
        # The session module creates its resource at import time, possibly (when other test modules
        # were collected first) in another region: point it, and anything that cached it, at the fake.
        resource = boto3.resource("dynamodb", region_name="us-west-2")
        monkeypatch.setattr("repositories.ddb_session.dynamodb", resource)
        boto3.client("s3", region_name="us-west-2").create_bucket(
            Bucket="jmanage-test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        yield resource
