# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
source .venv/bin/activate
python app.py                  # Run locally on port 8085 (uvicorn, hot reload)

# Package for Lambda deployment (requires Docker)
./package_for_lambda.sh        # Produces lambda_function.zip
```

## Architecture

FastAPI 0.115+ backend deployed as AWS Lambda via the Mangum adapter (`handler = Mangum(app)`). Python 3.12. Data stored in DynamoDB (one table per entity). Files in S3.

### Layered structure

```
api/           → FastAPI routers; one file per domain (tours, users, calendar, …)
api/schemas/   → Pydantic request/response models
services/      → Business logic — no HTTP, no DB access
repositories/  → DynamoDB + S3 only; one file per entity
di.py          → Manual dependency injection; all service/repo wiring lives here
core/          → Cross-cutting: logging, error handlers, request context, camelCase aliasing
builders/      → Complex object construction (e.g., tour_builder.py)
migrations/    → One-off data migration scripts (run manually)
```

When adding a new domain: create the router in `api/`, schemas in `api/schemas/`, service in `services/`, repo in `repositories/`, and wire them together in `di.py`.

### Dependency injection

`di.py` contains factory functions (`get_*_service()`) used as FastAPI `Depends()` arguments. All construction of services and repos is done here — routers never instantiate repos directly.

### Auth & permissions (`auth.py`)

- `get_current_user` — validates Cognito JWT, returns claims
- `get_user_accounts` — looks up account memberships from DynamoDB (not JWT claims, to avoid staleness)
- `get_account_id` — resolves account from `X-Account-Id` header or `account_id` query param
- `PermissionChecker(["admin"])` — account-level role guard, used as a `Depends()`
- `WorkspacePermissionChecker(["admin"])` — workspace-level role guard

### API response format

Pydantic models use camelCase aliases via `camel_alias` from `core/casing.py` so the API speaks camelCase to the frontend while Python code stays snake_case internally.

## Env vars (`.env`)

DynamoDB table names and Cognito pool IDs are injected by CDK at deploy time as Lambda environment variables. For local development, set them manually in `.env`.
