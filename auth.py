import os

import requests
from dotenv import load_dotenv
from fastapi import Depends, HTTPException
from starlette.status import HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED

from JWTBearer import JWKS, JWTBearer, JWTAuthorizationCredentials

load_dotenv()

jwks = JWKS.parse_obj(
    requests.get(
        f"https://cognito-idp.us-west-2.amazonaws.com/{os.environ.get('USER_POOL_ID')}/.well-known/jwks.json",
        headers={"x-api-key": os.environ.get("USER_POOL_API_CLIENT_ID")},
    ).json()
)

auth = JWTBearer(jwks)

async def get_current_user(
    credentials: JWTAuthorizationCredentials = Depends(auth)
) -> str:
    try:
        return credentials.claims
    except KeyError:
        HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Username missing")

class PermissionChecker:

    def __init__(self, required_permissions: list[str]) -> None:
        self.required_permissions = required_permissions

    def __call__(self, user = Depends(get_current_user)) -> bool:
        if user['custom:role'] not in self.required_permissions:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail='User does not have the required permissions to access this resource.'
            )
        return True
    