"""Public invitation endpoints — no auth required for GET; auth is optional on POST."""

from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from jose import JWTError, jwt
from starlette.status import HTTP_401_UNAUTHORIZED

from JWTBearer import JWTAuthorizationCredentials
from di import get_tournament_invitation_service
from services.tournament_invitation_service import (
    InvitationAuthenticationRequired,
    TournamentInvitationService,
)
from api.schemas.invitations import (
    AcceptInvitationRequest,
    AcceptInvitationResponse,
    InvitationSummary,
)

router = APIRouter(prefix="/public/invites", tags=["public-invitations"])


def _verify_bearer(authorization: str) -> tuple[str, str]:
    """Parse and verify a 'Bearer <token>' header value.

    Returns (user_id, user_email) extracted from the verified JWT claims.
    Raises HTTPException(401) on any failure.
    """
    # Import lazily to avoid circular imports at module load time.
    from auth import auth as _jwt_bearer  # JWTBearer instance wired to Cognito JWKS

    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authorization header must use Bearer scheme")

    raw_token = authorization.split(" ", 1)[1].strip()
    message, _, signature = raw_token.rpartition(".")

    try:
        creds = JWTAuthorizationCredentials(
            jwt_token=raw_token,
            header=jwt.get_unverified_header(raw_token),
            claims=jwt.get_unverified_claims(raw_token),
            signature=signature,
            message=message,
        )
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid bearer token: {exc}")

    try:
        valid = _jwt_bearer.verify_jwk_token(creds)
    except HTTPException:
        # verify_jwk_token may raise HTTPException(403) when kid is not in the JWKS
        # map; normalise all auth failures to 401 as documented on this router.
        raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail="Invalid bearer token")
    if not valid:
        raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail="Bearer token signature verification failed")

    user_id: str = creds.claims.get("sub", "")
    user_email: str = creds.claims.get("email", "")
    if not user_id or not user_email:
        raise HTTPException(status_code=401, detail="Bearer token missing required claims (sub, email)")

    return user_id, user_email


@router.get("/{token}", response_model=InvitationSummary)
def get_invite(
    token: str,
    svc: TournamentInvitationService = Depends(get_tournament_invitation_service),
):
    """Return a public summary of the invitation — no auth required."""
    summary = svc.get_public_summary(token=token)
    if not summary:
        raise HTTPException(status_code=404, detail="Invitation not found or expired")
    return summary


@router.post("/{token}/accept", response_model=AcceptInvitationResponse)
def accept_invite(
    token: str,
    body: AcceptInvitationRequest,
    svc: TournamentInvitationService = Depends(get_tournament_invitation_service),
    authorization: Optional[str] = Header(None),
):
    """Accept a team-owner invitation.

    Auth is *optional*:
    - If an ``Authorization: Bearer <jwt>`` header is present, the JWT is verified
      against the Cognito JWKS.  The ``sub`` and ``email`` claims are extracted and
      passed to the service, which enforces that the email matches the invitation.
    - If no header is present (or it is absent), the unauthenticated path runs:
      a password must be supplied in the request body, a new Cognito user is created,
      and access/id/refresh tokens are returned so the frontend can sign in immediately.

    Note: the bearer token **must** be a Cognito **id_token** (not access_token).
    The accept flow requires the ``email`` claim, which is only present in the id_token.
    Frontend: obtain it via ``fetchAuthSession()`` as ``tokens.idToken``.
    """
    user_id: Optional[str] = None
    user_email: Optional[str] = None

    if authorization:
        user_id, user_email = _verify_bearer(authorization)

    try:
        result = svc.accept(
            token=token,
            password=body.password,
            authenticated_user_id=user_id,
            authenticated_email=user_email,
        )
    except InvitationAuthenticationRequired as exc:
        # Tell the client the call is well-formed but needs auth. WWW-Authenticate
        # is a hint that a Bearer id_token is expected; the frontend ignores it.
        raise HTTPException(
            status_code=401,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return result
