import os
from urllib.parse import quote, unquote, urlparse
import boto3
from repositories.s3_keys import KeyBuilder
from utils.env_utils import _env

def _bucket_name() -> str:
    bucket = os.environ.get("BUCKET_NAME")
    if bucket is None:
        raise ValueError("BUCKET_NAME environment variable is not set")
    return bucket

def public_url_for(bucket: str, region: str, key: str) -> str:
    """Region-specific public URL (no redirect). Pass-through when `key` is already a URL."""
    if key.startswith(("http://", "https://")):
        return key
    return f"https://{bucket}.s3.{region}.amazonaws.com/{quote(key, safe='/')}"


def key_from_public_url(bucket: str, value: str) -> str:
    """Inverse of public_url_for; returns `value` unchanged when it is not a URL of this bucket."""
    if not value.startswith(("http://", "https://")):
        return value
    parsed = urlparse(value)
    if parsed.netloc.startswith(f"{bucket}.s3"):
        return unquote(parsed.path).lstrip("/")
    return value


class S3Adapter:
    def __init__(self):
        self._s3 = boto3.client("s3")
        self._kb = KeyBuilder(env=_env())

    def _presign_put(
        self,
        *,
        key: str,
        content_type: str,
        expires_in: int = 3600,
        content_length: int | None = None,
    ) -> dict[str, str]:
        params = {"Bucket": _bucket_name(), "Key": key, "ContentType": content_type}
        if content_length is not None:
            params["ContentLength"] = content_length
        url = self._s3.generate_presigned_url(
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=expires_in,
        )
        return {"key": key, "url": url}

    def _presign_get(self, *, key: str, content_type: str, expires_in: int = 3600) -> str:
        params = {
            "Bucket": _bucket_name(), 
            "Key": key,
            "ResponseContentType": content_type
        }
            
        return self._s3.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=expires_in,
        )
    
    def get_s3_public_url(self, key: str) -> str:
        # Presigned URLs from Lambda role credentials die with the session (hours), so they
        # cannot satisfy a >= 7 day expiry. The bucket policy allows public s3:GetObject
        # (verified read-only), so we return the stable, region-specific public URL.
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
        return public_url_for(_bucket_name(), region, key)

    def key_from_url(self, value: str) -> str:
        return key_from_public_url(_bucket_name(), value)


    def presign_get_from_explicit_key(
        self,
        *,
        key: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> str:
        return self._presign_get(key=key, content_type=content_type, expires_in=expires_in)

    def presign_invoice_put(
        self,
        *,
        account_id: str,
        user_id: str,
        payment_request_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.invoice_file(account_id, user_id, payment_request_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)

    def presign_tour_image_put(
        self,
        *,
        account_id: str,
        tour_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.tour_image(account_id, tour_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)
    
    def presign_user_profile_photo_put(
        self,
        *,
        account_id: str,
        user_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.user_profile_photos(account_id, user_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)
    
    def presign_product_image_put(
        self,
        *,
        account_id: str,
        product_id: str,
        filename: str,
        content_type: str,
        content_length: int | None = None,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.product_image(account_id, product_id, filename)
        return self._presign_put(
            key=key, content_type=content_type, expires_in=expires_in, content_length=content_length
        )
    
    def presign_file_put(
        self,
        *,
        account_id: str,
        file_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.file(account_id, file_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)
    
    def presign_team_document_put(
        self,
        *,
        account_id: str,
        team_id: str,
        doc_type: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.team_document(account_id, team_id, doc_type, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)

    def presign_tournament_logo_put(
        self,
        *,
        account_id: str,
        tournament_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.tournament_logo(account_id, tournament_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)

    def presign_team_logo_put(
        self,
        *,
        account_id: str,
        team_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.team_logo(account_id, team_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)

    def presign_player_avatar_put(
        self,
        *,
        account_id: str,
        player_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.player_avatar(account_id, player_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)

    def delete_file(self, key: str) -> None:
        """Delete a file from S3"""
        self._s3.delete_object(Bucket=_bucket_name(), Key=key)