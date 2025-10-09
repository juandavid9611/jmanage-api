import os
import boto3
from typing import Dict
from repositories.s3_keys import KeyBuilder

def _bucket_name() -> str:
    bucket = os.environ.get("BUCKET_NAME")
    if bucket is None:
        raise ValueError("BUCKET_NAME environment variable is not set")
    return bucket

def _env() -> str:
    return os.environ.get("ENV", "dev")

class S3Adapter:
    def __init__(self):
        self._s3 = boto3.client("s3")
        self._kb = KeyBuilder(env=_env())

    # -------- Generic presigners --------
    def _presign_put(
        self,
        *,
        key: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> Dict[str, str]:
        params = {"Bucket": _bucket_name(), "Key": key, "ContentType": content_type}
        url = self._s3.generate_presigned_url(
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=expires_in,
        )
        return {"key": key, "url": url}

    def _presign_get(self, *, key: str, expires_in: int = 3600) -> str:
            return self._s3.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": _bucket_name(), "Key": key},
                ExpiresIn=expires_in,
            )
    
    # -------- Generic helpers --------
    def presign_get_from_explicit_key(
        self,
        *,
        key: str,
        expires_in: int = 3600,
    ) -> str:
        return self._presign_get(key=key, expires_in=expires_in)

    # -------- Invoices (payment requests) helpers --------
    def presign_invoice_put(
        self,
        *,
        user_id: str,
        payment_request_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> Dict[str, str]:
        key = self._kb.invoice_file(user_id, payment_request_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)

    # -------- Tours helpers --------
    def presign_tour_image_put(
        self,
        *,
        tour_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> Dict[str, str]:
        key = self._kb.tour_image(tour_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)
    
    # -------- User images helpers --------
    def presign_user_profile_photo_put(
        self,
        *,
        user_id: str,
        filename: str,
        content_type: str,
        expires_in: int = 3600,
    ) -> Dict[str, str]:
        key = self._kb.user_profile_photos(user_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)