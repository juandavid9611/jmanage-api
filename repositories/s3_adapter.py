import os
import boto3
from repositories.s3_keys import KeyBuilder
from utils.env_utils import _env

def _bucket_name() -> str:
    bucket = os.environ.get("BUCKET_NAME")
    if bucket is None:
        raise ValueError("BUCKET_NAME environment variable is not set")
    return bucket

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
    ) -> dict[str, str]:
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
                Params={"Bucket": _bucket_name(), "Key": key, 'ResponseContentType': 'image/png'},
                ExpiresIn=expires_in,

            )
    
    def get_s3_public_url(self, key: str) -> str :
        return f"https://{_bucket_name()}.s3.amazonaws.com/{key}"


    def presign_get_from_explicit_key(
        self,
        *,
        key: str,
        expires_in: int = 3600,
    ) -> str:
        return self._presign_get(key=key, expires_in=expires_in)

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
        expires_in: int = 3600,
    ) -> dict[str, str]:
        key = self._kb.product_image(account_id, product_id, filename)
        return self._presign_put(key=key, content_type=content_type, expires_in=expires_in)