import re

def _clean(segment: str) -> str:
    # Prevent path traversal and weird chars; keep letters, digits, dash, underscore, dot
    s = segment.replace("/", "").replace("\\", "")
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    return s or "file"

def join(*parts: str) -> str:
    trimmed = [p.strip("/\\") for p in parts if p is not None]
    return "/".join([p for p in trimmed if p])

class KeyBuilder:
    def __init__(self, env: str = "dev"):
        self.env = env

    def user_root(self, user_id: str) -> str:
        return join(self.env, "users", _clean(user_id))

    def invoice_prefix(self, user_id: str, payment_request_id: str) -> str:
        """
        Returns the *directory-like* prefix for a payment request:
        {env}/users/{user_id}/invoices/{payment_request_id}/
        """
        prefix = join(self.user_root(user_id), "invoices", _clean(payment_request_id))
        return prefix + "/"
    
    def invoice_file(self, user_id: str, payment_request_id: str, filename: str) -> str:
            """
            Full object key under the invoice prefix, including filename.
            {env}/users/{user_id}/invoices/{payment_request_id}/{filename}
            """
            return join(self.user_root(user_id), "invoices", _clean(payment_request_id), _clean(filename))
    
    def tour_image(self, tour_id: str, filename: str) -> str:
        """
        Full object key under the tour images prefix, including filename.
        {env}/tours/{tour_id}/{filename}
        """
        return join(self.env, "tours", _clean(tour_id), _clean(filename))
    
    def user_profile_photos(self, user_id: str, filename: str) -> str:
        """
        Full object key under the user profile photos prefix, including filename.
        {env}/users/{user_id}/profile_photos/{filename}
        """
        return join(self.user_root(user_id), "profile_photos", _clean(filename))
    
    def product_image(self, product_id: str, filename: str) -> str:
        """
        Full object key under the product images prefix, including filename.
        {env}/products/{product_id}/{filename}
        """
        return join(self.env, "products", _clean(product_id), _clean(filename))
