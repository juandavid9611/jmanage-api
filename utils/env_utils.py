import os


def _env() -> str:
    return os.environ.get("ENV", "dev")

def _use_mangum():
    return os.environ.get("USE_MANGUM", False)
