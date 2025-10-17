import os


def _env() -> str:
    return os.environ.get("ENV", "dev")

def _use_mangum() -> str:
    return os.environ.get("USE_MANGUM", "false")
