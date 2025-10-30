import os
import json
import time
import hashlib
from urllib import request, error

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
APP_NAME = os.environ.get("APP_NAME", "SportsManagement")
ENV = os.environ.get("SM_ENV", "dev")

# Evita spam del mismo error (por contenedor caliente de Lambda)
_LAST_SENT: dict[str, float] = {}
_SUPPRESS_SEC = int(os.environ.get("SLACK_ALERT_SUPPRESS_SEC", "60"))  # configurable

def _truncate(text: str, max_len: int = 2500) -> str:
    return text if len(text) <= max_len else text[:max_len] + "\n… [truncated]"

def _digest(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8", "ignore"))
    return h.hexdigest()

def _should_suppress(d: str) -> bool:
    now = time.time()
    last = _LAST_SENT.get(d, 0.0)
    if now - last < _SUPPRESS_SEC:
        return True
    _LAST_SENT[d] = now
    return False

def send_slack_alert(title: str, detail_md: str = "", stack: str = "", level: str = "error"):
    """
    Envía alerta a Slack usando Incoming Webhook con bloques.
    level: info | warning | error | critical
    """
    if not SLACK_WEBHOOK_URL:
        # Evita romper producción si olvidaste la var de entorno
        print("[slack_alerts] SLACK_WEBHOOK_URL not set. Title:", title)
        return

    icon = {
        "info": ":information_source:",
        "warning": ":warning:",
        "error": ":rotating_light:",
        "critical": ":fire:",
    }.get(level, ":rotating_light:")

    header = f"{icon} *{APP_NAME}* · `{ENV}` · *{title}*"
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": header}}]

    if detail_md:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": detail_md}})

    if stack:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{_truncate(stack)}```"}
        })

    payload = {"text": f"{APP_NAME} {ENV} {title}", "blocks": blocks}

    try:
        req = request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=5) as resp:
            _ = resp.read()
    except error.URLError as e:
        print("[slack_alerts] Failed sending to Slack:", e)

from typing import Union
def alert_with_stack(title: str, detail_fields: dict[str, Union[str, None]], stack: str, level="critical"):
    """
    Construye un bloque de detalles en Markdown, aplica deduplicación y envía.
    """
    # MD con campos clave
    lines = []
    for k, v in detail_fields.items():
        if v is not None and v != "":
            lines.append(f"*{k}:* `{v}`")
    detail_md = "\n".join(lines)

    d = _digest(title, detail_md, stack or "")
    if _should_suppress(d):
        return

    send_slack_alert(title=title, detail_md=detail_md, stack=stack, level=level)
