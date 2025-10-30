from datetime import datetime
import os
import json
import time
import hashlib
from typing import Any, Union, Dict, Optional
from urllib import request, error
from zoneinfo import ZoneInfo

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
APP_NAME = os.environ.get("APP_NAME", "SportsManagement")
ENV = os.environ.get("ENV", "dev")

# Evita spam del mismo error (por contenedor caliente de Lambda)
_LAST_SENT: Dict[str, float] = {}
_SUPPRESS_SEC = int(os.environ.get("SLACK_ALERT_SUPPRESS_SEC", "60"))  # configurable
SLACK_ALERT_MENTION = os.environ.get("SLACK_ALERT_MENTION", "off")  # "here" | "channel" | "off"


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

def _cop(value: Union[int, float]) -> str:
    """Formato consistente de moneda COP sin locale."""
    try:
        return f"${float(value):,.0f} COP".replace(",", ".")
    except Exception:
        return "$0 COP"

def _slack_post_blocks(blocks: list, fallback: str):
    """Envía bloque a Slack; seguro ante fallos."""
    if not SLACK_WEBHOOK_URL:
        print("[slack] (dry-run)", fallback)
        return
    mention = ""
    if SLACK_ALERT_MENTION == "here":
        mention = "<!here> "
    elif SLACK_ALERT_MENTION == "channel":
        mention = "<!channel> "

    payload = {"text": f"{mention}{fallback}", "blocks": blocks}
    req = request.Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=6) as resp:
            _ = resp.read()
    except Exception as e:
        print("[slack] send error:", e)

def _header(text: str) -> dict:
    return {"type": "header", "text": {"type": "plain_text", "text": text}}

def _section_md(md: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": md}}

def _fields(kv: dict[str, Union[str, int, float]]) -> dict:
    f = []
    for k, v in kv.items():
        f.append({"type": "mrkdwn", "text": f"*{k}*"})
        f.append({"type": "mrkdwn", "text": f"{v}"})
    return {"type": "section", "fields": f}

def _divider() -> dict:
    return {"type": "divider"}

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

def alert_with_stack(title: str, detail_fields: Dict[str, Optional[str]], stack: str, level="critical"):
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

# ---------------------- Alertas ----------------------

def send_overdue_summary(
    user_name: str,
    pending_count: int,
    overdue_payments: list[Dict[str, Any]],
):
    """Publica en Slack el resumen del job nocturno de pagos vencidos."""
    tz = ZoneInfo("America/Bogota")
    run_date = datetime.now(tz).date().isoformat()
    count = len(overdue_payments)
    total = sum(float(p.get("user_price") or 0) for p in overdue_payments)

    preview = overdue_payments[:10]
    rows = [
        f"• `{p.get('id')}` · {p.get('concept')} · {p.get('to_name')} · {_cop(p.get('user_price') or 0)}"
        for p in preview
    ]
    if count > len(preview):
        rows.append(f"… y {count - len(preview)} más")

    blocks = [
        _header("📣 Resumen job pagos vencidos"),
        _section_md(f"*Fecha:* `{run_date}` · *Encontrados:* `{pending_count}` · *Marcados overdue:* `{count}`"),
        _fields({"Monto total afectado": _cop(total)}),
        _divider(),
    ]
    if rows:
        blocks += [
            _section_md("*Preview (máx. 10):*"),
            _section_md("\n".join(rows)),
        ]
    blocks += [
        _divider(),
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"_Env:_ `{ENV}`  ·  _App:_ `{APP_NAME}`  ·  _By:_ `{user_name}`"},
            ],
        },
    ]

    _slack_post_blocks(blocks, fallback=f"{APP_NAME} {ENV} – resumen pagos vencidos")