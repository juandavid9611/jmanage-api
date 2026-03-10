import json
import logging
import os
import urllib.request

from di import get_payment_request_service

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    try:
        svc = get_payment_request_service()
        result = svc.process_overdue_payments()
        logger.info("Processed %d overdue payments", len(result))
        return {"processed": len(result), "payments": result}
    except Exception as e:
        _notify_slack(f":red_circle: *Scheduled payment processor failed*\n```{e}```")
        raise


def _notify_slack(message: str):
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return
    try:
        body = json.dumps({"text": message}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        logger.warning("Failed to send Slack alert", exc_info=True)
