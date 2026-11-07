# fiber.dev.console.env
from __future__ import annotations

import os
from typing import Any, Dict

from fiber.infra.ops.account.state import state


def get_slack_status() -> Dict[str, Any]:
    s = state.security
    bot = os.environ.get("FIBER_SLACK_BOT_TOKEN", "").strip()
    app = os.environ.get("FIBER_SLACK_APP_TOKEN", "").strip()
    ch = (os.environ.get("FIBER_SLACK_CHANNEL_ID", "") or getattr(s, "slack_channel", "")).strip()

    missing = [
        k for k, v in [("FIBER_SLACK_BOT_TOKEN", bot), ("FIBER_SLACK_CHANNEL_ID", ch)] if not v
    ]
    ok = not missing
    mode = "SocketMode" if app else "Webhook"

    return {
        "ok": ok,
        "channel": ch,
        "meta": f"Ch: {ch} ({mode})" if ok else f"Missing: {', '.join(missing)}",
        "missing": missing,
    }


def get_oob_status() -> Dict[str, Dict[str, Any]]:
    """하위 호환용 래퍼"""
    return {"slack": get_slack_status()}