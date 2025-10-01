from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import requests

LEVEL_COLORS = {
    "error": 15158332,
    "warning": 15844367,
    "info": 3447003,
}


@dataclass(frozen=True)
class DiscordAlertConfig:
    webhook_url: str
    username: Optional[str] = None
    avatar_url: Optional[str] = None
    timeout_s: int = 5
    environment: Optional[str] = None
    service: Optional[str] = None
    include_args: bool = True
    include_traceback: bool = True


def _safe_truncate(text: str, max_len: int = 1800) -> str:
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


def _build_embed(title: str, description: str, fields: Dict[str, str], level: str = "error") -> Dict[str, Any]:
    color = LEVEL_COLORS.get(level, LEVEL_COLORS["error"])
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": [{"name": k, "value": v, "inline": False} for k, v in fields.items() if v],
    }
    return embed


def send_discord_alert(
        cfg: DiscordAlertConfig,
        title: str,
        description: str,
        fields: Dict[str, str],
        level: str = "error",
) -> bool:
    payload: Dict[str, Any] = {
        "embeds": [_build_embed(title, description, fields, level=level)]
    }
    if cfg.username:
        payload["username"] = cfg.username
    if cfg.avatar_url:
        payload["avatar_url"] = cfg.avatar_url

    try:
        resp = requests.post(cfg.webhook_url, json=payload, timeout=cfg.timeout_s)
        return 200 <= resp.status_code < 300
    except Exception:
        return False
