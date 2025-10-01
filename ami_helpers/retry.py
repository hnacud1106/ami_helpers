from __future__ import annotations
from typing import Tuple, Type, Optional, Any, Dict
from tenacity import (
    retry,
    wait_exponential_jitter,
    stop_after_delay,
    stop_after_attempt,
    retry_if_exception_type,
    RetryCallState,
)
import traceback

from .alerts.discord_alerts import DiscordAlertConfig, send_discord_alert

def retriable(
    ex_types: Tuple[Type[BaseException], ...],
    *,
    max_seconds: Optional[int] = 30,
    max_attempts: Optional[int] = None,
    initial_wait: float = 0.2,
    max_wait: float = 3.0,
    reraise: bool = True,
):
    stop_cond = None
    if max_seconds is not None and max_attempts is not None:
        stop_cond = (stop_after_delay(max_seconds) | stop_after_attempt(max_attempts))
    elif max_seconds is not None:
        stop_cond = stop_after_delay(max_seconds)
    elif max_attempts is not None:
        stop_cond = stop_after_attempt(max_attempts)
    else:
        stop_cond = stop_after_delay(30)

    return retry(
        wait=wait_exponential_jitter(initial=initial_wait, max=max_wait),
        stop=stop_cond,
        retry=retry_if_exception_type(ex_types),
        reraise=reraise,
    )

def retriable_with_discord(
    ex_types: Tuple[Type[BaseException], ...],
    alert_cfg: DiscordAlertConfig,
    *,
    max_seconds: Optional[int] = 30,
    max_attempts: Optional[int] = None,
    initial_wait: float = 0.2,
    max_wait: float = 3.0,
    notify_every_retry: bool = False,
    reraise: bool = True,
):
    if max_seconds is not None and max_attempts is not None:
        stop_cond = (stop_after_delay(max_seconds) | stop_after_attempt(max_attempts))
    elif max_seconds is not None:
        stop_cond = stop_after_delay(max_seconds)
    elif max_attempts is not None:
        stop_cond = stop_after_attempt(max_attempts)
    else:
        stop_cond = stop_after_delay(30)

    def _before_sleep(rs: RetryCallState) -> None:
        if not notify_every_retry:
            return
        exc = rs.outcome.exception() if rs.outcome else None
        title = f"[{alert_cfg.environment or 'env'}] Retry #{rs.attempt_number} for {getattr(rs.fn, '__name__', 'call')}"
        desc = str(exc) if exc else "Retrying..."
        fields: Dict[str, str] = {
            "service": alert_cfg.service or "",
            "function": getattr(rs.fn, "__name__", "<callable>"),
            "attempt": str(rs.attempt_number),
            "elapsed_s": f"{rs.seconds_since_start:.2f}",
        }
        if alert_cfg.include_args and rs.args:
            fields["args"] = repr(rs.args)[:1800]
        if alert_cfg.include_args and rs.kwargs:
            fields["kwargs"] = repr(rs.kwargs)[:1800]
        send_discord_alert(alert_cfg, title, desc, fields, level="warning")

    def _final_fail(rs: RetryCallState):
        exc = rs.outcome.exception() if rs.outcome else None
        title = f"[{alert_cfg.environment or 'env'}] Pipeline FAILED: {getattr(rs.fn, '__name__', 'call')}"
        desc = str(exc) if exc else "Unknown error"
        tb = ""
        if alert_cfg.include_traceback and exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[:1800]
        fields: Dict[str, str] = {
            "service": alert_cfg.service or "",
            "function": getattr(rs.fn, "__name__", "<callable>"),
            "attempts": str(rs.attempt_number),
            "elapsed_s": f"{rs.seconds_since_start:.2f}",
            "traceback": tb,
        }
        if alert_cfg.include_args and rs.args:
            fields["args"] = repr(rs.args)[:1800]
        if alert_cfg.include_args and rs.kwargs:
            fields["kwargs"] = repr(rs.kwargs)[:1800]
        send_discord_alert(alert_cfg, title, desc, fields, level="error")
        if reraise and exc:
            raise exc
        return None

    return retry(
        wait=wait_exponential_jitter(initial=initial_wait, max=max_wait),
        stop=stop_cond,
        retry=retry_if_exception_type(ex_types),
        before_sleep=_before_sleep,
        retry_error_callback=_final_fail,
        reraise=reraise,
    )
