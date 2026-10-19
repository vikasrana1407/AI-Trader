"""
FastAPI Bot Telegram Alert Dispatcher with Secret Masking.
"""

import json
import logging
import threading
import urllib.request
from datetime import datetime
from typing import Optional
from .config import settings

logger = logging.getLogger("FastAPIBot.Telegram")


def mask_secrets(text: str) -> str:
    secrets = [
        settings.KOTAK_CONSUMER_KEY, settings.KOTAK_CONSUMER_SECRET,
        settings.KOTAK_PASSWORD, settings.KOTAK_MPIN, settings.KOTAK_TOTP_SECRET,
        settings.TELEGRAM_BOT_TOKEN
    ]
    out = text
    for s in secrets:
        if s and len(s) > 3 and s in out:
            out = out.replace(s, "[PROTECTED]")
    return out


import time
from typing import Optional, Dict


class TelegramAlerts:
    _alert_history: Dict[str, float] = {}
    _active_incidents: set = set()

    @staticmethod
    def send(msg: str, *, blocking: bool = False) -> bool:
        """
        Send Telegram message.
        Default is non-blocking so protect/place paths are not stalled by Telegram RTT
        (often several seconds on the first HTTPS call).
        """
        if not settings.TELEGRAM_ALERTS_ENABLED:
            logger.warning("[TELEGRAM] Alerts disabled (TELEGRAM_ALERTS_ENABLED=false). Message suppressed.")
            return False
        token = settings.TELEGRAM_BOT_TOKEN
        chat_id = settings.TELEGRAM_CHAT_ID
        if not token or not chat_id or "your_" in str(token):
            logger.error("[TELEGRAM] Credentials missing or placeholder. Message suppressed.")
            return False

        clean_text = mask_secrets(msg)

        def _dispatch() -> bool:
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                data = json.dumps({"chat_id": chat_id, "text": clean_text, "parse_mode": "HTML"}).encode()
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=5.0)
                logger.info("[TELEGRAM] Message sent OK")
                return True
            except Exception as e:
                logger.error(f"Telegram dispatch failed: {e}")
                return False

        if blocking:
            return _dispatch()

        threading.Thread(target=_dispatch, name="telegram-send", daemon=True).start()
        return True

    @classmethod
    def send_throttled(cls, incident_key: str, msg: str, cooldown_sec: float = 300.0) -> bool:
        """Sends an alert throttled by unique incident_key.
        Prevents unrelated critical alerts from suppressing each other."""
        now = time.time()
        last_ts = cls._alert_history.get(incident_key, 0.0)
        if now - last_ts < cooldown_sec:
            logger.debug(f"[TELEGRAM] Alert for '{incident_key}' suppressed by cooldown ({int(now - last_ts)}s < {cooldown_sec}s)")
            return False

        cls._alert_history[incident_key] = now
        cls._active_incidents.add(incident_key)
        cls.send(msg)
        return True

    @classmethod
    def incident_resolved(cls, incident_key: str, recovery_msg: str):
        """Sends a recovery alert if the incident was previously active, and clears active state."""
        if incident_key in cls._active_incidents:
            cls._active_incidents.discard(incident_key)
            cls._alert_history.pop(incident_key, None)
            cls.send(f"✅ <b>INCIDENT RESOLVED</b>\n{recovery_msg}")

    @classmethod
    def bot_started(cls):
        cls.send(
            f"🤖 <b>Kotak Neo FastAPI Trading Bot Started</b>\n"
            f"• <b>Mode:</b> {'🟡 DRY_RUN' if settings.DRY_RUN else '🟢 LIVE'}\n"
            f"• <b>Target Offset:</b> +₹{settings.TARGET_OFFSET}\n"
            f"• <b>SL Offset:</b> -₹{settings.SL_OFFSET}\n"
            f"• <b>SL Buffer:</b> ₹{settings.SL_LIMIT_BUFFER}\n"
            f"• <b>Time:</b> {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    @classmethod
    def orders_placed(cls, symbol: str, side: str, entry_price: float, target_qty: int, target_price: float,
                      sl_qty: int, sl_trigger: float, sl_limit: float, latency_ms: float):
        cls.send(
            f"🎯 <b>PROTECTIVE ORDERS PLACED ({latency_ms:.2f}ms)</b>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code> ({side})\n"
            f"• <b>Entry:</b> ₹{entry_price:.2f}\n"
            f"• <b>Target ({target_qty} qty):</b> Limit ₹{target_price:.2f}\n"
            f"• <b>SL ({sl_qty} qty):</b> Trigger ₹{sl_trigger:.2f} | Limit ₹{sl_limit:.2f}\n"
            f"• <b>Time:</b> {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    @classmethod
    def sl_only_placed(cls, symbol: str, side: str, entry_price: float,
                       sl_qty: int, sl_trigger: float, sl_limit: float, latency_ms: float,
                       sl_side: Optional[str] = None):
        entry_side = (side or "").upper()
        exit_side = (sl_side or "").upper()
        if not exit_side:
            exit_side = "SELL" if entry_side in ("BUY", "B") else "BUY" if entry_side in ("SELL", "S") else "—"
        cls.send(
            f"🛡️ <b>100% STOP-LOSS ORDER PLACED ({latency_ms:.2f}ms)</b>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code>\n"
            f"• <b>Your entry:</b> {entry_side} @ ₹{entry_price:.2f}\n"
            f"• <b>Bot SL order:</b> <u>{exit_side}</u> (exit only — not a new entry)\n"
            f"• <b>Structure:</b> 100% SL ONLY (No Target)\n"
            f"• <b>SL ({sl_qty} qty):</b> Trigger ₹{sl_trigger:.2f} | Limit ₹{sl_limit:.2f}\n"
            f"• <b>Time:</b> {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    @classmethod
    def target_hit(cls, symbol: str, price: float, cancelled_sl_id: str):
        cls.send(f"✅ <b>TARGET EXECUTED:</b> {symbol} @ ₹{price:.2f}\n• Cancelled companion SL <code>{cancelled_sl_id}</code>")

    @classmethod
    def sl_hit(cls, symbol: str, trigger: float, cancelled_target_id: Optional[str] = None):
        msg = f"🛑 <b>STOP LOSS HIT:</b> {symbol} @ ₹{trigger:.2f}"
        if cancelled_target_id and cancelled_target_id != "N/A":
            msg += f"\n• Cancelled companion Target <code>{cancelled_target_id}</code>"
        cls.send(msg)

    @classmethod
    def critical_error(cls, stage: str, err: str):
        cls.send(f"🚨 <b>CRITICAL TRADING ERROR</b>\n• <b>Stage:</b> {stage}\n• <b>Error:</b> <i>{err}</i>")

    @classmethod
    def position_unprotected(cls, order_id: str, symbol: str, filled_qty: int, target_qty: int, sl_qty: int, target_status: str, sl_status: str, err_msg: str):
        cls.send(
            f"🚨 <b>CRITICAL: Position partially unprotected!</b>\n"
            f"• <b>Order ID:</b> <code>{order_id}</code>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code>\n"
            f"• <b>Filled Qty:</b> {filled_qty}\n"
            f"• <b>Target ({target_qty} qty):</b> {target_status}\n"
            f"• <b>SL ({sl_qty} qty):</b> {sl_status}\n"
            f"• <b>Error:</b> <i>{err_msg}</i>\n"
            f"⚠️ <b>Action required:</b> retry/reconciliation/emergency handling."
        )

    @classmethod
    def legs_status_alert(cls, order_id: str, symbol: str, filled_qty: int,
                          leg_statuses: dict, err_msg: str):
        """
        Alerts when any leg fails in multi-leg protective placement.
        Lists each leg (e.g. A-target, A-SL, B-SL) with SUCCESS/FAILED.
        """
        lines = [f"• <b>{leg}:</b> {st}" for leg, st in leg_statuses.items()]
        legs_str = "\n".join(lines)
        cls.send(
            f"🚨 <b>CRITICAL: Position UNPROTECTED (Leg Failure)!</b>\n"
            f"• <b>Order ID:</b> <code>{order_id}</code>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code> ({filled_qty} qty)\n"
            f"{legs_str}\n"
            f"• <b>Error:</b> <i>{err_msg}</i>\n"
            f"⚠️ <b>Action required:</b> immediate manual protection needed."
        )

    @classmethod
    def protection_failed_idle_alert(
        cls, symbol: str, entry_order_id: str, filled_qty: int,
        failed_order_type: str, broker_error: str,
        failed_order_id: Optional[str] = None,
        position_status: str = "UNPROTECTED",
        protection_confirmed: bool = False
    ):
        """
        P0 Critical Alert when SL or target placement fails.
        Enforces instruction: bot has stopped automated trading actions for this position and requires manual review.
        """
        failed_id_str = f"<code>{failed_order_id}</code>" if failed_order_id else "N/A"
        cls.send(
            f"🚨 <b>CRITICAL: PROTECTION FAILED — POSITION UNPROTECTED</b>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code>\n"
            f"• <b>Entry Order ID:</b> <code>{entry_order_id}</code>\n"
            f"• <b>Filled Qty:</b> {filled_qty}\n"
            f"• <b>Failed Order Type:</b> {failed_order_type}\n"
            f"• <b>Failed Order ID:</b> {failed_id_str}\n"
            f"• <b>Broker Status / Error:</b> <i>{broker_error}</i>\n"
            f"• <b>Protection Confirmed:</b> {'YES' if protection_confirmed else 'NO'}\n"
            f"• <b>Position Status:</b> {position_status}\n"
            f"⛔ <b>AUTOMATED TRADING STOPPED:</b> The bot has stopped automated trading actions for this position and requires manual review. Persistent entry lock activated."
        )

    @classmethod
    def test_message(cls):
        cls.send("🔔 <b>Telegram Alert Test:</b> Bot communications channel operational.")

    @classmethod
    def needs_manual_action(cls, symbol: str, reason: str, order_id: str = ""):
        cls.send(
            f"🚨 <b>NEEDS MANUAL ACTION:</b> <code>{symbol}</code>\n"
            f"• <b>Order ID:</b> <code>{order_id or 'N/A'}</code>\n"
            f"• <b>Reason:</b> {reason}\n"
            f"⚠️ <b>Action required:</b> Protective orders could not be placed automatically. Manage position manually!"
        )

    @classmethod
    def notice(cls, title: str, symbol: str, reason: str, order_id: str = ""):
        """Informational alert (not a hard failure / lock)."""
        cls.send(
            f"ℹ️ <b>{title}</b>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code>\n"
            f"• <b>Order ID:</b> <code>{order_id or 'N/A'}</code>\n"
            f"• <b>Detail:</b> {reason}"
        )

    @classmethod
    def websocket_disconnected(cls, duration_sec: float):
        cls.send_throttled(
            "WS_DISCONNECT",
            f"⚠️ <b>WEBSOCKET DISCONNECTED</b>\n"
            f"• <b>Duration:</b> {round(duration_sec, 0)}s\n"
            f"• <b>Status:</b> Reconnecting with exponential backoff.\n"
            f"• <b>Fallback:</b> REST order poller is actively monitoring."
        )

    @classmethod
    def websocket_recovered(cls):
        cls.incident_resolved(
            "WS_DISCONNECT",
            f"• <b>Subsystem:</b> WebSocket Order Feed\n"
            f"• <b>Status:</b> Connection restored and real-time streaming active."
        )

    @classmethod
    def rest_poller_failed(cls, consecutive_errors: int, error_cat: str, error_detail: str):
        cls.send_throttled(
            f"REST_POLL_FAIL_{error_cat}",
            f"⚠️ <b>REST BACKUP POLLER DEGRADED</b>\n"
            f"• <b>Consecutive Failures:</b> {consecutive_errors}\n"
            f"• <b>Category:</b> {error_cat}\n"
            f"• <b>Detail:</b> <i>{mask_secrets(error_detail)}</i>\n"
            f"⚠️ <b>Action:</b> Monitoring broker API health."
        )

    @classmethod
    def rest_poller_recovered(cls):
        for cat in ("BROKER_API_ERROR", "AUTH_FAILURE", "RATE_LIMIT"):
            cls.incident_resolved(
                f"REST_POLL_FAIL_{cat}",
                f"• <b>Subsystem:</b> REST Backup Poller\n"
                f"• <b>Status:</b> Broker order book queries responding normally."
            )

    @classmethod
    def critical_monitoring_outage(cls, detail: str = ""):
        cls.send_throttled(
            "CRITICAL_MONITORING_OUTAGE",
            f"🚨 <b>CRITICAL MONITORING OUTAGE</b>\n"
            f"• Both WebSocket feed and REST poller are DOWN!\n"
            f"• <b>Detail:</b> {mask_secrets(detail) or 'No real-time execution detection available.'}\n"
            f"⚠️ <b>IMMEDIATE ACTION REQUIRED:</b> Verify VPS network, broker API, and process health!"
        )

    @classmethod
    def critical_monitoring_recovered(cls):
        cls.incident_resolved(
            "CRITICAL_MONITORING_OUTAGE",
            f"• <b>Subsystem:</b> Execution Detection\n"
            f"• <b>Status:</b> At least one execution monitoring channel is operational."
        )

    @classmethod
    def auth_failure(cls, stage: str, error_detail: str):
        cls.send_throttled(
            f"AUTH_FAIL_{stage}",
            f"🚨 <b>KOTAK NEO AUTHENTICATION FAILURE</b>\n"
            f"• <b>Stage:</b> {stage}\n"
            f"• <b>Detail:</b> <i>{mask_secrets(error_detail)}</i>\n"
            f"⚠️ Session expired or invalid credentials. Attempting thread-safe re-login."
        )

    @classmethod
    def rate_limited(cls, stage: str, retry_after: float = 0):
        cls.send_throttled(
            f"RATE_LIMIT_{stage}",
            f"⚠️ <b>KOTAK NEO RATE LIMIT HIT (HTTP 429)</b>\n"
            f"• <b>Stage:</b> {stage}\n"
            f"• <b>Backoff:</b> Backing off for {retry_after:.1f}s to respect Kotak API quotas."
        )

    @classmethod
    def sl_placement_failed(cls, order_id: str, symbol: str, qty: int, err: str):
        cls.send_throttled(
            f"SL_FAIL_{order_id}",
            f"🚨 <b>STOP-LOSS PLACEMENT FAILED</b>\n"
            f"• <b>Entry ID:</b> <code>{order_id}</code>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code> ({qty} qty)\n"
            f"• <b>Error:</b> <i>{mask_secrets(err)}</i>\n"
            f"⚠️ <b>Action required:</b> Position is UNPROTECTED! Manual hedge or exit required immediately."
        )

    @classmethod
    def sl_unverified(cls, order_id: str, symbol: str, sl_id: str, err: str):
        cls.send_throttled(
            f"SL_UNVERIFIED_{order_id}",
            f"⚠️ <b>STOP-LOSS UNCONFIRMED ON BROKER</b>\n"
            f"• <b>Entry ID:</b> <code>{order_id}</code>\n"
            f"• <b>Symbol:</b> <code>{symbol}</code>\n"
            f"• <b>SL ID:</b> <code>{sl_id}</code>\n"
            f"• <b>Issue:</b> <i>{mask_secrets(err)}</i>\n"
            f"⚠️ Protective order could not be positively verified on broker order book."
        )
