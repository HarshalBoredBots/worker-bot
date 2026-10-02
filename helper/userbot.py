"""Lazy Pyrogram userbot for >2 GB uploads."""
from __future__ import annotations
import logging
from typing import Optional
from pyrogram import Client
from config import Config

log = logging.getLogger(__name__)
_ub: Optional[Client] = None
_started = False


async def get_userbot() -> Optional[Client]:
    global _ub, _started
    if not Config.STRING_SESSION:
        return None
    if _started and _ub is not None:
        return _ub
    try:
        _ub = Client(
            name="worker_userbot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            session_string=Config.STRING_SESSION,
            no_updates=True,
            in_memory=True,
        )
        await _ub.start()
        me = await _ub.get_me()
        log.info("[userbot] started id=%s", me.id)
        _started = True
        return _ub
    except Exception as exc:
        log.error("[userbot] start failed: %s", type(exc).__name__)
        _ub, _started = None, False
        return None


async def stop_userbot() -> None:
    global _ub, _started
    if _ub and _started:
        try:
            await _ub.stop()
        except Exception:
            pass
    _ub, _started = None, False


def userbot_available() -> bool:
    return bool(Config.STRING_SESSION and Config.BIN_CHANNEL)
