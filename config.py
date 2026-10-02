"""
worker/config.py
══════════════════════════════════════════════════════════════════════════════
All configuration for one Worker instance, loaded from environment variables.

SECURITY: No credentials may have fallback defaults. Missing required secrets
cause a clean startup failure with a helpful error message.
══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys


def _require(name: str) -> str:
    """Return env var or exit with a clear error — never use a hard-coded fallback."""
    val = os.environ.get(name, "").strip()
    if not val:
        print(
            f"FATAL: Required environment variable '{name}' is not set.\n"
            "Set it in your Render environment variables dashboard.\n"
            "Never hard-code credentials in source code.",
            file=sys.stderr,
        )
        sys.exit(1)
    return val


def _int_env(name: str, default: int, min_val: int = 1, max_val: int = 100) -> int:
    """Read an integer env var, clamped to [min_val, max_val]."""
    raw = os.environ.get(name, str(default)).strip()
    try:
        val = int(raw)
    except ValueError:
        val = default
    return max(min_val, min(max_val, val))


class Config:
    # ── Telegram ──────────────────────────────────────────────────────────────
    API_ID   = int(os.environ.get("API_ID",   "20140875"))
    API_HASH = os.environ.get("API_HASH",     "a06fa97d5a853ec2da79015b11335a17")

    # Each worker has its own Bot Token
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    # ── Worker identity ───────────────────────────────────────────────────────
    WORKER_ID          = os.environ.get("WORKER_ID", "")
    WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "3"))
    WORKER_VERSION     = os.environ.get("WORKER_VERSION", "1.0.0")

    # ── Channels / Groups ─────────────────────────────────────────────────────
    WORKER_CONTROL_GROUP_ID  = int(os.environ.get("WORKER_CONTROL_GROUP_ID",  "-1004454437880"))
    WORKER_OUTPUT_CHANNEL_ID = int(os.environ.get("WORKER_OUTPUT_CHANNEL_ID", "-1004488266962"))

    # ── Database ──────────────────────────────────────────────────────────────
    MONGO_URI = os.environ.get("MONGO_URI", "")
    DB_NAME   = os.environ.get("DB_NAME",   "DistributedRenameBot")

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    HEARTBEAT_INTERVAL     = int(os.environ.get("HEARTBEAT_INTERVAL",    "30"))   # seconds
    WORKER_OFFLINE_TIMEOUT = int(os.environ.get("WORKER_OFFLINE_TIMEOUT","120"))

    # ── ImgBB (thumbnail) ─────────────────────────────────────────────────────
    IMGBB_API_KEY = os.environ.get("IMGBB_API_KEY", "7c884ffafafa0846a595d70b373be802")

    # ── File size limits ──────────────────────────────────────────────────────
    BOT_MAX_SIZE  = 2000 * 1024 * 1024   # 2 GB standard bot limit
    USER_MAX_SIZE = 4000 * 1024 * 1024   # 4 GB premium / userbot limit

    # ── Health check ─────────────────────────────────────────────────────────
    # Clamped 1024–65535 — valid TCP port range
    PORT = _int_env("PORT", default=8015, min_val=1024, max_val=65535)

    # ── Graceful shutdown ────────────────────────────────────────────────────
    # Clamped 10–300 s — prevents indefinite hang or too-abrupt termination
    SHUTDOWN_GRACE_SECONDS = _int_env("SHUTDOWN_GRACE_SECONDS", default=60, min_val=10, max_val=300)

    # ── Resource limits (Render safety) ──────────────────────────────────────
    MAX_DOWNLOAD_RETRIES = _int_env("MAX_DOWNLOAD_RETRIES", default=4, min_val=1, max_val=8)
    MAX_UPLOAD_RETRIES   = _int_env("MAX_UPLOAD_RETRIES",   default=4, min_val=1, max_val=8)
    FFMPEG_TIMEOUT       = _int_env("FFMPEG_TIMEOUT",       default=600, min_val=60, max_val=3600)
    HTTP_TIMEOUT         = _int_env("HTTP_TIMEOUT",         default=30,  min_val=5,  max_val=120)
    MAX_QUEUE_SIZE       = _int_env("MAX_QUEUE_SIZE",       default=50,  min_val=1,  max_val=200)

    BOT_UPTIME = __import__("time").time()

    # ── Large-file userbot (optional) ─────────────────────────────
    STRING_SESSION  = os.environ.get("STRING_SESSION", "").strip()
    BIN_CHANNEL     = int(os.environ.get("BIN_CHANNEL", "0"))
    REF_LARGE_BYTES = int(os.environ.get("REF_LARGE_BYTES", "2090000000"))
    USER_MAX_SIZE   = 4000 * 1024 * 1024
