"""
worker/worker_bot.py
══════════════════════════════════════════════════════════════════════════════
Worker Bot entry point — Bot-only edition (no String Session).

Deployment: Docker on Render (env: docker in render.yaml).
FFmpeg is installed via apt in Dockerfile — build.sh is NOT used.

Startup sequence
────────────────
  1. Validate config (missing secrets → clean exit)
  2. Check FFmpeg availability
  3. Connect Worker Bot (Pyrogram Client with BOT_TOKEN)
  4. Instantiate JobPipeline + WorkerProtocolHandler
  5. Start protocol handler (sends REGISTER, starts heartbeat)
  6. Start aiohttp health-check server (Render keep-alive)
  7. Block until SIGINT/SIGTERM

Shutdown sequence
─────────────────
  1. Stop WorkerProtocolHandler (cancels heartbeat, stops task intake)
  2. Wait up to SHUTDOWN_GRACE_SECONDS for in-flight jobs
  3. Cancel remaining tasks
  4. Stop Worker Bot client
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import signal
import sys

import pyrogram.utils
from aiohttp import web
from pyrogram import Client

from config import Config

# ── FFmpeg bootstrap — download static binary at dyno startup ────────────────
# Heroku dynos have an ephemeral filesystem: anything written during the
# release/build phase is gone when the dyno starts.  We must download the
# static FFmpeg binary here, at startup, every time the dyno boots.
# The binary is placed in ./bin/ which _find_binary() already prefers over
# the ancient system FFmpeg at /app/.heroku/activestorage-preview/bin/.

def _bootstrap_ffmpeg() -> None:
    import os
    import tarfile
    import tempfile
    import urllib.request

    bin_dir   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")
    ffmpeg_path  = os.path.join(bin_dir, "ffmpeg")
    ffprobe_path = os.path.join(bin_dir, "ffprobe")

    if (
        os.path.isfile(ffmpeg_path)  and os.access(ffmpeg_path,  os.X_OK) and
        os.path.isfile(ffprobe_path) and os.access(ffprobe_path, os.X_OK)
    ):
        print(f"[bootstrap] Static FFmpeg already present at {bin_dir}", flush=True)
        return

    os.makedirs(bin_dir, exist_ok=True)

    URLS = [
        "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz",
        "https://github.com/yt-dlp/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz",
    ]

    archive_path = None
    for url in URLS:
        try:
            print(f"[bootstrap] Downloading FFmpeg from {url} ...", flush=True)
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tar.xz")
            tmp.close()
            urllib.request.urlretrieve(url, tmp.name)
            # Quick sanity check — a real xz archive starts with \xfd7zXZ
            with open(tmp.name, "rb") as f:
                magic = f.read(6)
            if magic[:6] != b"\xfd7zXZ\x00":
                print(f"[bootstrap] Download from {url} is not a valid xz archive (got {magic!r}), trying next ...", flush=True)
                os.unlink(tmp.name)
                continue
            archive_path = tmp.name
            print(f"[bootstrap] Download OK ({os.path.getsize(archive_path)} bytes)", flush=True)
            break
        except Exception as exc:
            print(f"[bootstrap] Download from {url} failed: {exc}", flush=True)
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    if not archive_path:
        print("[bootstrap] FATAL: all FFmpeg download URLs failed. Exiting.", file=sys.stderr, flush=True)
        sys.exit(1)

    print("[bootstrap] Extracting ffmpeg and ffprobe ...", flush=True)
    try:
        with tarfile.open(archive_path, "r:xz") as tar:
            for member in tar.getmembers():
                basename = os.path.basename(member.name)
                if basename in ("ffmpeg", "ffprobe") and member.isfile():
                    dest = ffmpeg_path if basename == "ffmpeg" else ffprobe_path
                    with tar.extractfile(member) as src, open(dest, "wb") as dst:
                        dst.write(src.read())
                    os.chmod(dest, 0o755)
                    print(f"[bootstrap] Extracted {basename} → {dest}", flush=True)
    except Exception as exc:
        print(f"[bootstrap] Extraction failed: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        try:
            os.unlink(archive_path)
        except Exception:
            pass

    if not (os.path.isfile(ffmpeg_path) and os.path.isfile(ffprobe_path)):
        print("[bootstrap] FATAL: ffmpeg/ffprobe not found after extraction.", file=sys.stderr, flush=True)
        sys.exit(1)

    print(f"[bootstrap] FFmpeg ready at {bin_dir}", flush=True)


_bootstrap_ffmpeg()


def _bootstrap_mkvtoolnix() -> None:
    """
    Download a static mkvpropedit binary at dyno startup.
    mkvpropedit edits MKV tags IN-PLACE without remuxing any streams —
    this is the only reliable way to add metadata to MKVs that have
    broken attachment streams (font/sfnt) that cause FFmpeg exit 183.
    """
    import os
    import tarfile
    import tempfile
    import urllib.request

    bin_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")
    mkvpropedit_path = os.path.join(bin_dir, "mkvpropedit")

    if os.path.isfile(mkvpropedit_path) and os.access(mkvpropedit_path, os.X_OK):
        print(f"[bootstrap] mkvpropedit already present at {mkvpropedit_path}", flush=True)
        return

    os.makedirs(bin_dir, exist_ok=True)

    # mkvtoolnix.download keeps only the CURRENT release tarball, so the version
    # in the URL goes stale quickly.  We discover the live version first by
    # scraping the directory index, then fall back to a wide static list.
    import re as _re

    def _discover_mkvtoolnix_urls() -> list:
        """Return candidate URLs, newest-first, by scraping the download index."""
        try:
            index_url = "https://mkvtoolnix.download/linux/"
            with urllib.request.urlopen(index_url, timeout=10) as _r:
                _html = _r.read().decode(errors="replace")
            # Find all 64-bit tarball filenames in the directory listing
            _found = _re.findall(
                r'mkvtoolnix-64bit-([\d.]+)\.tar\.xz', _html
            )
            # Sort by version descending (numeric tuple sort)
            def _ver(v):
                try:
                    return tuple(int(x) for x in v.split("."))
                except Exception:
                    return (0,)
            _sorted = sorted(set(_found), key=_ver, reverse=True)
            return [
                f"https://mkvtoolnix.download/linux/mkvtoolnix-64bit-{v}.tar.xz"
                for v in _sorted
            ]
        except Exception as _de:
            print(f"[bootstrap] mkvtoolnix index discovery failed: {_de}", flush=True)
            return []

    # Combine discovered URLs with a wide static fallback list
    # (covers recent releases in case the index scrape fails)
    _static_fallback = [
        # Keep this list up-to-date when mkvtoolnix releases new versions.
        # The index-scrape above handles discovery automatically, but this
        # list is the safety net if mkvtoolnix.download is unreachable.
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-95.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-94.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-93.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-92.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-91.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-90.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-89.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-88.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-87.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-86.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-85.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-84.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-83.0.tar.xz",
        "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-82.0.tar.xz",
    ]
    _discovered = _discover_mkvtoolnix_urls()
    # Merge: discovered first (newest live version at the top), then static fallbacks
    # De-duplicate while preserving order
    _seen = set()
    STATIC_URLS = []
    for _u in _discovered + _static_fallback:
        if _u not in _seen:
            _seen.add(_u)
            STATIC_URLS.append(_u)

    for url in STATIC_URLS:
        try:
            print(f"[bootstrap] Downloading mkvtoolnix from {url} ...", flush=True)
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tar.xz")
            tmp.close()
            urllib.request.urlretrieve(url, tmp.name)
            with open(tmp.name, "rb") as f:
                magic = f.read(6)
            if magic[:6] != b"\xfd7zXZ\x00":
                print(f"[bootstrap] mkvtoolnix download not valid xz, skipping.", flush=True)
                os.unlink(tmp.name)
                continue
            print(f"[bootstrap] mkvtoolnix download OK ({os.path.getsize(tmp.name)} bytes)", flush=True)
            with tarfile.open(tmp.name, "r:xz") as tar:
                for member in tar.getmembers():
                    if os.path.basename(member.name) == "mkvpropedit" and member.isfile():
                        with tar.extractfile(member) as src, open(mkvpropedit_path, "wb") as dst:
                            dst.write(src.read())
                        os.chmod(mkvpropedit_path, 0o755)
                        print(f"[bootstrap] Extracted mkvpropedit → {mkvpropedit_path}", flush=True)
                        break
            os.unlink(tmp.name)
            if os.path.isfile(mkvpropedit_path):
                break
        except Exception as exc:
            print(f"[bootstrap] mkvtoolnix download failed: {exc}", flush=True)
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    if not os.path.isfile(mkvpropedit_path):
        print("[bootstrap] WARNING: mkvpropedit not available — FFmpeg fallback will be used.", flush=True)
    else:
        print(f"[bootstrap] mkvpropedit ready at {mkvpropedit_path}", flush=True)


_bootstrap_mkvtoolnix()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for _noisy in ("pyrogram", "aiohttp"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(f"worker.{Config.WORKER_ID}")

# ── Pyrogram large-channel ID patch ──────────────────────────────────────────
pyrogram.utils.MIN_CHAT_ID    = -999_999_999_999
pyrogram.utils.MIN_CHANNEL_ID = -1_009_999_999_999

# ── Global references ─────────────────────────────────────────────────────────
_bot_client    = None
_proto_handler = None
_pipeline      = None
_active_tasks: set[asyncio.Task] = set()
_shutdown_event = asyncio.Event()


# ──────────────────────────────────────────────────────────────────────────────
# Task dispatcher
# ──────────────────────────────────────────────────────────────────────────────

async def _on_task(task: dict) -> None:
    job_id = task.get("job_id", "?")
    t = asyncio.create_task(_pipeline.run(task), name=f"job_{job_id}")
    _active_tasks.add(t)
    t.add_done_callback(_active_tasks.discard)
    await t


# ──────────────────────────────────────────────────────────────────────────────
# Health server — lightweight, no DB/Telegram/FFmpeg calls
# ──────────────────────────────────────────────────────────────────────────────

async def _web_server() -> web.Application:
    async def health(_):
        active = len(_active_tasks)
        cap    = Config.WORKER_CONCURRENCY
        return web.Response(
            text=f"Worker {Config.WORKER_ID} OK  active={active}/{cap}"
        )

    app = web.Application()
    app.router.add_get("/",       health)
    app.router.add_get("/health", health)
    return app


# ──────────────────────────────────────────────────────────────────────────────
# Startup
# ──────────────────────────────────────────────────────────────────────────────

async def _startup() -> None:
    global _bot_client, _proto_handler, _pipeline

    logger.info(
        "[worker] Starting — id=%s concurrency=%d",
        Config.WORKER_ID, Config.WORKER_CONCURRENCY,
    )

    _bot_client = Client(
        name            = f"worker_bot_{Config.WORKER_ID}",
        api_id          = Config.API_ID,
        api_hash        = Config.API_HASH,
        bot_token       = Config.BOT_TOKEN,
        workers         = Config.WORKER_CONCURRENCY + 1,
        sleep_threshold = 30,
        in_memory       = True,
    )
    await _bot_client.start()
    me = await _bot_client.get_me()
    logger.info("[worker] Bot started: %s (@%s)", me.first_name, me.username)

    from helper.pipeline import JobPipeline
    from helper.protocol_handler import WorkerProtocolHandler as _WPH

    # FIX BUG 9: Pass _on_task directly to the constructor instead of first
    # passing _task_stub and then monkey-patching _proto_handler._on_task.
    # The old approach introduced a startup race: WorkerProtocolHandler.__init__
    # calls bot.add_handler(), making the bot immediately eligible to receive
    # TASK messages. If a TASK arrived between add_handler() and the subsequent
    # _proto_handler._on_task = _on_task reassignment, it would be dispatched
    # to _task_stub (which just logs a warning) instead of the real handler.
    # By passing the real _on_task from the start there is zero race window.
    _proto_handler = _WPH(
        bot_client         = _bot_client,
        worker_id          = Config.WORKER_ID,
        control_group_id   = Config.WORKER_CONTROL_GROUP_ID,
        capacity           = Config.WORKER_CONCURRENCY,
        heartbeat_interval = Config.HEARTBEAT_INTERVAL,
        on_task            = _on_task,        # real handler, no stub needed
    )

    _pipeline = JobPipeline(
        bot_client  = _bot_client,
        send_state  = _proto_handler.send_state,
        send_result = _proto_handler.send_result,
        send_failed = _proto_handler.send_failed,
    )

    await _proto_handler.start()

    runner = web.AppRunner(await _web_server())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", Config.PORT).start()
    logger.info("[worker] Health server on port %d", Config.PORT)

    logger.info(
        "[worker] Ready — id=%s capacity=%d group=%s",
        Config.WORKER_ID, Config.WORKER_CONCURRENCY, Config.WORKER_CONTROL_GROUP_ID,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Shutdown
# ──────────────────────────────────────────────────────────────────────────────

async def _shutdown() -> None:
    if _shutdown_event.is_set():
        return   # already shutting down
    _shutdown_event.set()

    logger.info("[worker] Initiating shutdown — id=%s", Config.WORKER_ID)

    from helper.userbot import stop_userbot
    await stop_userbot()

    # 1. Stop accepting new tasks + cancel heartbeat
    if _proto_handler:
        await _proto_handler.stop()

    # 2. Wait for in-flight jobs (up to grace period)
    if _active_tasks:
        logger.info(
            "[worker] Waiting up to %ds for %d active job(s)…",
            Config.SHUTDOWN_GRACE_SECONDS, len(_active_tasks),
        )
        try:
            await asyncio.wait_for(
                asyncio.gather(*list(_active_tasks), return_exceptions=True),
                timeout=Config.SHUTDOWN_GRACE_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "[worker] Grace period expired — cancelling %d job(s)",
                len(_active_tasks),
            )
            for t in list(_active_tasks):
                t.cancel()

    # 3. Stop bot client
    if _bot_client:
        try:
            await _bot_client.stop()
        except Exception as exc:
            logger.debug("[worker] Bot stop error: %s", type(exc).__name__)

    logger.info("[worker] Shutdown complete — id=%s", Config.WORKER_ID)


def _handle_signal(sig, loop: asyncio.AbstractEventLoop) -> None:
    logger.info("[worker] Signal %s received", sig.name)
    loop.create_task(_shutdown())


async def _main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig, loop)

    await _startup()

    try:
        await _shutdown_event.wait()
    except asyncio.CancelledError:
        pass

    await _shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
