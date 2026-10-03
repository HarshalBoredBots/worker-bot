"""
worker/worker_bot.py
══════════════════════════════════════════════════════════════════════════════
Worker Bot entry point — Bot-only edition (no String Session).

Deployment: Heroku (web dyno). FFmpeg and mkvpropedit are downloaded at
dyno startup because Heroku's ephemeral filesystem means any binary written
during the release phase is gone when the dyno restarts.

Startup sequence
────────────────
  1. Validate config (missing secrets → clean exit)
  2. Bootstrap FFmpeg  (download static binary + verify)
  3. Bootstrap mkvpropedit  (download static binary + verify, non-fatal)
  4. Connect Worker Bot (Pyrogram Client with BOT_TOKEN)
  5. Instantiate JobPipeline + WorkerProtocolHandler
  6. Start protocol handler (sends REGISTER, starts heartbeat)
  7. Start aiohttp health-check server (Render/Heroku keep-alive)
  8. Block until SIGINT/SIGTERM

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


# ── FFmpeg bootstrap ──────────────────────────────────────────────────────────
#
# ROOT CAUSE FIXED HERE:
#
# The original code used urllib.request.urlretrieve() which:
#   a) Does NOT raise an exception on HTTP 4xx/5xx responses — it silently
#      saves whatever bytes the server sent (an HTML error page, a redirect
#      stub, a Heroku "too many connections" page, etc.)
#   b) Does NOT check Content-Length vs bytes received, so a dropped
#      connection mid-stream produces a partial/truncated file silently.
#   c) Does NOT verify that the extracted binaries are actually executable
#      and functional.
#
# On Heroku, fresh dynos experience transient network issues in the first
# ~10 seconds (CDN rate-limiting, connection resets during dyno cold-start).
# urlretrieve completes immediately with a short error-page body that starts
# with the correct XZ magic bytes by coincidence or log-display artifact,
# so the magic-bytes check can pass but tarfile.open() then fails — OR the
# check itself fails because the body is something else entirely.
#
# The fix:
#   1. Use urllib.request.urlopen() which exposes the HTTP response object.
#   2. Explicitly check response.status == 200 before writing any bytes.
#   3. Stream the download in 1 MB chunks so large files are not buffered.
#   4. After download, verify size > 10 MB (a stub/error page is always small).
#   5. After extraction, run "ffmpeg -version" and "ffprobe -version" to
#      confirm the binaries are functional before declaring bootstrap complete.
#   6. Cache: if both binaries are present AND pass -version, skip download.
#
# ─────────────────────────────────────────────────────────────────────────────

def _bootstrap_ffmpeg() -> None:
    import os
    import subprocess
    import tarfile
    import tempfile
    import time
    import urllib.error
    import urllib.request

    bin_dir      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")
    ffmpeg_path  = os.path.join(bin_dir, "ffmpeg")
    ffprobe_path = os.path.join(bin_dir, "ffprobe")

    # ── Cache check: skip download if binaries are already present AND work ──
    def _verify(path: str, name: str) -> bool:
        if not (os.path.isfile(path) and os.access(path, os.X_OK)):
            return False
        try:
            r = subprocess.run(
                [path, "-version"], capture_output=True, timeout=15
            )
            return r.returncode == 0
        except Exception:
            return False

    if _verify(ffmpeg_path, "ffmpeg") and _verify(ffprobe_path, "ffprobe"):
        print(f"[bootstrap] FFmpeg already present and verified at {bin_dir}", flush=True)
        return

    os.makedirs(bin_dir, exist_ok=True)

    URLS = [
        "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz",
        "https://github.com/yt-dlp/FFmpeg-Builds/releases/download/latest/"
        "ffmpeg-master-latest-linux64-gpl.tar.xz",
    ]

    XZ_MAGIC         = b"\xfd7zXZ\x00"
    MIN_ARCHIVE_SIZE = 10 * 1024 * 1024   # 10 MB — any valid FFmpeg tarball is >30 MB
    CHUNK            = 1024 * 1024         # 1 MB read chunks

    archive_path: str | None = None
    tmp_name: str | None = None

    for attempt, url in enumerate(URLS, 1):
        tmp_name = None
        try:
            print(
                f"[bootstrap] Downloading FFmpeg ({attempt}/{len(URLS)}): {url}",
                flush=True,
            )
            # urlopen raises urllib.error.HTTPError on 4xx/5xx — urlretrieve does not.
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "worker-bot/1.0 (Heroku dyno startup)"},
            )
            with urllib.request.urlopen(req, timeout=180) as resp:
                if resp.status != 200:
                    print(
                        f"[bootstrap] HTTP {resp.status} for {url} — skipping",
                        flush=True,
                    )
                    continue

                fd, tmp_name = tempfile.mkstemp(suffix=".tar.xz")
                total = 0
                t0 = time.monotonic()
                with os.fdopen(fd, "wb") as f:
                    while True:
                        chunk = resp.read(CHUNK)
                        if not chunk:
                            break
                        f.write(chunk)
                        total += len(chunk)

            elapsed = time.monotonic() - t0
            print(
                f"[bootstrap] Downloaded {total:,} bytes in {elapsed:.1f}s",
                flush=True,
            )

        except urllib.error.HTTPError as exc:
            print(f"[bootstrap] HTTP error {exc.code} for {url}: {exc.reason}", flush=True)
            _cleanup(tmp_name)
            # Brief wait before next URL so Heroku's network can stabilise
            if attempt < len(URLS):
                time.sleep(3)
            continue
        except urllib.error.URLError as exc:
            print(f"[bootstrap] Network error for {url}: {exc.reason}", flush=True)
            _cleanup(tmp_name)
            if attempt < len(URLS):
                time.sleep(3)
            continue
        except Exception as exc:
            print(f"[bootstrap] Unexpected error for {url}: {exc}", flush=True)
            _cleanup(tmp_name)
            if attempt < len(URLS):
                time.sleep(3)
            continue

        # ── Validate the downloaded file ─────────────────────────────────────
        # 1. Size check — error pages / stubs are always tiny
        if total < MIN_ARCHIVE_SIZE:
            print(
                f"[bootstrap] Downloaded only {total:,} bytes — not a real archive,"
                " skipping (server may have returned an error page).",
                flush=True,
            )
            _cleanup(tmp_name)
            if attempt < len(URLS):
                time.sleep(3)
            continue

        # 2. XZ magic-bytes check
        try:
            with open(tmp_name, "rb") as f:
                magic = f.read(6)
        except Exception as exc:
            print(f"[bootstrap] Cannot read downloaded file: {exc}", flush=True)
            _cleanup(tmp_name)
            continue

        if magic != XZ_MAGIC:
            print(
                f"[bootstrap] Bad magic bytes {magic!r} (expected {XZ_MAGIC!r}) — "
                "server returned non-XZ content, skipping.",
                flush=True,
            )
            _cleanup(tmp_name)
            if attempt < len(URLS):
                time.sleep(3)
            continue

        archive_path = tmp_name
        print(f"[bootstrap] Archive validated OK ({total:,} bytes)", flush=True)
        break

    if not archive_path:
        print(
            "[bootstrap] FATAL: all FFmpeg download URLs failed.\n"
            "Heroku tip: this is often a transient network issue on dyno cold-start.\n"
            "Redeploy or restart the dyno to retry.",
            file=sys.stderr, flush=True,
        )
        sys.exit(1)

    # ── Extract binaries ─────────────────────────────────────────────────────
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
                    print(
                        f"[bootstrap] Extracted {basename} "
                        f"({os.path.getsize(dest):,} bytes) → {dest}",
                        flush=True,
                    )
    except Exception as exc:
        print(f"[bootstrap] Extraction failed: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        _cleanup(archive_path)

    # ── Verify extracted binaries actually run ───────────────────────────────
    for path, name in [(ffmpeg_path, "ffmpeg"), (ffprobe_path, "ffprobe")]:
        if not os.path.isfile(path):
            print(
                f"[bootstrap] FATAL: {name} not found in archive — "
                "tarball structure may have changed.",
                file=sys.stderr, flush=True,
            )
            sys.exit(1)
        if not os.access(path, os.X_OK):
            print(
                f"[bootstrap] FATAL: {name} is not executable after chmod.",
                file=sys.stderr, flush=True,
            )
            sys.exit(1)
        try:
            result = subprocess.run(
                [path, "-version"], capture_output=True, timeout=15
            )
        except Exception as exc:
            print(
                f"[bootstrap] FATAL: {name} -version raised an exception: {exc}",
                file=sys.stderr, flush=True,
            )
            sys.exit(1)
        if result.returncode != 0:
            stderr = result.stderr.decode(errors="replace")[:200]
            print(
                f"[bootstrap] FATAL: {name} -version returned exit "
                f"{result.returncode}: {stderr}",
                file=sys.stderr, flush=True,
            )
            sys.exit(1)
        version_line = result.stdout.decode(errors="replace").split("\n")[0].strip()
        print(f"[bootstrap] Verified: {version_line}", flush=True)

    print(f"[bootstrap] FFmpeg ready at {bin_dir}", flush=True)


def _cleanup(path: str | None) -> None:
    """Silently remove a temp file; safe to call with None."""
    if path:
        try:
            import os
            os.unlink(path)
        except Exception:
            pass


_bootstrap_ffmpeg()


# ── mkvpropedit bootstrap ─────────────────────────────────────────────────────
#
# ROOT CAUSE FIXED HERE:
#
# mkvpropedit edits MKV tags IN-PLACE without remuxing any stream data.
# This is CRITICAL for anime MKV files whose video stream triggers FFmpeg
# exit 183 ("Could not write header: Invalid data found when processing
# input").  FFmpeg cannot remux these files but mkvpropedit never touches
# the streams — it only rewrites the Matroska Tags element.
#
# The original bootstrap had two problems:
#
#   Problem 1 — Static URL list was stale:
#     mkvtoolnix.download only keeps the CURRENT release tarball.
#     The static fallback list topped out at 90.0 but by the time logs
#     were collected the live version was higher (logs show 88, 86, 84
#     all returning 404). Every URL 404'd → mkvpropedit never installed.
#
#   Problem 2 — No reliable version discovery:
#     The HTML scraper searched for mkvtoolnix-64bit-*.tar.xz in the
#     directory index, but mkvtoolnix.download may not list tarballs in
#     standard anchor tags — it may only list distro packages.
#     The GitHub API is a more reliable way to find the current version.
#
# The fix:
#   1. Query GitHub API (Matroska-Org/mkvtoolnix releases/latest) to get
#      the current version tag, then build the exact download URL.
#   2. Also scrape the mkvtoolnix.download linux index as before (belt
#      and suspenders).
#   3. Extend the static fallback list to cover a wider range of recent
#      versions so the worker can still boot if both discovery methods fail.
#   4. Same HTTP validation improvements as _bootstrap_ffmpeg.
#   5. Run "mkvpropedit --version" to verify the binary before marking it
#      as ready.
#   6. NON-FATAL: if mkvpropedit is unavailable the worker still starts,
#      but add_metadata() will log a clear warning and the FFmpeg fallback
#      will be used (which may also fail for problematic MKV files —
#      this is a known limitation until the binary is successfully cached).
#
# ─────────────────────────────────────────────────────────────────────────────

def _bootstrap_mkvtoolnix() -> None:
    import json
    import os
    import re as _re
    import subprocess
    import tarfile
    import tempfile
    import time
    import urllib.error
    import urllib.request

    bin_dir          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")
    mkvpropedit_path = os.path.join(bin_dir, "mkvpropedit")

    # ── Cache check ──────────────────────────────────────────────────────────
    if os.path.isfile(mkvpropedit_path) and os.access(mkvpropedit_path, os.X_OK):
        try:
            r = subprocess.run(
                [mkvpropedit_path, "--version"], capture_output=True, timeout=10
            )
            if r.returncode == 0:
                ver = r.stdout.decode(errors="replace").strip().split("\n")[0]
                print(f"[bootstrap] mkvpropedit already verified: {ver}", flush=True)
                return
        except Exception:
            pass
        # Binary exists but didn't run — remove and re-download
        try:
            os.unlink(mkvpropedit_path)
        except Exception:
            pass

    os.makedirs(bin_dir, exist_ok=True)

    # ── Version discovery ────────────────────────────────────────────────────

    def _github_latest_version() -> str | None:
        """
        Ask GitHub API for the latest mkvtoolnix release tag.
        Tags look like 'release-91.0' or 'v91.0'; we extract the numeric part.
        """
        try:
            req = urllib.request.Request(
                "https://api.github.com/repos/Matroska-Org/mkvtoolnix/releases/latest",
                headers={
                    "Accept":     "application/vnd.github+json",
                    "User-Agent": "worker-bot/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode())
            tag = data.get("tag_name", "")
            m = _re.search(r"(\d+\.\d+(?:\.\d+)?)", tag)
            if m:
                ver = m.group(1)
                print(f"[bootstrap] GitHub API: latest mkvtoolnix = {ver}", flush=True)
                return ver
        except Exception as exc:
            print(f"[bootstrap] GitHub API lookup failed: {exc}", flush=True)
        return None

    def _discover_index_versions() -> list[str]:
        """Scrape mkvtoolnix.download/linux/ for tarball filenames."""
        try:
            with urllib.request.urlopen(
                "https://mkvtoolnix.download/linux/", timeout=10
            ) as r:
                html = r.read().decode(errors="replace")
            found = _re.findall(r"mkvtoolnix-64bit-([\d.]+)\.tar\.xz", html)

            def _ver_key(v: str):
                try:
                    return tuple(int(x) for x in v.split("."))
                except Exception:
                    return (0,)

            return sorted(set(found), key=_ver_key, reverse=True)
        except Exception as exc:
            print(f"[bootstrap] mkvtoolnix index scrape failed: {exc}", flush=True)
            return []

    # Assemble candidate URL list: GitHub API → index scrape → wide static list
    # The static list is deliberately wide — mkvtoolnix.download keeps only
    # the CURRENT release, so we need the exact current version number.
    # If both discovery methods fail, we work backwards from recent to old.
    _STATIC_VERSIONS = [
    "102.0", "101.0", "100.0", "99.0", "98.0", "97.0", "96.0", "95.0",
    "94.0", "93.0", "92.0", "91.0", "90.0", "89.0", "88.0", "87.0",
]
    _BASE = "https://mkvtoolnix.download/linux/mkvtoolnix-64bit-{v}.tar.xz"

    github_ver   = _github_latest_version()
    index_vers   = _discover_index_versions()

    _seen: set[str] = set()
    URLS: list[str] = []

    def _add(v: str) -> None:
        url = _BASE.format(v=v)
        if url not in _seen:
            _seen.add(url)
            URLS.append(url)

    if github_ver:
        _add(github_ver)
    for v in index_vers:
        _add(v)
    for v in _STATIC_VERSIONS:
        _add(v)

    # ── Download loop ────────────────────────────────────────────────────────
    XZ_MAGIC         = b"\xfd7zXZ\x00"
    MIN_ARCHIVE_SIZE = 5 * 1024 * 1024   # 5 MB minimum for a real mkvtoolnix tarball
    CHUNK            = 512 * 1024

    for url in URLS:
        tmp_name = None
        try:
            print(f"[bootstrap] Trying mkvtoolnix: {url}", flush=True)
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "worker-bot/1.0"},
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    print(f"[bootstrap] HTTP {resp.status}, skipping.", flush=True)
                    continue

                fd, tmp_name = tempfile.mkstemp(suffix=".tar.xz")
                total = 0
                with os.fdopen(fd, "wb") as f:
                    while True:
                        chunk = resp.read(CHUNK)
                        if not chunk:
                            break
                        f.write(chunk)
                        total += len(chunk)

        except urllib.error.HTTPError as exc:
            print(f"[bootstrap] HTTP {exc.code} for {url}", flush=True)
            _cleanup(tmp_name)
            continue
        except urllib.error.URLError as exc:
            print(f"[bootstrap] Network error: {exc.reason}", flush=True)
            _cleanup(tmp_name)
            time.sleep(2)
            continue
        except Exception as exc:
            print(f"[bootstrap] Error: {exc}", flush=True)
            _cleanup(tmp_name)
            continue

        # ── Validate ─────────────────────────────────────────────────────────
        if total < MIN_ARCHIVE_SIZE:
            print(
                f"[bootstrap] Only {total:,} bytes — likely a 404/error page, skipping.",
                flush=True,
            )
            _cleanup(tmp_name)
            continue

        try:
            with open(tmp_name, "rb") as f:
                magic = f.read(6)
        except Exception:
            _cleanup(tmp_name)
            continue

        if magic != XZ_MAGIC:
            print(f"[bootstrap] Bad magic {magic!r}, skipping.", flush=True)
            _cleanup(tmp_name)
            continue

        print(f"[bootstrap] mkvtoolnix downloaded OK ({total:,} bytes)", flush=True)

        # ── Extract mkvpropedit ───────────────────────────────────────────────
        try:
            with tarfile.open(tmp_name, "r:xz") as tar:
                for member in tar.getmembers():
                    if (
                        os.path.basename(member.name) == "mkvpropedit"
                        and member.isfile()
                    ):
                        with tar.extractfile(member) as src, \
                             open(mkvpropedit_path, "wb") as dst:
                            dst.write(src.read())
                        os.chmod(mkvpropedit_path, 0o755)
                        print(
                            f"[bootstrap] Extracted mkvpropedit "
                            f"({os.path.getsize(mkvpropedit_path):,} bytes) "
                            f"→ {mkvpropedit_path}",
                            flush=True,
                        )
                        break
        except Exception as exc:
            print(f"[bootstrap] Extraction error: {exc}", flush=True)
            _cleanup(tmp_name)
            _cleanup(mkvpropedit_path if os.path.exists(mkvpropedit_path) else None)
            continue
        finally:
            _cleanup(tmp_name)

        if not os.path.isfile(mkvpropedit_path):
            print(
                "[bootstrap] mkvpropedit not found in tarball — "
                "trying next version.",
                flush=True,
            )
            continue

        # ── Verify ───────────────────────────────────────────────────────────
        try:
            r = subprocess.run(
                [mkvpropedit_path, "--version"], capture_output=True, timeout=10
            )
            if r.returncode != 0:
                raise RuntimeError(f"exit {r.returncode}")
            ver = r.stdout.decode(errors="replace").strip().split("\n")[0]
            print(f"[bootstrap] mkvpropedit verified: {ver}", flush=True)
            return   # ← success
        except Exception as exc:
            print(f"[bootstrap] mkvpropedit failed verification: {exc}", flush=True)
            _cleanup(mkvpropedit_path)
            continue

    # Non-fatal: the worker starts without mkvpropedit and uses the FFmpeg
    # fallback.  Jobs on problematic MKV files will fail metadata embed but
    # will not crash the worker.
    print(
        "[bootstrap] WARNING: mkvpropedit could not be installed.\n"
        "  Metadata embed for MKV files will use the FFmpeg fallback,\n"
        "  which may fail on files with broken attachment streams (exit 183).\n"
        "  Fix: ensure one of the static version URLs is reachable, or update\n"
        "  _STATIC_VERSIONS in worker_bot.py to include the current release.",
        flush=True,
    )


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
# Health server
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

    _proto_handler = _WPH(
        bot_client         = _bot_client,
        worker_id          = Config.WORKER_ID,
        control_group_id   = Config.WORKER_CONTROL_GROUP_ID,
        capacity           = Config.WORKER_CONCURRENCY,
        heartbeat_interval = Config.HEARTBEAT_INTERVAL,
        on_task            = _on_task,
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
        return
    _shutdown_event.set()

    logger.info("[worker] Initiating shutdown — id=%s", Config.WORKER_ID)

    from helper.userbot import stop_userbot
    await stop_userbot()

    if _proto_handler:
        await _proto_handler.stop()

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
