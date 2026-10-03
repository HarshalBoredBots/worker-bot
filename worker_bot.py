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
#   1. Query GitLab API (mbunkus/mkvtoolnix) for the latest release tag —
#      MKVToolNix is on GitLab, not GitHub; the GitHub API always 404s.
#   2. Also scrape mkvtoolnix.download for tarball links (belt & suspenders).
#   3. Wide static fallback list (130.0 → 91.0) so the worker can still boot
#      if both live discovery methods fail.
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
    """
    Download mkvpropedit from the official MKVToolNix Ubuntu apt repository.

    ROOT CAUSE of previous failures
    ────────────────────────────────
    • mkvtoolnix.download stopped hosting generic static Linux tarballs.
      Every URL of the form mkvtoolnix-64bit-{v}.tar.xz returns 404 for ALL
      versions — the URL format is permanently dead.
    • The GitHub API was wrong (MKVToolNix is on GitLab, not GitHub).
    • The GitLab API also returns 404 from Heroku IPs (likely because the
      project uses tags only, not formal GitLab Releases, or GitLab blocks
      unauthenticated API from cloud egress ranges).

    New strategy — use the official apt repository
    ────────────────────────────────────────────────
    1. Fetch Packages.gz from mkvtoolnix.download/ubuntu for the closest
       Ubuntu codename (Heroku-22 = jammy).  This gives the current .deb
       Filename without any version hardcoding.
    2. Download that .deb package.
    3. Extract mkvpropedit from data.tar.* inside the .deb using pure Python
       (simple ar-format parser + tarfile).  No system tools (ar, dpkg) needed.
    4. Verify with "mkvpropedit --version".
    5. NON-FATAL: worker starts without mkvpropedit and uses FFmpeg fallback,
       which will fail for MKV files with broken attachment streams (exit 183).
    """
    import gzip
    import io
    import os
    import re as _re
    import struct
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
        try:
            os.unlink(mkvpropedit_path)
        except Exception:
            pass

    os.makedirs(bin_dir, exist_ok=True)

    # ── Step 1: Find the .deb URL from the apt Packages index ────────────────
    # Try codenames from newest to oldest; Heroku-22 runs Ubuntu 22.04 (jammy).
    CODENAMES = ["oracular", "noble", "jammy", "focal"]
    BASE      = "https://mkvtoolnix.download/ubuntu"

    def _find_deb_url() -> str | None:
        for codename in CODENAMES:
            pkg_index_url = (
                f"{BASE}/dists/{codename}/main/binary-amd64/Packages.gz"
            )
            try:
                req = urllib.request.Request(
                    pkg_index_url,
                    headers={"User-Agent": "worker-bot/1.0"},
                )
                with urllib.request.urlopen(req, timeout=15) as r:
                    raw = r.read()
                text = gzip.decompress(raw).decode(errors="replace")
            except Exception as exc:
                print(
                    f"[bootstrap] Packages.gz [{codename}] failed: {exc}",
                    flush=True,
                )
                continue

            # Stanzas are separated by blank lines
            for stanza in text.split("\n\n"):
                if not _re.search(r"^Package: mkvtoolnix$", stanza, _re.MULTILINE):
                    continue
                m_file = _re.search(r"^Filename: (.+)$", stanza, _re.MULTILINE)
                if not m_file:
                    continue
                rel_path = m_file.group(1).strip()
                m_ver    = _re.search(r"^Version: (.+)$",  stanza, _re.MULTILINE)
                ver_str  = m_ver.group(1).strip() if m_ver else "unknown"
                url      = f"{BASE}/{rel_path}"
                print(
                    f"[bootstrap] apt index [{codename}]: "
                    f"mkvtoolnix {ver_str} → {url}",
                    flush=True,
                )
                return url

            print(
                f"[bootstrap] mkvtoolnix not found in Packages.gz [{codename}]",
                flush=True,
            )
        return None

    # ── Step 2: Pure-Python .deb extraction ──────────────────────────────────

    def _ar_extract(ar_bytes: bytes, name_prefix: str) -> bytes | None:
        """
        Extract the first member whose name starts with `name_prefix` from
        an ar(1) archive.  The ar format is:
          - 8-byte magic  "!<arch>\n"
          - Per member: 60-byte fixed header + data (padded to even length)
        """
        MAGIC = b"!<arch>\n"
        if not ar_bytes.startswith(MAGIC):
            print("[bootstrap] .deb: bad ar magic", flush=True)
            return None
        pos = len(MAGIC)
        while pos + 60 <= len(ar_bytes):
            hdr      = ar_bytes[pos : pos + 60]
            name     = hdr[0:16].rstrip(b" \x00").decode(errors="replace")
            size_str = hdr[48:58].rstrip(b" \x00")
            size     = int(size_str) if size_str.strip() else 0
            pos     += 60
            member   = ar_bytes[pos : pos + size]
            pos     += size + (size & 1)   # pad to even
            if name.rstrip("/").startswith(name_prefix):
                return member
        return None

    def _extract_mkvpropedit(deb_bytes: bytes) -> bytes | None:
        """
        Pull /usr/bin/mkvpropedit out of a .deb package.
        A .deb is an ar archive; we find data.tar.* and open it with tarfile.
        """
        data_tar = _ar_extract(deb_bytes, "data.tar")
        if data_tar is None:
            print("[bootstrap] data.tar not found in .deb ar archive", flush=True)
            return None

        try:
            with tarfile.open(fileobj=io.BytesIO(data_tar)) as tar:
                for member in tar.getmembers():
                    # normalise ./usr/bin/mkvpropedit → usr/bin/mkvpropedit
                    norm = member.name.lstrip("./")
                    if norm == "usr/bin/mkvpropedit" and member.isfile():
                        fobj = tar.extractfile(member)
                        if fobj:
                            data = fobj.read()
                            print(
                                f"[bootstrap] Extracted mkvpropedit "
                                f"({len(data):,} bytes) from data.tar",
                                flush=True,
                            )
                            return data
        except Exception as exc:
            print(f"[bootstrap] tarfile extraction error: {exc}", flush=True)
        return None

    # ── Main flow ─────────────────────────────────────────────────────────────
    deb_url = _find_deb_url()
    if not deb_url:
        # Non-fatal — fall through to warning below
        pass
    else:
        try:
            print(f"[bootstrap] Downloading .deb: {deb_url}", flush=True)
            req = urllib.request.Request(
                deb_url, headers={"User-Agent": "worker-bot/1.0"}
            )
            with urllib.request.urlopen(req, timeout=120) as r:
                deb_bytes = r.read()
            print(
                f"[bootstrap] .deb downloaded ({len(deb_bytes):,} bytes)",
                flush=True,
            )
        except Exception as exc:
            print(f"[bootstrap] .deb download failed: {exc}", flush=True)
            deb_bytes = None

        if deb_bytes:
            binary = _extract_mkvpropedit(deb_bytes)
            if binary:
                try:
                    with open(mkvpropedit_path, "wb") as f:
                        f.write(binary)
                    os.chmod(mkvpropedit_path, 0o755)

                    r = subprocess.run(
                        [mkvpropedit_path, "--version"],
                        capture_output=True,
                        timeout=10,
                    )
                    if r.returncode == 0:
                        ver = r.stdout.decode(errors="replace").strip().split("\n")[0]
                        print(f"[bootstrap] mkvpropedit verified: {ver}", flush=True)
                        return  # ← success
                    else:
                        print(
                            f"[bootstrap] mkvpropedit verification failed "
                            f"(exit {r.returncode})",
                            flush=True,
                        )
                        try:
                            os.unlink(mkvpropedit_path)
                        except Exception:
                            pass
                except Exception as exc:
                    print(f"[bootstrap] write/verify error: {exc}", flush=True)
            else:
                print(
                    "[bootstrap] mkvpropedit binary not found inside .deb",
                    flush=True,
                )

    # Non-fatal: worker starts and uses FFmpeg fallback for metadata embed.
    # FFmpeg will fail with exit 183 on MKV files that have broken attachment
    # streams — this is a known limitation when mkvpropedit is unavailable.
    print(
        "[bootstrap] WARNING: mkvpropedit could not be installed.\n"
        "  Metadata embed for MKV files will use the FFmpeg fallback,\n"
        "  which may fail on files with broken attachment streams (exit 183).\n"
        "  Fix: verify that mkvtoolnix.download/ubuntu is reachable from Heroku\n"
        "  and that the Packages.gz index lists mkvtoolnix for one of:\n"
        f"  {CODENAMES}",
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
