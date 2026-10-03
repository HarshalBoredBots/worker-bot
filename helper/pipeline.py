"""
worker/helper/pipeline.py
══════════════════════════════════════════════════════════════════════════════
Worker job pipeline.

ROOT CAUSES FIXED HERE:
────────────────────────
1. SIZE-RATIO VALIDATION AFTER METADATA EMBED
   The original code checked only output_size > 0.  A 277 MB output from a
   1.46 GB input (80% shrinkage caused by silently dropped video stream) was
   accepted as success.  Now we check output_size / input_size ≥ 90%.
   If the ratio is below the threshold the job is failed, the output is
   deleted, and the original download is preserved untouched.

2. DISK-SPACE PRE-CHECK
   A metadata operation temporarily requires:
     input file + output file (both on disk simultaneously) ≈ 2× input size
   Added a disk-space pre-check before starting work.  If insufficient space
   exists, the job is failed immediately with a clear message.

3. ENRICHED FAILURE LOGGING
   When metadata embed fails, the pipeline now logs:
     - job_id, input_size, output_size (if any), size_ratio
     - detected container extension
     - whether mkvpropedit was available
   This is enough information to diagnose any future regression.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import time
from typing import Callable, Awaitable, Optional

from config import Config
from helper.reliable_download import download_with_retry, DownloadFailedError
from helper.ffmpeg import add_metadata, get_duration_hachoir, _find_binary
from helper.utils import add_prefix_suffix, humanbytes, convert
from helper.upload_manager import upload_with_floodwait
from shared.protocol import (
    STATE_DOWNLOADING, STATE_PROCESSING, STATE_UPLOADING, STATE_UPLOADED,
)

logger = logging.getLogger(__name__)

_PROGRESS_THROTTLE = 3   # seconds between progress edits

# Minimum acceptable output/input size ratio for a metadata-only copy operation.
# Must match the constant in helper/ffmpeg.py.  Keeping them in sync is
# intentional: both layers independently catch the stream-drop bug.
_MIN_SIZE_RATIO = 0.90

# Disk safety margin: require at least 200 MB free beyond the expected need.
_DISK_SAFETY_MARGIN = 200 * 1024 * 1024


class JobPipeline:
    def __init__(
        self,
        bot_client,
        send_state:  Callable[[str, str], Awaitable[None]],
        send_result: Callable[..., Awaitable[None]],
        send_failed: Callable[[str, str], Awaitable[None]],
    ):
        self._bot         = bot_client
        self._send_state  = send_state
        self._send_result = send_result
        self._send_failed = send_failed

    async def run(self, task: dict) -> None:
        job_id = task.get("job_id", "?")
        try:
            await self._run(task)
        except asyncio.CancelledError:
            logger.info("[pipeline] job=%s cancelled", job_id)
            await self._send_failed(job_id, "Job cancelled")
            raise
        except Exception as exc:
            logger.exception("[pipeline] job=%s unhandled exception: %s", job_id, exc)
            await self._send_failed(job_id, f"{type(exc).__name__}: {exc}")

    async def _run(self, task: dict) -> None:
        job_id            = task["job_id"]
        user_id           = int(task["user_id"])
        source_chat_id    = int(task.get("source_chat_id", 0))
        source_message_id = int(task.get("source_message_id", 0))
        rename_pattern    = task.get("rename_pattern", "")
        prefix            = task.get("prefix", "")
        suffix            = task.get("suffix", "")
        metadata = {
            "title":    "@Animes_Ocean",
            "artist":   "@Animes_Ocean",
            "author":   "@Animes_Ocean",
            "comment":  "@Animes_Ocean",
            "audio":    "@Animes_Ocean",
            "video":    "@Animes_Ocean",
            "subtitle": "@Animes_Ocean",
        }
        thumbnail_url = task.get("thumbnail_url")

        download_path: Optional[str] = None
        metadata_path: Optional[str] = None
        thumb_path:    Optional[str] = None

        try:
            if not source_chat_id or not source_message_id:
                await self._send_failed(job_id, "Missing source_chat_id or source_message_id")
                return

            _ub = None

            try:
                message = await self._bot.get_messages(source_chat_id, source_message_id)
            except Exception as exc:
                await self._send_failed(job_id, f"Cannot fetch source message: {exc}")
                return
            if not message or not message.media:
                await self._send_failed(job_id, "Source message has no media")
                return

            if message.document:
                file_obj  = message.document
                base_name = file_obj.file_name or "file"
                base_type = "document"
            elif message.video:
                file_obj  = message.video
                base_name = file_obj.file_name or "video.mp4"
                base_type = "video"
            elif message.audio:
                file_obj  = message.audio
                base_name = file_obj.file_name or "audio.mp3"
                base_type = "audio"
            else:
                await self._send_failed(job_id, "Unsupported media type")
                return

            file_caption = (message.caption or "").strip()
            file_size    = getattr(file_obj, "file_size", 0) or 0

            if file_size > Config.USER_MAX_SIZE:
                await self._send_failed(job_id, "File exceeds 4 GB hard limit")
                return

            if file_size > Config.BOT_MAX_SIZE:
                from helper.userbot import get_userbot, userbot_available
                if not userbot_available():
                    await self._send_failed(job_id, "File > 2 GB but userbot not configured")
                    return
                _ub = await get_userbot()
                if _ub is None:
                    await self._send_failed(job_id, "File > 2 GB but userbot failed to start")
                    return

            if not rename_pattern:
                rename_pattern = base_name

            final_name = add_prefix_suffix(rename_pattern, prefix, suffix)

            # ── Paths ─────────────────────────────────────────────────────────
            folder       = os.path.join("downloads", str(user_id), job_id)
            metadata_dir = os.path.join("Metadata",  str(user_id), job_id)
            os.makedirs(folder,       exist_ok=True)
            os.makedirs(metadata_dir, exist_ok=True)

            download_path = os.path.join(folder,       final_name)
            metadata_path = os.path.join(metadata_dir, final_name)

            # ── Disk-space pre-check ──────────────────────────────────────────
            # Metadata path needs: download (~file_size) + output (~file_size)
            # = 2× file_size + safety margin.
            _required = file_size * 2 + _DISK_SAFETY_MARGIN
            _disk_stat = shutil.disk_usage(folder)
            _disk_free = _disk_stat.free
            logger.info(
                "[pipeline] job=%s disk_free=%s  input_size=%s  required=%s",
                job_id,
                humanbytes(_disk_free),
                humanbytes(file_size),
                humanbytes(_required),
            )
            if _disk_free < _required:
                await self._send_failed(
                    job_id,
                    f"Insufficient disk space: {humanbytes(_disk_free)} free, "
                    f"{humanbytes(_required)} required "
                    f"(input={humanbytes(file_size)} × 2 + safety margin). "
                    "A previous job may have left temporary files.",
                )
                return

            # ── Metadata eligibility ──────────────────────────────────────────
            _FFMPEG_SUPPORTED_EXTS = {
                ".mkv", ".mp4", ".m4v", ".mov", ".avi", ".wmv", ".flv",
                ".webm", ".ts", ".m2ts", ".mts", ".mpeg", ".mpg", ".vob",
                ".3gp", ".3g2", ".ogv", ".rm", ".rmvb", ".divx", ".asf",
                ".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wav",
                ".wma", ".aiff", ".aif", ".ape", ".wv", ".mka", ".mpa",
            }
            _ext            = os.path.splitext(final_name)[1].lower()
            _ffmpeg_capable = _ext in _FFMPEG_SUPPORTED_EXTS

            has_meta = (
                bool(metadata)
                and any((v or "").strip() for v in metadata.values())
                and _ffmpeg_capable
            )
            if bool(metadata) and not _ffmpeg_capable:
                logger.info(
                    "[pipeline] job=%s Skipping metadata — extension '%s' not "
                    "a supported media container.",
                    job_id, _ext,
                )
            logger.info(
                "[pipeline] job=%s has_meta=%s  ext=%s  metadata_keys=%s",
                job_id, has_meta, _ext, list(metadata.keys()),
            )

            # ── NO-METADATA PATH ──────────────────────────────────────────────
            if not has_meta:
                await self._send_state(job_id, STATE_DOWNLOADING)
                logger.info(
                    "[pipeline] job=%s NO-META PATH — download+rename+upload  size=%s",
                    job_id, humanbytes(file_size),
                )

                out_channel    = Config.WORKER_OUTPUT_CHANNEL_ID
                last_prog_edit = [0.0]

                async def _progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    logger.debug("[pipeline] job=%s DL %d%%", job_id, pct)

                _dl_max_attempts = 6 if file_size > 200 * 1024 * 1024 else 4
                _dl_client = _ub if _ub is not None else self._bot

                try:
                    file_path = await download_with_retry(
                        client        = _dl_client,
                        message       = file_obj,
                        file_name     = download_path,
                        expected_size = file_size,
                        progress      = _progress,
                        max_attempts  = _dl_max_attempts,
                        job_id        = job_id,
                    )
                except DownloadFailedError as exc:
                    await self._send_failed(
                        job_id,
                        f"Download failed after {exc.attempts} attempt(s): {exc.last_exc}",
                    )
                    return

                if not file_path or not os.path.exists(file_path):
                    await self._send_failed(job_id, "Download produced no file")
                    return

                actual_size = os.path.getsize(file_path)
                if actual_size == 0:
                    await self._send_failed(job_id, "Downloaded file is 0 bytes")
                    return

                if thumbnail_url:
                    try:
                        thumb_path = await self._download_thumbnail(
                            thumbnail_url, folder, job_id
                        )
                    except Exception as exc:
                        logger.warning("[pipeline] job=%s Thumb download failed: %s", job_id, exc)
                        thumb_path = None

                if not thumb_path and base_type == "video":
                    if message.video and message.video.thumbs:
                        try:
                            thumb_path = await self._bot.download_media(
                                message.video.thumbs[0].file_id,
                                file_name=os.path.join(folder, f"thumb_{job_id}.jpg"),
                            )
                        except Exception:
                            pass

                duration = 0
                if base_type in ("video", "audio"):
                    try:
                        duration = await get_duration_hachoir(file_path)
                    except Exception:
                        pass

                _cap = f"<b>{final_name}</b>"
                await self._send_state(job_id, STATE_UPLOADING)
                logger.info(
                    "[pipeline] job=%s UPLOADING  type=document  size=%s",
                    job_id, humanbytes(actual_size),
                )

                last_prog_edit[0] = 0.0
                c_time = time.time()

                async def _ul_progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    spd = cur / max(time.time() - c_time, 1)
                    logger.debug(
                        "[pipeline] job=%s UL %d%%  %s/s",
                        job_id, pct, humanbytes(int(spd)),
                    )

                # Always send_document: preserves raw bytes without Telegram
                # server-side re-encode (send_video causes quality destruction).
                async def _ul_coro():
                    return await self._bot.send_document(
                        out_channel,
                        document=file_path,
                        file_name=final_name,
                        thumb=thumb_path,
                        caption=_cap,
                        progress=_ul_progress,
                    )

                sent = await upload_with_floodwait(_ul_coro, job_id=job_id, status_msg=None)
                if not sent:
                    await self._send_failed(
                        job_id,
                        "Upload to output channel failed (FloodWait retries exhausted)",
                    )
                    return

                await self._send_state(job_id, STATE_UPLOADED)
                logger.info(
                    "[pipeline] job=%s Uploaded  msg_id=%s", job_id, sent.id
                )
                meta_applied = False

            # ── FULL (METADATA) PATH ──────────────────────────────────────────
            else:
                await self._send_state(job_id, STATE_DOWNLOADING)
                logger.info(
                    "[pipeline] job=%s FULL PATH — download+embed+upload  size=%s",
                    job_id, humanbytes(file_size),
                )

                last_prog_edit = [0.0]

                async def _progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    logger.debug("[pipeline] job=%s DL %d%%", job_id, pct)

                _dl_client = _ub if _ub is not None else self._bot

                try:
                    file_path = await download_with_retry(
                        client        = _dl_client,
                        message       = file_obj,
                        file_name     = download_path,
                        expected_size = file_size,
                        progress      = _progress,
                        max_attempts  = 4,
                        job_id        = job_id,
                    )
                except DownloadFailedError as exc:
                    await self._send_failed(
                        job_id,
                        f"Download failed after {exc.attempts} attempt(s): {exc.last_exc}",
                    )
                    return

                if not file_path or not os.path.exists(file_path):
                    await self._send_failed(job_id, "Download produced no file")
                    return

                _dl_size = os.path.getsize(file_path)
                if _dl_size == 0:
                    logger.error(
                        "[pipeline] job=%s Downloaded file is 0 bytes — possible "
                        "flood-wait truncation.",
                        job_id,
                    )
                    await self._send_failed(
                        job_id,
                        "Download produced an empty file (0 bytes). "
                        "Please re-send the file to retry.",
                    )
                    return

                logger.info(
                    "[pipeline] job=%s Download OK — %s (%d bytes)  "
                    "starting metadata embed  container=%s  "
                    "mkvpropedit_available=%s",
                    job_id, humanbytes(_dl_size), _dl_size, _ext,
                    bool(_find_binary("mkvpropedit")),
                )

                await self._send_state(job_id, STATE_PROCESSING)

                meta_applied = False
                result = await add_metadata(file_path, metadata_path, metadata, None)

                if result and os.path.exists(metadata_path):
                    output_size = os.path.getsize(metadata_path)

                    if output_size == 0:
                        logger.error(
                            "[pipeline] job=%s Metadata output is 0 bytes",
                            job_id,
                        )
                        _remove_if_exists(metadata_path)
                        await self._send_failed(
                            job_id,
                            "Metadata injection produced an empty file.",
                        )
                        return

                    # ── Size-ratio validation (pipeline-level defence) ─────────
                    # add_metadata() already validates this internally, but we
                    # do a second independent check here in case the logic ever
                    # diverges.  This is the last line of defence before upload.
                    size_ratio = output_size / _dl_size if _dl_size > 0 else 1.0
                    logger.info(
                        "[pipeline] job=%s Metadata embed complete — "
                        "input=%s  output=%s  ratio=%.3f",
                        job_id,
                        humanbytes(_dl_size),
                        humanbytes(output_size),
                        size_ratio,
                    )

                    if size_ratio < _MIN_SIZE_RATIO and _dl_size > 1024 * 1024:
                        logger.error(
                            "[pipeline] job=%s OUTPUT SIZE SUSPICIOUS: "
                            "input=%d  output=%d  ratio=%.3f  threshold=%.2f\n"
                            "This almost certainly means the video stream was "
                            "silently dropped during FFmpeg processing. "
                            "Aborting upload to protect the user from a "
                            "corrupted (audio/subtitle-only) file.",
                            job_id, _dl_size, output_size, size_ratio, _MIN_SIZE_RATIO,
                        )
                        _remove_if_exists(metadata_path)
                        await self._send_failed(
                            job_id,
                            f"Metadata inject output is suspiciously small "
                            f"(input={humanbytes(_dl_size)}, "
                            f"output={humanbytes(output_size)}, "
                            f"ratio={size_ratio:.2f} < {_MIN_SIZE_RATIO:.2f}). "
                            "The video stream may have been dropped. "
                            "This is a known issue with certain MKV files when "
                            "mkvpropedit is unavailable.",
                        )
                        return

                    file_path    = metadata_path
                    meta_applied = True
                else:
                    logger.error(
                        "[pipeline] job=%s Metadata embed FAILED — "
                        "input=%d bytes  container=%s  "
                        "mkvpropedit_available=%s",
                        job_id, _dl_size, _ext,
                        bool(_find_binary("mkvpropedit")),
                    )
                    _remove_if_exists(metadata_path)
                    await self._send_failed(
                        job_id,
                        "Metadata injection failed — FFmpeg could not process the "
                        "file. Check logs for FFmpeg stderr (exit code, strategy).",
                    )
                    return

                duration = 0
                try:
                    duration = await get_duration_hachoir(file_path)
                except Exception:
                    pass

                if thumbnail_url:
                    try:
                        thumb_path = await self._download_thumbnail(
                            thumbnail_url, folder, job_id
                        )
                    except Exception as exc:
                        logger.warning(
                            "[pipeline] job=%s Thumb download failed: %s", job_id, exc
                        )
                        thumb_path = None

                if not thumb_path and base_type == "video":
                    if message.video and message.video.thumbs:
                        try:
                            thumb_path = await self._bot.download_media(
                                message.video.thumbs[0].file_id,
                                file_name=os.path.join(folder, f"thumb_{job_id}.jpg"),
                            )
                        except Exception:
                            pass

                actual_size = os.path.getsize(file_path) if os.path.exists(file_path) else file_size
                if actual_size == 0:
                    await self._send_failed(job_id, "Processed file is 0 bytes")
                    return

                caption     = f"<b>{final_name}</b>"
                out_channel = Config.WORKER_OUTPUT_CHANNEL_ID

                await self._send_state(job_id, STATE_UPLOADING)
                logger.info(
                    "[pipeline] job=%s UPLOADING  type=document  size=%s",
                    job_id, humanbytes(actual_size),
                )

                last_prog_edit[0] = 0.0
                c_time = time.time()

                async def _ul_progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    spd = cur / max(time.time() - c_time, 1)
                    logger.debug(
                        "[pipeline] job=%s UL %d%%  %s/s",
                        job_id, pct, humanbytes(int(spd)),
                    )

                async def _ul_coro():
                    return await self._bot.send_document(
                        out_channel,
                        document=file_path,
                        file_name=final_name,
                        thumb=thumb_path,
                        caption=caption,
                        progress=_ul_progress,
                    )

                sent = await upload_with_floodwait(_ul_coro, job_id=job_id, status_msg=None)
                if not sent:
                    await self._send_failed(
                        job_id,
                        "Upload to output channel failed (FloodWait retries exhausted)",
                    )
                    return

                await self._send_state(job_id, STATE_UPLOADED)
                logger.info(
                    "[pipeline] job=%s Uploaded  msg_id=%s", job_id, sent.id
                )

            # ── MediaInfo (non-fatal) ─────────────────────────────────────────
            mediainfo_url: Optional[str] = None
            try:
                from plugins.mediainfo import run_mediainfo_and_telegraph
                bot_me    = await self._bot.get_me()
                bot_uname = bot_me.username or "RenameWorkerBot"
                mi_path   = (
                    metadata_path
                    if meta_applied and os.path.exists(metadata_path)
                    else file_path
                )
                mediainfo_url = await run_mediainfo_and_telegraph(
                    mi_path, final_name, bot_uname
                )
            except Exception as exc:
                logger.debug("[pipeline] job=%s MediaInfo failed (non-fatal): %s", job_id, exc)

            # ── Send RESULT ───────────────────────────────────────────────────
            await self._send_result(
                job_id            = job_id,
                output_chat_id    = sent.chat.id,
                output_message_id = sent.id,
                filename          = final_name,
                original_filename = base_name,
                file_size         = actual_size,
                mediainfo_url     = mediainfo_url,
            )

            logger.info("[pipeline] job=%s DONE  file=%s", job_id, final_name)

        finally:
            # ── Cleanup ───────────────────────────────────────────────────────
            for _p in [download_path, thumb_path]:
                _remove_if_exists(_p)
            if metadata_path and metadata_path != download_path:
                _remove_if_exists(metadata_path)
            for _d in [
                os.path.join("downloads", str(user_id), job_id),
                os.path.join("Metadata",  str(user_id), job_id),
            ]:
                try:
                    if os.path.isdir(_d) and not os.listdir(_d):
                        os.rmdir(_d)
                except OSError:
                    pass

    async def _download_thumbnail(
        self, url: str, folder: str, job_id: str
    ) -> Optional[str]:
        import aiohttp
        dest = os.path.join(folder, f"thumb_{job_id}.jpg")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    raise ValueError(f"HTTP {resp.status} fetching thumbnail")
                data = await resp.read()
        with open(dest, "wb") as f:
            f.write(data)
        return dest if os.path.getsize(dest) > 0 else None


def _remove_if_exists(path: Optional[str]) -> None:
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass
