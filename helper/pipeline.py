"""
worker/helper/pipeline.py
══════════════════════════════════════════════════════════════════════════════
Worker job pipeline — the file-processing heart of the distributed arch.

Adapts _run_pipeline() from the original auto_rename.py to work without a
live Telegram Message object.  Instead it receives a task dict from the
protocol layer (TASK message) which contains all the info needed:

  task = {
      "job_id":             str,
      "batch_id":           str,
      "user_id":            int,
      "source_chat_id":     int,
      "source_message_id":  int,
      "rename_pattern":     str,   # the computed final filename (Manager already resolved template)
      "prefix":             str,
      "suffix":             str,
      "metadata":           dict,  # {title, author, artist, audio, video, subtitle, comment}
                                   # already merged (global override applied by Manager)
      "metadata_version":   int,
      "thumbnail_url":      str | None,   # ImgBB HTTPS URL or None
      "dump_enabled":       bool,
  }

Pipeline steps
──────────────
  1. Fetch source message from Telegram (bot client; source chat)
  2. Send STATE=DOWNLOADING
  3. Download file via download_with_retry (bot client; 4 attempts)
  4. Send STATE=PROCESSING
  5. Embed metadata via FFmpeg if metadata dict is non-empty
  6. Download / resolve thumbnail from ImgBB URL (if any)
  7. Compute final caption and apply prefix/suffix
  8. Send STATE=UPLOADING
  9. Upload to Worker Output Channel (bot client for ≤2 GB;
     NOT userbot — the String Session is protocol-only in distributed arch)
 10. Send RESULT with (output_chat_id, output_message_id, filename, file_size)
 11. Clean up local temp files

Error handling
──────────────
  Any unrecoverable error at any step calls proto.send_failed() and cleans up.
  CancelledError is re-raised after cleanup so the task terminates cleanly.

Boundary rule (CRITICAL)
─────────────────────────
  • The String Session (proto_handler._session) is NEVER used here.
  • All download_media / send_document / send_video / send_audio calls use
    self._bot (the Worker Bot client).
  • The pipeline imports nothing from helper.protocol_handler — it only
    accepts send_state / send_result / send_failed as injected callables.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Callable, Awaitable, Optional

from config import Config
from helper.reliable_download import download_with_retry, DownloadFailedError
from helper.ffmpeg import add_metadata, get_duration_hachoir
from helper.utils import add_prefix_suffix, humanbytes, convert
from helper.upload_manager import upload_with_floodwait
from shared.protocol import (
    STATE_DOWNLOADING, STATE_PROCESSING, STATE_UPLOADING, STATE_UPLOADED,
)

logger = logging.getLogger(__name__)

_PROGRESS_THROTTLE = 3   # seconds between progress edits


class JobPipeline:
    """
    Stateless job executor.  One instance is typically shared; each call to
    run() is fully independent.

    Parameters
    ──────────
    bot_client    : Pyrogram Client (BOT_TOKEN) — all file I/O goes through this
    send_state    : Callable(job_id, state) — sends a STATE protocol message
    send_result   : Callable(**kwargs) — sends a RESULT protocol message
    send_failed   : Callable(job_id, reason) — sends a FAILED protocol message
    """

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

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────────────────────
    # Internal pipeline
    # ──────────────────────────────────────────────────────────────────────────

    async def _run(self, task: dict) -> None:
        job_id            = task["job_id"]
        user_id           = int(task["user_id"])
        source_chat_id    = int(task.get("source_chat_id", 0))
        source_message_id = int(task.get("source_message_id", 0))
        rename_pattern    = task.get("rename_pattern", "")
        prefix            = task.get("prefix", "")
        suffix            = task.get("suffix", "")
        # Hardcoded metadata — always embed @Animes_Ocean regardless of
        # whatever the manager bot sends. Cannot be changed by any command.
        metadata = {
            "title":    "@Animes_Ocean",
            "artist":   "@Animes_Ocean",
            "author":   "@Animes_Ocean",
            "comment":  "@Animes_Ocean",
            "audio":    "@Animes_Ocean",
            "video":    "@Animes_Ocean",
            "subtitle": "@Animes_Ocean",
        }
        thumbnail_url     = task.get("thumbnail_url")

        download_path: Optional[str] = None
        metadata_path: Optional[str] = None
        thumb_path:    Optional[str] = None

        try:
            # ── 1. Fetch source message via bot client ────────────────────────
            if not source_chat_id or not source_message_id:
                await self._send_failed(job_id, "Missing source_chat_id or source_message_id")
                return

            # ── Large-file / userbot setup — resolve BEFORE fetching the message
            # FIX BUG 2: _ub was referenced on line below BEFORE it was defined.
            # The original code called (_ub or self._bot).get_messages() at the
            # top of the try block, but _ub was assigned ~35 lines later after
            # the file-size gate.  At message-fetch time file_size is not yet
            # known so _ub cannot be determined then anyway.  Solution: always
            # fetch the source message with self._bot (the bot always has read
            # access to the output channel where the file was staged), resolve
            # _ub after the size check, and use it only for download if needed.
            # FIX BUG 4: The large-file gate also appeared TWICE — once before
            # the _ub assignment (using an already-failed _ub check) and once
            # after it, causing a confusing double-bail-out. Merged into one
            # clean gate below.
            _ub = None  # resolved after file_size is known

            try:
                message = await self._bot.get_messages(source_chat_id, source_message_id)
            except Exception as exc:
                await self._send_failed(job_id, f"Cannot fetch source message: {exc}")
                return
            if not message or not message.media:
                await self._send_failed(job_id, "Source message has no media")
                return

            # ── Identify file object ──────────────────────────────────────────
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

            # ── Large-file gate (single, authoritative check) ─────────────────
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

            # ── 2. Resolve final filename ─────────────────────────────────────
            # Manager sends rename_pattern which is either the user's manual
            # filename or the fully-rendered template result (Manager applied
            # the episode/season/quality parser against the source filename).
            # Worker just applies prefix/suffix to the pattern.
            if not rename_pattern:
                rename_pattern = base_name

            final_name = add_prefix_suffix(rename_pattern, prefix, suffix)

            # ── Paths (isolated per job) ──────────────────────────────────────
            folder        = os.path.join("downloads", str(user_id), job_id)
            metadata_dir  = os.path.join("Metadata",  str(user_id), job_id)
            os.makedirs(folder,       exist_ok=True)
            os.makedirs(metadata_dir, exist_ok=True)

            download_path = os.path.join(folder,       final_name)
            metadata_path = os.path.join(metadata_dir, final_name)

            # ── 3. Check metadata need — decide fast-path vs full-path ──────
            #
            # IMPORTANT: FFmpeg can only embed metadata into standard media
            # containers (MKV, MP4, AVI, MOV, MP3, FLAC, OGG, etc.).
            # Non-media files sent as documents (fonts, ZIPs, PDFs, images,
            # executables, etc.) will make FFmpeg fail with:
            #   "Invalid data found when processing input" / exit 183
            # Guard: if the file extension is NOT in the FFmpeg-supported set,
            # force the no-metadata path regardless of metadata settings.
            _FFMPEG_SUPPORTED_EXTS = {
                # Video containers
                ".mkv", ".mp4", ".m4v", ".mov", ".avi", ".wmv", ".flv",
                ".webm", ".ts", ".m2ts", ".mts", ".mpeg", ".mpg", ".vob",
                ".3gp", ".3g2", ".ogv", ".rm", ".rmvb", ".divx", ".asf",
                # Audio containers
                ".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wav",
                ".wma", ".aiff", ".aif", ".ape", ".wv", ".mka", ".mpa",
            }
            _ext = os.path.splitext(final_name)[1].lower()
            _ffmpeg_capable = _ext in _FFMPEG_SUPPORTED_EXTS

            has_meta = (
                bool(metadata)
                and any((v or "").strip() for v in metadata.values())
                and _ffmpeg_capable
            )
            if bool(metadata) and any((v or "").strip() for v in metadata.values()) and not _ffmpeg_capable:
                logger.info(
                    "[pipeline] job=%s Skipping metadata embed — file extension '%s' "
                    "is not an FFmpeg-supported media container (font/zip/pdf/image?). "
                    "Falling through to rename-only path.",
                    job_id, _ext,
                )
            logger.info(
                "[pipeline] job=%s has_meta=%s  metadata_keys=%s  metadata=%s",
                job_id, has_meta, list(metadata.keys()), metadata,
            )

            if not has_meta:
                # ═══════════════════════════════════════════════════════════
                # NO-METADATA PATH: download → rename locally → re-upload.
                #
                # Telegram's file_id re-send (copy_message / send_document
                # with a file_id) IGNORES the file_name parameter — the name
                # is baked into the file_id on Telegram's servers and cannot
                # be changed without re-uploading the raw bytes.  The only
                # reliable way to rename is:
                #   1. Download to a local path named final_name
                #   2. Upload that local file (Telegram reads the filename
                #      from the path / file_name arg of the multipart upload)
                # ═══════════════════════════════════════════════════════════
                await self._send_state(job_id, STATE_DOWNLOADING)
                logger.info(
                    "[pipeline] job=%s NO-META PATH — download+rename+upload  size=%s",
                    job_id, humanbytes(file_size),
                )

                out_channel = Config.WORKER_OUTPUT_CHANNEL_ID
                last_prog_edit = [0.0]

                async def _progress(cur, total, *_):
                    if time.time() - last_prog_edit[0] < _PROGRESS_THROTTLE:
                        return
                    last_prog_edit[0] = time.time()
                    pct = cur * 100 // total if total else 0
                    logger.debug("[pipeline] job=%s DL %d%%", job_id, pct)

                # Large files (>200 MB) are more likely to hit multiple
                # Telegram flood-waits mid-stream — give them more attempts.
                _dl_max_attempts = 6 if file_size > 200 * 1024 * 1024 else 4

                # Use userbot for download if file > 2 GB and _ub is available
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

                # FIX BUG 8: actual_size was missing from the NO-META path.
                # It was only computed in the FULL (metadata) path, causing a
                # NameError when _send_result referenced it at the bottom of
                # the pipeline. Added here immediately after the 0-byte check
                # so the variable is defined in both branches.
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

                duration = 0
                if base_type in ("video", "audio"):
                    try:
                        duration = await get_duration_hachoir(file_path)
                    except Exception:
                        pass

                _cap = f"<b>{final_name}</b>"
                await self._send_state(job_id, STATE_UPLOADING)
                logger.info(
                    "[pipeline] job=%s UPLOADING  type=%s  size=%s",
                    job_id, base_type, humanbytes(actual_size),
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

                # Always upload as send_document regardless of base_type.
                # send_video / send_audio tell Telegram to process (and
                # potentially re-encode) the file server-side, which destroys
                # quality (HEVC → mjpeg, 1080p → 360p, 1.33 GB → 277 MB).
                # send_document uploads raw bytes untouched — the filename
                # extension (.mkv, .mp4, .mp3 …) still shows the correct type
                # in Telegram clients, and quality is 100% preserved.
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
                    "[pipeline] job=%s Uploaded to output channel  msg_id=%s",
                    job_id, sent.id,
                )
                meta_applied = False

            else:
                # ═══════════════════════════════════════════════════════════
                # FULL PATH: metadata injection needed — must download,
                # embed with FFmpeg, then re-upload.
                # ═══════════════════════════════════════════════════════════
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

                # Guard: 0-byte file = download was silently truncated
                # (Telegram flood-wait interrupted the stream before EOF).
                # FFmpeg sees "Invalid data" — catch it here with a clear message.
                _dl_size = os.path.getsize(file_path)
                if _dl_size == 0:
                    logger.error(
                        "[pipeline] job=%s Downloaded file is 0 bytes — flood-wait truncation?",
                        job_id,
                    )
                    await self._send_failed(
                        job_id,
                        "Download produced an empty file (0 bytes). "
                        "A Telegram flood-wait likely interrupted the transfer. "
                        "Please re-send the file to retry.",
                    )
                    return

                logger.info(
                    "[pipeline] job=%s Download OK — %d bytes, starting metadata embed",
                    job_id, _dl_size,
                )

                await self._send_state(job_id, STATE_PROCESSING)

                meta_applied = False
                result = await add_metadata(file_path, metadata_path, metadata, None)
                if result and os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0:
                    file_path    = metadata_path
                    meta_applied = True
                else:
                    # Metadata embed failed (most likely an MKV with broken font-attachment
                    # streams and mkvpropedit not available). Rather than failing the whole
                    # job, warn and continue with the original downloaded file so the user
                    # still receives their renamed file — just without the embedded tags.
                    logger.warning(
                        "[pipeline] job=%s Metadata embed FAILED "
                        "(input=%d bytes) — continuing with original file (no tags embedded)",
                        job_id, _dl_size,
                    )
                    meta_applied = False
                    # file_path stays pointing at the downloaded file

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
                    "[pipeline] job=%s UPLOADING  type=%s  size=%s",
                    job_id, base_type, humanbytes(actual_size),
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

                # Always upload as send_document regardless of base_type.
                # send_video / send_audio tell Telegram to process (and
                # potentially re-encode) the file server-side, which destroys
                # quality (HEVC → mjpeg, 1080p → 360p, 1.33 GB → 277 MB).
                # send_document uploads raw bytes untouched — the filename
                # extension (.mkv, .mp4, .mp3 …) still shows the correct type
                # in Telegram clients, and quality is 100% preserved.
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
                    "[pipeline] job=%s Uploaded to output channel  msg_id=%s",
                    job_id, sent.id,
                )

            # ── 6. Get MediaInfo URL (non-fatal) ──────────────────────────────
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
                mediainfo_url = await run_mediainfo_and_telegraph(mi_path, final_name, bot_uname)
            except Exception as exc:
                logger.debug("[pipeline] job=%s MediaInfo failed (non-fatal): %s", job_id, exc)

            # ── 7. Send RESULT ────────────────────────────────────────────────
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
            # ── 8. Cleanup temp files ─────────────────────────────────────────
            for _p in [download_path, thumb_path]:
                if _p and os.path.exists(_p):
                    try:
                        os.remove(_p)
                    except OSError:
                        pass
            if (metadata_path
                    and metadata_path != download_path
                    and os.path.exists(metadata_path)):
                try:
                    os.remove(metadata_path)
                except OSError:
                    pass
            for _d in [
                os.path.join("downloads", str(user_id), job_id),
                os.path.join("Metadata",  str(user_id), job_id),
            ]:
                try:
                    if os.path.isdir(_d) and not os.listdir(_d):
                        os.rmdir(_d)
                except OSError:
                    pass

    # ──────────────────────────────────────────────────────────────────────────
    # Thumbnail helper
    # ──────────────────────────────────────────────────────────────────────────

    async def _download_thumbnail(
        self, url: str, folder: str, job_id: str
    ) -> Optional[str]:
        """Download a thumbnail from an HTTPS URL (ImgBB) to a local file."""
        import aiohttp
        dest = os.path.join(folder, f"thumb_{job_id}.jpg")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    raise ValueError(f"HTTP {resp.status} fetching thumbnail")
                data = await resp.read()
        with open(dest, "wb") as f:
            f.write(data)
        return dest if os.path.getsize(dest) > 0 else None
