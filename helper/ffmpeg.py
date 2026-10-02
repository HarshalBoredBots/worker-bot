"""
helper/ffmpeg.py
─────────────────
FFmpeg/FFprobe helpers — every CPU-bound / subprocess call is async-safe.

run_blocking(func, *args)
  Routes sync functions through loop.run_in_executor so they NEVER block
  the event loop.  asyncio.create_subprocess_exec is used for all
  subprocess calls so FFmpeg never holds the event loop either.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import random
import shutil
import time
from typing import Any

from messages import log, Msg


# ══════════════════════════════════════════════════════════════════════════════
# _find_binary — locate ffmpeg / mkvpropedit / etc.
# ══════════════════════════════════════════════════════════════════════════════

def _find_binary(name: str) -> str | None:
    """
    Locate a binary by name.

    Search order:
      1. <repo_root>/bin/<name>  — binaries bootstrapped at dyno startup
         (ffmpeg, ffprobe, mkvpropedit downloaded by worker_bot.py bootstrap)
      2. shutil.which(name)      — anything on the system PATH

    Returns the full path string if found and executable, else None.
    """
    # repo_root is one level above this file's directory (helper/)
    bin_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bin",
    )
    local = os.path.join(bin_dir, name)
    if os.path.isfile(local) and os.access(local, os.X_OK):
        return local
    return shutil.which(name)


# ══════════════════════════════════════════════════════════════════════════════
# run_blocking — universal bridge from sync → async
# ══════════════════════════════════════════════════════════════════════════════

async def run_blocking(func, *args, **kwargs) -> Any:
    """
    Run a synchronous (blocking) callable in the default thread-pool executor
    so it NEVER stalls the event loop.

    Usage:
        result = await run_blocking(my_sync_func, arg1, arg2)

    For keyword-argument functions wrap with functools.partial first:
        result = await run_blocking(functools.partial(my_func, key=val), arg1)
    """
    loop = asyncio.get_running_loop()
    if kwargs:
        func = functools.partial(func, **kwargs)
    return await loop.run_in_executor(None, func, *args)


# ══════════════════════════════════════════════════════════════════════════════
# fix_thumb — PIL in thread pool via run_blocking
# ══════════════════════════════════════════════════════════════════════════════

def _fix_thumb_sync(thumb: str):
    """Sync PIL worker — called via run_blocking, never touches event loop."""
    from hachoir.metadata import extractMetadata
    from hachoir.parser import createParser
    from PIL import Image

    width = height = 0
    parser = createParser(thumb)
    if parser:
        meta = extractMetadata(parser)
        if meta:
            if meta.has("width"):
                width = meta.get("width")
            if meta.has("height"):
                height = meta.get("height")
        parser.stream._input.close()

    img = Image.open(thumb).convert("RGB")
    if width and height:
        img = img.resize((width, height), Image.LANCZOS)
    else:
        width, height = img.size
    img.save(thumb, "JPEG", subsampling=0, quality=95)
    return width, height, thumb


async def fix_thumb(thumb: str):
    """
    Ensure *thumb* is a valid Baseline JPEG.
    Returns (width, height, path). path is None on failure.
    Non-blocking: PIL/hachoir run in thread pool via run_blocking.

    Guards:
      • Empty path           → (0, 0, None)
      • File does not exist  → (0, 0, None)
      • File is 0 bytes      → (0, 0, None)  ← fixes hachoir "Input size is nul"
    """
    if not thumb:
        return 0, 0, None
    if not os.path.exists(thumb):
        log.warning(Msg.THUMB_FIX_ERR, error=f"thumb file not found: {thumb}")
        return 0, 0, None
    if os.path.getsize(thumb) == 0:
        log.warning(Msg.THUMB_FIX_ERR, error=f"thumb is 0 bytes: {thumb}")
        try:
            os.remove(thumb)
        except Exception:
            pass
        return 0, 0, None
    try:
        return await run_blocking(_fix_thumb_sync, thumb)
    except Exception as e:
        log.error(Msg.THUMB_FIX_ERR, error=e)
        return 0, 0, None


# ══════════════════════════════════════════════════════════════════════════════
# take_screen_shot — async subprocess (never blocks event loop)
# ══════════════════════════════════════════════════════════════════════════════

async def take_screen_shot(video_file: str, output_directory: str, ttl: int):
    """
    Extract a single frame at *ttl* seconds.
    -y      : overwrite output without prompting (prevents hang)
    -i first: accurate seeking (avoids missing frames near start of file)
    -ss after -i: frame-accurate seek (slower but correct for short seeks)
    """
    out_file = os.path.join(output_directory, f"{time.time()}.jpg")
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-i", video_file,
        "-ss", str(ttl),
        "-vframes", "1",
        "-q:v", "2",
        out_file,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.communicate()
    return out_file if os.path.exists(out_file) and os.path.getsize(out_file) > 0 else None


# ══════════════════════════════════════════════════════════════════════════════
# get_video_duration — async ffprobe subprocess
# ══════════════════════════════════════════════════════════════════════════════

async def get_video_duration(file_path: str) -> float:
    """Return duration in seconds. Returns 0.0 on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "json", file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode(errors="replace"))
        return float(data["format"]["duration"])
    except Exception as e:
        log.warning(Msg.FFPROBE_DUR_ERR, error=e)
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# get_duration_hachoir — hachoir in thread pool via run_blocking
# ══════════════════════════════════════════════════════════════════════════════

def _hachoir_duration_sync(file_path: str) -> int:
    from hachoir.metadata import extractMetadata
    from hachoir.parser import createParser
    try:
        parser = createParser(file_path)
        if parser:
            meta = extractMetadata(parser)
            secs = meta.get("duration").seconds if (meta and meta.has("duration")) else 0
            parser.stream._input.close()
            return secs
    except Exception:
        pass
    return 0


async def get_duration_hachoir(file_path: str) -> int:
    """Non-blocking hachoir duration extraction via run_blocking."""
    return await run_blocking(_hachoir_duration_sync, file_path)


# ══════════════════════════════════════════════════════════════════════════════
# add_metadata — async ffmpeg subprocess
# ✔ asyncio.create_subprocess_exec → never blocks event loop
# ══════════════════════════════════════════════════════════════════════════════

async def add_metadata(
    input_path: str,
    output_path: str,
    metadata_fields: dict,
    ms,
) -> str | None:
    """
    Embed metadata tags into *input_path* → *output_path*.
    Stream copy — no re-encode. Returns output_path on success, None on failure.

    Strategy:
      1. ffprobe all streams → get exact indices of video/audio/subtitle streams.
         Exclude attachment/data/font streams entirely by index.
         This is the ONLY reliable fix for anime MKVs with embedded fonts
         (codec_type=attachment, codec_name=none) that cause FFmpeg exit 183
         ("Could not write header: Invalid data found when processing input").
      2. Run ffmpeg with explicit -map 0:INDEX for each good stream.
         Fallback: -map 0:V? -map 0:a? -map 0:s? if probe failed.
      3. Last resort: -map 0 -ignore_unknown.

    Progress bar uses -progress pipe:1 (key=value lines on stdout).
    Throttled to 1 edit per 5 seconds.
    """
    import math
    from helper.utils import humanbytes, TimeFormatter
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    try:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        # ── 1. Probe: duration + exact good stream indices ─────────────────────
        total_us: int = 0
        good_indices: list[int] = []   # video/audio/subtitle stream indices only
        probe_ok = False

        try:
            _probe = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams", "-show_format",
                input_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            _probe_out, _ = await _probe.communicate()
            if _probe.returncode == 0 and _probe_out:
                _pd = json.loads(_probe_out.decode(errors="replace"))
                # Duration
                dur_str  = _pd.get("format", {}).get("duration", "0")
                total_us = int(float(dur_str) * 1_000_000)
                # Good stream indices: video, audio, subtitle only
                # Skip: attachment (fonts), data (chapters/timecodes), none codec
                _SKIP_TYPES = {"attachment", "data"}
                _SKIP_CODECS = {"none", "unknown", ""}
                for s in _pd.get("streams", []):
                    ct   = s.get("codec_type", "")
                    cn   = s.get("codec_name", "none").lower()
                    idx  = s.get("index")
                    if (
                        ct not in _SKIP_TYPES
                        and cn not in _SKIP_CODECS
                        and idx is not None
                    ):
                        good_indices.append(int(idx))
                probe_ok = True
                log.debug(
                    "[add_metadata] probe OK — good stream indices: {idxs}",
                    idxs=good_indices,
                )
        except Exception as _pe:
            log.warning("[add_metadata] probe failed: {err}", err=_pe)

        # ── 2. Build metadata args ─────────────────────────────────────────────
        meta_args: list[str] = []
        for tag in ("title", "artist", "author", "comment"):
            val = (metadata_fields.get(tag) or "").strip()
            if val:
                meta_args += ["-metadata", f"{tag}={val}"]

        audio_title = (metadata_fields.get("audio") or "").strip()
        video_title = (metadata_fields.get("video") or "").strip()
        sub_title   = (metadata_fields.get("subtitle") or "").strip()
        if audio_title:
            meta_args += ["-metadata:s:a", f"title={audio_title}"]
        if video_title:
            meta_args += ["-metadata:s:v", f"title={video_title}"]
        if sub_title:
            meta_args += ["-metadata:s:s", f"title={sub_title}"]

        # ── 3. Strategy: mkvpropedit in-place (MKV only) ─────────────────────
        # mkvpropedit edits tags DIRECTLY in the MKV file without touching
        # any streams. It never remuxes, so broken attachment streams
        # (font/sfnt with unknown codec params) are completely irrelevant.
        # This is the ONLY 100% reliable approach for these anime MKVs.
        #
        # For non-MKV files we fall back to FFmpeg remux with explicit
        # stream index mapping.

        _input_ext = os.path.splitext(input_path)[1].lower()

        if _input_ext == ".mkv":
            # ── mkvpropedit path ──────────────────────────────────────────────
            _mkvpropedit = _find_binary("mkvpropedit")
            if _mkvpropedit and os.path.isfile(_mkvpropedit):
                # mkvpropedit edits the INPUT file in-place.
                # We copy input → output first, then edit output in-place.
                import shutil as _shutil
                try:
                    _shutil.copy2(input_path, output_path)
                except Exception as _ce:
                    log.warning("[add_metadata] copy for mkvpropedit failed: {err}", err=_ce)
                    if os.path.exists(output_path):
                        os.remove(output_path)

                if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    # Build mkvpropedit command
                    # --tags all: clears existing tags, --add-track-statistics-tags: optional
                    mkv_cmd = [_mkvpropedit, output_path, "--tags", "all:"]

                    # Global tags via XML (mkvpropedit needs XML format for tags)
                    # Build a minimal tag XML
                    _tag_lines = []
                    _TITLE_TAG = (metadata_fields.get("title") or "").strip()
                    if _TITLE_TAG:
                        _tag_lines.append(f'    <Simple><Name>TITLE</Name><String>{_TITLE_TAG}</String></Simple>')
                    _ARTIST = (metadata_fields.get("artist") or "").strip()
                    if _ARTIST:
                        _tag_lines.append(f'    <Simple><Name>ARTIST</Name><String>{_ARTIST}</String></Simple>')
                    _AUTHOR = (metadata_fields.get("author") or "").strip()
                    if _AUTHOR:
                        _tag_lines.append(f'    <Simple><Name>AUTHOR</Name><String>{_AUTHOR}</String></Simple>')
                    _COMMENT = (metadata_fields.get("comment") or "").strip()
                    if _COMMENT:
                        _tag_lines.append(f'    <Simple><Name>COMMENT</Name><String>{_COMMENT}</String></Simple>')

                    if _tag_lines:
                        import tempfile as _tf
                        _xml_content = (
                            '<?xml version="1.0"?>\n'
                            '<!DOCTYPE Tags SYSTEM "matroskatags.dtd">\n'
                            '<Tags>\n'
                            '  <Tag>\n'
                            '    <Targets/>\n'
                            + "\n".join(_tag_lines) + "\n"
                            '  </Tag>\n'
                            '</Tags>\n'
                        )
                        _xml_fd, _xml_path = _tf.mkstemp(suffix=".xml")
                        try:
                            with os.fdopen(_xml_fd, "w") as _xf:
                                _xf.write(_xml_content)
                            mkv_cmd += ["--tags", f"all:{_xml_path}"]

                            # Track title tags
                            _audio_title = (metadata_fields.get("audio") or "").strip()
                            _video_title = (metadata_fields.get("video") or "").strip()
                            _sub_title   = (metadata_fields.get("subtitle") or "").strip()
                            if _audio_title:
                                mkv_cmd += ["--edit", "track:a1", "--set", f"name={_audio_title}"]
                            if _video_title:
                                mkv_cmd += ["--edit", "track:v1", "--set", f"name={_video_title}"]
                            if _sub_title:
                                mkv_cmd += ["--edit", "track:s1", "--set", f"name={_sub_title}"]

                            log.debug(
                                "[add_metadata] mkvpropedit cmd: {cmd}",
                                cmd=" ".join(mkv_cmd),
                            )
                            _mp = await asyncio.create_subprocess_exec(
                                *mkv_cmd,
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE,
                            )
                            _mp_out, _mp_err = await _mp.communicate()
                            _mp_stderr = _mp_err.decode(errors="replace").strip()

                            if _mp.returncode == 0:
                                log.debug("[add_metadata] mkvpropedit succeeded")
                                await _safe_edit(ms, "✅ Metadata added.")
                                return output_path
                            else:
                                log.warning(
                                    "[add_metadata] mkvpropedit failed (exit={rc}): {err}",
                                    rc=_mp.returncode, err=_mp_stderr[-300:],
                                )
                                # Fall through to FFmpeg strategies below
                        finally:
                            try:
                                os.unlink(_xml_path)
                            except Exception:
                                pass
                    else:
                        # No tags to write — just copy and return
                        await _safe_edit(ms, "✅ Metadata added.")
                        return output_path
            else:
                log.warning("[add_metadata] mkvpropedit not found — using FFmpeg fallback")

        # ── 4. FFmpeg fallback (non-MKV or mkvpropedit failed) ───────────────

        _ffmpeg = _find_binary("ffmpeg")
        _base = [
            _ffmpeg, "-y",
            "-probesize", "100M", "-analyzeduration", "100M",
            "-threads", "1", "-i", input_path,
        ]

        ffmpeg_strategies: list[list[str]] = []

        # Strategy A: explicit per-index map (excludes all bad streams)
        if probe_ok and good_indices:
            _map_args: list[str] = []
            for idx in good_indices:
                _map_args += ["-map", f"0:{idx}"]
            ffmpeg_strategies.append(
                _base + _map_args + ["-c", "copy"]
                + meta_args + ["-progress", "pipe:1", "-nostats", output_path]
            )

        # Strategy B: specifier map
        ffmpeg_strategies.append(
            _base + ["-map", "0:V?", "-map", "0:a?", "-map", "0:s?"]
            + ["-c", "copy"] + meta_args
            + ["-progress", "pipe:1", "-nostats", output_path]
        )

        # Strategy C: map all + ignore_unknown
        ffmpeg_strategies.append(
            _base + ["-map", "0", "-ignore_unknown", "-c", "copy"]
            + meta_args + ["-progress", "pipe:1", "-nostats", output_path]
        )

        file_size  = os.path.getsize(input_path)
        start_time = time.time()
        last_edit  = 0.0

        async def _drain_progress(stdout) -> None:
            nonlocal last_edit
            out_time_us = 0
            async for raw in stdout:
                line = raw.decode(errors="replace").strip()
                if line.startswith("out_time_us="):
                    try:
                        out_time_us = int(line.split("=", 1)[1])
                    except ValueError:
                        pass
                now = time.time()
                if (now - last_edit) < 5 or ms is None:
                    continue
                last_edit = now
                elapsed = now - start_time
                pct = min((out_time_us / total_us * 100) if total_us > 0 else 0, 99.9)
                eta_str = "..."
                if elapsed > 0 and pct > 0:
                    eta_str = TimeFormatter(milliseconds=int((elapsed / pct) * (100 - pct) * 1000))
                bar = "▣" * math.floor(pct / 5) + "▢" * (20 - math.floor(pct / 5))
                try:
                    await ms.edit(
                        text=(
                            f"🏷 Adding Metadata... ⚡\n\n{bar}\n\n"
                            f"<b>📦 Size :</b>  {humanbytes(file_size)}\n"
                            f"<b>✅ Done :</b>  {round(pct, 1)}%\n"
                            f"<b>⏱ ETA :</b>   {eta_str}\n"
                            f"<b>⏳ Time :</b>  {TimeFormatter(milliseconds=int(elapsed * 1000))}\n"
                        ),
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("✖️ Cancel", callback_data="close")]]
                        ),
                    )
                except Exception:
                    pass

        proc = None
        stderr_txt = ""
        success = False

        for attempt, cmd in enumerate(ffmpeg_strategies, 1):
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except OSError:
                    pass

            log.debug(
                "[add_metadata] ffmpeg strategy {n}/{t}: {cmd}",
                n=attempt, t=len(ffmpeg_strategies), cmd=" ".join(cmd),
            )

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr_bytes = await asyncio.gather(
                _drain_progress(proc.stdout),
                proc.stderr.read(),
            )
            await proc.wait()
            stderr_txt = stderr_bytes.decode(errors="replace").strip()
            if stderr_txt:
                log.debug(Msg.META_FFMPEG_STDERR, stderr=stderr_txt[-600:])

            if (
                proc.returncode == 0
                and os.path.exists(output_path)
                and os.path.getsize(output_path) > 0
            ):
                log.debug("[add_metadata] ffmpeg strategy {n} succeeded", n=attempt)
                success = True
                break

            log.warning(
                "[add_metadata] strategy {n} failed (exit={rc}) — trying next",
                n=attempt, rc=proc.returncode,
            )

        if not success:
            rc = proc.returncode if proc else -1
            log.error(
                Msg.META_ERROR,
                error=f"all strategies failed. ffmpeg exit {rc}: {stderr_txt[-300:]}",
            )
            await _safe_edit(ms, "❌ Metadata injection failed.")
            return None

        await _safe_edit(ms, "✅ Metadata added.")
        return output_path

    except Exception as e:
        log.error(Msg.META_ERROR, error=e)
        await _safe_edit(ms, f"❌ Metadata injection failed: `{e}`")
        return None


async def _safe_edit(ms, text: str) -> None:
    try:
        await ms.edit(text)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# generate_sample_video — async subprocess, stream copy + re-encode fallback
# ══════════════════════════════════════════════════════════════════════════════

async def generate_sample_video(
    input_path: str,
    output_directory: str,
    duration: int = 30,
) -> str | None:
    """
    Cut a *duration*-second sample. Fully async subprocess.
    Never blocks the event loop.
    """
    total = await get_video_duration(input_path)

    if total <= 0:
        start = 0.0
    elif total <= duration:
        start = 0.0
    else:
        lo    = total * 0.10
        hi    = max(total * 0.70, lo + 1.0)
        start = random.uniform(lo, hi)
        start = min(start, total - duration - 0.5)

    out_file = os.path.join(output_directory, f"sample_{int(time.time())}.mp4")

    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        # -fflags +genpts: regenerate PTS for partial/non-seekable streams
        # -ignore_unknown: skip unrecognised streams (data/attachment tracks)
        "-fflags", "+genpts",
        "-ss", f"{start:.2f}", "-i", input_path,
        "-t", str(duration),
        "-map", "0",
        "-ignore_unknown",
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        "-movflags", "+faststart",
        out_file,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()

    if proc.returncode == 0 and os.path.exists(out_file) and os.path.getsize(out_file) > 0:
        return out_file

    log.warning(Msg.SAMPLE_COPY_FAIL, stderr=stderr.decode(errors="replace")[-200:])
    if os.path.exists(out_file):
        os.remove(out_file)

    # Re-encode fallback — works even on truncated partial streams
    proc2 = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-fflags", "+genpts+discardcorrupt",
        "-ss", f"{start:.2f}", "-i", input_path,
        "-t", str(duration),
        "-c:v", "libx264", "-c:a", "aac",
        "-preset", "ultrafast", "-crf", "28",
        "-movflags", "+faststart",
        out_file,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr2 = await proc2.communicate()

    if proc2.returncode != 0:
        log.error(Msg.SAMPLE_ENCODE_FAIL, stderr=stderr2.decode(errors="replace")[-200:])
        return None

    return out_file if os.path.exists(out_file) and os.path.getsize(out_file) > 0 else None


# ══════════════════════════════════════════════════════════════════════════════
# take_multi_screenshots — kept for internal use (raw frame list)
# ══════════════════════════════════════════════════════════════════════════════

async def take_multi_screenshots(
    video_file: str,
    output_directory: str,
    count: int = 6,
) -> list[tuple[str, float]]:
    """
    Capture *count* evenly-spaced frames concurrently via asyncio.gather.
    Returns list of (path, timestamp_seconds) tuples.
    All FFmpeg subprocesses run in parallel — never sequential.
    """
    duration = await get_video_duration(video_file)

    if duration <= 0:
        result = await take_screen_shot(video_file, output_directory, 0)
        return [(result, 0.0)] if result else []

    # Generate *count* evenly-spaced timestamps across the video.
    # Start at 2% (skip black intro) and end at 97% (skip credits).
    # Works for any count from 1 to 12 — no hardcoded list.
    if count <= 1:
        timestamps = [duration * 0.50]
    else:
        start_pct, end_pct = 0.02, 0.97
        step = (end_pct - start_pct) / (count - 1)
        timestamps = [duration * (start_pct + i * step) for i in range(count)]

    async def _one_shot(ts: float) -> tuple[str, float] | None:
        out = os.path.join(output_directory, f"ss_{int(time.time() * 1000)}_{ts:.0f}.jpg")
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-ss", f"{ts:.2f}", "-i", video_file,
            "-vframes", "1", "-q:v", "2", out,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        return (out, ts) if os.path.exists(out) and os.path.getsize(out) > 0 else None

    results = await asyncio.gather(*[_one_shot(ts) for ts in timestamps])
    return [r for r in results if r]


# ══════════════════════════════════════════════════════════════════════════════
# generate_screenshot_grid — combines 6 frames into one 3×2 grid image
#
# Layout (matches reference image):
#   [ frame1 | frame2 | frame3 ]
#   [ frame4 | frame5 | frame6 ]
#
# Each frame has its timestamp burned in the bottom-right corner:
#   HH:MM:SS  white text, black shadow for visibility on any background.
#
# PIL work runs in run_blocking() — never touches event loop.
# ══════════════════════════════════════════════════════════════════════════════

def _build_grid_sync(
    frames: list[tuple[str, float]],
    output_path: str,
    cols: int = 3,
    thumb_w: int = 426,
    thumb_h: int = 240,
    padding: int = 10,
    bg_color: tuple = (15, 15, 15),
) -> str:
    """
    Synchronous PIL worker — called via run_blocking().

    frames  : list of (image_path, seconds) — must have 1..6 entries
    output_path : where to save the final JPEG grid
    cols    : number of columns (3)
    thumb_w / thumb_h : size each frame is resized to
    padding : gap between frames and border
    bg_color: canvas background (near-black)
    """
    from PIL import Image, ImageDraw, ImageFont

    rows = (len(frames) + cols - 1) // cols

    canvas_w = cols * thumb_w + (cols + 1) * padding
    canvas_h = rows * thumb_h + (rows + 1) * padding
    canvas   = Image.new("RGB", (canvas_w, canvas_h), bg_color)

    # Try to load a monospace font; fall back to default
    font = None
    font_shadow = None
    for font_path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf",
        "/usr/share/fonts/TTF/DejaVuSansMono-Bold.ttf",
    ):
        if os.path.exists(font_path):
            try:
                font        = ImageFont.truetype(font_path, 18)
                font_shadow = font
                break
            except Exception:
                pass
    if font is None:
        font = ImageFont.load_default()

    for idx, (img_path, ts_sec) in enumerate(frames):
        row = idx // cols
        col = idx %  cols

        x = padding + col * (thumb_w + padding)
        y = padding + row * (thumb_h + padding)

        # Open + resize frame
        try:
            frame = Image.open(img_path).convert("RGB")
            frame = frame.resize((thumb_w, thumb_h), Image.LANCZOS)
        except Exception:
            # Blank frame on load error
            frame = Image.new("RGB", (thumb_w, thumb_h), (30, 30, 30))

        # ── Timestamp overlay ────────────────────────────────────────────────
        ts_str = _seconds_to_ts(ts_sec)
        draw   = ImageDraw.Draw(frame)

        # Measure text size
        try:
            bbox   = draw.textbbox((0, 0), ts_str, font=font)
            txt_w  = bbox[2] - bbox[0]
            txt_h  = bbox[3] - bbox[1]
        except AttributeError:
            # Older Pillow fallback
            txt_w, txt_h = draw.textsize(ts_str, font=font)

        tx = thumb_w - txt_w - 8
        ty = thumb_h - txt_h - 8

        # Shadow / outline (draw offset copies in black)
        for dx, dy in ((-1, -1), (1, -1), (-1, 1), (1, 1), (0, 1), (1, 0)):
            draw.text((tx + dx, ty + dy), ts_str, font=font, fill=(0, 0, 0))
        # White foreground
        draw.text((tx, ty), ts_str, font=font, fill=(255, 255, 255))

        canvas.paste(frame, (x, y))

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    canvas.save(output_path, "JPEG", quality=92, optimize=True)
    return output_path


def _seconds_to_ts(seconds: float) -> str:
    """Convert float seconds → HH:MM:SS string."""
    s   = int(seconds)
    h   = s // 3600
    m   = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


async def generate_screenshot_grid(
    video_file: str,
    output_directory: str,
    count: int = 6,
    cols: int = 3,
) -> str | None:
    """
    High-level async entry point:
      1. Capture *count* frames in parallel (async subprocesses).
      2. Build 3×2 grid with PIL in thread pool (run_blocking).
      3. Clean up raw frame files.
      4. Return path to the combined grid JPEG, or None on failure.

    This is the ONLY function callers should use — never send individual
    screenshots separately.
    """
    frames = await take_multi_screenshots(video_file, output_directory, count)
    if not frames:
        return None

    grid_path = os.path.join(output_directory, "screenshot_grid.jpg")
    try:
        result = await run_blocking(_build_grid_sync, frames, grid_path, cols)
    except Exception as e:
        log.error("Grid build failed: {error}", error=e)
        return None
    finally:
        # Clean up individual raw frames regardless of grid success/failure
        for img_path, _ in frames:
            try:
                if os.path.exists(img_path):
                    os.remove(img_path)
            except Exception:
                pass

    return result if os.path.exists(result) else None
