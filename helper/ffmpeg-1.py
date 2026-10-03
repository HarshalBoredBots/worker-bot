"""
helper/ffmpeg.py
─────────────────
FFmpeg/FFprobe helpers — every CPU-bound / subprocess call is async-safe.

run_blocking(func, *args)
  Routes sync functions through loop.run_in_executor so they NEVER block
  the event loop.  asyncio.create_subprocess_exec is used for all
  subprocess calls so FFmpeg never holds the event loop either.

ROOT CAUSES FIXED IN add_metadata():
─────────────────────────────────────
1. ATTACHED PICTURE BUG (stream shrink to ~277 MB)
   The probe excluded codec_type=attachment and codec_type=data streams, but
   it did NOT exclude streams with disposition.attached_pic=1.  Attached
   pictures are stored with codec_type=video (e.g. codec_name=mjpeg) and
   therefore passed the filter.  When FFmpeg tries to mux an attached-picture
   stream into a Matroska output with -c copy, it exits 183:
     [out#0/matroska] Could not write header (incorrect codec parameters?)
   The previous null-mux workaround "fixed" this by EXCLUDING stream 0 from
   the output, which happened to be the ~1.2 GB video track — producing a
   277 MB audio+subtitle-only file that was silently uploaded as "success".
   FIX: check disposition["attached_pic"] in the probe loop and skip those
   streams from the good_indices list.

2. MISSING SIZE-RATIO VALIDATION
   A metadata-only copy operation must produce output ≥ 90% of input size
   (stream copy preserves byte-for-byte the audio/video data; only container
   overhead changes).  If the output is dramatically smaller, something went
   wrong (a stream was silently dropped).  Added a hard check:
     output_size / input_size < MIN_SIZE_RATIO → treat as failure, delete
     output, preserve input, report clearly.

3. FFMPEG EXIT 183 ON PROBLEMATIC MKV FILES
   These anime MKV files have a video stream whose codec extradata causes
   avformat_write_header() to return AVERROR_INVALIDDATA (exit 183).  Added
   -fflags +genpts -avoid_negative_ts make_zero -max_muxing_queue_size 9999
   to give FFmpeg the best chance of succeeding.  The mkvpropedit path is
   the only 100% reliable approach — FFmpeg is kept as a last-resort fallback.

4. TRANSACTIONAL PROCESSING
   Output is always written to output_path (a separate temp location from
   input_path) and only declared success after size validation.  The input
   is NEVER modified in-place by any FFmpeg strategy.  On failure the output
   temp file is removed and input_path is left untouched.
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
    Search order:
      1. <repo_root>/bin/<name>  — bootstrapped at dyno startup
      2. shutil.which(name)      — system PATH
    Returns full path if found and executable, else None.
    """
    bin_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bin",
    )
    local = os.path.join(bin_dir, name)
    if os.path.isfile(local) and os.access(local, os.X_OK):
        return local
    return shutil.which(name)


# ══════════════════════════════════════════════════════════════════════════════
# run_blocking — bridge sync → async
# ══════════════════════════════════════════════════════════════════════════════

async def run_blocking(func, *args, **kwargs) -> Any:
    loop = asyncio.get_running_loop()
    if kwargs:
        func = functools.partial(func, **kwargs)
    return await loop.run_in_executor(None, func, *args)


# ══════════════════════════════════════════════════════════════════════════════
# fix_thumb
# ══════════════════════════════════════════════════════════════════════════════

def _fix_thumb_sync(thumb: str):
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
# take_screen_shot
# ══════════════════════════════════════════════════════════════════════════════

async def take_screen_shot(video_file: str, output_directory: str, ttl: int):
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
# get_video_duration
# ══════════════════════════════════════════════════════════════════════════════

async def get_video_duration(file_path: str) -> float:
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
# get_duration_hachoir
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
    return await run_blocking(_hachoir_duration_sync, file_path)


# ══════════════════════════════════════════════════════════════════════════════
# probe_media — structured media inspection used before AND after processing
# ══════════════════════════════════════════════════════════════════════════════

async def probe_media(file_path: str) -> dict | None:
    """
    Run ffprobe and return the parsed JSON, or None on failure.

    Returned dict has at minimum:
        format       : dict (format_name, duration, size, bit_rate, tags)
        streams      : list of stream dicts
        video_codecs : list[str]
        audio_codecs : list[str]
        has_video    : bool
        has_audio    : bool
        duration_s   : float
    """
    _ffprobe = _find_binary("ffprobe") or "ffprobe"
    try:
        proc = await asyncio.create_subprocess_exec(
            _ffprobe, "-v", "quiet",
            "-print_format", "json",
            "-show_streams", "-show_format",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0 or not out:
            return None
        data = json.loads(out.decode(errors="replace"))
        streams = data.get("streams", [])
        fmt     = data.get("format", {})
        data["video_codecs"] = [
            s.get("codec_name", "unknown")
            for s in streams
            if s.get("codec_type") == "video"
            and not s.get("disposition", {}).get("attached_pic", 0)
        ]
        data["audio_codecs"] = [
            s.get("codec_name", "unknown")
            for s in streams
            if s.get("codec_type") == "audio"
        ]
        data["has_video"]   = bool(data["video_codecs"])
        data["has_audio"]   = bool(data["audio_codecs"])
        try:
            data["duration_s"] = float(fmt.get("duration", 0))
        except (TypeError, ValueError):
            data["duration_s"] = 0.0
        return data
    except Exception as exc:
        log.warning("[probe_media] ffprobe failed for {path}: {err}", path=file_path, err=exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# add_metadata — embed tags without re-encoding
# ══════════════════════════════════════════════════════════════════════════════

# Minimum acceptable size ratio for a metadata-only stream-copy operation.
# Stream copy preserves ALL bytes of audio/video/subtitle data — the only
# size change is container overhead (a few KB at most).
# Anything below 90% of input signals that streams were silently dropped.
# This threshold is intentionally conservative: even a file with 10 audio
# tracks and 20 subtitle tracks losing ALL non-video tracks would be caught.
_MIN_SIZE_RATIO = 0.90


async def add_metadata(
    input_path: str,
    output_path: str,
    metadata_fields: dict,
    ms,
) -> str | None:
    """
    Embed metadata tags into *input_path* → *output_path*.
    Stream copy ONLY — never re-encodes. Returns output_path on success.

    Strategy order:
      1. mkvpropedit (MKV only) — edits tags IN-PLACE on a copy of the input.
         Never touches streams, never fails due to codec parameter issues.
      2. FFmpeg — explicit per-stream-index map with multiple flag sets tried
         in sequence. Each strategy is more permissive than the last.

    Size validation:
      After any strategy succeeds, output_size / input_size is checked.
      If the ratio is below _MIN_SIZE_RATIO (90%), the output is deleted
      and the job is marked as failed (streams were silently dropped).

    Transactional guarantee:
      input_path is NEVER modified. All writes go to output_path. On failure
      output_path is deleted (if it exists) and input_path is left intact.
    """
    import math
    from helper.utils import humanbytes, TimeFormatter
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    input_size = os.path.getsize(input_path) if os.path.exists(input_path) else 0

    try:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

        # ── 1. Probe: duration + good stream indices ───────────────────────────
        total_us: int      = 0
        good_indices: list[int] = []
        probe_ok           = False

        try:
            _ffprobe = _find_binary("ffprobe") or "ffprobe"
            _probe = await asyncio.create_subprocess_exec(
                _ffprobe, "-v", "quiet",
                "-print_format", "json",
                "-show_streams", "-show_format",
                input_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            _probe_out, _ = await _probe.communicate()
            if _probe.returncode == 0 and _probe_out:
                _pd = json.loads(_probe_out.decode(errors="replace"))

                dur_str  = _pd.get("format", {}).get("duration", "0")
                total_us = int(float(dur_str) * 1_000_000)

                # Skip streams that FFmpeg cannot copy into Matroska output:
                #
                # _SKIP_TYPES: codec_type=attachment (fonts, sfnt) and
                #              codec_type=data (timecodes, etc.)
                #   These trigger "Invalid data" / exit 183 in avformat_write_header.
                #
                # _SKIP_CODECS: unknown/none codec — FFmpeg cannot determine
                #               codec parameters at all.
                #
                # attached_pic disposition: streams with codec_type=video but
                #   actually storing a cover-art JPEG/PNG image.  These are
                #   declared as video by ffprobe but fail in the matroska muxer.
                #   ROOT CAUSE OF THE 277 MB BUG: these were previously included
                #   in good_indices, causing exit 183.  The old null-mux workaround
                #   then excluded them together with the REAL video track.
                #
                _SKIP_TYPES  = {"attachment", "data"}
                _SKIP_CODECS = {"none", "unknown", ""}

                for s in _pd.get("streams", []):
                    ct   = s.get("codec_type", "")
                    cn   = s.get("codec_name", "none").lower()
                    idx  = s.get("index")
                    disp = s.get("disposition", {})
                    # FIX: skip attached pictures even though codec_type=video
                    is_attached_pic = bool(disp.get("attached_pic", 0))

                    if (
                        ct not in _SKIP_TYPES
                        and cn not in _SKIP_CODECS
                        and idx is not None
                        and not is_attached_pic          # ← THE BUG FIX
                    ):
                        good_indices.append(int(idx))

                probe_ok = True
                log.info(
                    "[add_metadata] probe OK — "
                    "good_indices={idxs}  total_streams={n}  "
                    "attached_pics_skipped={pics}",
                    idxs=good_indices,
                    n=len(_pd.get("streams", [])),
                    pics=sum(
                        1 for s in _pd.get("streams", [])
                        if s.get("disposition", {}).get("attached_pic", 0)
                    ),
                )
        except Exception as _pe:
            log.warning("[add_metadata] probe failed: {err}", err=_pe)

        # ── 2. Build FFmpeg metadata args ──────────────────────────────────────
        meta_args: list[str] = []
        for tag in ("title", "artist", "author", "comment"):
            val = (metadata_fields.get(tag) or "").strip()
            if val:
                meta_args += ["-metadata", f"{tag}={val}"]
        audio_title = (metadata_fields.get("audio")    or "").strip()
        video_title = (metadata_fields.get("video")    or "").strip()
        sub_title   = (metadata_fields.get("subtitle") or "").strip()
        if audio_title:
            meta_args += ["-metadata:s:a", f"title={audio_title}"]
        if video_title:
            meta_args += ["-metadata:s:v", f"title={video_title}"]
        if sub_title:
            meta_args += ["-metadata:s:s", f"title={sub_title}"]

        _input_ext = os.path.splitext(input_path)[1].lower()

        # ── 3. mkvpropedit path (MKV only — no stream remux at all) ───────────
        if _input_ext == ".mkv":
            _mkvpropedit = _find_binary("mkvpropedit")
            if _mkvpropedit:
                result = await _run_mkvpropedit(
                    _mkvpropedit, input_path, output_path, metadata_fields, ms
                )
                if result:
                    # Size validation — mkvpropedit is in-place so ratio ≈ 1.0
                    ok, ratio = _size_ok(input_size, output_path)
                    if ok:
                        return output_path
                    else:
                        log.error(
                            "[add_metadata] mkvpropedit output size suspicious "
                            "(ratio={r:.3f}) — aborting",
                            r=ratio,
                        )
                        _remove_safe(output_path)
                        return None
                # mkvpropedit failed — fall through to FFmpeg
            else:
                log.warning(
                    "[add_metadata] mkvpropedit not available — "
                    "using FFmpeg fallback. "
                    "Metadata embed may fail for MKV files with broken streams."
                )

        # ── 4. FFmpeg strategies ───────────────────────────────────────────────
        _ffmpeg = _find_binary("ffmpeg") or "ffmpeg"

        # Base flags applied to all strategies.
        # -fflags +genpts       : generate PTS when missing — resolves many
        #                         "Invalid data" errors at header write time.
        # -avoid_negative_ts    : prevent negative timestamps from breaking muxer.
        # -max_muxing_queue_size: prevents queue-overflow errors on files with
        #                         many streams at different bitrates.
        # -probesize / -analyzeduration: probe more data so FFmpeg finds the
        #                         codec extradata (SPS/PPS for H.264/HEVC).
        _base = [
            _ffmpeg, "-y",
            "-fflags", "+genpts",
            "-probesize", "200M", "-analyzeduration", "200M",
            "-i", input_path,
            "-avoid_negative_ts", "make_zero",
            "-max_muxing_queue_size", "9999",
        ]

        ffmpeg_strategies: list[tuple[str, list[str]]] = []

        # Strategy A — explicit per-index map (safest: only known-good streams)
        # This excludes ALL attachment, data, and attached_pic streams by index.
        if probe_ok and good_indices:
            _map_args = []
            for idx in good_indices:
                _map_args += ["-map", f"0:{idx}"]
            ffmpeg_strategies.append((
                "explicit-index-map",
                _base + _map_args + ["-c", "copy"]
                + meta_args + ["-progress", "pipe:1", "-nostats", output_path],
            ))

        # Strategy B — codec-type specifiers
        # -map 0:V? : all non-attached-picture video streams (capital V)
        # -map 0:a? : all audio streams
        # -map 0:s? : all subtitle streams
        # Omits attachment and data streams by design.
        ffmpeg_strategies.append((
            "type-specifier-map",
            _base
            + ["-map", "0:V?", "-map", "0:a?", "-map", "0:s?"]
            + ["-c", "copy"] + meta_args
            + ["-progress", "pipe:1", "-nostats", output_path],
        ))

        # Strategy C — map everything, tell FFmpeg to ignore unknown streams
        # Last resort: may include attachment streams depending on FFmpeg version.
        ffmpeg_strategies.append((
            "map-all-ignore-unknown",
            _base
            + ["-map", "0", "-ignore_unknown", "-copy_unknown", "-c", "copy"]
            + meta_args + ["-progress", "pipe:1", "-nostats", output_path],
        ))

        file_size  = input_size
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
                    eta_str = TimeFormatter(
                        milliseconds=int((elapsed / pct) * (100 - pct) * 1000)
                    )
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

        proc       = None
        stderr_txt = ""
        success    = False

        for strategy_name, cmd in ffmpeg_strategies:
            _remove_safe(output_path)

            log.info(
                "[add_metadata] FFmpeg strategy={name} cmd={cmd}",
                name=strategy_name,
                cmd=" ".join(cmd),
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
                log.info(
                    "[add_metadata] FFmpeg stderr (strategy={name} exit={rc}):\n{err}",
                    name=strategy_name,
                    rc=proc.returncode,
                    err=stderr_txt[-800:],
                )

            if proc.returncode != 0:
                log.warning(
                    "[add_metadata] strategy={name} exit={rc} — trying next",
                    name=strategy_name, rc=proc.returncode,
                )
                _remove_safe(output_path)
                continue

            if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
                log.warning(
                    "[add_metadata] strategy={name} exit=0 but no output file — "
                    "trying next",
                    name=strategy_name,
                )
                continue

            # Size-ratio validation — catches the "stream silently dropped" bug
            ok, ratio = _size_ok(input_size, output_path)
            if not ok:
                log.error(
                    "[add_metadata] strategy={name} output suspicious: "
                    "input={inp} bytes  output={out} bytes  ratio={r:.3f} "
                    "(threshold={thresh:.2f}) — streams were dropped. "
                    "Discarding output and trying next strategy.",
                    name=strategy_name,
                    inp=input_size,
                    out=os.path.getsize(output_path),
                    r=ratio,
                    thresh=_MIN_SIZE_RATIO,
                )
                _remove_safe(output_path)
                continue

            log.info(
                "[add_metadata] strategy={name} succeeded — "
                "input={inp}  output={out}  ratio={r:.3f}",
                name=strategy_name,
                inp=humanbytes(input_size),
                out=humanbytes(os.path.getsize(output_path)),
                r=ratio,
            )
            success = True
            break

        if not success:
            rc = proc.returncode if proc else -1
            log.error(
                "[add_metadata] ALL strategies failed.\n"
                "  container   : {ext}\n"
                "  good_indices: {idxs}\n"
                "  last exit   : {rc}\n"
                "  last stderr : {err}\n"
                "  input_size  : {sz} bytes\n"
                "NOTE: If exit=183 ('Could not write header: Invalid data found'),\n"
                "  this MKV file has a video stream whose codec extradata cannot\n"
                "  be remuxed by this FFmpeg build.  mkvpropedit is the only\n"
                "  reliable fix — ensure the mkvtoolnix bootstrap succeeds.",
                ext=_input_ext,
                idxs=good_indices,
                rc=rc,
                err=stderr_txt[-400:],
                sz=input_size,
            )
            _remove_safe(output_path)
            await _safe_edit(ms, "❌ Metadata injection failed.")
            return None

        await _safe_edit(ms, "✅ Metadata added.")
        return output_path

    except Exception as e:
        log.error(Msg.META_ERROR, error=e)
        _remove_safe(output_path)
        await _safe_edit(ms, f"❌ Metadata injection failed: `{e}`")
        return None


# ── mkvpropedit helper ────────────────────────────────────────────────────────

async def _run_mkvpropedit(
    mkvpropedit: str,
    input_path: str,
    output_path: str,
    metadata_fields: dict,
    ms,
) -> bool:
    """
    Copy input → output, then edit tags in output in-place via mkvpropedit.
    Returns True on success, False on failure.
    """
    import shutil as _shutil
    import tempfile as _tf

    # Copy input → output (mkvpropedit edits in-place)
    try:
        _shutil.copy2(input_path, output_path)
    except Exception as exc:
        log.warning("[add_metadata] copy for mkvpropedit failed: {err}", err=exc)
        _remove_safe(output_path)
        return False

    if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
        log.warning("[add_metadata] output copy is empty after shutil.copy2")
        return False

    # Build tags XML
    tag_lines = []
    _field_to_mkv = {
        "title":   "TITLE",
        "artist":  "ARTIST",
        "author":  "AUTHOR",
        "comment": "COMMENT",
    }
    for field, mkv_tag in _field_to_mkv.items():
        val = (metadata_fields.get(field) or "").strip()
        if val:
            # Escape XML special chars
            val_esc = (
                val.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace('"', "&quot;")
            )
            tag_lines.append(
                f"    <Simple>"
                f"<Name>{mkv_tag}</Name>"
                f"<String>{val_esc}</String>"
                f"</Simple>"
            )

    xml_path = None
    cmd = [mkvpropedit, output_path, "--tags", "all:"]

    if tag_lines:
        xml_content = (
            '<?xml version="1.0"?>\n'
            '<!DOCTYPE Tags SYSTEM "matroskatags.dtd">\n'
            '<Tags>\n'
            '  <Tag>\n'
            '    <Targets/>\n'
            + "\n".join(tag_lines) + "\n"
            '  </Tag>\n'
            '</Tags>\n'
        )
        fd, xml_path = _tf.mkstemp(suffix=".xml")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as xf:
                xf.write(xml_content)
        except Exception as exc:
            log.warning("[add_metadata] could not write XML: {err}", err=exc)
            _cleanup_fd(fd)
            return False

        cmd += ["--tags", f"all:{xml_path}"]

    # Track-level title tags
    audio_title = (metadata_fields.get("audio")    or "").strip()
    video_title = (metadata_fields.get("video")    or "").strip()
    sub_title   = (metadata_fields.get("subtitle") or "").strip()
    if audio_title:
        cmd += ["--edit", "track:a1", "--set", f"name={audio_title}"]
    if video_title:
        cmd += ["--edit", "track:v1", "--set", f"name={video_title}"]
    if sub_title:
        cmd += ["--edit", "track:s1", "--set", f"name={sub_title}"]

    log.info("[add_metadata] mkvpropedit cmd: {cmd}", cmd=" ".join(cmd))

    try:
        _mp = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _mp_out, _mp_err = await _mp.communicate()
        stderr_txt = _mp_err.decode(errors="replace").strip()

        if _mp.returncode == 0:
            log.info("[add_metadata] mkvpropedit succeeded")
            return True
        else:
            log.warning(
                "[add_metadata] mkvpropedit failed (exit={rc}): {err}",
                rc=_mp.returncode, err=stderr_txt[-400:],
            )
            _remove_safe(output_path)
            return False
    finally:
        if xml_path:
            _remove_safe(xml_path)


def _cleanup_fd(fd) -> None:
    try:
        os.close(fd)
    except Exception:
        pass


def _remove_safe(path: str | None) -> None:
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def _size_ok(input_size: int, output_path: str) -> tuple[bool, float]:
    """
    Return (True, ratio) if output size is ≥ _MIN_SIZE_RATIO of input.
    Return (False, ratio) if the ratio is suspicious (streams dropped).
    Always returns True if input_size is 0 (unknown) or output is very small
    (metadata-only files like short MP3s can legitimately be small).
    """
    if input_size <= 0:
        return True, 1.0
    if not os.path.exists(output_path):
        return False, 0.0
    out_size = os.path.getsize(output_path)
    # Files under 1 MB: skip ratio check (cover art, short clips)
    if input_size < 1024 * 1024:
        return True, out_size / input_size if input_size else 1.0
    ratio = out_size / input_size
    return ratio >= _MIN_SIZE_RATIO, ratio


async def _safe_edit(ms, text: str) -> None:
    try:
        await ms.edit(text)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# generate_sample_video
# ══════════════════════════════════════════════════════════════════════════════

async def generate_sample_video(
    input_path: str,
    output_directory: str,
    duration: int = 30,
) -> str | None:
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
    _remove_safe(out_file)

    # Re-encode fallback
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
# take_multi_screenshots
# ══════════════════════════════════════════════════════════════════════════════

async def take_multi_screenshots(
    video_file: str,
    output_directory: str,
    count: int = 6,
) -> list[tuple[str, float]]:
    duration = await get_video_duration(video_file)

    if duration <= 0:
        result = await take_screen_shot(video_file, output_directory, 0)
        return [(result, 0.0)] if result else []

    if count <= 1:
        timestamps = [duration * 0.50]
    else:
        start_pct, end_pct = 0.02, 0.97
        step = (end_pct - start_pct) / (count - 1)
        timestamps = [duration * (start_pct + i * step) for i in range(count)]

    async def _one_shot(ts: float) -> tuple[str, float] | None:
        out = os.path.join(
            output_directory, f"ss_{int(time.time() * 1000)}_{ts:.0f}.jpg"
        )
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
# generate_screenshot_grid
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
    from PIL import Image, ImageDraw, ImageFont

    rows = (len(frames) + cols - 1) // cols

    canvas_w = cols * thumb_w + (cols + 1) * padding
    canvas_h = rows * thumb_h + (rows + 1) * padding
    canvas   = Image.new("RGB", (canvas_w, canvas_h), bg_color)

    font = None
    for font_path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf",
        "/usr/share/fonts/TTF/DejaVuSansMono-Bold.ttf",
    ):
        if os.path.exists(font_path):
            try:
                font = ImageFont.truetype(font_path, 18)
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
        try:
            frame = Image.open(img_path).convert("RGB")
            frame = frame.resize((thumb_w, thumb_h), Image.LANCZOS)
        except Exception:
            frame = Image.new("RGB", (thumb_w, thumb_h), (30, 30, 30))

        ts_str = _seconds_to_ts(ts_sec)
        draw   = ImageDraw.Draw(frame)
        try:
            bbox   = draw.textbbox((0, 0), ts_str, font=font)
            txt_w  = bbox[2] - bbox[0]
            txt_h  = bbox[3] - bbox[1]
        except AttributeError:
            txt_w, txt_h = draw.textsize(ts_str, font=font)

        tx = thumb_w - txt_w - 8
        ty = thumb_h - txt_h - 8
        for dx, dy in ((-1, -1), (1, -1), (-1, 1), (1, 1), (0, 1), (1, 0)):
            draw.text((tx + dx, ty + dy), ts_str, font=font, fill=(0, 0, 0))
        draw.text((tx, ty), ts_str, font=font, fill=(255, 255, 255))
        canvas.paste(frame, (x, y))

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    canvas.save(output_path, "JPEG", quality=92, optimize=True)
    return output_path


def _seconds_to_ts(seconds: float) -> str:
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
        for img_path, _ in frames:
            try:
                if os.path.exists(img_path):
                    os.remove(img_path)
            except Exception:
                pass

    return result if os.path.exists(result) else None
