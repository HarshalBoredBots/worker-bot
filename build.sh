#!/usr/bin/env bash
# build.sh — Heroku release-phase script
# FFmpeg is downloaded at dyno startup by worker_bot.py (_bootstrap_ffmpeg).
# This script only handles Python deps and mkvtoolnix.

set -euo pipefail

echo "==> Removing conflicting ffmpeg pip packages (python-ffmpeg / ffmpeg)"
pip uninstall -y python-ffmpeg ffmpeg ffmpeg-python 2>/dev/null || true

echo "==> Installing Python dependencies"
pip install --upgrade pip
pip install -r requirements.txt

echo "==> Installing mkvtoolnix (mkvpropedit) via apt into ./bin/"
apt-get update -qq && apt-get install -y -qq mkvtoolnix

mkdir -p bin
MKV_BIN="$(which mkvpropedit 2>/dev/null || true)"
if [ -n "$MKV_BIN" ]; then
    cp "$MKV_BIN" bin/mkvpropedit
    chmod +x bin/mkvpropedit
    echo "    mkvpropedit ready: $(bin/mkvpropedit --version 2>&1 | head -1)"
else
    echo "    WARNING: mkvpropedit not available — mutagen Python fallback will be used"
fi

echo "==> Build complete (FFmpeg will be downloaded at dyno startup)"
