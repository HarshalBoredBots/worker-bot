#!/usr/bin/env bash
# build.sh — Render/Heroku build script

set -euo pipefail

echo "==> Removing conflicting ffmpeg pip packages (python-ffmpeg / ffmpeg)"
pip uninstall -y python-ffmpeg ffmpeg ffmpeg-python 2>/dev/null || true

echo "==> Installing Python dependencies"
pip install --upgrade pip
pip install -r requirements.txt

echo "==> Installing static FFmpeg into ./bin/"
mkdir -p bin

ARCHIVE="ffmpeg-static.tar.xz"

echo "    Downloading FFmpeg static build..."
# Primary: johnvansickle.com static build
# Fallback: evermeet.cx (another reliable static build mirror)
curl -L --fail --retry 3 --retry-delay 5 \
    "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz" \
    -o "$ARCHIVE" \
|| curl -L --fail --retry 3 --retry-delay 5 \
    "https://github.com/yt-dlp/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz" \
    -o "$ARCHIVE"

echo "    Extracting..."
mkdir -p _ffmpeg_tmp
tar -xf "$ARCHIVE" -C _ffmpeg_tmp/
find _ffmpeg_tmp/ -maxdepth 2 -name "ffmpeg"  -type f -exec cp {} bin/ffmpeg  \;
find _ffmpeg_tmp/ -maxdepth 2 -name "ffprobe" -type f -exec cp {} bin/ffprobe \;
rm -rf "$ARCHIVE" _ffmpeg_tmp/

chmod +x bin/ffmpeg bin/ffprobe

echo "    FFmpeg version: $(bin/ffmpeg -version 2>&1 | head -1)"
echo "    FFprobe version: $(bin/ffprobe -version 2>&1 | head -1)"

echo "==> Installing mkvtoolnix (mkvpropedit) via apt into ./bin/"
# AppImage extraction requires FUSE which is unavailable on Heroku/Render dynos.
# apt-get is available during the build phase on both platforms.
apt-get update -qq && apt-get install -y -qq mkvtoolnix

MKV_BIN="$(which mkvpropedit 2>/dev/null || true)"
if [ -n "$MKV_BIN" ]; then
    cp "$MKV_BIN" bin/mkvpropedit
    chmod +x bin/mkvpropedit
    echo "    mkvpropedit ready: $(bin/mkvpropedit --version 2>&1 | head -1)"
else
    echo "    WARNING: mkvpropedit not available — mutagen Python fallback will be used"
fi

echo "==> Build complete"
