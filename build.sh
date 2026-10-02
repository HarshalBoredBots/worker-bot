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

FFMPEG_URL="https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
ARCHIVE="ffmpeg-static.tar.xz"

echo "    Downloading FFmpeg static build..."
curl -L "$FFMPEG_URL" -o "$ARCHIVE"

echo "    Extracting..."
tar -xf "$ARCHIVE" --strip-components=1 --wildcards "*/ffmpeg" "*/ffprobe" -C bin/
rm "$ARCHIVE"

chmod +x bin/ffmpeg bin/ffprobe

echo "    FFmpeg version: $(bin/ffmpeg -version 2>&1 | head -1)"
echo "    FFprobe version: $(bin/ffprobe -version 2>&1 | head -1)"
echo "==> Build complete"
