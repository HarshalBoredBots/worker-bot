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

echo "==> Installing static mkvtoolnix (mkvpropedit) into ./bin/"
MKV_URL="https://mkvtoolnix.download/appimage/MKVToolNix_GUI-$(curl -s https://mkvtoolnix.download/appimage/latest_version.txt | tr -d '\n')-x86_64.AppImage"
# Use a pinned release as fallback if latest_version fetch fails
MKV_PINNED_URL="https://mkvtoolnix.download/appimage/MKVToolNix_GUI-88.0-x86_64.AppImage"

echo "    Trying latest mkvtoolnix AppImage..."
if curl -fL "$MKV_URL" -o bin/mkvtoolnix.appimage 2>/dev/null; then
    echo "    Downloaded latest mkvtoolnix AppImage"
else
    echo "    Falling back to pinned version..."
    curl -fL "$MKV_PINNED_URL" -o bin/mkvtoolnix.appimage
fi

chmod +x bin/mkvtoolnix.appimage

# Extract mkvpropedit from the AppImage (no FUSE needed — just --appimage-extract)
cd bin
./mkvtoolnix.appimage --appimage-extract usr/bin/mkvpropedit >/dev/null 2>&1 || true
if [ -f squashfs-root/usr/bin/mkvpropedit ]; then
    cp squashfs-root/usr/bin/mkvpropedit ./mkvpropedit
    chmod +x ./mkvpropedit
    echo "    mkvpropedit extracted from AppImage"
else
    # Alternative: extract all and find it
    ./mkvtoolnix.appimage --appimage-extract >/dev/null 2>&1 || true
    find squashfs-root -name mkvpropedit -type f 2>/dev/null | head -1 | xargs -I{} cp {} ./mkvpropedit || true
    [ -f ./mkvpropedit ] && chmod +x ./mkvpropedit && echo "    mkvpropedit found and extracted"
fi
rm -rf squashfs-root mkvtoolnix.appimage
cd ..

if [ -f bin/mkvpropedit ]; then
    echo "    mkvpropedit ready: $(bin/mkvpropedit --version 2>&1 | head -1)"
else
    echo "    WARNING: mkvpropedit not available — mutagen Python fallback will be used"
fi

echo "==> Build complete"
