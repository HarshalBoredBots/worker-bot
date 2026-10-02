# ══════════════════════════════════════════════════════════════════════════════
# Worker Rename Bot — Dockerfile
# Authoritative deployment path: Docker (render.yaml type: docker)
# FFmpeg is installed here via apt — build.sh static-binary approach is NOT
# used in Docker mode. Do not run build.sh inside this image.
# ══════════════════════════════════════════════════════════════════════════════

FROM python:3.10-slim-bookworm

LABEL maintainer="Worker Rename Bot"
LABEL description="Telegram file rename worker with FFmpeg metadata injection"

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        mkvtoolnix \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Non-root user (security hardening) ───────────────────────────────────────
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# ── Python dependencies (cached layer) ───────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Application code ──────────────────────────────────────────────────────────
COPY --chown=appuser:appuser . .

# ── Temp directory for downloads / ffmpeg processing ─────────────────────────
RUN mkdir -p /tmp/worker_downloads && chown appuser:appuser /tmp/worker_downloads

# ── Switch to non-root ────────────────────────────────────────────────────────
USER appuser

# ── Health-check port (Render uses PORT env var) ──────────────────────────────
EXPOSE 8015

# ── Health check — uses $PORT so it works with Render's PORT override ─────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f "http://localhost:${PORT:-8015}/health" || exit 1

# ── Environment defaults (non-secret only) ────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8015 \
    WORKER_CONCURRENCY=3 \
    HEARTBEAT_INTERVAL=30 \
    WORKER_OFFLINE_TIMEOUT=120 \
    SHUTDOWN_GRACE_SECONDS=60

# ── Entry point ───────────────────────────────────────────────────────────────
CMD ["python", "worker_bot.py"]
