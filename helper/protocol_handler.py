"""
worker/helper/protocol_handler.py
══════════════════════════════════════════════════════════════════════════════
Worker-side protocol handler — Bot-only edition.

Changes from original:
  - Duplicate task idempotency: a job_id seen in the last 500 entries is
    rejected without re-processing (prevents double-execution on reconnect).
  - Capacity gate: worker refuses tasks when already at full capacity instead
    of silently over-committing (the Manager should not send more than
    capacity, but this is a safety net).
  - Heartbeat loop uses bounded backoff on Telegram errors — no tight retry.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from typing import Callable, Awaitable, TYPE_CHECKING

from pyrogram.handlers import MessageHandler
from pyrogram import filters

from shared.protocol import (
    decode, is_proto,
    MSG_TASK,
    make_register, make_heartbeat, make_ack,
    make_state, make_result, make_failed,
    WORKER_ONLINE, WORKER_BUSY,
)

if TYPE_CHECKING:
    from pyrogram import Client

logger = logging.getLogger(__name__)

# LRU-bounded set of recently-seen job IDs (prevents duplicate execution)
_SEEN_JOBS_MAX = 500


class WorkerProtocolHandler:

    def __init__(
        self,
        bot_client:         "Client",
        worker_id:          str,
        control_group_id:   int,
        capacity:           int,
        heartbeat_interval: int,
        on_task:            Callable[[dict], Awaitable[None]],
    ):
        self._bot          = bot_client
        self._worker_id    = worker_id
        self._group_id     = int(control_group_id)
        self._capacity     = capacity
        self._hb_interval  = heartbeat_interval
        self._on_task      = on_task

        self._active_jobs  = 0
        self._active_lock  = asyncio.Lock()
        self._hb_task: asyncio.Task | None = None
        self._running      = False

        # Duplicate-task guard: OrderedDict used as bounded LRU set
        self._seen_jobs: OrderedDict[str, bool] = OrderedDict()

        self._bot.add_handler(
            MessageHandler(self._on_any_message, filters=filters.all)
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        bot_me = await self._bot.get_me()
        await self.send_register(
            bot_id   = bot_me.id,
            username = f"@{bot_me.username}" if bot_me.username else str(bot_me.id),
        )
        self._hb_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"hb_{self._worker_id}"
        )
        logger.info(
            "[worker] Protocol handler started — worker=%s bot=@%s capacity=%d",
            self._worker_id, bot_me.username, self._capacity,
        )

    async def stop(self) -> None:
        self._running = False
        if self._hb_task and not self._hb_task.done():
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass

    # ── Active job counter ────────────────────────────────────────────────────

    async def increment_active(self) -> None:
        async with self._active_lock:
            self._active_jobs += 1

    async def decrement_active(self) -> None:
        async with self._active_lock:
            self._active_jobs = max(0, self._active_jobs - 1)

    def active_count(self) -> int:
        return self._active_jobs

    def has_capacity(self) -> bool:
        return self._active_jobs < self._capacity

    # ── Outbound senders ──────────────────────────────────────────────────────

    async def send_register(self, bot_id: int, username: str) -> None:
        from config import Config
        await self._safe_send(make_register(
            worker_id = self._worker_id,
            bot_id    = bot_id,
            username  = username,
            capacity  = self._capacity,
            version   = Config.WORKER_VERSION,
        ), "REGISTER")

    async def send_ack(self, job_id: str) -> None:
        await self._safe_send(make_ack(self._worker_id, job_id), f"ACK {job_id}")

    async def send_state(self, job_id: str, state: str) -> None:
        await self._safe_send(make_state(self._worker_id, job_id, state), f"STATE {state}")

    async def send_result(
        self,
        job_id: str,
        output_chat_id: int,
        output_message_id: int,
        filename: str,
        original_filename: str,
        file_size: int,
        mediainfo_url: str | None = None,
    ) -> None:
        await self._safe_send(make_result(
            worker_id         = self._worker_id,
            job_id            = job_id,
            output_chat_id    = output_chat_id,
            output_message_id = output_message_id,
            filename          = filename,
            original_filename = original_filename,
            file_size         = file_size,
            mediainfo_url     = mediainfo_url,
        ), f"RESULT {job_id}")

    async def send_failed(self, job_id: str, reason: str) -> None:
        # Truncate reason to avoid leaking internal details in protocol messages
        safe_reason = str(reason)[:200] if reason else "Unknown error"
        await self._safe_send(make_failed(self._worker_id, job_id, safe_reason), f"FAILED {job_id}")

    async def _safe_send(self, text: str, label: str) -> None:
        try:
            await self._bot.send_message(self._group_id, text)
            logger.info("[worker] Sent %s", label)
        except Exception as exc:
            logger.error("[worker] Failed to send %s: %s", label, type(exc).__name__)

    # ── Heartbeat loop ────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        consecutive_errors = 0
        while self._running:
            try:
                status = WORKER_BUSY if self._active_jobs > 0 else WORKER_ONLINE
                await self._safe_send(make_heartbeat(
                    worker_id   = self._worker_id,
                    bot_id      = 0,
                    active_jobs = self._active_jobs,
                    capacity    = self._capacity,
                    status      = status,
                ), "HEARTBEAT")
                consecutive_errors = 0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                consecutive_errors += 1
                logger.warning("[worker] Heartbeat error (#%d): %s", consecutive_errors, type(exc).__name__)
                # Bounded backoff: max 5 min wait, then give up spamming
                if consecutive_errors >= 10:
                    logger.error("[worker] Heartbeat: too many consecutive errors, backing off 5 min")
                    await asyncio.sleep(300)
                    consecutive_errors = 0
                    continue
            try:
                await asyncio.sleep(self._hb_interval)
            except asyncio.CancelledError:
                break

    # ── Inbound: receive all group messages via bot ───────────────────────────

    async def _on_any_message(self, client, message) -> None:
        # Gate 1 — only control group
        chat_id = getattr(message.chat, "id", None)
        if chat_id != self._group_id:
            return

        # Gate 2 — protocol message check
        text = message.text or message.caption or ""
        if not is_proto(text):
            return

        msg = decode(text)
        if msg is None:
            return

        # Gate 3 — TASK addressed to this worker
        if msg.get("type") != MSG_TASK:
            return
        if msg.get("worker_id") != self._worker_id:
            return

        job_id = msg.get("job_id", "?")

        # Gate 4 — Idempotency: reject duplicate job_id
        if job_id in self._seen_jobs:
            logger.warning("[worker] Duplicate TASK job=%s — ignored", job_id)
            return

        # Atomic capacity check + duplicate check + slot reservation
        async with self._active_lock:
            if self._active_jobs >= self._capacity:
                logger.warning("[worker] at capacity (%d/%d) — refusing job=%s",
                               self._active_jobs, self._capacity, job_id)
                asyncio.create_task(self.send_failed(job_id, "Worker at capacity"))
                return
            if job_id in self._seen_jobs:
                logger.warning("[worker] duplicate TASK job=%s — ignored", job_id)
                return
            self._seen_jobs[job_id] = True
            if len(self._seen_jobs) > _SEEN_JOBS_MAX:
                self._seen_jobs.popitem(last=False)
            self._active_jobs += 1

        logger.info("[worker] TASK accepted job=%s active=%d/%d",
                    job_id, self._active_jobs, self._capacity)
        asyncio.create_task(self._run_task_reserved(msg), name=f"task_{job_id}")

    async def _run_task_reserved(self, task: dict) -> None:
        """Slot already reserved by _on_any_message."""
        job_id = task.get("job_id", "?")
        await self.send_ack(job_id)
        try:
            await self._on_task(task)
        except asyncio.CancelledError:
            await self.send_failed(job_id, "Task cancelled")
        except Exception as exc:
            logger.exception("[worker] task error job=%s: %s", job_id, type(exc).__name__)
            await self.send_failed(job_id, f"{type(exc).__name__}: task error")
        finally:
            async with self._active_lock:
                self._active_jobs = max(0, self._active_jobs - 1)

    # FIX BUG 11 (ADDITIONAL): _run_task() was a dead method that called
    # self.increment_active() AFTER the slot was already reserved in
    # _on_any_message — causing a double-increment if it were ever called.
    # It was never called (only _run_task_reserved was), so it is removed
    # entirely to prevent accidental future use.
    #
    # The slot lifecycle is now:
    #   reserve  → _on_any_message  (inside _active_lock, +1)
    #   release  → _run_task_reserved finally block (inside _active_lock, -1)
