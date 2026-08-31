from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Optional

from pipeline.modules.shared.db import StateRecord
from pipeline.modules.shared.metrics import ModuleMetrics


logger = logging.getLogger(__name__)


class StateCheckpointCoordinator:
    """Coalesce non-business state advances without weakening Kafka durability.

    A submitter gets a future that completes only after PostgreSQL contains a state
    version at least as new as every submitted record. BaseKafkaConsumer keeps the
    business path moving, but will not expose the corresponding Kafka offset to its
    commit coordinator until this future succeeds.
    """

    def __init__(
        self,
        name: str,
        persist: Callable[[Sequence[StateRecord]], Awaitable[None]],
        metrics: ModuleMetrics,
    ) -> None:
        self.name = name
        self._persist = persist
        self.metrics = metrics
        self.interval_seconds = max(
            0.001, float(os.getenv("SWAP_CHECKPOINT_INTERVAL_MS", "150")) / 1000.0
        )
        self.max_records = max(1, int(os.getenv("SWAP_CHECKPOINT_MAX_RECORDS", "256")))
        self.queue_records = max(
            self.max_records, int(os.getenv("SWAP_CHECKPOINT_QUEUE_RECORDS", "4096"))
        )
        self._states: dict[str, StateRecord] = {}
        self._waiters: list[asyncio.Future[None]] = []
        self._queued_records = 0
        self._wake = asyncio.Event()
        self._space = asyncio.Event()
        self._space.set()
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._fatal: Optional[BaseException] = None
        self._oldest_at: Optional[float] = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(
                self._run(), name=f"{self.name}-state-checkpoint"
            )
            logger.info(
                "[%s] state checkpoint ready interval=%.1fms max_records=%d queue=%d",
                self.name, self.interval_seconds * 1000.0, self.max_records,
                self.queue_records,
            )

    @staticmethod
    def _version(record: StateRecord) -> tuple:
        return record[2], record[4], record[5]

    async def submit(self, records: Sequence[StateRecord]) -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        if not records:
            completed: asyncio.Future[None] = loop.create_future()
            completed.set_result(None)
            return completed
        if self._fatal is not None:
            raise RuntimeError(f"{self.name} checkpoint coordinator failed") from self._fatal
        if self._task is None:
            raise RuntimeError(f"{self.name} checkpoint coordinator is not started")

        # Bound memory and push back on partition workers when PostgreSQL cannot
        # drain checkpoints. The queue stores only the newest state per MSISDN.
        while self._queued_records >= self.queue_records and not self._stop.is_set():
            self._space.clear()
            await self._space.wait()
        if self._stop.is_set():
            raise RuntimeError(f"{self.name} checkpoint coordinator is stopping")

        waiter: asyncio.Future[None] = loop.create_future()
        now = time.monotonic()
        if self._oldest_at is None:
            self._oldest_at = now
        for record in records:
            current = self._states.get(record[0])
            if current is None or self._version(record) > self._version(current):
                self._states[record[0]] = record
        self._waiters.append(waiter)
        self._queued_records += len(records)
        self.metrics.set_checkpoint_queue(
            len(self._states), self._queue_age_ms(), flushing=False
        )
        if len(self._states) >= self.max_records:
            self._wake.set()
        return waiter

    def _queue_age_ms(self) -> float:
        if self._oldest_at is None:
            return 0.0
        return max(0.0, (time.monotonic() - self._oldest_at) * 1000.0)

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self.interval_seconds
                    )
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
                await self.flush()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._fatal = exc
            self._fail_waiters(exc)
            logger.exception("[%s] state checkpoint coordinator failed", self.name)

    async def flush(self) -> None:
        if not self._states:
            return
        states = list(self._states.values())
        waiters = self._waiters
        queue_age_ms = self._queue_age_ms()
        self._states = {}
        self._waiters = []
        self._queued_records = 0
        self._oldest_at = None
        self._space.set()
        self.metrics.set_checkpoint_queue(0, 0.0, flushing=True)
        started = time.monotonic()
        try:
            await self._persist(states)
        except BaseException as exc:
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(exc)
            self.metrics.observe_checkpoint(
                time.monotonic() - started, len(states), queue_age_ms, failed=True
            )
            raise
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)
        self.metrics.increment("postgres_records", len(states))
        self.metrics.observe_checkpoint(
            time.monotonic() - started, len(states), queue_age_ms, failed=False
        )
        self.metrics.set_checkpoint_queue(
            len(self._states), self._queue_age_ms(), flushing=False
        )

    def _fail_waiters(self, exc: BaseException) -> None:
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_exception(exc)
        self._waiters = []

    async def close(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        self._wake.set()
        await asyncio.gather(self._task, return_exceptions=True)
        if self._fatal is None and self._states:
            await self.flush()
        elif self._fatal is not None:
            self._fail_waiters(self._fatal)
        self.metrics.set_checkpoint_queue(0, 0.0, flushing=False)
        self._task = None
