from __future__ import annotations

import asyncio

from pipeline.ingestion import producer as producer_module
from pipeline.ingestion.producer import QueueItem, RadiusLogProducer


def _item(key: str) -> QueueItem:
    return QueueItem("radius.accounting.raw", key, {"key": key}, "raw")


def test_recommended_capacity_never_drops_before_queue(monkeypatch) -> None:
    """Capacity targets are telemetry only, even far above the configured PPS."""
    monkeypatch.setattr(producer_module, "RECOMMENDED_SUSTAINED_PPS", 1)
    monkeypatch.setattr(producer_module, "RECOMMENDED_BURST_PPS", 1)
    ingestion = RadiusLogProducer()
    ingestion._worker_queues = [asyncio.Queue(maxsize=3)]

    assert ingestion._put_udp_item(_item("84910000001")) is True
    assert ingestion._put_udp_item(_item("84910000002")) is True
    assert ingestion._put_udp_item(_item("84910000003")) is True
    assert ingestion._counts["queue_dropped"] == 0


def test_queue_drop_is_reported_only_when_worker_queue_is_full() -> None:
    ingestion = RadiusLogProducer()
    ingestion._worker_queues = [asyncio.Queue(maxsize=1)]

    assert ingestion._put_udp_item(_item("84910000001")) is True
    assert ingestion._put_udp_item(_item("84910000002")) is False
    assert ingestion._counts["queued"] == 1
    assert ingestion._counts["queue_dropped"] == 1
