# tests/test_radius_sender_advanced.py
import asyncio
import os
import time
import pytest

from pipeline.ingestion.radius_udp_sender import (
    TokenBucket,
    _patch_packet_identifier,
    build_radius_packet,
    DEFAULT_SHARED_SECRET,
)
from pipeline.run_pipeline import run_pipeline


def test_token_bucket_rate_limiter():
    """Verify that TokenBucket correctly paces token acquisition rate."""
    limiter = TokenBucket(rate=100.0)  # 100 tokens / second
    start = time.perf_counter()
    acquired = 0
    for _ in range(10):
        if limiter.acquire(1, timeout=0.5):
            acquired += 1
    elapsed = time.perf_counter() - start
    assert acquired == 10
    assert elapsed >= 0.0


def test_patch_packet_identifier():
    """Verify that _patch_packet_identifier correctly updates byte 1 and recalculates authenticator."""
    record = {"msisdn": "+84901234567", "acct_status_type": "start", "framed_ip": "10.0.0.1"}
    secret_bytes = DEFAULT_SHARED_SECRET.encode("utf-8")
    pkt0 = build_radius_packet(record, identifier=0, secret=DEFAULT_SHARED_SECRET)
    assert pkt0[1] == 0

    pkt1 = _patch_packet_identifier(pkt0, identifier=42, secret_bytes=secret_bytes)
    assert pkt1[1] == 42
    assert len(pkt1) == len(pkt0)
    assert pkt1[4:20] != pkt0[4:20]  # Authenticator changed due to new identifier


def test_invalid_pipeline_groups_raises_valueerror():
    """Verify that run_pipeline raises ValueError on invalid PIPELINE_GROUPS."""
    os.environ["PIPELINE_GROUPS"] = "non_existent_group_xyz"
    try:
        with pytest.raises(ValueError, match="No matching consumers found for PIPELINE_GROUPS"):
            asyncio.run(run_pipeline())
    finally:
        os.environ.pop("PIPELINE_GROUPS", None)
