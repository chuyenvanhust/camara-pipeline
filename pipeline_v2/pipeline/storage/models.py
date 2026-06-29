#!/usr/bin/env python3
"""
pipeline/storage/models.py

SQLAlchemy ORM models cho 5 bảng PostgreSQL trong hệ thống CAMARA Pipeline.

Schema khớp 1:1 với storage/README.md — nếu thay đổi migration,
phải cập nhật models tương ứng.

Bảng chính:
  - radius_sessions: RADIUS records đã qua pipeline, partitioned monthly
  - swap_event: SIM Swap / Device Swap events

Bảng audit (log) — records bị loại kèm lý do, không partition:
  - duplicate_log: records trùng lặp bị loại bởi S3
  - conflict_log: records xung đột A/B/C phân loại bởi S4
  - invalid_log: records không hợp lệ bị loại bởi S2

Lưu ý:
  - Models dùng chung cho cả pipeline/storage/writer.py (S5 ghi radius_sessions)
    và api/ layer (query radius_sessions + swap_event).
  - Mỗi model export INSERT_COLUMNS (cột cần INSERT, loại trừ auto-generated)
    và CONFLICT_COLUMNS (cho ON CONFLICT DO NOTHING) nếu cần.
"""

from sqlalchemy import (
    Column, BigInteger, String, Boolean, DateTime, Integer, Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """SQLAlchemy declarative base dùng chung cho toàn bộ project."""
    pass


# ==============================================================================
# BẢNG CHÍNH
# ==============================================================================

class RadiusSession(Base):
    """
    Lưu toàn bộ RADIUS record đã qua pipeline (radius.clean → S5).
    Partition RANGE by event_timestamp (monthly) — xem ADR-003.

    Ghi bởi: pipeline/storage/writer.py (Stage S5)
    Query bởi: api/routers/ (SIM Swap, Device Swap, Number Verification)
    """
    __tablename__ = "radius_sessions"
    __table_args__ = (
        UniqueConstraint(
            "acct_session_id", "event_timestamp",
            name="uq_session_event",
        ),
        {"info": {"partition_by": "RANGE (event_timestamp)"}},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    acct_session_id = Column(String(36), nullable=False)
    acct_status_type = Column(String(16), nullable=False)
    event_timestamp = Column(DateTime(timezone=True), nullable=False)
    ingest_timestamp = Column(DateTime(timezone=True), server_default=func.now())
    msisdn = Column(String(16), nullable=False)
    imsi = Column(String(15), nullable=False)
    imei = Column(String(15), nullable=False)
    rat_type = Column(String(8), nullable=True)
    framed_ip = Column(String(45), nullable=True)   # IPv4/IPv6 as text
    nas_ip = Column(String(45), nullable=True)
    mcc_mnc = Column(String(6), nullable=True)
    late_arrival = Column(Boolean, default=False)

    #: Cột cần INSERT (id auto-generated, ingest_timestamp server_default)
    INSERT_COLUMNS = (
        "acct_session_id", "acct_status_type", "event_timestamp",
        "msisdn", "imsi", "imei",
        "rat_type", "framed_ip", "nas_ip", "mcc_mnc", "late_arrival",
    )

    #: Unique constraint columns cho ON CONFLICT DO NOTHING
    CONFLICT_COLUMNS = ("acct_session_id", "event_timestamp")

    def __repr__(self):
        return (
            f"<RadiusSession(id={self.id}, session={self.acct_session_id}, "
            f"msisdn={self.msisdn}, ts={self.event_timestamp})>"
        )


class SwapEvent(Base):
    """
    SIM Swap / Device Swap events phát hiện bởi S4 conflict resolution.

    Ghi bởi: pipeline/conflict_resolution/swap_detector.py
    Query bởi: api/routers/sim_swap.py, api/routers/device_swap.py
    """
    __tablename__ = "swap_event"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    msisdn = Column(String(16), nullable=False)
    old_imsi = Column(String(15), nullable=True)
    new_imsi = Column(String(15), nullable=True)
    old_imei = Column(String(15), nullable=True)
    new_imei = Column(String(15), nullable=True)
    swap_type = Column(String(16), nullable=False)    # SIM_SWAP | DEVICE_SWAP
    detected_at = Column(DateTime(timezone=True), nullable=False)
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    source = Column(String(32), nullable=False)       # RADIUS_CONFLICT_C

    VALID_SWAP_TYPES = ("SIM_SWAP", "DEVICE_SWAP")

    def __repr__(self):
        return (
            f"<SwapEvent(id={self.id}, msisdn={self.msisdn}, "
            f"type={self.swap_type}, detected={self.detected_at})>"
        )


# ==============================================================================
# BẢNG AUDIT (LOG) — records bị loại kèm lý do, không partition
# ==============================================================================

class DuplicateLog(Base):
    """
    Audit log: records trùng lặp bị loại bởi S3 deduplication.
    Ghi bởi: pipeline/deduplication/dedup_job.py
    """
    __tablename__ = "duplicate_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    acct_session_id = Column(String(36), nullable=False)
    duplicate_count = Column(Integer, nullable=False, default=1)
    logged_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return (
            f"<DuplicateLog(session={self.acct_session_id}, "
            f"count={self.duplicate_count})>"
        )


class ConflictLog(Base):
    """
    Audit log: records xung đột A/B/C phân loại bởi S4 conflict resolution.
    Ghi bởi: pipeline/conflict_resolution/resolver.py
    """
    __tablename__ = "conflict_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    acct_session_id = Column(String(36), nullable=False)
    msisdn = Column(String(16), nullable=True)
    imsi = Column(String(15), nullable=True)
    event_timestamp = Column(DateTime(timezone=True), nullable=True)
    conflict_type = Column(String(1), nullable=False)  # A | B | C
    logged_at = Column(DateTime(timezone=True), server_default=func.now())

    VALID_CONFLICT_TYPES = ("A", "B", "C")

    def __repr__(self):
        return (
            f"<ConflictLog(session={self.acct_session_id}, "
            f"type={self.conflict_type})>"
        )


class InvalidLog(Base):
    """
    Audit log: records không hợp lệ bị loại bởi S2 validation.
    Ghi bởi: pipeline/validation/validator.py
    """
    __tablename__ = "invalid_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    acct_session_id = Column(String(36), nullable=True)
    msisdn = Column(String(16), nullable=True)
    imsi = Column(String(15), nullable=True)
    event_timestamp = Column(DateTime(timezone=True), nullable=True)
    error_code = Column(String(64), nullable=False)
    error_message = Column(Text, nullable=True)
    logged_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return (
            f"<InvalidLog(session={self.acct_session_id}, "
            f"error={self.error_code})>"
        )


# ==============================================================================
# MODULE EXPORTS
# ==============================================================================

ALL_MODELS = [RadiusSession, SwapEvent, DuplicateLog, ConflictLog, InvalidLog]
ALL_TABLE_NAMES = tuple(m.__tablename__ for m in ALL_MODELS)
