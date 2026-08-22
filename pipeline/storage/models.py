#!/usr/bin/env python3
"""
pipeline/storage/models.py

SQLAlchemy ORM models cho các bảng PostgreSQL trong hệ thống CAMARA Pipeline (Refactored).

Bảng trạng thái:
  - MsisdnDevice: msisdn_device
  - MsisdnSim: msisdn_sim

Bảng lịch sử swap:
  - DeviceSwapHistory: device_swap_history
  - SimSwapHistory: sim_swap_history

Bảng Open Gateway & Audit:
  - Subscription: subscription
  - AuditLog: audit_log
  - NotificationLog: notification_log
"""

from sqlalchemy import (
    Column, BigInteger, String, DateTime, Text, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class MsisdnDevice(Base):
    __tablename__ = "msisdn_device"

    msisdn = Column(String(16), primary_key=True)
    imei_current = Column(String(15), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class MsisdnSim(Base):
    __tablename__ = "msisdn_sim"

    msisdn = Column(String(16), primary_key=True)
    imsi_current = Column(String(15), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DeviceSwapHistory(Base):
    __tablename__ = "device_swap_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    msisdn = Column(String(16), nullable=False)
    imei_old = Column(String(15), nullable=True)
    imei_new = Column(String(15), nullable=False)
    changed_at = Column(DateTime(timezone=True), nullable=False)


class SimSwapHistory(Base):
    __tablename__ = "sim_swap_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    msisdn = Column(String(16), nullable=False)
    imsi_old = Column(String(15), nullable=True)
    imsi_new = Column(String(15), nullable=False)
    changed_at = Column(DateTime(timezone=True), nullable=False)


class Subscription(Base):
    __tablename__ = "subscription"

    subscription_id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid())
    msisdn = Column(String(16), nullable=False)
    event_type = Column(String(32), nullable=False)
    callback_url = Column(String(2048), nullable=False)
    status = Column(String(16), nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_type = Column(String(32), nullable=False)
    msisdn = Column(String(16), nullable=True)
    details = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class NotificationLog(Base):
    __tablename__ = "notification_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    subscription_id = Column(UUID(as_uuid=True), nullable=True)
    event_type = Column(String(32), nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String(16), nullable=False, default="PENDING")
    attempts = Column(BigInteger, nullable=False, default=0)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
    error_detail = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


ALL_MODELS = [
    MsisdnDevice, MsisdnSim, DeviceSwapHistory, SimSwapHistory,
    Subscription, AuditLog, NotificationLog
]
ALL_TABLE_NAMES = tuple(m.__tablename__ for m in ALL_MODELS)
