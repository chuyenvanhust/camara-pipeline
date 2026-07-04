from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
from mock_services.hlr_hss.models import (
    SubscriberProfile, ImsiHistoryResponse, ImsiHistoryEntry,
    MsisdnHistoryResponse, MsisdnHistoryEntry,
    BatchLookupRequest, BatchLookupResponse, BatchLookupItemResult,
    IdentityHistoryResponse, IdentityHistoryEntry,
    BatchHistoryRequest, BatchHistoryResponse, BatchHistoryItemResult,
)
from mock_services.hlr_hss.seed import SUBSCRIBERS_BY_IMSI, SUBSCRIBERS_BY_MSISDN, DEVICES_BY_MSISDN

def clean_iso_date(date_str: Optional[str]) -> Optional[str]:
    if isinstance(date_str, str) and date_str.endswith("Z"):
        return date_str.rstrip("Z")
    return date_str

router = APIRouter(prefix="/subscribers")


def _build_identity_history(
    msisdn: str, records: List[dict], identity_field: str, date_field: str,
) -> IdentityHistoryResponse:
    """
    Helper DÙNG CHUNG cho imsi-history (Conflict C) VÀ imei-history (Conflict D)
    — tránh lặp code + lặp bug giữa 2 nhánh.

    [FIX Bug A] latest_swap_at trước đây gán thẳng rec[date_field], không qua
    clean_iso_date() -> chuỗi "...+00:00Z" (double suffix) không parse được
    thành datetime -> Pydantic ValidationError -> HTTP 500 -> swap_detector
    coi là unreachable -> KHÔNG BAO GIỜ confirm được, dù history đúng.
    """
    sorted_records = sorted(records, key=lambda x: x[date_field])  # ASCENDING: cũ -> mới
    n = len(sorted_records)
    history_entries: List[IdentityHistoryEntry] = []
    latest_swap_at = None

    for idx, rec in enumerate(sorted_records):
        is_current = (idx == n - 1)
        unassign_t = None if is_current else clean_iso_date(sorted_records[idx + 1][date_field])
        if is_current and n > 1:
            latest_swap_at = clean_iso_date(rec[date_field])   # [FIX Bug A]

        history_entries.append(IdentityHistoryEntry(
            value=rec[identity_field],
            assigned_at=clean_iso_date(rec[date_field]),
            unassigned_at=unassign_t,
            is_current=is_current,
            swap_reason="initial_activation" if idx == 0 else "customer_request",
        ))

    return IdentityHistoryResponse(
        msisdn=msisdn, identity_type=identity_field, history=history_entries,
        total=n, swap_count=max(0, n - 1), latest_swap_at=latest_swap_at,
    )


@router.get("/by-imsi/{imsi}", response_model=SubscriberProfile)
def get_by_imsi(imsi: str):
    if imsi not in SUBSCRIBERS_BY_IMSI:
        raise HTTPException(status_code=404, detail="IMSI not found in HLR")
    return SUBSCRIBERS_BY_IMSI[imsi][-1]

@router.get("/by-msisdn/{msisdn}", response_model=SubscriberProfile)
def get_by_msisdn(msisdn: str):
    if msisdn not in SUBSCRIBERS_BY_MSISDN:
        raise HTTPException(status_code=404, detail="MSISDN not found")
    return SUBSCRIBERS_BY_MSISDN[msisdn][-1]

@router.get("/{msisdn}/imsi-history", response_model=ImsiHistoryResponse)
def get_imsi_history(msisdn: str):
    if msisdn not in SUBSCRIBERS_BY_MSISDN:
        raise HTTPException(status_code=404, detail="MSISDN history not found")
    g = _build_identity_history(msisdn, SUBSCRIBERS_BY_MSISDN[msisdn], "imsi", "registered_at")
    return ImsiHistoryResponse(
        msisdn=g.msisdn,
        history=[ImsiHistoryEntry(imsi=e.value, assigned_at=e.assigned_at,
                                   unassigned_at=e.unassigned_at, is_current=e.is_current,
                                   swap_reason=e.swap_reason) for e in g.history],
        total=g.total, sim_swap_count=g.swap_count, latest_swap_at=g.latest_swap_at,
    )

@router.get("/{msisdn}/imei-history", response_model=IdentityHistoryResponse)
def get_imei_history(msisdn: str):
    """[MỚI] Đối xứng với /imsi-history, dùng cho Conflict D (Device Swap)."""
    if msisdn not in DEVICES_BY_MSISDN:
        raise HTTPException(status_code=404, detail="Device history not found")
    return _build_identity_history(msisdn, DEVICES_BY_MSISDN[msisdn], "imei", "assigned_at")

@router.post("/batch-history", response_model=BatchHistoryResponse)
def batch_history(payload: BatchHistoryRequest):
    """
    [MỚI] Batch verify cho Conflict C VÀ D trong 1 round-trip. Trước đây
    swap_detector.py gọi GET /imsi-history TỪNG record một (~160 request/
    micro-batch) — chính là nguyên nhân "Current batch is falling behind"
    trong log Spark. Endpoint này nhận 1 danh sách msisdn, trả cả loạt.
    """
    source = SUBSCRIBERS_BY_MSISDN if payload.identity_type == "imsi" else DEVICES_BY_MSISDN
    date_field = "registered_at" if payload.identity_type == "imsi" else "assigned_at"

    results: List[BatchHistoryItemResult] = []
    found_cnt = 0
    for msisdn in payload.msisdns:
        records = source.get(msisdn)
        if not records:
            results.append(BatchHistoryItemResult(msisdn=msisdn, found=False, history=None))
            continue
        found_cnt += 1
        results.append(BatchHistoryItemResult(
            msisdn=msisdn, found=True,
            history=_build_identity_history(msisdn, records, payload.identity_type, date_field),
        ))

    total = len(payload.msisdns)
    return BatchHistoryResponse(results=results, total=total, found=found_cnt, not_found=total - found_cnt)

@router.get("/{imsi}/msisdn-history", response_model=MsisdnHistoryResponse)
def get_msisdn_history(imsi: str, from_date: Optional[datetime] = Query(None),
                        to_date: Optional[datetime] = Query(None), limit: int = Query(20, ge=1)):
    if imsi not in SUBSCRIBERS_BY_IMSI:
        raise HTTPException(status_code=404, detail="IMSI history not found")
    records = SUBSCRIBERS_BY_IMSI[imsi]
    sorted_records = sorted(records, key=lambda x: x["registered_at"], reverse=True)
    history_entries = []
    for idx, rec in enumerate(sorted_records):
        is_current = (idx == 0)
        unassign_t = None if is_current else clean_iso_date(sorted_records[idx - 1]["registered_at"])
        history_entries.append(MsisdnHistoryEntry(
            msisdn=rec["msisdn"], assigned_at=clean_iso_date(rec["registered_at"]),
            unassigned_at=unassign_t, is_current=is_current,
        ))
    if from_date:
        history_entries = [e for e in history_entries if e.assigned_at >= from_date]
    if to_date:
        history_entries = [e for e in history_entries if e.assigned_at <= to_date]
    history_entries = history_entries[:limit]
    return MsisdnHistoryResponse(imsi=imsi, history=history_entries, total=len(history_entries))

@router.post("/batch-lookup", response_model=BatchLookupResponse)
def batch_lookup(payload: BatchLookupRequest):
    results = []
    found_cnt = 0
    for query in payload.lookups:
        found = False
        sub_profile = None
        if query.type == "imsi" and query.value in SUBSCRIBERS_BY_IMSI:
            found = True
            sub_profile = SUBSCRIBERS_BY_IMSI[query.value][-1]
        elif query.type == "msisdn" and query.value in SUBSCRIBERS_BY_MSISDN:
            found = True
            sub_profile = SUBSCRIBERS_BY_MSISDN[query.value][-1]
        if found:
            found_cnt += 1
        results.append(BatchLookupItemResult(query=query, found=found, subscriber=sub_profile))
    total = len(payload.lookups)
    return BatchLookupResponse(results=results, total=total, found=found_cnt, not_found=total - found_cnt)