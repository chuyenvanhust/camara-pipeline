from fastapi import APIRouter, HTTPException, Query, status
from typing import List, Optional
from datetime import datetime
from mock_services.hlr_hss.models import (
    SubscriberProfile, ImsiHistoryResponse, ImsiHistoryEntry,
    MsisdnHistoryResponse, MsisdnHistoryEntry,
    BatchLookupRequest, BatchLookupResponse, BatchLookupItemResult
)
from mock_services.hlr_hss.seed import SUBSCRIBERS_BY_IMSI, SUBSCRIBERS_BY_MSISDN

router = APIRouter(prefix="/subscribers")

@router.get("/by-imsi/{imsi}", response_model=SubscriberProfile)
def get_by_imsi(imsi: str):
    if imsi not in SUBSCRIBERS_BY_IMSI:
        raise HTTPException(status_code=404, detail="IMSI not found in HLR")
    # Lấy profile mới nhất (MSISDN hiện tại của IMSI này, sau portability nếu có)
    return SUBSCRIBERS_BY_IMSI[imsi][-1]

@router.get("/by-msisdn/{msisdn}", response_model=SubscriberProfile)
def get_by_msisdn(msisdn: str):
    if msisdn not in SUBSCRIBERS_BY_MSISDN:
        raise HTTPException(status_code=404, detail="MSISDN not found")
    # Lấy profile mới nhất (dòng cuối cùng gán với msisdn này)
    return SUBSCRIBERS_BY_MSISDN[msisdn][-1]

@router.get("/{msisdn}/imsi-history", response_model=ImsiHistoryResponse)
def get_imsi_history(msisdn: str):
    if msisdn not in SUBSCRIBERS_BY_MSISDN:
        raise HTTPException(status_code=404, detail="MSISDN history not found")
        
    records = SUBSCRIBERS_BY_MSISDN[msisdn]
    # Sắp xếp hồ sơ từ mới đến cũ dựa trên registered_at
    sorted_records = sorted(records, key=lambda x: x["registered_at"], reverse=True)
    
    history_entries = []
    latest_swap_at = None
    n = len(sorted_records)
    
    for idx, rec in enumerate(sorted_records):
        is_current = (idx == 0)
        unassign_t = None if is_current else sorted_records[idx - 1]["registered_at"]
        
        if is_current and n > 1:
            latest_swap_at = rec["registered_at"]
        
        # Bản ghi cũ nhất (idx cuối) là lần kích hoạt gốc, các bản ghi sau đó
        # là kết quả của 1 lần SIM swap -> suy luận từ vị trí, không cần cột riêng trong CSV.
        swap_reason = "initial_activation" if idx == n - 1 else "customer_request"
            
        history_entries.append(ImsiHistoryEntry(
            imsi=rec["imsi"],
            assigned_at=rec["registered_at"],
            unassigned_at=unassign_t,
            is_current=is_current,
            swap_reason=swap_reason
        ))
        
    return ImsiHistoryResponse(
        msisdn=msisdn,
        history=history_entries,
        total=len(history_entries),
        sim_swap_count=max(0, len(history_entries) - 1),
        latest_swap_at=latest_swap_at
    )

@router.get("/{imsi}/msisdn-history", response_model=MsisdnHistoryResponse)
def get_msisdn_history(
    imsi: str,
    from_date: Optional[datetime] = Query(None),
    to_date: Optional[datetime] = Query(None),
    limit: int = Query(20, ge=1)
):
    # Lịch sử thay đổi MSISDN của 1 IMSI (number portability / reassignment).
    if imsi not in SUBSCRIBERS_BY_IMSI:
        raise HTTPException(status_code=404, detail="IMSI history not found")

    records = SUBSCRIBERS_BY_IMSI[imsi]
    # Sắp xếp hồ sơ từ mới đến cũ dựa trên registered_at
    sorted_records = sorted(records, key=lambda x: x["registered_at"], reverse=True)

    history_entries = []
    for idx, rec in enumerate(sorted_records):
        is_current = (idx == 0)
        unassign_t = None if is_current else sorted_records[idx - 1]["registered_at"]

        history_entries.append(MsisdnHistoryEntry(
            msisdn=rec["msisdn"],
            assigned_at=rec["registered_at"],
            unassigned_at=unassign_t,
            is_current=is_current
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
        
        if query.type == "imsi":
            if query.value in SUBSCRIBERS_BY_IMSI:
                found = True
                sub_profile = SUBSCRIBERS_BY_IMSI[query.value][-1]
        elif query.type == "msisdn":
            if query.value in SUBSCRIBERS_BY_MSISDN:
                found = True
                sub_profile = SUBSCRIBERS_BY_MSISDN[query.value][-1]
                
        if found:
            found_cnt += 1
            
        results.append(BatchLookupItemResult(
            query=query,
            found=found,
            subscriber=sub_profile
        ))
        
    total = len(payload.lookups)
    return BatchLookupResponse(
        results=results, total=total, found=found_cnt, not_found=total - found_cnt
    )