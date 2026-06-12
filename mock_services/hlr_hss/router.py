from fastapi import APIRouter, HTTPException, status
from typing import List
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
    return SUBSCRIBERS_BY_IMSI[imsi]

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
    
    for idx, rec in enumerate(sorted_records):
        is_current = (idx == 0)
        unassign_t = None if is_current else sorted_records[idx - 1]["registered_at"]
        
        if is_current and len(sorted_records) > 1:
            latest_swap_at = rec["registered_at"]
            
        history_entries.append(ImsiHistoryEntry(
            imsi=rec["imsi"],
            assigned_at=rec["registered_at"],
            unassigned_at=unassign_t,
            is_current=is_current,
            swap_reason=rec["swap_reason"]
        ))
        
    return ImsiHistoryResponse(
        msisdn=msisdn,
        history=history_entries,
        total=len(history_entries),
        sim_swap_count=max(0, len(history_entries) - 1),
        latest_swap_at=latest_swap_at
    )

@router.get("/by-imsi-raw/{imsi}/msisdn-history", response_model=MsisdnHistoryResponse)
def get_msisdn_history(imsi: str):
    # Thiết kế gọn phục vụ đặc tả cấu trúc nội bộ của 1 IMSI
    if imsi not in SUBSCRIBERS_BY_IMSI:
        raise HTTPException(status_code=404, detail="IMSI history not found")
    rec = SUBSCRIBERS_BY_IMSI[imsi]
    entry = MsisdnHistoryEntry(
        msisdn=rec["msisdn"],
        assigned_at=rec["registered_at"],
        unassigned_at=None,
        is_current=True
    )
    return MsisdnHistoryResponse(imsi=imsi, history=[entry], total=1)

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
                sub_profile = SUBSCRIBERS_BY_IMSI[query.value]
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