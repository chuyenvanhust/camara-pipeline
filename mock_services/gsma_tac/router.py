#mock_services\gsma_tac\router.py
import time
import uuid
from fastapi import APIRouter, HTTPException, Query, Header, status
from fastapi.responses import JSONResponse
from mock_services.gsma_tac.models import TacLookupResponse, BatchRequest, BatchResponse, BatchResultItem, PaginatedTacResponse, HealthResponse
from mock_services.gsma_tac.seed import TAC_IN_MEMORY_DB
from typing import Optional

router = APIRouter()
START_TIME = time.time()

def check_fault_injection(x_mock_fault: Optional[str]):
    if x_mock_fault == "500":
        raise HTTPException(status_code=500, detail="Mocked Internal Server Error")
    if x_mock_fault == "timeout":
        time.sleep(2)  # Giả lập timeout nghẽn mạch
        raise HTTPException(status_code=504, detail="Mocked Gateway Timeout")

@router.get("/tac/{tac_code}", response_model=TacLookupResponse)
def get_tac_details(tac_code: str, x_mock_fault: Optional[str] = Header(None)):
    check_fault_injection(x_mock_fault)
    
    if not tac_code.isdigit() or len(tac_code) != 6:
        return JSONResponse(
            status_code=422,
            content={"error": "INVALID_INPUT", "message": "TAC must be exactly 6 digits", "field": "tac_code"}
        )
        
    if tac_code not in TAC_IN_MEMORY_DB:
        return JSONResponse(
            status_code=404,
            content={"error": "NOT_FOUND", "message": f"TAC '{tac_code}' not found in database", "request_id": str(uuid.uuid4())}
        )
        
    return TAC_IN_MEMORY_DB[tac_code]

@router.post("/tac/batch", response_model=BatchResponse)
def batch_lookup_tacs(payload: BatchRequest, x_mock_fault: Optional[str] = Header(None)):
    check_fault_injection(x_mock_fault)
    
    results = {}
    found_cnt = 0
    
    for tac in payload.tac_codes:
        if tac in TAC_IN_MEMORY_DB:
            record = TAC_IN_MEMORY_DB[tac]
            results[tac] = BatchResultItem(
                found=True, manufacturer=record.manufacturer,
                model=record.model, device_type=record.device_type, status=record.status
            )
            found_cnt += 1
        else:
            results[tac] = BatchResultItem(found=False)
            
    total = len(payload.tac_codes)
    return BatchResponse(
        results=results, total=total, found=found_cnt, not_found=total - found_cnt
    )

@router.get("/tac", response_model=PaginatedTacResponse)
def list_tacs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    manufacturer: Optional[str] = Query(None),
    device_type: Optional[str] = Query(None),
    x_mock_fault: Optional[str] = Header(None)
):
    check_fault_injection(x_mock_fault)
    
    filtered = list(TAC_IN_MEMORY_DB.values())
    if manufacturer:
        filtered = [r for r in filtered if r.manufacturer.lower() == manufacturer.lower()]
    if device_type:
        filtered = [r for r in filtered if r.device_type.lower() == device_type.lower()]
        
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    
    items = filtered[start:end]
    pages = (total + page_size - 1) // page_size if total > 0 else 0
    
    return PaginatedTacResponse(
        items=items, total=total, page=page, page_size=page_size, pages=pages
    )

@router.get("/health", response_model=HealthResponse)
def health_check():
    return HealthResponse(
        status="ok",
        service="gsma-tac-mock",
        records=len(TAC_IN_MEMORY_DB),
        uptime_seconds=time.time() - START_TIME
    )