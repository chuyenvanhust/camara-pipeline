#!/usr/bin/env python3
"""
debug_pipeline.py — Chạy toàn bộ validation pipeline trên CSV thật,
log chi tiết từng record, từng rule, từng HTTP call.

Cách chạy (trong spark-master container hoặc máy host có đủ deps):
  python scripts/debug_pipeline.py --csv data/radius_log.csv

Options:
  --csv PATH         Đường dẫn tới radius_log.csv (bắt buộc)
  --limit N          Chỉ test N records đầu (mặc định: 10)
  --all              Test toàn bộ records (bỏ qua --limit)
  --itu  URL         Override ITU_E164_SERVICE_URL
  --hlr  URL         Override HLR_HSS_SERVICE_URL
  --gsma URL         Override GSMA_TAC_SERVICE_URL
  --no-mock          Bỏ qua các rule cần gọi mock service (chỉ test rule nội bộ)
  --timeout FLOAT    Timeout (giây) cho mỗi HTTP call (mặc định: 2.0)
"""

import argparse
import asyncio
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ─────────────────────────────────────────────────────────────
# ANSI colors
# ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(s):   return f"{GREEN}✓ {s}{RESET}"
def fail(s): return f"{RED}✗ {s}{RESET}"
def warn(s): return f"{YELLOW}⚠ {s}{RESET}"
def info(s): return f"{CYAN}  {s}{RESET}"

# ─────────────────────────────────────────────────────────────
# Config (overridable via CLI)
# ─────────────────────────────────────────────────────────────
ITU_URL  = "http://localhost:8300"
HLR_URL  = "http://localhost:8200"
GSMA_URL = "http://localhost:8100"
TIMEOUT  = 2.0

# ─────────────────────────────────────────────────────────────
# Stats
# ─────────────────────────────────────────────────────────────
@dataclass
class Stats:
    total: int = 0
    passed: int = 0
    failed: int = 0
    fail_by_rule: Dict[str, int] = field(default_factory=dict)
    http_errors: Dict[str, List[str]] = field(default_factory=lambda: {
        "ITU_E164": [], "HLR_HSS": [], "GSMA_TAC": []
    })

STATS = Stats()

# ─────────────────────────────────────────────────────────────
# HTTP helper — log mọi call
# ─────────────────────────────────────────────────────────────
async def http_get(client: httpx.AsyncClient, service: str, url: str) -> Tuple[Optional[httpx.Response], str]:
    t0 = time.time()
    try:
        res = await client.get(url, timeout=TIMEOUT)
        ms = (time.time() - t0) * 1000
        print(info(f"GET  {url}  →  {res.status_code}  ({ms:.0f}ms)"))
        return res, ""
    except httpx.ConnectError as e:
        ms = (time.time() - t0) * 1000
        msg = f"ConnectError: {e}"
        print(fail(f"GET  {url}  →  {msg}  ({ms:.0f}ms)"))
        STATS.http_errors[service].append(msg)
        return None, "ERR_EXTERNAL_CONN_FAIL"
    except httpx.TimeoutException as e:
        ms = (time.time() - t0) * 1000
        msg = f"Timeout: {e}"
        print(fail(f"GET  {url}  →  {msg}  ({ms:.0f}ms)"))
        STATS.http_errors[service].append(msg)
        return None, "ERR_EXTERNAL_TIMEOUT"
    except Exception as e:
        ms = (time.time() - t0) * 1000
        msg = f"{type(e).__name__}: {e}"
        print(fail(f"GET  {url}  →  {msg}  ({ms:.0f}ms)"))
        STATS.http_errors[service].append(msg)
        return None, "ERR_EXTERNAL_CONN_FAIL"


async def http_post(client: httpx.AsyncClient, service: str, url: str, payload: dict) -> Tuple[Optional[httpx.Response], str]:
    t0 = time.time()
    try:
        res = await client.post(url, json=payload, timeout=TIMEOUT)
        ms = (time.time() - t0) * 1000
        print(info(f"POST {url}  body={json.dumps(payload)}  →  {res.status_code}  ({ms:.0f}ms)"))
        try:
            print(info(f"     response body: {res.text[:300]}"))
        except Exception:
            pass
        return res, ""
    except httpx.ConnectError as e:
        ms = (time.time() - t0) * 1000
        msg = f"ConnectError: {e}"
        print(fail(f"POST {url}  →  {msg}  ({ms:.0f}ms)"))
        STATS.http_errors[service].append(msg)
        return None, "ERR_EXTERNAL_CONN_FAIL"
    except httpx.TimeoutException as e:
        ms = (time.time() - t0) * 1000
        msg = f"Timeout: {e}"
        print(fail(f"POST {url}  →  {msg}  ({ms:.0f}ms)"))
        STATS.http_errors[service].append(msg)
        return None, "ERR_EXTERNAL_TIMEOUT"
    except Exception as e:
        ms = (time.time() - t0) * 1000
        msg = f"{type(e).__name__}: {e}"
        print(fail(f"POST {url}  →  {msg}  ({ms:.0f}ms)"))
        STATS.http_errors[service].append(msg)
        return None, "ERR_EXTERNAL_CONN_FAIL"

# ─────────────────────────────────────────────────────────────
# Rules (tự chứa, không import từ pipeline để tránh side-effects)
# ─────────────────────────────────────────────────────────────

def check_r1(record: dict) -> Tuple[bool, str]:
    """R1: Mandatory fields."""
    for f in ["acct_status_type", "acct_session_id", "msisdn", "imsi", "imei", "event_timestamp"]:
        v = record.get(f)
        if v is None or str(v).strip() == "":
            return False, f"ERR_MISSING_FIELD: '{f}' trống/null  →  value={repr(v)}"
    return True, ""


async def check_r2(record: dict, client: httpx.AsyncClient, skip_mock: bool) -> Tuple[bool, str]:
    """R2: MSISDN format + ITU service."""
    msisdn = str(record.get("msisdn", "")).strip()
    if not re.match(r"^\+[1-9]\d{1,14}$", msisdn):
        return False, f"ERR_INVALID_MSISDN: regex fail  →  value={repr(msisdn)}"

    if skip_mock:
        return True, "(mock skipped)"

    url = f"{ITU_URL}/validate"
    # ── Đây là điểm debug quan trọng: gửi đúng field name ──
    res, err = await http_post(client, "ITU_E164", url, {"phone_number": msisdn})
    if err:
        return False, f"{err}: ITU unreachable  →  url={url}"
    if res.status_code == 200:
        body = res.json()
        if body.get("is_valid") is True or body.get("valid") is True:
            return True, ""
        return False, f"ERR_INVALID_MSISDN: ITU trả valid=False  →  body={body}"
    return False, f"ERR_INVALID_MSISDN: ITU status={res.status_code}  →  body={res.text[:200]}"


async def check_r3(record: dict, client: httpx.AsyncClient, skip_mock: bool) -> Tuple[bool, str]:
    """R3: IMSI trong HLR."""
    if skip_mock:
        return True, "(mock skipped)"

    imsi = str(record.get("imsi", "")).strip()
    url = f"{HLR_URL}/subscribers/by-imsi/{imsi}"
    res, err = await http_get(client, "HLR_HSS", url)
    if err:
        return False, f"{err}: HLR unreachable  →  url={url}"
    if res.status_code == 200:
        return True, ""
    if res.status_code == 404:
        return False, f"ERR_IMSI_NOT_IN_HLR: 404  →  imsi={imsi}"
    return False, f"ERR_IMSI_NOT_IN_HLR: status={res.status_code}  →  body={res.text[:200]}"


def check_r4a(record: dict) -> Tuple[bool, str]:
    """R4a: IMEI Luhn."""
    imei = str(record.get("imei", "")).strip()
    if not imei.isdigit() or len(imei) != 15:
        return False, f"ERR_IMEI_LUHN_FAIL: bắt buộc 15 chữ số  →  value={repr(imei)}"
    s = 0
    for i in range(14):
        d = int(imei[i])
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        s += d
    check = (10 - (s % 10)) % 10
    if check != int(imei[14]):
        return False, f"ERR_IMEI_LUHN_FAIL: check digit sai (expect {check}, got {imei[14]})  →  imei={imei}"
    return True, ""


async def check_r4b(record: dict, client: httpx.AsyncClient, skip_mock: bool) -> Tuple[bool, str]:
    """R4b: TAC trong GSMA."""
    if skip_mock:
        return True, "(mock skipped)"

    imei = str(record.get("imei", "")).strip()
    tac = imei[:6]
    url = f"{GSMA_URL}/tac/{tac}"
    res, err = await http_get(client, "GSMA_TAC", url)
    if err:
        return False, f"{err}: GSMA unreachable  →  url={url}"
    if res.status_code == 200:
        return True, ""
    if res.status_code == 404:
        return False, f"ERR_IMEI_TAC_UNKNOWN: 404  →  tac={tac}"
    return False, f"ERR_IMEI_TAC_UNKNOWN: status={res.status_code}  →  body={res.text[:200]}"


def check_r5(record: dict) -> Tuple[bool, str]:
    """R5: acct_status_type hợp lệ."""
    v = str(record.get("acct_status_type", "")).strip()
    if v not in {"Start", "Stop", "Interim-Update"}:
        return False, f"ERR_INVALID_STATUS  →  value={repr(v)}"
    return True, ""


def check_r6(record: dict) -> Tuple[bool, str]:
    """R6: event_timestamp hợp lệ."""
    raw = str(record.get("event_timestamp", "")).strip()
    try:
        ts = int(raw)
        if not (946684800 <= ts <= 4102444800):
            return False, f"ERR_INVALID_TIMESTAMP: ngoài khoảng [2000,2100)  →  value={ts}"
        return True, ""
    except ValueError:
        return False, f"ERR_INVALID_TIMESTAMP: không parse được int  →  value={repr(raw)}"

# ─────────────────────────────────────────────────────────────
# Validate 1 record, log từng rule
# ─────────────────────────────────────────────────────────────
async def validate_record(
    idx: int,
    record: dict,
    client: httpx.AsyncClient,
    skip_mock: bool,
    verbose: bool,
) -> bool:
    STATS.total += 1
    msisdn = record.get("msisdn", "?")[:20]
    imsi   = record.get("imsi",   "?")[:20]
    print(f"\n{BOLD}[#{idx:03d}] msisdn={msisdn}  imsi={imsi}{RESET}")

    rules = [
        ("R1 mandatory",    lambda: check_r1(record)),
        ("R2 msisdn/ITU",    lambda: check_r2(record, client, skip_mock)),
        ("R3 imsi/HLR",      lambda: check_r3(record, client, skip_mock)),
        ("R4a imei_luhn",    lambda: check_r4a(record)),
        ("R4b tac/GSMA",     lambda: check_r4b(record, client, skip_mock)),
        ("R5 status_type",  lambda: check_r5(record)),
        ("R6 timestamp",    lambda: check_r6(record)),
    ]

    # unwrap sync rules
    async def run(fn):
        coro = fn()
        if asyncio.iscoroutine(coro):
            return await coro
        return coro

    for name, fn in rules:
        passed, detail = await run(fn)
        if passed:
            note = f"  {detail}" if detail and verbose else ""
            print(f"  {ok(name)}{note}")
        else:
            print(f"  {fail(name)}: {detail}")
            STATS.failed += 1
            STATS.fail_by_rule[name] = STATS.fail_by_rule.get(name, 0) + 1
            return False

    STATS.passed += 1
    print(f"  {ok('ALL RULES PASSED')}")
    return True

# ─────────────────────────────────────────────────────────────
# Kiểm tra mock services có sống không
# ─────────────────────────────────────────────────────────────
async def check_mock_services():
    print(f"\n{BOLD}{'='*60}")
    print("BƯỚC 0: Kiểm tra kết nối mock services")
    print(f"{'='*60}{RESET}")

    checks = [
        ("ITU E164",  f"{ITU_URL}/health"),
        ("HLR/HSS",   f"{HLR_URL}/health"),
        ("GSMA TAC",  f"{GSMA_URL}/health"),
    ]
    all_ok = True
    async with httpx.AsyncClient() as client:
        for name, url in checks:
            t0 = time.time()
            try:
                res = await client.get(url, timeout=3.0)
                ms = (time.time() - t0) * 1000
                if res.status_code == 200:
                    print(ok(f"{name:<12} {url}  →  200  ({ms:.0f}ms)"))
                else:
                    print(warn(f"{name:<12} {url}  →  {res.status_code}  ({ms:.0f}ms)"))
                    all_ok = False
            except httpx.ConnectError as e:
                ms = (time.time() - t0) * 1000
                print(fail(f"{name:<12} {url}  →  ConnectError: {e}  ({ms:.0f}ms)"))
                all_ok = False
            except Exception as e:
                ms = (time.time() - t0) * 1000
                print(fail(f"{name:<12} {url}  →  {type(e).__name__}: {e}  ({ms:.0f}ms)"))
                all_ok = False

    if not all_ok:
        print(warn("\nMột số mock service không reach được."))
        print(warn("Dùng --no-mock để chỉ test validation nội bộ (R1/R4a/R5/R6)."))
    return all_ok

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
async def main(args):
    global ITU_URL, HLR_URL, GSMA_URL, TIMEOUT

    if args.itu:   ITU_URL  = args.itu
    if args.hlr:   HLR_URL  = args.hlr
    if args.gsma:  GSMA_URL = args.gsma
    if args.timeout: TIMEOUT = args.timeout

    print(f"\n{BOLD}{'='*60}")
    print(f"RADIUS PIPELINE DEBUG TOOL")
    print(f"{'='*60}{RESET}")
    print(f"CSV       : {args.csv}")
    print(f"ITU  URL  : {ITU_URL}")
    print(f"HLR  URL  : {HLR_URL}")
    print(f"GSMA URL  : {GSMA_URL}")
    print(f"Timeout   : {TIMEOUT}s")
    print(f"Skip mock : {args.no_mock}")

    # Đọc CSV
    records = []
    with open(args.csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append({k.strip(): v.strip() for k, v in row.items() if k})

    limit = len(records) if args.all else min(args.limit, len(records))
    print(f"\nTổng records trong CSV : {len(records)}")
    print(f"Sẽ test                : {limit} records")

    # Check mock services
    if not args.no_mock:
        mock_ok = await check_mock_services()
        if not mock_ok:
            print(warn("\n→ Tiếp tục test nhưng các rule cần mock service có thể fail do kết nối.\n"))

    # Validate từng record
    print(f"\n{BOLD}{'='*60}")
    print(f"BƯỚC 1: Chạy validation từng record (fail-fast per record)")
    print(f"{'='*60}{RESET}")

    async with httpx.AsyncClient() as client:
        for i, record in enumerate(records[:limit], start=1):
            await validate_record(i, record, client, args.no_mock, verbose=args.verbose)

    # Summary
    print(f"\n{BOLD}{'='*60}")
    print(f"KẾT QUẢ TỔNG HỢP")
    print(f"{'='*60}{RESET}")
    print(f"Tổng test : {STATS.total}")
    print(ok(f"Passed    : {STATS.passed}"))
    if STATS.failed:
        print(fail(f"Failed    : {STATS.failed}"))

    if STATS.fail_by_rule:
        print(f"\n{BOLD}Phân bố lỗi theo rule:{RESET}")
        for rule, count in sorted(STATS.fail_by_rule.items(), key=lambda x: -x[1]):
            pct = count / STATS.total * 100
            print(fail(f"  {rule:<20} {count:>4} records  ({pct:.0f}%)"))

    if any(STATS.http_errors[k] for k in STATS.http_errors):
        print(f"\n{BOLD}Lỗi HTTP theo service:{RESET}")
        for svc, errors in STATS.http_errors.items():
            if errors:
                unique = list(dict.fromkeys(errors))[:3]
                print(warn(f"  {svc}: {len(errors)} lỗi  →  {unique[0]}"))

    if STATS.passed == 0:
        print(f"\n{RED}{BOLD}⚠ KHÔNG có record nào pass validation → radius.clean sẽ luôn rỗng{RESET}")
    elif STATS.passed < STATS.total:
        pct = STATS.passed / STATS.total * 100
        print(f"\n{YELLOW}{BOLD}⚠ Chỉ {STATS.passed}/{STATS.total} ({pct:.0f}%) records pass → kiểm tra các rule bị fail ở trên{RESET}")
    else:
        print(f"\n{GREEN}{BOLD}✓ Toàn bộ records pass → lỗi nằm ở tầng khác (Dedup/Conflict/S3){RESET}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Debug validation pipeline cho từng record CSV",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--csv",     required=True, help="Đường dẫn tới radius_log.csv")
    parser.add_argument("--limit",   type=int, default=10, help="Số records test (mặc định: 10)")
    parser.add_argument("--all",     action="store_true",  help="Test toàn bộ records")
    parser.add_argument("--itu",     default="",  help="Override ITU_E164_SERVICE_URL")
    parser.add_argument("--hlr",     default="",  help="Override HLR_HSS_SERVICE_URL")
    parser.add_argument("--gsma",    default="",  help="Override GSMA_TAC_SERVICE_URL")
    parser.add_argument("--no-mock", action="store_true",  help="Skip R2/R3/R4b (không gọi mock service)")
    parser.add_argument("--timeout", type=float, default=2.0, help="HTTP timeout giây (mặc định: 2.0)")
    parser.add_argument("--verbose", action="store_true",  help="In thêm detail khi rule pass")
    args = parser.parse_args()

    asyncio.run(main(args))