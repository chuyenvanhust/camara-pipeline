#!/usr/bin/env python3
"""
run_pipeline.py — Orchestrator pipeline 3 giai đoạn

  Giai đoạn 1 (S1): CSV  → radius.raw         [ingestion/producer.py]
  Giai đoạn 2 (S2): radius.raw → radius.clean  [processing/processor.py]
  Giai đoạn 3 (S3): radius.clean → PostgreSQL  [storage/writer.py]

Sử dụng:
  python run_pipeline.py --input /path/to/data.csv

Mỗi stage được khởi chạy là subprocess độc lập, giống pattern gốc.
S1 khởi động trước để bơm dữ liệu vào Kafka, sau đó S2 và S3 chạy song song.
"""

import argparse
import os
import subprocess
import signal
import time


def run_stage(module: str, *extra_args: str) -> subprocess.Popen:
    """Chạy `module` dưới dạng `python3 -m module [extra_args]`."""
    cmd = ["python3", "-m", module, *extra_args]
    return subprocess.Popen(
        cmd,
        cwd="/workspace",
        env={**os.environ, "PYTHONPATH": "/workspace"},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="RADIUS Pipeline Orchestrator (3 stages)")
    parser.add_argument("--input", required=True, help="Đường dẫn file CSV đầu vào")
    args = parser.parse_args()

    processes: list[subprocess.Popen] = []

    # ── Stage 1: CSV → radius.raw ─────────────────────────────────────────
    print(f">>> [S1] Ingestion: {args.input} → radius.raw")
    s1 = run_stage("pipeline.ingestion.producer", "--file", args.input)
    processes.append(s1)
    time.sleep(2)   # cho S1 kịp kết nối Kafka trước khi S2 subscribe

    # ── Stage 2: radius.raw → radius.clean ───────────────────────────────
    print(">>> [S2] Processing: radius.raw → radius.clean")
    processes.append(run_stage("pipeline.processing.processor"))
    time.sleep(1)

    # ── Stage 3: radius.clean → PostgreSQL ───────────────────────────────
    print(">>> [S3] Storage: radius.clean → PostgreSQL")
    processes.append(run_stage("pipeline.storage.writer"))

    print(">>> Pipeline 3-stage đang chạy. Spark UI: http://localhost:4040")
    print(">>> Nhấn Ctrl+C để dừng an toàn.")

    def _shutdown(signum, frame):
        print("\n>>> Đang dừng pipeline...")
        for p in processes:
            try:
                p.terminate()
            except OSError:
                pass
        print(">>> Pipeline đã dừng.")

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Chờ S1 xong (producer kết thúc sau khi hết CSV)
    s1.wait()
    print(">>> [S1] Ingestion hoàn thành.")

    # S2 và S3 chạy streaming liên tục — giữ process sống đến khi Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(None, None)


if __name__ == "__main__":
    main()
