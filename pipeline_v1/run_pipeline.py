#!/usr/bin/env python3
#pipeline\run_pipeline.py
import argparse
import subprocess
import time
import signal
import os

def run_stage(command_str):
    parts = command_str.split()

    script = parts[0]
    args = parts[1:]

    module = script.replace("/", ".").replace("\\", ".")
    if module.endswith(".py"):
        module = module[:-3]

    full_command = ["python3", "-m", module] + args

    return subprocess.Popen(
        full_command,
        cwd="/workspace",
        env={
            **os.environ,
            "PYTHONPATH": "/workspace"
        }
    )

def main():
    parser = argparse.ArgumentParser(description="RADIUS Pipeline Orchestrator")
    parser.add_argument("--input", required=True, help="Path to raw CSV file")
    args = parser.parse_args()

    # Định nghĩa luồng 5 stage
    stages = [
        "pipeline/ingestion/producer.py",
        "pipeline/validation/validator.py",
        "pipeline/deduplication/dedup_job.py",
        "pipeline/conflict_resolution/resolver.py",
        "pipeline/storage/writer.py"
    ]

    processes = []
    
    # 1. Start Ingestion trước để đưa dữ liệu vào Kafka
    print(f">>> [S1] Starting Ingestion with input: {args.input}")
    processes.append(run_stage(f"pipeline/ingestion/producer.py --file {args.input}"))
    time.sleep(2)

    # 2. Start các Streaming Stages
    for stage in stages[1:]:
        print(f">>> Starting {stage}...")
        processes.append(run_stage(stage))
        time.sleep(1)

    print(">>> All 5 stages are active. Tracking at http://localhost:4040")

    # Graceful Shutdown
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n>>> Shutdown signal received. Stopping all stages...")
        for p in processes:
            p.terminate()
        print(">>> Pipeline stopped safely.")

if __name__ == "__main__":
    main()