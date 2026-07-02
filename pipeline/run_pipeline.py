#!/usr/bin/env python3
#pipeline\run_pipeline.py
"""
run_pipeline.py

Pipeline Orchestrator

Stage 1:
    CSV -> Kafka (radius.raw)

Stage 2:
    radius.raw -> radius.clean

Stage 3:
    radius.clean -> PostgreSQL
"""

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from pipeline.pipeline.spark_jars import ensure_ivy_dirs


# ---------------------------------------------------------------------
# Directory Layout
#
# /workspace
# ├── data/
# └── pipeline/
#     ├── run_pipeline.py
#     └── pipeline/
#         ├── ingestion/
#         ├── processing/
#         └── storage/
# ---------------------------------------------------------------------

# /workspace/pipeline
WORKSPACE = os.path.dirname(os.path.abspath(__file__))

# /workspace
PROJECT_ROOT = os.path.dirname(WORKSPACE)

DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")
DRAIN_POLL_INTERVAL = int(os.getenv("PIPELINE_DRAIN_POLL_INTERVAL", "5"))
DRAIN_TIMEOUT = int(os.getenv("PIPELINE_DRAIN_TIMEOUT_SECONDS", "1800"))
SPARK_READY_TIMEOUT = int(os.getenv("PIPELINE_SPARK_READY_TIMEOUT", "180"))
S1_HEARTBEAT_INTERVAL = int(os.getenv("PIPELINE_S1_HEARTBEAT_INTERVAL", "10"))

# Map tên biến trong .env sang tên code đọc
_ENV_ALIASES = {
    "GSMA_TAC_API_URL": "GSMA_TAC_SERVICE_URL",
    "HLR_HSS_API_URL": "HLR_HSS_SERVICE_URL",
    "ITU_E164_API_URL": "ITU_E164_SERVICE_URL",
}


def _log(msg: str) -> None:
    print(msg, flush=True)


def load_project_env() -> None:
    """Nạp .env từ project root để các stage con kế thừa biến môi trường."""
    if os.path.isfile(DOTENV_PATH):
        load_dotenv(dotenv_path=DOTENV_PATH, override=True)
    else:
        load_dotenv(override=True)

    for src, dst in _ENV_ALIASES.items():
        val = os.getenv(src)
        if val and not os.getenv(dst):
            os.environ[dst] = val


def resolve_input_path(path: str) -> str:
    """Convert input path to an absolute path."""
    if os.path.isabs(path):
        abs_path = os.path.abspath(path)
    else:
        abs_path = os.path.abspath(os.path.join(PROJECT_ROOT, path))

    if not os.path.isfile(abs_path):
        raise FileNotFoundError("Input CSV not found:\n{}".format(abs_path))

    return abs_path


def count_csv_records(path: str) -> int:
    with open(path, encoding="utf-8") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def ensure_kafka_topics(
    bootstrap_servers: str,
    topics: list,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    """Tạo Kafka topics nếu chưa tồn tại."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic as AIONewTopic
    from aiokafka.errors import TopicAlreadyExistsError as AIOTopicExistsError

    async def _create():
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin.start()
        try:
            new_topics = [
                AIONewTopic(
                    name=t,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
                for t in topics
            ]
            await admin.create_topics(new_topics)
            _log(f">>> Created Kafka topics: {topics}")
        except AIOTopicExistsError:
            _log(f">>> Kafka topics already exist: {topics}")
        finally:
            await admin.close()

    asyncio.run(_create())


def run_stage(script_relpath: str, *args: str) -> subprocess.Popen:
    """Launch one pipeline stage."""
    script_path = os.path.join(WORKSPACE, script_relpath)
    if not os.path.isfile(script_path):
        raise FileNotFoundError(script_path)

    env = os.environ.copy()
    env["PYTHONPATH"] = WORKSPACE
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("SPARK_IVY_DIR", "/tmp/ivy2")
    env.setdefault("HOME", "/opt/spark/work-dir")

    command = [sys.executable, "-u", script_path, *args]
    return subprocess.Popen(command, cwd=WORKSPACE, env=env,stdout=sys.stdout, stderr=sys.stderr)


def stop_all(processes: List[subprocess.Popen]) -> None:
    """Stop all child processes."""
    _log("\n>>> Stopping pipeline...")

    for p in processes:
        if p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass

    for p in processes:
        if p.poll() is None:
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass

    _log(">>> Pipeline stopped.")


async def _get_topic_end_offsets(
    bootstrap_servers: str,
    topic: str,
) -> Dict[int, int]:
    """High watermark theo partition."""
    from aiokafka import AIOKafkaConsumer
    from aiokafka.structs import TopicPartition

    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {}
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        end_map = await consumer.end_offsets(tps)
        return {tp.partition: end_map[tp] for tp in tps}
    finally:
        await consumer.stop()


def get_topic_end_offsets(bootstrap_servers: str, topic: str) -> Dict[int, int]:
    return asyncio.run(_get_topic_end_offsets(bootstrap_servers, topic))


def _sum_offsets(offsets: Dict[int, int]) -> int:
    return sum(offsets.values())


def _extract_topic_offsets(obj: Any, topic: str) -> Optional[Dict[int, int]]:
    """Tìm {partition: offset} trong JSON checkpoint Spark (nhiều format)."""
    if isinstance(obj, dict):
        if topic in obj and isinstance(obj[topic], dict):
            try:
                return {int(k): int(v) for k, v in obj[topic].items()}
            except (TypeError, ValueError):
                pass

        for value in obj.values():
            found = _extract_topic_offsets(value, topic)
            if found is not None:
                return found

    if isinstance(obj, list):
        for item in obj:
            found = _extract_topic_offsets(item, topic)
            if found is not None:
                return found

    return None


def read_spark_topic_offsets(
    checkpoint_dir: str,
    topic: str,
) -> Optional[Dict[int, int]]:
    """Đọc offset Kafka mà Spark streaming đã commit trong checkpoint."""
    candidate_dirs = [
        os.path.join(checkpoint_dir, "offsets"),
        os.path.join(checkpoint_dir, "sources", "0"),
    ]

    offset_dir = next((d for d in candidate_dirs if os.path.isdir(d)), None)
    if not offset_dir:
        return None

    batch_files = sorted(
        (
            name
            for name in os.listdir(offset_dir)
            if name.isdigit() or name.startswith("offsets.")
        ),
        key=lambda name: int(name.split(".")[0]) if name.split(".")[0].isdigit() else -1,
    )
    if not batch_files:
        return None

    latest_path = os.path.join(offset_dir, batch_files[-1])
    try:
        with open(latest_path, encoding="utf-8") as fh:
            content = fh.read().strip()
    except OSError:
        return None

    for line in reversed(content.splitlines()):
        line = line.strip()
        if not line or line == "v1":
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        found = _extract_topic_offsets(data, topic)
        if found is not None:
            return found

    return None


def spark_stream_ready(checkpoint_dir: str) -> bool:
    """Spark streaming đã khởi tạo checkpoint (job đang chạy)."""
    markers = [
        os.path.join(checkpoint_dir, "metadata"),
        os.path.join(checkpoint_dir, "offsets"),
        os.path.join(checkpoint_dir, "sources", "0"),
    ]
    return any(os.path.exists(p) for p in markers)


def wait_for_spark_ready(label: str, checkpoint_dir: str, timeout: int) -> None:
    """Chờ Spark stage khởi động xong (checkpoint xuất hiện)."""
    _log(f">>> Đang chờ {label} khởi động Spark (tối đa {timeout}s)...")
    deadline = time.time() + timeout
    last_heartbeat = 0.0

    while time.time() < deadline:
        if spark_stream_ready(checkpoint_dir):
            _log(f">>> {label} Spark streaming đã sẵn sàng.")
            return

        now = time.time()
        if now - last_heartbeat >= S1_HEARTBEAT_INTERVAL:
            _log(f"    ... {label}: đang tải jar / khởi tạo Spark session...")
            last_heartbeat = now

        time.sleep(2)

    raise TimeoutError(f"{label} không khởi động Spark trong {timeout}s.")


def stream_caught_up(
    consumed: Optional[Dict[int, int]],
    end_offsets: Dict[int, int],
) -> bool:
    """Spark đã đọc hết message hiện có trên topic."""
    if not end_offsets:
        return True
    if not consumed:
        return False
    return all(consumed.get(part, 0) >= end for part, end in end_offsets.items())


def get_db_row_count() -> Optional[int]:
    """Đếm bản ghi trong radius_sessions (None nếu không kết nối được)."""
    try:
        import psycopg2

        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "camara_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "camara"),
            host=os.getenv("DB_HOST", "camara-postgres"),
            port=int(os.getenv("DB_PORT", "5432")),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM radius_sessions")
                return int(cur.fetchone()[0])
        finally:
            conn.close()
    except Exception as exc:
        _log(f"    ... không đọc được PostgreSQL: {exc}")
        return None


def wait_for_s1(producer: subprocess.Popen, expected_records: int) -> None:
    """Chờ S1 xong, in heartbeat để tránh cảm giác treo."""
    _log(f">>> Đang chờ S1 đẩy ~{expected_records} records lên Kafka...")
    last_heartbeat = time.time()

    while producer.poll() is None:
        now = time.time()
        if now - last_heartbeat >= S1_HEARTBEAT_INTERVAL:
            raw_total = _sum_offsets(
                get_topic_end_offsets(
                    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092"),
                    os.getenv("KAFKA_TOPIC_RAW", "radius.raw"),
                )
            )
            _log(f"    ... S1 đang chạy — radius.raw hiện có {raw_total} message")
            last_heartbeat = now
        time.sleep(1)

    if producer.returncode != 0:
        raise RuntimeError(
            f"Stage 1 (producer) failed with exit code {producer.returncode}"
        )

    _log(">>> Producer finished.")


def wait_for_topic_drain(
    label: str,
    checkpoint_dir: str,
    topic: str,
    bootstrap_servers: str,
    timeout: int = DRAIN_TIMEOUT,
) -> None:
    """Chờ streaming job consume hết message trên topic."""
    deadline = time.time() + timeout
    end_offsets: Dict[int, int] = {}
    last_consumed_sum = -1
    idle_polls = 0
    last_heartbeat = 0.0

    _log(f">>> Đang chờ {label} xử lý xong topic '{topic}'...")

    while time.time() < deadline:
        end_offsets = get_topic_end_offsets(bootstrap_servers, topic)
        consumed = read_spark_topic_offsets(checkpoint_dir, topic)

        if stream_caught_up(consumed, end_offsets):
            total = _sum_offsets(end_offsets)
            _log(
                f">>> {label} đã xử lý xong '{topic}' "
                f"({total} message trên {len(end_offsets)} partition)."
            )
            return

        consumed_sum = _sum_offsets(consumed) if consumed else 0
        total = _sum_offsets(end_offsets)

        now = time.time()
        if now - last_heartbeat >= S1_HEARTBEAT_INTERVAL:
            if consumed:
                _log(f"    ... {label}: {consumed_sum}/{total} offset đã consume")
            else:
                _log(
                    f"    ... {label}: checkpoint chưa có offset, "
                    f"topic có {total} message — đang chờ micro-batch đầu tiên"
                )
            last_heartbeat = now

        if consumed and consumed_sum == last_consumed_sum and total > 0:
            idle_polls += 1
            if idle_polls >= 6:
                _log(
                    f">>> {label}: offset ổn định ({consumed_sum}/{total}), "
                    f"coi như đã xử lý xong '{topic}'."
                )
                return
        else:
            idle_polls = 0
            last_consumed_sum = consumed_sum

        time.sleep(DRAIN_POLL_INTERVAL)

    raise TimeoutError(
        f"{label} không hoàn tất trong {timeout}s "
        f"(topic={topic}, end_offsets={end_offsets})."
    )


def wait_for_s3_db_drain(
    bootstrap_servers: str,
    clean_topic: str,
    storage_checkpoint: str,
    timeout: int,
) -> None:
    """Chờ S3 ghi PostgreSQL: kết hợp Kafka checkpoint + row count ổn định."""
    wait_for_topic_drain("[S3]", storage_checkpoint, clean_topic, bootstrap_servers, timeout)

    _log(">>> Kiểm tra dữ liệu đã ghi vào PostgreSQL...")
    deadline = time.time() + min(timeout, 120)
    last_count: Optional[int] = None
    stable = 0

    while time.time() < deadline:
        count = get_db_row_count()
        clean_total = _sum_offsets(get_topic_end_offsets(bootstrap_servers, clean_topic))

        if count is not None:
            _log(f"    ... radius_sessions: {count} rows (radius.clean: {clean_total} msg)")

        if clean_total == 0:
            _log(">>> S3: radius.clean rỗng — không có bản ghi hợp lệ để ghi DB.")
            return

        if count is not None:
            if count == last_count and count > 0:
                stable += 1
                if stable >= 2:
                    _log(f">>> S3 đã ghi xong {count} rows vào PostgreSQL.")
                    return
            else:
                stable = 0
            last_count = count

        time.sleep(DRAIN_POLL_INTERVAL)

    _log(">>> S3: hết thời gian chờ DB ổn định — tiếp tục shutdown.")


def main():
    parser = argparse.ArgumentParser(description="RADIUS Pipeline Orchestrator")
    parser.add_argument("--input", required=True, help="CSV input file")
    parser.add_argument(
        "--clear-ivy",
        action="store_true",
        help="Xóa Ivy cache trước khi chạy (mặc định: giữ cache để khởi động nhanh)",
    )
    args = parser.parse_args()

    load_project_env()
    ensure_ivy_dirs(os.getenv("SPARK_IVY_DIR", "/tmp/ivy2"))

    input_file = resolve_input_path(args.input)
    expected_records = count_csv_records(input_file)

    _log("==================================================")
    _log(f"Workspace : {WORKSPACE}")
    _log(f"Project   : {PROJECT_ROOT}")
    _log(f"Input CSV : {input_file} (~{expected_records} records)")
    _log("==================================================")

    processes: List[subprocess.Popen] = []

    try:
        import shutil

        if args.clear_ivy:
            for ivy_cache in ("/tmp/ivy2", "/tmp/ivy2-s2", "/tmp/ivy2-s3"):
                if os.path.isdir(ivy_cache):
                    shutil.rmtree(ivy_cache, ignore_errors=True)
                    _log(f">>> Cleared Ivy cache: {ivy_cache}")

        checkpoint_dirs = [
            os.getenv("PROCESSING_CHECKPOINT_DIR", "/tmp/spark-pipeline-processing-checkpoint"),
            os.getenv("STORAGE_CHECKPOINT_DIR", "/tmp/spark-pipeline-storage-checkpoint"),
        ]
        for ckpt in checkpoint_dirs:
            if os.path.isdir(ckpt):
                shutil.rmtree(ckpt, ignore_errors=True)
                _log(f">>> Cleared checkpoint: {ckpt}")

        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
        processing_checkpoint = checkpoint_dirs[0]
        storage_checkpoint = checkpoint_dirs[1]
        topic_raw = os.getenv("KAFKA_TOPIC_RAW", "radius.raw")
        topic_clean = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")

        ensure_kafka_topics(
            kafka_bootstrap,
            ["radius.raw", "radius.clean", "radius.invalid"],
            num_partitions=4,
        )

        # Consumers trước, producer sau — tránh message chờ consumer khởi động
        _log(">>> [S2] radius.raw -> radius.clean")
        s2 = run_stage("pipeline/processing/processor.py")
        processes.append(s2)
        wait_for_spark_ready("[S2]", processing_checkpoint, SPARK_READY_TIMEOUT)

        _log(">>> [S3] radius.clean -> PostgreSQL")
        s3 = run_stage("pipeline/storage/writer.py")
        processes.append(s3)
        wait_for_spark_ready("[S3]", storage_checkpoint, SPARK_READY_TIMEOUT)

        _log(">>> [S1] CSV -> radius.raw")
        s1 = run_stage("pipeline/ingestion/producer.py", "--file", input_file)
        processes.append(s1)

        def shutdown(signum, frame):
            stop_all(processes)
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        _log("")
        _log("Pipeline is running (S2 + S3 đã sẵn sàng, S1 đang ingest).")
        _log("Spark UI: http://localhost:4040 (S3 thường ở :4041)")
        _log("")

        wait_for_s1(s1, expected_records)

        wait_for_topic_drain(
            "[S2]",
            processing_checkpoint,
            topic_raw,
            kafka_bootstrap,
        )
        wait_for_s3_db_drain(
            kafka_bootstrap,
            topic_clean,
            storage_checkpoint,
            DRAIN_TIMEOUT,
        )

        s3_commit_grace = int(os.getenv("SPARK_COMMIT_INTERVAL_SECONDS", "30")) + 10
        _log(f">>> Chờ S3 commit batch cuối ({s3_commit_grace}s)...")
        time.sleep(s3_commit_grace)

        stop_all(processes)
        _log(">>> Pipeline hoàn tất — tất cả stage đã xử lý xong.")

    except KeyboardInterrupt:
        stop_all(processes)

    except Exception:
        stop_all(processes)
        raise


if __name__ == "__main__":
    main()
