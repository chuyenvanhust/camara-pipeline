import pytest
import asyncio
import os
import csv
import json
from aiokafka import AIOKafkaConsumer
from pipeline_v1.ingestion.producer import RadiusLogProducer

@pytest.fixture
def integration_csv():
    file_path = "tests/unit/pipeline/ingestion/integration_temp.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    headers = ["acct_status_type", "acct_session_id", "msisdn", "imsi", "imei"]
    data = [
        ["Start", "SESS_INTEG_1", "+84971111111", "452010000000111", "356123000000001"],
        ["Start", "SESS_INTEG_2", "+84972222222", "452010000000222", "356123000000002"],
        ["Stop", "SESS_INTEG_1", "+84971111111", "452010000000111", "356123000000001"]
    ]
    
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)
        
    yield file_path
    
    if os.path.exists(file_path):
        os.remove(file_path)

@pytest.mark.asyncio
async def test_producer_publish_and_distribution(integration_csv):
    # Sử dụng config từ môi trường nhưng đổi topic sang môi trường test để cô lập
    KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
    TEST_TOPIC = "radius.raw.test"
    
    # 1. Khởi tạo và đẩy dữ liệu lên Kafka (Bây giờ __init__ đã nhận tham số bình thường)
    producer = RadiusLogProducer(bootstrap_servers=KAFKA_SERVER, topic=TEST_TOPIC)
    await producer.start()
    
    try:
        records_sent = await producer.publish_csv_to_kafka(integration_csv)
        assert records_sent == 3
        await asyncio.sleep(0.5)  # Chờ Kafka cập nhật metadata
    finally:
        await producer.stop()
        
    # 2. Khởi tạo Consumer để rút ngược dữ liệu về verify
    consumer = AIOKafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    await consumer.start()
    
    consumed_records = []
    try:
        while len(consumed_records) < 3:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=3.0)
                consumed_records.append(msg)
            except asyncio.TimeoutError:
                break
    finally:
        await consumer.stop()
        
    # 3. Tiến hành đối soát kết quả đầu cuối
    assert len(consumed_records) == 3
    
    first_msg_value = json.loads(consumed_records[0].value.decode("utf-8"))
    assert first_msg_value["acct_session_id"] == "SESS_INTEG_1"
    
    assert consumed_records[0].key.decode("utf-8") == "+84971111111"
    assert consumed_records[1].key.decode("utf-8") == "+84972222222"