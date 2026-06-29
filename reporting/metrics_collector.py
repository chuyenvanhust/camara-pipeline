# reporting/metrics_collector.py
import json

class SparkMetricsCollector:
    """Thu thập thông số vận hành của Spark Structured Streaming."""
    def __init__(self, streaming_query):
        self.query = streaming_query

    def get_latest_progress(self):
        """Lấy các metrics xử lý thời gian thực từ progress của Spark Query."""
        if not self.query or not self.query.lastProgress:
            return {"status": "INACTIVE", "throughput_rec_s": 0, "input_rate": 0}
            
        progress = self.query.lastProgress
        return {
            "status": "ACTIVE",
            "query_id": progress.get("id"),
            "name": progress.get("name"),
            "input_rate": progress.get("inputRowsPerSecond", 0),
            "throughput_rec_s": progress.get("processedRowsPerSecond", 0),
            "batch_duration_ms": progress.get("durationMs", {}).get("triggerExecution", 0)
        }

class KafkaLagCollector:
    """Giả lập/Thu thập Consumer lag từ các Kafka Partition."""
    def __init__(self, bootstrap_servers, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def get_consumer_lag(self, topic):
        # Trả về thông số lag giả lập phân phối làm đầu vào quan sát hệ thống
        return {
            "topic": topic,
            "group_id": self.group_id,
            "total_lag": 150,
            "partition_lags": {"partition_0": 45, "partition_1": 55, "partition_2": 50}
        }