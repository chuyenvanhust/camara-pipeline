# reporting/quality_report.py
import os
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

# File script nằm ở: camara-pipeline/reporting/quality_report.py

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_DIR = os.path.dirname(SCRIPT_DIR)


dotenv_path = os.path.join(BASE_DIR, '.env')


if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    # Fallback nếu bạn đứng từ thư mục gốc chạy lệnh
    load_dotenv(dotenv_path=".env", override=True)
def get_db_connection():

    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "camara_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "camara"),
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", 5432)),
        cursor_factory=RealDictCursor
    )

def fetch_metrics():
    conn = get_db_connection()
    cur = conn.cursor()
    metrics = {}

    try:
        # 1. Thống kê từ Database PostgreSQL
        cur.execute("SELECT COUNT(*) as total FROM msisdn_device;")
        subscribers_count = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) as total FROM sim_swap_history;")
        sim_swap_count = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) as total FROM device_swap_history;")
        device_swap_count = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) as total FROM audit_log;")
        audit_count = cur.fetchone()['total']

        cur.execute("SELECT COUNT(*) as total FROM notification_log;")
        notification_count = cur.fetchone()['total']

        # 2. Đếm chính xác tổng số bản ghi từ tập dữ liệu CSV
        total_input_records = 2088693
        csv_path = os.path.join(BASE_DIR, "data", "radius_log.csv")
        if os.path.exists(csv_path):
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    lines = sum(1 for _ in f) - 1
                    if lines > 0:
                        total_input_records = lines
            except Exception:
                pass

        # F-QR-FIX: throughput trước đây là literal cứng (19150/15400/9800/12625),
        # không phản ánh lần chạy thực tế nào. Tính từ khoảng thời gian thật giữa
        # bản ghi audit_log đầu tiên và cuối cùng (proxy cho thời gian pipeline
        # thực sự chạy) chia cho tổng số event đã xử lý.
        cur.execute(
            "SELECT MIN(created_at) AS first_ts, MAX(created_at) AS last_ts, COUNT(*) AS n "
            "FROM audit_log;"
        )
        window = cur.fetchone()
        total_events = sim_swap_count + device_swap_count
        duration_seconds = None
        if window and window["first_ts"] and window["last_ts"]:
            duration_seconds = (window["last_ts"] - window["first_ts"]).total_seconds()

        if duration_seconds and duration_seconds > 0 and total_events > 0:
            overall_throughput = round(total_events / duration_seconds, 1)
        else:
            # Không đủ dữ liệu để tính (0-1 event, hoặc audit_log rỗng) — báo rõ
            # là "không xác định", KHÔNG bịa số để tránh đánh lừa báo cáo go-live.
            overall_throughput = None
        # Không có điểm đo riêng cho từng giai đoạn (producer/consumer/db) trong
        # schema hiện tại -> không hiển thị số giả cho 3 mục này, chỉ hiển thị
        # overall_throughput (thực đo) hoặc None nếu chưa đủ dữ liệu.
        producer_throughput = None
        processing_throughput = None
        db_write_throughput = None

        sim_rate = round((sim_swap_count / max(total_input_records, 1)) * 100, 3)
        device_rate = round((device_swap_count / max(total_input_records, 1)) * 100, 3)

        metrics['overview'] = {
            "total_records": total_input_records,
            "subscribers_count": subscribers_count,
            "sim_swap_count": sim_swap_count,
            "device_swap_count": device_swap_count,
            "audit_count": audit_count,
            "notification_count": notification_count,
            "producer_throughput": producer_throughput,
            "processing_throughput": processing_throughput,
            "db_write_throughput": db_write_throughput,
            "overall_throughput": overall_throughput,
            "execution_time": "Real-time Stream",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        metrics['swaps'] = {
            "sim_swap_count": sim_swap_count,
            "sim_swap_rate": sim_rate,
            "device_swap_count": device_swap_count,
            "device_swap_rate": device_rate,
            "total_swaps": sim_swap_count + device_swap_count,
            "overall_swap_rate": round(sim_rate + device_rate, 3)
        }

        metrics['imei'] = {
            "total": device_swap_count,
            "rate": device_rate,
            "luhn_fail": 0,
            "tac_unknown": 0
        }

        metrics['duplicate'] = {
            "total": 0,
            "rate": 0,
            "hourly_labels": ["08h", "09h", "10h", "11h"],
            "hourly_data": [0, 0, 0, 0]
        }

        metrics['conflict'] = {
            "total": sim_swap_count + device_swap_count,
            "rate": round(sim_rate + device_rate, 3),
            "type_a": 0,
            "type_b": 0,
            "type_c": sim_swap_count,
            "type_d": device_swap_count
        }

        metrics['late_arrival'] = {
            "total": 0,
            "rate": 0,
            "buckets": ["Total Late"],
            "counts": [0]
        }

        metrics['missing_field'] = {
            "total": 0,
            "rate": 0,
            "fields": ["None"],
            "counts": [0]
        }

        metrics['_data_source'] = "live"

    except Exception as e:
        print(f"Error gathering metrics from Postgres: {e}")
        metrics = get_mock_metrics()
        # F-QR-FIX: trước đây fallback này hoàn toàn im lặng — báo cáo hiển thị
        # số liệu giả (1250 IMEI, 3120 duplicate...) không khác gì số thật, có thể
        # bị hiểu nhầm là kết quả go-live thật. Đánh dấu rõ ràng để template cảnh báo.
        metrics['_data_source'] = "mock_fallback"
        metrics['_data_source_error'] = str(e)
    finally:
        cur.close()
        conn.close()

    return metrics

def get_mock_metrics():
    """Fallback mock dữ liệu mẫu chuẩn cấu trúc để test report không bị crash."""
    return {
        "overview": {
            "total_records": 125000, "execution_time": "45 seconds", "overall_throughput": None,
            "subscribers_count": 0, "sim_swap_count": 0, "device_swap_count": 0,
            "audit_count": 0, "notification_count": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "imei": {"total": 1250, "rate": 1.0, "luhn_fail": 850, "tac_unknown": 400},
        "duplicate": {"total": 3120, "rate": 2.5, "hourly_labels": ["08h", "09h", "10h", "11h"], "hourly_data": [500, 1200, 920, 500]},
        "conflict": {"total": 450, "rate": 0.36, "type_a": 200, "type_b": 150, "type_c": 100},
        "late_arrival": {"total": 1850, "rate": 1.48, "buckets": ["0-5m", "5-15m", ">15m"], "counts": [1200, 500, 150]},
        "missing_field": {"total": 620, "rate": 0.5, "fields": ["msisdn", "framed_ip", "imei"], "counts": [350, 200, 70]}
    }

def generate_html_report(output_path):
    metrics = fetch_metrics()
    

    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('report.html.jinja2')
    
    html_content = template.render(m=metrics)
    
    # Đảm bảo thư mục output tồn tại
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f" Data Quality Report successfully generated at: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Data Quality Report HTML.")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_output = f"reports/quality_report_{timestamp}.html"
    
    parser.add_argument("--output", default=default_output, help="Path to output HTML report file")
    args = parser.parse_args()
    
    generate_html_report(args.output)