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
        # --- SECTION 1: TỔNG QUAN ---
        
        cur.execute("SELECT COUNT(*) as total FROM radius_sessions;")
        result = cur.fetchone()
        total_records = result['total'] if result and result['total'] is not None else 100
        
        metrics['overview'] = {
            "total_records": total_records,
            "execution_time": "N/A",
            "throughput": 0, 
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

       
        # Section 2: INVALID IMEI
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE error_code = 'ERR_IMEI_LUHN_FAIL') as luhn_fail,
                COUNT(*) FILTER (WHERE error_code = 'ERR_IMEI_TAC_UNKNOWN') as tac_unknown,
                COUNT(*) as total
            FROM invalid_log 
            WHERE error_code LIKE 'ERR_IMEI%';
        """)
        imei_data = cur.fetchone()
        metrics['imei'] = {
            "total": imei_data['total'],
            "rate": round((imei_data['total'] / total_records * 100), 2) if total_records > 0 else 0,
            "luhn_fail": imei_data['luhn_fail'],
            "tac_unknown": imei_data['tac_unknown']
        }

        # Section 3: DUPLICATE
        
        cur.execute("SELECT COUNT(*) as total FROM duplicate_log;")
        dup_total = cur.fetchone()['total']
        
        cur.execute("""
            SELECT EXTRACT(HOUR FROM detected_at) as hour, COUNT(*) as count 
            FROM duplicate_log 
            GROUP BY hour ORDER BY hour;
        """)
        dup_hourly = cur.fetchall()
        
        metrics['duplicate'] = {
            "total": dup_total,
            "rate": round((dup_total / total_records * 100), 2) if total_records > 0 else 0,
            "hourly_labels": [f"{int(r['hour'])}h" for r in dup_hourly],
            "hourly_data": [r['count'] for r in dup_hourly]
        }

        # Section 4: CONFLICT
        
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE conflict_type = 'A') as type_a,
                COUNT(*) FILTER (WHERE conflict_type = 'B') as type_b,
                COUNT(*) FILTER (WHERE conflict_type = 'C') as type_c,
                COUNT(*) as total
            FROM conflict_log;
        """)
        conflict_data = cur.fetchone()
        metrics['conflict'] = {
            "total": conflict_data['total'],
            "rate": round((conflict_data['total'] / total_records * 100), 2) if total_records > 0 else 0,
            "type_a": conflict_data['type_a'],
            "type_b": conflict_data['type_b'],
            "type_c": conflict_data['type_c']
        }

    

        # --- SECTION 5: LATE ARRIVAL ---
        
        cur.execute("""
            SELECT COUNT(*) as total 
            FROM invalid_log 
            WHERE error_code = 'ERR_LATE_ARRIVAL';
        """)
        late_total = cur.fetchone()['total']
        
       
        metrics['late_arrival'] = {
            "total": late_total,
            "rate": round((late_total / total_records * 100), 2) if total_records > 0 else 0,
            "buckets": ["Total Late"],
            "counts": [late_total]
        }

        # --- SECTION 6: MISSING FIELD ---
        cur.execute("SELECT COUNT(*) as total FROM invalid_log WHERE error_code = 'ERR_MISSING_FIELD';")
        missing_total = cur.fetchone()['total']
        
     
        cur.execute("""
            SELECT details, COUNT(*) as count 
            FROM invalid_log 
            WHERE error_code = 'ERR_MISSING_FIELD'
            GROUP BY details ORDER BY count DESC LIMIT 5;
        """)
        top_missing = cur.fetchall()

        
        metrics['missing_field'] = {
            "total": missing_total,
            "rate": round((missing_total / total_records * 100), 2) if total_records > 0 else 0,
            "fields": [r['details'] for r in top_missing] if top_missing else ["None"],
            "counts": [r['count'] for r in top_missing] if top_missing else [0]
        }

    except Exception as e:
        print(f"Error gathering metrics from Postgres: {e}")
       
        metrics = get_mock_metrics()
    finally:
        cur.close()
        conn.close()

    return metrics

def get_mock_metrics():
    """Fallback mock dữ liệu mẫu chuẩn cấu trúc để test report không bị crash."""
    return {
        "overview": {"total_records": 125000, "execution_time": "45 seconds", "throughput": 2777.78, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
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