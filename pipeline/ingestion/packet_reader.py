#pipeline\ingestion\packet_reader.py
import socket
from typing import Generator, Dict, Any
import datetime

class PacketReader:
    # ==========================================================
    # Cấu hình tĩnh (Class Constants)
    # ==========================================================
    RADIUS_IDX = 0
    PAIR_IDX = 20
    DEFAULT_RADIUS_PORT = 1813

    FIELD_SCHEMA = {
        "acct_status_type": 0x28,
        "acct_session_id": 0x2c,
        "acct_session_time": 0x2d,
        "msisdn": 0x1f,
        "Calling_Station_Id": 0x1f,
        "framed_ip": 0x08,
        "Framed_IP_Address": 0x08,
        "nas_ip": 0x04,
        "NAS_Identifier": 0x20,
        "nas_identifier": 0x20,
        "vendor_specific": {
            "type": 0x1a, 
            "fields": {
                "imsi": 0x01,
                "imei": 0x14,
                "rat_type": 0x15,
                "mcc_mnc": 0x08,
            }
        }
    }

    def __init__(self):
        """Khởi tạo và chuẩn bị bản đồ tra cứu ngược để tăng tốc độ xử lý"""
        # Tạo Reverse Map cho Standard Attributes
        self.REVERSE_ATTR_MAP = {}
        for k, v in self.FIELD_SCHEMA.items():
            if isinstance(v, int) and v != -1:
                self.REVERSE_ATTR_MAP.setdefault(v, []).append(k)

        # Tạo Reverse Map cho Vendor Attributes
        self.REVERSE_VENDOR_MAP = {
            v: k for k, v in self.FIELD_SCHEMA["vendor_specific"]["fields"].items()
        }

    def decode_radius(self, packet: bytes) -> Dict[str, Any]:
        """Dịch gói tin RADIUS bytes sang Dictionary dựa trên FIELD_SCHEMA"""
        
        # Tạo cấu trúc trống dựa trên Schema để đảm bảo dữ liệu đầu ra đồng nhất
        result = {k: None for k in self.FIELD_SCHEMA.keys() if k != "vendor_specific"}
        for k in self.FIELD_SCHEMA["vendor_specific"]["fields"].keys():
            result[k] = None
        
        # Gán nhãn thời gian nạp vào hệ thống
        now = datetime.datetime.now(datetime.timezone.utc)
        result["ingest_timestamp"] = now.isoformat()
        result["timestamp"] = int(now.timestamp())

        # Kiểm tra code RADIUS (0x04 = Accounting Request)
        if len(packet) > self.RADIUS_IDX and packet[self.RADIUS_IDX] == 0x04:
            idx = self.PAIR_IDX
            packet_len = len(packet)

            while idx < packet_len:
                if idx + 2 > packet_len: break 
                
                attr_type = packet[idx]
                attr_len = packet[idx+1]
                
                if attr_len < 2: break 
                
                attr_value = packet[idx + 2 : idx + attr_len]

                # 1. Xử lý Vendor Specific (0x1a)
                if attr_type == self.FIELD_SCHEMA["vendor_specific"]["type"]:
                    sub_idx = 4 
                    while sub_idx < len(attr_value):
                        if sub_idx + 2 > len(attr_value): break
                        v_type = attr_value[sub_idx]
                        v_len = attr_value[sub_idx+1]
                        v_data = attr_value[sub_idx + 2 : sub_idx + v_len]
                        
                        v_field_name = self.REVERSE_VENDOR_MAP.get(v_type)
                        if v_field_name:
                            result[v_field_name] = v_data.decode(errors="ignore").strip()
                        sub_idx += v_len

                # 2. Xử lý Standard Attributes
                elif attr_type in self.REVERSE_ATTR_MAP:
                    field_names = self.REVERSE_ATTR_MAP[attr_type]
                    
                   
                    parsed_value = None
                    if attr_type in [0x04, 0x08]: # IP
                        try: parsed_value = socket.inet_ntoa(attr_value)
                        except: parsed_value = None
                    elif attr_type in [0x2d, 0x28]: # Integer
                        parsed_value = int.from_bytes(attr_value, byteorder='big')
                    else: # String
                        parsed_value = attr_value.decode(errors="ignore").strip()
                    
                    for f in field_names:
                        result[f] = parsed_value

                idx += attr_len

        return result

    def listen_radius_packets(self, port=DEFAULT_RADIUS_PORT) -> Generator[Dict[str, Any], None, None]:
        """Mở socket UDP lắng nghe gói tin và yield kết quả sạch"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            sock.bind(("0.0.0.0", port))
            print(f"[*] PacketReader: Listening on UDP/{port}...")
            
            while True:
                
                packet, addr = sock.recvfrom(4096)
                radius_data = self.decode_radius(packet)
                
                
                if radius_data.get("acct_session_id"):
                    yield radius_data
                    
        except Exception as e:
            print(f"[!] Socket Error: {e}")
        finally:
            sock.close()


if __name__ == "__main__":
    reader = PacketReader()

    try:
        for record in reader.listen_radius_packets(port=1813):
            
            print(f"Captured: {record['acct_session_id']} | IMSI: {record['imsi']}")
            
            
    except KeyboardInterrupt:
        print("\n[!] Ingestion stopped.")