#!/usr/bin/env python3
#pipeline\pipeline\ingestion\csv_reader.py
import csv
from typing import Generator, Dict, Any

class LocalCSVReader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_records(self) -> Generator[Dict[str, Any], None, None]:
        """Đọc và yield từng dòng bản ghi dưới dạng Dictionary từ file CSV"""
        with open(self.file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Trả về bản ghi bóc tách khoảng trắng thừa (nếu có)
                yield {k.strip(): v.strip() for k, v in row.items() if k}