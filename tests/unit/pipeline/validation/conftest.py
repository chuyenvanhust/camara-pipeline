"""
Root conftest cho toàn bộ test suite (unit + integration + api + pipeline).

Trách nhiệm chính của file này:
1. Set PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON TRƯỚC khi bất kỳ module
   nào import pyspark / tạo SparkSession. Trên Windows, PySpark mặc
   định tìm executable "python3" để spawn worker process -- nếu chỉ
   có "python.exe" (cài qua python.org installer), Spark job sẽ lỗi:

       java.io.IOException: Cannot run program "python3":
       CreateProcess error=2, The system cannot find the file specified

   Set PYSPARK_PYTHON = sys.executable trỏ đúng tới python.exe hiện
   tại đang chạy pytest, fix lỗi này trên mọi OS (không ảnh hưởng
   Linux/Mac vì sys.executable luôn đúng).

2. (Mở rộng sau) seed_db, db_client, api_client fixtures cho
   integration test -- xem tests/README.md.
"""

import os
import sys

# Phải set TRƯỚC import pyspark/SparkSession ở bất kỳ đâu trong suite.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)