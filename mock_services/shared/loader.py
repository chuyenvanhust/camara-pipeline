import time
import threading
import logging
from typing import Callable

logging.basicConfig(level=logging.INFO)

class DataAutoLoader:
    def __init__(self, load_func: Callable, service_name: str, interval: int = 300):
        self.load_func = load_func
        self.service_name = service_name
        self.interval = interval
        
        # Khởi chạy thread
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while True:
            try:
                logging.info(f"[{self.service_name}] Loading/Refreshing data...")
                self.load_func()
                logging.info(f"[{self.service_name}] Refresh complete.")
            except Exception as e:
                logging.error(f"[{self.service_name}] Error: {e}")
            time.sleep(self.interval)