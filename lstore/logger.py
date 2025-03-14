import os
import json
import time
from threading import RLock
from datetime import datetime

class Logger:
    """
    Thread-safe logger for transaction and recovery management
    """
    _instance = None
    _lock = RLock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Logger, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
            
    def __init__(self):
        if not self._initialized:
            self.log_dir = "./data/logs"
            self.transaction_log = os.path.join(self.log_dir, "transaction.log")
            self.recovery_log = os.path.join(self.log_dir, "recovery.log")
            os.makedirs(self.log_dir, exist_ok=True)
            self._initialized = True
            
    def log_transaction(self, transaction_id: int, operation: str, table: str, key: int, columns=None, values=None):
        """
        Log transaction operations for recovery
        """
        with self._lock:
            log_entry = {
                "timestamp": time.time(),
                "transaction_id": transaction_id,
                "operation": operation,
                "table": table,
                "key": key,
                "columns": columns,
                "values": values
            }
            
            with open(self.transaction_log, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
    def log_recovery_point(self):
        """
        Create a recovery point for crash recovery
        """
        with self._lock:
            recovery_point = {
                "timestamp": time.time(),
                "datetime": datetime.now().isoformat()
            }
            
            with open(self.recovery_log, "a") as f:
                f.write(json.dumps(recovery_point) + "\n")
                
    def get_transactions_since(self, timestamp: float):
        """
        Get all transactions since a specific timestamp
        """
        transactions = []
        if os.path.exists(self.transaction_log):
            with open(self.transaction_log, "r") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        if entry["timestamp"] >= timestamp:
                            transactions.append(entry)
                    except json.JSONDecodeError:
                        continue
        return transactions
        
    def clear_logs(self):
        """
        Clear all logs (used for testing)
        """
        with self._lock:
            if os.path.exists(self.transaction_log):
                os.remove(self.transaction_log)
            if os.path.exists(self.recovery_log):
                os.remove(self.recovery_log) 