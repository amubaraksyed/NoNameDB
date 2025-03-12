from threading import Thread
from typing import List
from lstore.table import Table, Record
from lstore.index import Index
from lstore.transaction import Transaction

class TransactionWorker:
    """
    Handles concurrent execution of transactions in a separate thread.
    """
    def __init__(self, transactions: List[Transaction] = None):
        self.stats = []
        self.transactions = transactions or []
        self.result = 0
        self._thread = None

    def add_transaction(self, t: Transaction):
        """
        Appends transaction to the list of transactions to execute
        """
        self.transactions.append(t)

    def run(self):
        """
        Runs all transactions as a thread
        """
        self._thread = Thread(target=self.__run)
        self._thread.start()

    def join(self):
        """
        Waits for the worker to finish
        """
        if self._thread:
            self._thread.join()

    def __run(self):
        """
        Execute all transactions, retrying aborted ones.
        """
        for transaction in self.transactions:
            success = False
            max_retries = 3  # Limit retries to prevent infinite loops
            retries = 0
            
            while not success and retries < max_retries:
                success = transaction.run()
                if not success:
                    retries += 1
                    # Could add exponential backoff here
            
            self.stats.append(success)

        # Calculate number of successful transactions
        self.result = len([s for s in self.stats if s])

