from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        pass
        self._thread = threading.Thread(target=self.__run)
        self._thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if hasattr(self, "_thread"):
            self._thread.join()


    def __run(self):
        for transaction in self.transactions:
            self.stats.append(transaction.run())
        self.result = len(list(filter(lambda x: x, self.stats)))

