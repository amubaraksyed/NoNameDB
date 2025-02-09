from lstore.table import Table, Record
from lstore.index import Index
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.backup = {} # Stores original values before updates  
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        if query.__name__ == "update":
            key = args[0]
            existing_record = table.index.locate(table.key, key)
            if existing_record:
                # Take a snapshot of the current record
                self.backup[key] = existing_record[0].columns.copy()
        self.queries.append((query, args))
        # use grades_table for aborting

        
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        for key, original_values in self.backup.items():
            self.queries[0][1].update(key, *original_values)
        print("Transaction aborted.")
        # do roll-back by updating backup value (not test yet)
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        self.backup.clear()
        print("Transaction committed.")
        # TODO: commit to database
        return True

