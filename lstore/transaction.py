from lstore.table import Table, Record
from lstore.index import Index
import threading
import time
from lstore.query import Query
from lstore.lockmanager import LockManager, AbortTransactionException


lock_manager = LockManager()

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.timestamp = time.time()
        self.operations = []
        self.locks_held = set()
        self.aborted = False
        self.undo_log = []
    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        func_name = query.__name__
        key = args[0] if args else None
        if func_name == "insert":
            # when inserting, delete rollback data
            self.undo_log.append(("delete", table, key, None))
        elif func_name == "update":
            # call select_version(-1) to get rollback data
            q = Query(table)
            old_record = q.select_version(key, 0, [1]*table.num_columns, -1)
            if old_record:
                self.undo_log.append(("update", table, key, old_record[0].columns))
        elif func_name == "delete":
            q = Query(table)
            old_record = q.select_version(key, 0, [1]*table.num_columns, -1)
            if old_record:
                self.undo_log.append(("insert", table, key, old_record[0].columns))
        self.operations.append((query, table, args))

    def run(self):
        while True:  # retry loop
            self.aborted = False
            self.locks_held.clear()
            self.undo_log.clear()
            success = True
            # acquire locks and execute each operation
            for (func, table, args) in self.operations:
                resource = (table.name, args[0])
                if func.__name__ in ["insert", "update", "delete"]:
                    mode = 'X'
                else:
                    mode = 'S'
                if resource not in self.locks_held:
                    if not lock_manager.acquire(self, resource, mode):
                        success = False
                        break
                    self.locks_held.add(resource)
                result = func(*args)
                if result is False:
                    success = False
                    break
                    # if operation failed
            if success:
                self.commit()
                self.undo_log.clear()
                break
            else:
                self.abort()
                time.sleep(0.01)
                continue
        return True

    def rollback(self):
        # undo in reverse order of execution
        for op, table, key, old_values in reversed(self.undo_log):
            q = Query(table)
            if op == "delete":
                q.delete(key)
            elif op == "update":
                q.update(key, *old_values)
            elif op == "insert":
                q.insert(*old_values)
        for resource in list(self.locks_held):
            lock_manager.release(self, resource)
        self.locks_held.clear()
        self.undo_log.clear()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        self.rollback()
        self.aborted = True
        return False

    
    def commit(self):
        # TODO: commit to database
        for resource in list(self.locks_held):
            lock_manager.release(self, resource)
        self.locks_held.clear()
        return True

