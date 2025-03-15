from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager, LockType
from lstore.logger import Logger
from typing import Dict, List, Tuple, Any

class Transaction:
    """
    Handles a single transaction with support for concurrent execution.
    """
    _lock_manager = LockManager()  # Class-level lock manager shared by all transactions
    _logger = Logger()  # Class-level logger shared by all transactions
    
    def __init__(self):
        self.queries = []
        self.transaction_id = id(self)  # Use object id as transaction id
        self.acquired_locks = set()  # Track acquired locks for rollback
        self.modified_records = []  # Track modifications for rollback
        self.original_values = {}  # Store original values for rollback: (table_name, rid) -> {col: value}

    def add_query(self, query, table, *args):
        """
        Adds the given query to this transaction
        """
        self.queries.append((query, table, args))

    def _save_original_state(self, table: Table, key: int, cols: List[int]):
        """
        Saves the original state of records before modification
        """

        # If the columns are empty, return
        if not cols: return
            
        # Get current record state
        record = table.read_records(table.key_col, key, [1] * table.num_columns)

        # If the record is empty, return
        if not record: return
            
        # Get the first record since the key is the primary key
        record = record[0]

        # Get the state key
        state_key = (table.name, key)
        
        # If the state key is not in the original values, create a new dictionary
        if state_key not in self.original_values: self.original_values[state_key] = {}
            
        # Iterate through the columns
        for i, col in enumerate(cols):

            # If the column is not None, save the original value
            if col is not None: self.original_values[state_key][i] = record.columns[i]


    def run(self):
        """
        Runs all queries in this transaction and commits/aborts
        Returns True if the transaction committed successfully, and False otherwise
        """
        try:

            # Iterate through the queries
            for query, table, args in self.queries:

                # Log the operation before execution
                self._logger.log_transaction(
                    self.transaction_id,
                    query.__name__,
                    table.name,
                    args[0],  # key
                    args[1:] if len(args) > 1 else None,  # columns
                    None  # values will be logged after successful execution
                )

                # Determine lock type based on operation
                if query.__name__ in ['select', 'sum']: lock_type = LockType.SHARED
                else: lock_type = LockType.EXCLUSIVE

                # For operations that need a lock
                if query.__name__ in ['select', 'update', 'delete', 'sum']:

                    # Get the key
                    key = args[0]

                    # If the lock is not acquired, abort
                    if not self._lock_manager.acquire_lock(table.name, key, self.transaction_id, lock_type): 
                        return self.abort()

                    # Add the lock to the acquired locks
                    self.acquired_locks.add((table.name, key))

                # Save original state before modification
                if query.__name__ == 'update': self._save_original_state(table, args[0], args[1:])
                elif query.__name__ == 'delete': self._save_original_state(table, args[0], list(range(table.num_columns)))

                # Execute the query
                result = query(*args)
                
                # If query failed, abort
                if result == False: return self.abort()

                # Track modifications for potential rollback
                if query.__name__ in ['update', 'delete']:

                    # Add the modified record to the modified records
                    self.modified_records.append((query, table, args))
                    
                    # Create a version snapshot after modification
                    if hasattr(table, 'make_ver_copy'): table.make_ver_copy()

            # Commit the transaction
            return self.commit()
        
        # If an exception is raised, abort  
        except Exception as e:
            print(f"Transaction {self.transaction_id} failed: {e}")
            return self.abort()


    def abort(self):
        """
        Aborts the transaction, releasing all acquired locks and rolling back changes
        """
        try:

            # Log the abort
            self._logger.log_transaction(
                self.transaction_id,
                "abort",
                None,
                None
            )
            
            # Rollback modifications in reverse order
            for query, table, args in reversed(self.modified_records):

                # Get the key
                key = args[0]

                # Get the state key
                state_key = (table.name, key)
                
                # If the state key is in the original values
                if state_key in self.original_values:

                    # If the query is an update, restore the original values
                    if query.__name__ == 'update':

                        # Restore the original values
                        columns = [None] * table.num_columns
                        for col, value in self.original_values[state_key].items():  columns[col] = value

                        # Update the table
                        table.update(columns)

                    # If the query is a delete, unmark as deleted by restoring original record
                    elif query.__name__ == 'delete':

                        # Restore the original values
                        columns = []
                        for i in range(table.num_columns): columns.append(self.original_values[state_key].get(i))

                        # Update the table
                        table.write(columns)
                        
                    # Log the rollback
                    self._logger.log_transaction(
                        self.transaction_id,
                        "rollback",
                        table.name,
                        key,
                        list(self.original_values[state_key].keys()),
                        list(self.original_values[state_key].values())
                    )
        
        finally:
            # Release all locks
            self._lock_manager.release_all_locks(self.transaction_id)
            self.acquired_locks.clear()
            self.modified_records.clear()
            self.original_values.clear()

        # Return False
        return False

    def commit(self):
        """
        Commits the transaction, making all changes permanent
        """
        # Log the commit
        self._logger.log_transaction(
            self.transaction_id,
            "commit",
            None,
            None
        )
        
        # Create recovery point
        self._logger.log_recovery_point()
        
        # Release all locks
        self._lock_manager.release_all_locks(self.transaction_id)

        # Clear the acquired locks, modified records, and original values
        self.acquired_locks.clear()
        self.modified_records.clear()
        self.original_values.clear()

        # Return True   
        return True

