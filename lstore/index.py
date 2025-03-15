from BTrees._OOBTree import OOBTree
from threading import RLock

class Index:
    """
    Thread-safe index implementation using BTrees
    """
    def __init__(self, table):
        """
        Initialize the index with BTrees instead of dictionaries
        """
        self.table = table
        # Initialize indices for both metadata and data columns
        self.indices = [None] * table.total_columns
        self._lock = RLock()  # For thread-safe index operations
        
    def __getstate__(self):
        """
        Called when pickling - returns state to be pickled
        Remove unpicklable objects (locks)
        """
        state = self.__dict__.copy()
        state['_lock'] = None
        return state
        
    def __setstate__(self, state):
        """
        Called when unpickling - restores state
        Reinitialize unpicklable objects
        """
        self.__dict__.update(state)
        self._lock = RLock()
        
    def _initialize_after_load(self):
        """
        Reinitialize necessary objects after loading from disk
        """
        if not hasattr(self, '_lock') or self._lock is None: self._lock = RLock()

    def get_value_in_col_by_rid(self, column_number: int, rid: int) -> int:
        """
        Get value using RID from BTree. Thread-safe.
        """
        with self._lock:
            if column_number < len(self.indices) and self.indices[column_number] is not None and rid in self.indices[column_number]:
                return self.indices[column_number][rid]
            return None

    def get_rid_in_col_by_value(self, column_number: int, value: int) -> list:
        """
        Get RIDs by value from BTree. Thread-safe.
        BTrees maintain sorted order, but we still need to scan for matching values
        """

        # Lock the index
        with self._lock:

            # If index doesn't exist, return empty list
            if column_number >= len(self.indices) or self.indices[column_number] is None: return []

            # Return RIDs
            return [k for k, v in self.indices[column_number].items() if v == value]

    def create_index(self, column_number: int) -> True:
        """
        Create a new BTree index for the column. Thread-safe.
        """

        # Lock the index
        with self._lock:

            # Extend indices list if needed
            if column_number >= len(self.indices): self.indices.extend([None] * (column_number - len(self.indices) + 1))

            # If index doesn't exist, create it
            if self.indices[column_number] is None:
                self.indices[column_number] = OOBTree()
                self.restart_index_by_col(column_number)

            # Return True
            return True
    
    def drop_index(self, column_number: int) -> True:
        """
        Drop the BTree index for the column. Thread-safe.
        """

        # Lock the index
        with self._lock:

            # If index exists, drop it
            if column_number < len(self.indices): self.indices[column_number] = None

            # Return True
            return True

    def add_or_move_record_by_col(self, column_number: int, rid: int, value: int):
        """
        Add or update a record in the BTree index. Thread-safe.
        """

        # Lock the index
        with self._lock:

            # Extend indices list if needed
            if column_number >= len(self.indices): self.indices.extend([None] * (column_number - len(self.indices) + 1))

            # If index doesn't exist, create it
            if self.indices[column_number] is None: self.create_index(column_number)

            # Add or update record
            self.indices[column_number][rid] = value

    def delete_record(self, column_number: int, rid: int) -> bool:
        """
        Delete a record from the BTree index. Thread-safe.
        """

        # Lock the index
        with self._lock:

            # If index doesn't exist, return False
            if column_number >= len(self.indices) or self.indices[column_number] is None or rid not in self.indices[column_number]: return False

            # Delete record
            del self.indices[column_number][rid]

            # Return True
            return True
            
    def restart_index(self):
        """
        Rebuild all BTree indices. Thread-safe.
        """     

        # Lock the index
        with self._lock:

            # Initialize indices for total columns (metadata + data)
            self.indices = [None] * self.table.total_columns

            # Initialize indices
            for i in range(self.table.total_columns):

                # If index exists, initialize it
                if len(self.table.page_directory[i]) > 0:
                    self.indices[i] = OOBTree()

                    # Add records to index
                    for k, v in self.table.page_directory[i].items(): self.indices[i][k] = self.table.read_page(v[0], v[1])
            
    def restart_index_by_col(self, col):
        """
        Rebuild BTree index for a specific column. Thread-safe.
        """

        # Lock the index
        with self._lock:

            # Extend indices list if needed
            if col >= len(self.indices): self.indices.extend([None] * (col - len(self.indices) + 1))

            # If index doesn't exist, create it
            if self.indices[col] is None: self.indices[col] = OOBTree()

            # Add records to index
            for k, v in self.table.page_directory[col].items(): self.indices[col][k] = self.table.read_page(v[0], v[1])