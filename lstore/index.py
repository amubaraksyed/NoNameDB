from BTrees._OOBTree import OOBTree

class Index:

    def __init__(self, table):
        """
        Initialize the index with BTrees instead of dictionaries
        """
        self.table = table
        # Initialize indices for both metadata and data columns
        self.indices = [None] * table.total_columns
        
    def get_value_in_col_by_rid(self, column_number: int, rid: int) -> int:
        """
        Get value using RID from BTree
        """
        if column_number < len(self.indices) and self.indices[column_number] is not None and rid in self.indices[column_number]:
            return self.indices[column_number][rid]
        return None

    def get_rid_in_col_by_value(self, column_number: int, value: int) -> list:
        """
        Get RIDs by value from BTree
        BTrees maintain sorted order, but we still need to scan for matching values
        """
        if column_number >= len(self.indices) or self.indices[column_number] is None:
            return []
        return [k for k, v in self.indices[column_number].items() if v == value]

    def create_index(self, column_number: int) -> True:
        """
        Create a new BTree index for the column
        """
        if column_number >= len(self.indices):
            # Extend indices list if needed
            self.indices.extend([None] * (column_number - len(self.indices) + 1))
            
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()
            self.restart_index_by_col(column_number)
        return True
    
    def drop_index(self, column_number: int) -> True:
        """
        Drop the BTree index for the column
        """
        if column_number < len(self.indices):
            self.indices[column_number] = None
        return True

    def add_or_move_record_by_col(self, column_number: int, rid: int, value: int):
        """
        Add or update a record in the BTree index
        """
        if column_number >= len(self.indices):
            # Extend indices list if needed
            self.indices.extend([None] * (column_number - len(self.indices) + 1))
            
        if self.indices[column_number] is None:
            self.create_index(column_number)
        self.indices[column_number][rid] = value

    def delete_record(self, column_number: int, rid: int) -> bool:
        """
        Delete a record from the BTree index
        """
        if column_number >= len(self.indices) or self.indices[column_number] is None or rid not in self.indices[column_number]:
            return False
        del self.indices[column_number][rid]
        return True
            
    def restart_index(self):
        """
        Rebuild all BTree indices
        """
        # Initialize indices for total columns (metadata + data)
        self.indices = [None] * self.table.total_columns
        for i in range(self.table.total_columns):
            if len(self.table.page_directory[i]) > 0:
                self.indices[i] = OOBTree()
                for k, v in self.table.page_directory[i].items():
                    self.indices[i][k] = self.table.read_page(v[0], v[1])
            
    def restart_index_by_col(self, col):
        """
        Rebuild BTree index for a specific column
        """
        if col >= len(self.indices):
            # Extend indices list if needed
            self.indices.extend([None] * (col - len(self.indices) + 1))
            
        self.indices[col] = OOBTree()
        for k, v in self.table.page_directory[col].items():
            self.indices[col][k] = self.table.read_page(v[0], v[1])