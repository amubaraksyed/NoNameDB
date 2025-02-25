class Index:

    def __init__(self, table):
        """
        Initialize the index
        """
        self.table = table
        self.indices = [None] *  table.num_columns
        
    def get_value_in_col_by_rid(self, column_number: int, rid: int) -> int:
        """
        Get the value of a column for a specific record
        """
        if rid in self.indices[column_number]:
            return self.indices[column_number][rid]
        return None

    def get_rid_in_col_by_value(self, column_number: int, value: int) -> list:
        """
        Get the record IDs of records with a specific value in a column
        """
        return [k for k, v in self.indices[column_number].items() if v==value]

    def create_index(self, column_number: int) -> True:
        """
        Create an index for a column
        """
        if self.indices[column_number] is None:
            self.indices[column_number] = dict()
            self.restart_index_by_col(column_number)
        return True
    
    def drop_index(self, column_number: int) -> True:
        """
        Drop an index for a column
        """
        self.indices[column_number] = None
        return True

    def add_or_move_record_by_col(self, column_number: int, rid: int, value: int):
        """
        Add or move a record to an index for a column
        """
        if self.indices[column_number] is None:
            self.create_index(column_number)
        self.indices[column_number][rid] = value

    def delete_record(self, column_number: int, rid: int) -> bool:
        """
        Delete a record from an index for a column
        """
        if self.indices[column_number] is None or rid not in self.indices[column_number]:
            return False
        self.indices[column_number].pop(rid)
        return True
            
    def restart_index(self):
        """
        Restart an index
        """
        self.indices = [{k: self.table.read_page(v[0], v[1]) for k, v in dir.items()} for dir in self.table.page_directory]
            
    def restart_index_by_col(self, col):
        """
        Restart an index for a column
        """
        self.indices[col] = {k: self.table.read_page(v[0], v[1]) for k, v in self.table.page_directory[col].items()}