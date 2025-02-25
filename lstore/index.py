class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [None] *  table.num_columns
        
    def get_value_in_col_by_rid(self, column_number: int, rid: int) -> int:
        if rid in self.indices[column_number]:
            return self.indices[column_number][rid]
        return None

    def get_rid_in_col_by_value(self, column_number: int, value: int) -> list:
        return [k for k, v in self.indices[column_number].items() if v==value]

    def create_index(self, column_number: int) -> True:
        if self.indices[column_number] is None:
            self.indices[column_number] = dict()
            self.restart_index_by_col(column_number)
        return True
    
    def drop_index(self, column_number: int) -> True:
        self.indices[column_number] = None
        return True

    def add_or_move_record_by_col(self, column_number: int, rid: int, value: int):
        if self.indices[column_number] is None:
            self.create_index(column_number)
        self.indices[column_number][rid] = value

    def delete_record(self, column_number: int, rid: int) -> bool:
        if self.indices[column_number] is None or rid not in self.indices[column_number]:
            return False
        self.indices[column_number].pop(rid)
        return True
            
    def restart_index(self):
        self.indices = [{k: self.table.read_page(v[0], v[1]) for k, v in dir.items()} for dir in self.table.page_directory]
            
    def restart_index_by_col(self, col):
        self.indices[col] = {k: self.table.read_page(v[0], v[1]) for k, v in self.table.page_directory[col].items()}