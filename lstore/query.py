from lstore.table import Table, Record
from lstore.index import Index

class Query:
    def __init__(self, table):
        self.table = table
        self.caller = "insert"

    def version(self, caller):
        if self.caller != caller:
            self.caller = caller
            self.table.make_ver_copy()

    def delete(self, primary_key):
        self.version("delete")
        self.table.delete(primary_key)
        return True

    def insert(self, *columns):
        self.version("insert")
        self.table.write(columns)
        return True
    
    def select(self, search_key, search_key_index, projected_columns_index):
        records = self.table.read_records(search_key_index, search_key, projected_columns_index)
        return records

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        if self.table.versions and relative_version!=0:
            self.table.is_history = True
            current = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.page_directory]
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.versions[(len(self.table.versions)+relative_version)]]
            records = self.select(search_key, search_key_index, projected_columns_index)
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in current]
            self.table.is_history = False
        else:
            records = self.table.read_records(search_key_index, search_key, projected_columns_index)

        return records

    def update(self, primary_key, *columns):
        self.version("update")
        columns = list(columns)
        if not columns[self.table.key_col]:
            columns[self.table.key_col] = primary_key
        if (not primary_key in self.table.page_directory[self.table.key_col]) or (not columns[self.table.key_col] == primary_key):
            return False
        self.table.update(columns)
        return True
        
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        for rid in range(start_range, end_range+1):
            value = self.table.read_value(aggregate_column_index, rid)
            sum += value if value else 0
        return sum

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        sum = self.sum(start_range,end_range,aggregate_column_index)
        if self.table.versions and relative_version!=0:
            current = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.page_directory]
            self.table.is_history = True
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.versions[(len(self.table.versions)+relative_version)]]
            sum = self.sum(start_range, end_range, aggregate_column_index)
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in current]
            self.table.is_history = False
        return sum

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False