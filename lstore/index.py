"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. 
Indices are implemented using Python dictionaries with multiple value lists for duplicate keys.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.table = table
        
        # Create index on key column by default
        self.create_index(table.key)

    def locate(self, column, value):
        """
        Returns the RIDs of all records with the given value on column "column"
        """
        if self.indices[column] is None:
            return []
            
        return self.indices[column].get(value, [])

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        if self.indices[column] is None:
            return []
            
        rids = []
        index = self.indices[column]
        
        # Collect all RIDs for values in range
        for value in range(begin, end + 1):
            if value in index:
                rids.extend(index[value])
                
        return rids

    def create_index(self, column_number):
        """
        Create index on specific column
        """
        # Index already exists
        if self.indices[column_number] is not None:
            return
            
        # Create new index
        index = {}
        self.indices[column_number] = index
        
        # Scan table to build index
        for rid in self.table.page_directory:
            record = self.table.get_record(rid)
            if record:
                value = record.columns[column_number]
                if value not in index:
                    index[value] = []
                index[value].append(rid)

    def drop_index(self, column_number):
        """
        Drop index of specific column
        """
        self.indices[column_number] = None

    def update_index(self, column, old_value, new_value, rid):
        """
        Update index when a value changes
        """
        if self.indices[column] is None:
            return
            
        index = self.indices[column]
        
        # Remove old value
        if old_value in index and rid in index[old_value]:
            index[old_value].remove(rid)
            if not index[old_value]:  # Remove empty list
                del index[old_value]
                
        # Add new value
        if new_value not in index:
            index[new_value] = []
        index[new_value].append(rid)
