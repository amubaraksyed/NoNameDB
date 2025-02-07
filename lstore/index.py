"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] * table.num_columns
        self.table = table
        # Initialize index for key column
        self.create_index(table.key)

    def locate(self, column, value):
        """
        Returns the location of all records with the given value on column "column"
        """
        if self.indices[column] is None:
            return []
            
        # Return list of RIDs or empty list if value not found
        return [self.indices[column].get(value, [])]

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        if self.indices[column] is None:
            return []
            
        rids = []
        index = self.indices[column]
        
        # Scan through all values in range
        for value in range(begin, end + 1):
            if value in index:
                rids.append(index[value])
                
        return rids

    def create_index(self, column_number):
        """
        Create index on specific column
        """
        # Don't create index if it already exists
        if self.indices[column_number] is not None:
            return
            
        # Initialize new index
        self.indices[column_number] = {}
        
        # If table has existing records, index them
        for rid, (page_range_idx, offset) in self.table.page_directory.items():
            # Read value from the column
            page_range = self.table.base_pages[page_range_idx]
            value = page_range[column_number + 4].read(offset * 8)  # +4 for metadata columns
            
            # Add to index
            self.indices[column_number][value] = rid

    def drop_index(self, column_number):
        """
        Drop index of specific column
        """
        # Cannot drop index on primary key
        if column_number == self.table.key:
            return
            
        self.indices[column_number] = None
