from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import SCHEMA_ENCODING_COLUMN

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        """
        Delete record with specified primary key
        Returns True upon successful deletion
        Returns False if record doesn't exist
        """
        # First locate the record
        records = self.select(primary_key, self.table.key, [1] * self.table.num_columns)
        if not records:
            return False

        record = records[0]
        rid = record.rid

        # Remove from index
        if self.table.index.indices[self.table.key]:
            del self.table.index.indices[self.table.key][primary_key]

        # Remove from page directory
        del self.table.page_directory[rid]

        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        """
        Insert a record with specified columns
        """
        # Check if key exists
        key_col = columns[self.table.key]
        if self.table.index.indices[self.table.key] and key_col in self.table.index.indices[self.table.key]:
            return False

        return self.table.insert_record(*columns)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Read matching record with specified search key
        """
        # Check if searching on key column and index exists
        if search_key_index == self.table.key and self.table.index.indices[self.table.key]:
            rid = self.table.index.indices[self.table.key].get(search_key)
            if rid is None:
                return False
        else:
            return False  # For milestone 1, only support search on primary key

        # Get record location
        page_range_idx, offset = self.table.page_directory[rid]
        page_range = self.table.base_pages[page_range_idx]

        # Read all columns
        record_columns = []
        for i in range(self.table.num_columns):
            if projected_columns_index[i] == 1:
                value = page_range[i + 4].read(offset * 8)  # +4 to skip metadata columns
                record_columns.append(value)
            else:
                record_columns.append(None)

        return [Record(rid=rid, key=search_key, columns=record_columns)]

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Not implemented for milestone 1
        """
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        """
        Update a record with specified key and columns
        """
        # First locate the record
        records = self.select(primary_key, self.table.key, [1] * self.table.num_columns)
        if not records:
            return False

        record = records[0]
        rid = record.rid
        page_range_idx, offset = self.table.page_directory[rid]
        page_range = self.table.base_pages[page_range_idx]

        # Update schema encoding
        schema_encoding = 0
        for i, value in enumerate(columns):
            if value is not None:
                schema_encoding |= (1 << i)
                # Update the value
                page_range[i + 4].data[offset * 8:(offset + 1) * 8] = value.to_bytes(8, 'big', signed=True)

        # Update schema encoding column
        page_range[SCHEMA_ENCODING_COLUMN].data[offset * 8:(offset + 1) * 8] = schema_encoding.to_bytes(8, 'big', signed=True)

        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Sum values in a column within key range
        """
        sum_value = 0
        found_records = False

        # Scan through all records in range
        if self.table.index.indices[self.table.key]:
            for key in range(start_range, end_range + 1):
                if key in self.table.index.indices[self.table.key]:
                    records = self.select(key, self.table.key, [1] * self.table.num_columns)
                    if records:
                        found_records = True
                        sum_value += records[0].columns[aggregate_column_index]

        return sum_value if found_records else False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        Not implemented for milestone 1
        """
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
