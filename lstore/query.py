from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rids = self.table.index.locate(primary_key, self.table.key)
        if not rids:
            return False  # No record found

        for rid in rids:
            self.table.invalidate_record(rid)  # Mark record as deleted
            self.table.index.remove(primary_key)  # Remove from index

        return True


    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False  # Ensure correct number of columns

        schema_encoding = '0' * self.table.num_columns  # Initialize schema encoding

        # Assign a new RID (Record ID)
        new_rid = self.table.generate_new_rid()  

        # Insert into columnar storage
        self.table.insert_record(new_rid, columns, schema_encoding)

        # Update index for primary key
        primary_key = columns[self.table.key]  # Assuming self.table.key holds PK index
        self.table.index.insert(primary_key, new_rid)

        return True


    
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
        # Retrieve RIDs from index
        rids = self.table.index.locate(search_key, search_key_index)

        if not rids:
            return False  # No matching record

        results = []
        for rid in rids:
            record = self.table.get_record(rid, projected_columns_index)
            results.append(record)

        return results


    
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
        # Retrieve RID from index
        rid = self.table.index.locate(search_key, search_key_index)
        if not rid:
            return False  # No matching record

        # Follow lineage (indirection pointers) to get the requested version
        version_rid = rid  # Start from base record
        for _ in range(relative_version):
            version_rid = self.table.get_indirection(version_rid)
            if version_rid is None:
                return False  # No older version available

        # Retrieve and return the record at the requested version
        return self.table.get_record(version_rid, projected_columns_index)


    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        rid = self.table.index.locate(primary_key, self.table.key)
        if not rid:
            return False  # Record doesn't exist

        # Fill missing columns with `None`
        columns = list(columns)
        while len(columns) < self.table.num_columns:
            columns.append(None)  # Keep missing columns unchanged

        new_rid = self.table.generate_new_rid()  # Create new tail record RID
        self.table.update_record(rid, new_rid, columns)

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
        rids = self.table.index.range_query(start_range, end_range)

        if not rids:
            return False  # No records in range

        total_sum = sum(self.table.get_column_value(rid, aggregate_column_index) for rid in rids)
        
        return total_sum

    
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
        rids = self.table.index.range_query(start_range, end_range)

        if not rids:
            return False  # No records in range

        total_sum = 0
        for rid in rids:
            version_rid = rid
            for _ in range(relative_version):
                version_rid = self.table.get_indirection(version_rid)
                if version_rid is None:
                    return False  # No older version available

            total_sum += self.table.get_column_value(version_rid, aggregate_column_index)

        return total_sum


    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if not r or r == False:
            return False  # No record found or locked by 2PL
        r = r[0]  # Unpack first result

        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
