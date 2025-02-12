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
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        """
        # Delete a record with specified primary key
        # Returns True upon successful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """
        try:
            # Find the record using the index
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
                
            # Delete the first matching record
            return self.table.delete_record(rids[0])
        except:
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        """
        # Insert a record with specified columns
        # Return True upon successful insertion
        # Returns False if insert fails for whatever reason
        """
        try:
            if len(columns) != self.table.num_columns:
                return False
                
            # Insert the record and get its RID
            rid = self.table.insert_record(columns[self.table.key], columns)
            if rid is None:
                return False
                
            # Add to index
            self.table.index.indices[self.table.key][columns[self.table.key]] = [rid]
            return True
        except:
            return False

    
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
        Returns the most recent version of the record
        """
        try:
            # This is equivalent to select_version with version=0 (most recent)
            return self.select_version(search_key, search_key_index, projected_columns_index, 0)
        except Exception as e:
            print(f"Error in select: {e}")
            return []

    
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
        Returns a specific version of the record:
        version 0: most recent version
        version -1: previous version
        version -2: version before that, etc.
        """
        try:
            if len(projected_columns_index) != self.table.num_columns:
                return []
                
            # Find records using index
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []
                
            # Get the records
            records = []
            for rid in rids:
                record = self.table.get_record_version(rid, relative_version)
                if record:
                    # Filter columns based on projection
                    projected_columns = []
                    for i, include in enumerate(projected_columns_index):
                        if include:
                            projected_columns.append(record.columns[i])
                    records.append(Record(record.rid, record.key, projected_columns))
            
            return records
        except Exception as e:
            print(f"Error in select_version: {e}")
            return []

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        """
        # Update a record with specified key and columns
        # Returns True if update is successful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        try:
            # Find the record using the index
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
                
            # Update the first matching record
            return self.table.update_record(primary_key, rids[0], columns)
        except:
            return False

    
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
        Returns sum of the most recent versions
        """
        try:
            return self.sum_version(start_range, end_range, aggregate_column_index, 0)
        except:
            return False

    
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
        Returns sum of specific versions of records
        """
        try:
            # Get all records in range
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
                
            # Sum the specified column
            total = 0
            for rid in rids:
                record = self.table.get_record_version(rid, relative_version)
                if record:
                    total += record.columns[aggregate_column_index]
            
            return total
        except:
            return False

    
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
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
