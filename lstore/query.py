from lstore.table import Table, Record
from lstore.index import Index

class Query:
    """
    Represents a query object for interacting with a database table.
    """
    def __init__(self, table):
        self.table = table
        self.caller = "insert"

    def version(self, caller):
        """
        Update the caller and create a version copy of the table if necessary
        """
        if self.caller != caller:
            self.caller = caller
            self.table.make_ver_copy()

    def delete(self, primary_key):
        """
        Delete a record from the table
        """
        self.version("delete")
        self.table.delete(primary_key)
        return True

    def insert(self, *columns):
        """
        Insert a new record into the table
        """
        self.version("insert")
        self.table.write(columns)
        return True
    
    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Select records from the table
        """
        records = self.table.read_records(search_key_index, search_key, projected_columns_index)
        return records

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Select records from a specific version of the table
        """
        if self.table.versions and relative_version !=0:

            # Set the table to history mode
            self.table.is_history = True

            # Copy the current page directory to a variable
            current = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.page_directory]

            # Set the page directory to the version specified by relative_version
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.versions[(len(self.table.versions)+relative_version)]]
            
            # Select the records from the version
            records = self.select(search_key, search_key_index, projected_columns_index)

            # Set the page directory back to the original
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in current]

            # Set the table to not in history mode
            self.table.is_history = False
        else:
            # If no version is specified, select the records from the current version
            records = self.table.read_records(search_key_index, search_key, projected_columns_index)

        return records

    def update(self, primary_key, *columns):
        """
        Update a record in the table
        """
        # Update the caller and create a version copy of the table if necessary
        self.version("update")

        # Convert columns to a list
        columns = list(columns)

        # If the primary key is not provided, use the primary key from the columns
        if not columns[self.table.key_col]:
            columns[self.table.key_col] = primary_key

        # Check if the primary key exists in the page directory
        if (not primary_key in self.table.page_directory[self.table.key_col]) or (not columns[self.table.key_col] == primary_key):
            return False
        
        # Update the table with the new columns
        self.table.update(columns)
        return True
        
    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Sum the values of a column over a range of records
        """
        # Initialize the sum variable
        total = 0
        
        # Adjust column index to account for metadata columns
        actual_col = aggregate_column_index + self.table.metadata_columns if aggregate_column_index != self.table.key_col else aggregate_column_index + self.table.metadata_columns
        
        # Iterate over the range of records
        for rid in range(start_range, end_range + 1):
            # Check if the record exists
            if rid in self.table.page_directory[actual_col]:
                # Read the value using read_value which properly handles version chains
                value = self.table.read_value(actual_col, rid)
                if value is not None:
                    total += value
        
        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        Sum the values of a column over a range of records from a specific version of the table
        """
        # Sum the values of the aggregate column over the range of records
        sum = self.sum(start_range,end_range,aggregate_column_index)

        # If there are versions and the relative version is not 0, sum the values from the specified version
        if self.table.versions and relative_version!=0:
            # Copy the current page directory to a variable
            current = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.page_directory]

            # Set the table to history mode
            self.table.is_history = True

            # Set the page directory to the version specified by relative_version
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in self.table.versions[(len(self.table.versions)+relative_version)]]

            # Sum the values of the aggregate column over the range of records from the specified version
            sum = self.sum(start_range, end_range, aggregate_column_index)

            # Set the page directory back to the original
            self.table.page_directory = [{k:[v[0], v[1]] for k,v in col.items()} for col in current]

            # Set the table to not in history mode
            self.table.is_history = False

        # Return the sum of the values
        return sum

    def increment(self, key, column):
        """
        Increment the value of a column for a specific record
        """     
        # Select the record with the specified key
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]

        # If the record is found, increment the value of the specified column
        if r is not False:
            # Create a list of None values with the same length as the number of columns
            updated_columns = [None] * self.table.num_columns

            # Set the value of the specified column to the current value plus 1
            updated_columns[column] = r[column] + 1

            # Update the record with the new value
            u = self.update(key, *updated_columns)

            # Return the result of the update operation
            return u 
        
        # Return False if the record is not found
        return False
