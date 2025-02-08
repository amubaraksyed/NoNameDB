"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.create_index(table.key_index)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column] is None:
            raise ValueError(f"No index for column {column}.")
        return self.indices[column].get(value, [])    

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            raise ValueError(f"No index exists for column {column}.")
        result = []
        for key, rids in self.indices[column].items():
            if begin <= key <= end:
                result.extend(rids)
        return result
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is not None:
            raise ValueError(f"Index already exists for column {column_number}.")
        self.indices[column_number] = {}

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if self.indices[column_number] is None:
            raise ValueError(f"No index exists for column {column_number}.")
        self.indices[column_number] = None