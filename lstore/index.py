"""
A data structure holding indices for various columns of a table. 
The key column is indexed by default, other columns can be indexed through this object.
Indices are implemented using B-Trees for efficient lookups.
"""
from BTrees.OOBTree import OOBTree
import lstore.config as config

class Index:
    """
    Index class that manages B-Tree indices for table columns
    """
    def __init__(self, table):
        """
        Initialize index structure for a table
        :param table: Table     #The table this index belongs to
        """
        # Initialize indices array
        self.indices = [None] * table.num_columns  # One index for each column
        self.key = table.key                       # Primary key column index
        
        # Create primary key index
        self.indices[self.key] = OOBTree()         # Initialize primary key index

    def add(self, record):
        """
        Adds a record's values to all existing indices
        :param record: list     #Record values including metadata
        """
        # Get RID
        rid = record[config.RID_COLUMN]

        for i, column_index in enumerate(self.indices):
            # Check if index exists
            if column_index is not None:
                # Get key value
                key = record[i + config.METADATA_COLUMNS]

                # Store RIDs in a set to handle duplicate values
                if key not in column_index:
                    column_index[key] = set()
                column_index[key].add(rid)

    def delete(self, record):
        """
        Removes a record's values from all existing indices
        :param record: list     #Record values including metadata
        :raises: Exception if key not found in index
        """
        # Get RID
        rid = record[config.RID_COLUMN]

        # Remove from each index
        for i, column_index in enumerate(self.indices):
            # Check if index exists
            if column_index is not None:
                # Get key value
                key = record[i + config.METADATA_COLUMNS]

                # Remove RID from index
                if key in column_index:
                    # Remove RID from set
                    column_index[key].remove(rid)

                    # Clean up empty sets
                    if not column_index[key]:
                        del column_index[key]
                else:
                    raise Exception("Key not found in index")

    def locate(self, column, value):
        """
        Finds all records with the given value in the specified column
        :param column: int     #Index of the column to search
        :param value: int      #Value to search for
        :return: set          #Set of RIDs matching the search criteria, or None if not found
        """
        # Get index for column
        tree = self.indices[column]

        # Check if value exists
        if value not in tree:
            return None

        # Return matching RIDs
        return tree[value]

    def locate_range(self, begin, end, column):
        """
        Finds all records with values in the specified range
        :param begin: int     #Start of the range (inclusive)
        :param end: int       #End of the range (inclusive)
        :param column: int    #Index of the column to search
        :return: set         #Set of RIDs within the range
        """
        # Get index for column
        tree = self.indices[column]
        matching_rids = set()

        # Iterate through values in range
        for key in tree.keys(begin, end):
            matching_rids.update(tree[key])

        return matching_rids if matching_rids else None

    def create_index(self, column_number):
        """
        Creates a new index on the specified column
        :param column_number: int     #Index of the column to create index for
        """
        # Check if index already exists
        if self.indices[column_number] is not None:
            return

        # Create new B-Tree index
        self.indices[column_number] = OOBTree()

    def drop_index(self, column_number):
        """
        Removes the index on the specified column
        :param column_number: int     #Index of the column to drop index for
        """
        # Cannot drop primary key index
        if column_number != self.key:
            self.indices[column_number] = None