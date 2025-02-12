from lstore.index import Index
from lstore.page import PageRange
from time import time
import lstore.config as config

class Record:
    """
    Record class to store a single record's data and metadata
    """
    def __init__(self, indirection, rid, timestamp, schema_encoding, key, columns):
        self.indirection = indirection  # Points to the most recent version (tail record) of this record
        self.rid = rid                  # Unique identifier for this record
        self.timestamp = timestamp      # Time when this record was created/updated
        self.schema_encoding = schema_encoding  # Bitmap showing which columns have been updated
        self.key = key                  # Primary key value
        self.columns = columns          # Actual data values

    def __getitem__(self, column):
        """
        Allows array-like access to record columns
        """
        return self.columns[column]

    def __str__(self):
        """
        String representation of the record for debugging
        """
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

class Table:
    """
    Table class that manages all records and pages
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges = [PageRange(num_columns)]  # List of page ranges for this table
        self.page_ranges_index = 0                   # Current page range index
        self.page_directory = {}                     # Maps RID to record location
        self.index = Index(self)                     # Index for faster record lookup
        self.rid = 1                                 # Next available RID

    def new_rid(self):
        """
        Generates and returns a new unique RID
        """
        self.rid += 1
        return self.rid - 1

    def add_new_page_range(self):
        """
        Creates a new page range when current one is full
        """
        if self.page_ranges[-1].has_base_page_capacity(): return;
    
        self.page_ranges.append(PageRange(self.num_columns))
        self.page_ranges_index += 1

    def __merge(self):
        """
        Merges tail records into base records for better read performance
        TO BE IMPLEMENTED IN MILESTONE 2
        """
        print("merge is happening")
        pass