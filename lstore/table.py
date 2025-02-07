from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.config import INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     # Number of Columns: all columns are integer
    :param key: int             # Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}  # RID -> (page_range, base_page, offset)
        self.index = Index(self)
        
        # Number of base records
        self.num_records = 0
        
        # Create base pages for metadata columns and user columns
        self.num_total_cols = num_columns + 4  # User columns + metadata columns
        self.base_pages = []
        self.create_new_page_range()

    def create_new_page_range(self):
        """
        Creates a new page range with pages for each column
        """
        page_range = []
        for _ in range(self.num_total_cols):
            page_range.append(Page())
        self.base_pages.append(page_range)
        return len(self.base_pages) - 1

    def insert_record(self, *columns):
        """
        Insert a record with the given values
        """
        # Check if we need a new page range
        current_page_range = self.base_pages[-1]
        if not current_page_range[0].has_capacity():
            self.create_new_page_range()
            current_page_range = self.base_pages[-1]

        rid = self.num_records
        self.num_records += 1
        
        # Write metadata
        page_range_index = len(self.base_pages) - 1
        current_page_range[INDIRECTION_COLUMN].write(0)  # No updates yet
        current_page_range[RID_COLUMN].write(rid)
        current_page_range[TIMESTAMP_COLUMN].write(int(time()))
        current_page_range[SCHEMA_ENCODING_COLUMN].write(0)  # No updates yet
        
        # Write user data
        for i, value in enumerate(columns):
            current_page_range[i + 4].write(value)
            
        # Update page directory
        self.page_directory[rid] = (page_range_index, 0)  # Base page offset is 0
        
        # Update index
        self.index.indices[self.key] = self.index.indices[self.key] or {}
        self.index.indices[self.key][columns[self.key]] = rid
        
        return True

    def __merge(self):
        print("merge is happening")
        pass
 
