from lstore.index import Index
from lstore.page import Page, PageRange
from lstore.config import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}  # RID -> (page_range_index, base/tail, page_index, offset)
        self.index = Index(self)
        self.page_ranges = []
        self.next_rid = 0
        
        # Create first page range
        self.create_new_page_range()
    
    def create_new_page_range(self):
        self.page_ranges.append(PageRange(self.num_columns))
        return len(self.page_ranges) - 1
    
    def insert_record(self, key, columns):
        # Find page range with capacity
        page_range_index = -1
        for i, page_range in enumerate(self.page_ranges):
            if page_range.has_capacity():
                page_range_index = i
                break
        
        if page_range_index == -1:
            page_range_index = self.create_new_page_range()
        
        page_range = self.page_ranges[page_range_index]
        
        # Calculate where to insert the record
        base_page_index = page_range.num_records // RECORDS_PER_PAGE
        offset = page_range.num_records % RECORDS_PER_PAGE
        
        # Assign RID and create record
        rid = self.next_rid
        self.next_rid += 1
        
        # Store location in page directory
        self.page_directory[rid] = (page_range_index, 'base', base_page_index, offset)
        
        # Write metadata
        base_pages = page_range.base_pages[base_page_index]
        base_pages[INDIRECTION_COLUMN].write(offset, rid)  # Initially points to itself
        base_pages[RID_COLUMN].write(offset, rid)
        base_pages[TIMESTAMP_COLUMN].write(offset, int(time()))
        base_pages[SCHEMA_ENCODING_COLUMN].write(offset, 0)  # No updates yet
        
        # Write user data
        for i, value in enumerate(columns):
            base_pages[i + 4].write(offset, value)  # +4 for metadata columns
        
        page_range.num_records += 1
        
        # Update index
        if self.index.indices[self.key] is None:
            self.index.create_index(self.key)
        if columns[self.key] not in self.index.indices[self.key]:
            self.index.indices[self.key][columns[self.key]] = []
        self.index.indices[self.key][columns[self.key]].append(rid)
        
        return rid
    
    def get_record(self, rid):
        if rid not in self.page_directory:
            return None
            
        page_range_idx, page_type, page_idx, offset = self.page_directory[rid]
        if page_type != 'base':
            return None
            
        page_range = self.page_ranges[page_range_idx]
        base_pages = page_range.base_pages[page_idx]
        
        # Read values directly from base pages
        values = []
        for i in range(4, 4 + self.num_columns):  # Skip metadata columns
            values.append(base_pages[i].read(offset))
        return Record(rid, values[self.key], values)
    
    def update_record(self, key, rid, columns):
        if rid not in self.page_directory:
            return False
            
        # Get the base record location
        page_range_idx, page_type, page_idx, offset = self.page_directory[rid]
        if page_type != 'base':
            return False
            
        page_range = self.page_ranges[page_range_idx]
        base_pages = page_range.base_pages[page_idx]
        
        # Get current values for updating index if needed
        current_record = self.get_record(rid)
        if not current_record:
            return False
            
        # Update schema encoding
        old_schema = base_pages[SCHEMA_ENCODING_COLUMN].read(offset)
        new_schema = old_schema
        for i, value in enumerate(columns):
            if value is not None:
                new_schema |= (1 << i)
        base_pages[SCHEMA_ENCODING_COLUMN].write(offset, new_schema)
        
        # Write updated values directly to base pages
        for i, value in enumerate(columns):
            if value is not None:
                base_pages[i + 4].write(offset, value)  # +4 for metadata columns
                # Update index if this is the key column
                if i == self.key:
                    self.index.update_index(self.key, current_record.columns[self.key], value, rid)
        
        return True
    
    def delete_record(self, rid):
        if rid not in self.page_directory:
            return False
            
        # Get record to find key value
        record = self.get_record(rid)
        if record:
            # Remove from index
            if self.index.indices[self.key] is not None:
                key_value = record.columns[self.key]
                if key_value in self.index.indices[self.key]:
                    self.index.indices[self.key][key_value].remove(rid)
                    if not self.index.indices[self.key][key_value]:
                        del self.index.indices[self.key][key_value]
        
        # Mark record as deleted by setting RID to invalid value
        page_range_idx, page_type, page_idx, offset = self.page_directory[rid]
        page_range = self.page_ranges[page_range_idx]
        
        if page_type == 'base':
            pages = page_range.base_pages[page_idx]
        else:
            pages = page_range.tail_pages[page_idx]
            
        pages[RID_COLUMN].write(-1)  # Use -1 to indicate deleted record
        del self.page_directory[rid]
        return True

    def __merge(self):
        """
        To be implemented in milestone 2
        This will merge tail records into base records
        """
        print("merge is happening")
        pass
