from lstore.index import Index
from lstore.page import Page, PageRange
from lstore.config import *
from time import time
from config import *

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
        base_pages[INDIRECTION_COLUMN].write(rid)  # Initially points to itself
        base_pages[RID_COLUMN].write(rid)
        base_pages[TIMESTAMP_COLUMN].write(int(time()))
        base_pages[SCHEMA_ENCODING_COLUMN].write(0)  # No updates yet
        
        # Write user data
        for i, value in enumerate(columns):
            base_pages[i + 4].write(value)  # +4 for metadata columns
        
        page_range.num_records += 1
        
        # Update index
        if self.index.indices[self.key] is None:
            self.index.create_index(self.key)
        if columns[self.key] not in self.index.indices[self.key]:
            self.index.indices[self.key][columns[self.key]] = []
        self.index.indices[self.key][columns[self.key]].append(rid)
        
        return rid
    
    def get_record_version(self, rid, version):
        """
        Gets a specific version of a record:
        version 0: latest version
        version -1: next latest version
        version -2: next next latest version
        etc. until we reach base record
        If requested version is older than available, returns the base record.
        """
        if rid not in self.page_directory:
            return None
            
        page_range_idx, page_type, page_idx, offset = self.page_directory[rid]
        page_range = self.page_ranges[page_range_idx]
        
        # Start with base record
        base_pages = page_range.base_pages[page_idx]
        current_rid = rid
        indirection_chain = []
        
        # First get the base record
        base_indirection = base_pages[INDIRECTION_COLUMN].read(offset)
        schema_encoding = base_pages[SCHEMA_ENCODING_COLUMN].read(offset)
        
        # If schema encoding is 0, this record has no updates
        if schema_encoding == 0:
            # Only base record exists
            indirection_chain.append((base_pages, offset))
        else:
            # Record has updates, start with most recent tail record
            current_rid = base_indirection  # Most recent tail record
            
            # Add tail records to chain from most recent to oldest
            while current_rid != rid:  # Stop when we reach base record
                if current_rid not in self.page_directory:
                    break
                    
                page_range_idx, page_type, page_idx, offset = self.page_directory[current_rid]
                current_pages = page_range.tail_pages[page_idx]
                
                # Add to chain
                indirection_chain.append((current_pages, offset))
                
                # Move to previous version
                current_rid = current_pages[INDIRECTION_COLUMN].read(offset)
                
            # Add base record at the end
            indirection_chain.append((base_pages, offset))
        
        # Handle version selection
        # Version 0 is latest, -1 is next latest, etc.
        version_index = abs(version)
        
        # If requested version is older than available, return base record
        if version_index >= len(indirection_chain):
            version_index = len(indirection_chain) - 1  # Return base record
            
        # Get the requested version
        pages, offset = indirection_chain[version_index]
        
        # Read values
        values = []
        for i in range(4, 4 + self.num_columns):  # Skip metadata columns
            values.append(pages[i].read(offset))
            
        return Record(rid, values[self.key], values)

    def get_record(self, rid):
        """
        Gets the most recent version of a record
        """
        # Get version 0 (latest version)
        return self.get_record_version(rid, 0)
    
    def update_record(self, key, rid, columns):
        if rid not in self.page_directory:
            return False
            
        # Get the base record location
        base_range_idx, _, base_page_idx, base_offset = self.page_directory[rid]
        base_range = self.page_ranges[base_range_idx]
        base_pages = base_range.base_pages[base_page_idx]
        
        # Get current values
        current_record = self.get_record(rid)
        if not current_record:
            return False
            
        # Create new tail record
        current_tail_pages = base_range.tail_pages[-1]
        if not current_tail_pages[0].has_capacity():
            base_range.create_new_tail_page()
            current_tail_pages = base_range.tail_pages[-1]
        
        tail_offset = current_tail_pages[0].num_records
        tail_rid = self.next_rid
        self.next_rid += 1
        
        # Update page directory
        self.page_directory[tail_rid] = (base_range_idx, 'tail', len(base_range.tail_pages) - 1, tail_offset)
        
        # Write tail record metadata
        current_tail_pages[INDIRECTION_COLUMN].write(rid)  # Point to base record
        current_tail_pages[RID_COLUMN].write(tail_rid)
        current_tail_pages[TIMESTAMP_COLUMN].write(int(time()))
        
        # Calculate new schema encoding
        old_schema = base_pages[SCHEMA_ENCODING_COLUMN].read(base_offset)
        new_schema = old_schema
        for i, value in enumerate(columns):
            if value is not None:
                new_schema |= (1 << i)
        current_tail_pages[SCHEMA_ENCODING_COLUMN].write(new_schema)
        
        # Write user data and handle index updates
        new_values = list(current_record.columns)  # Start with current values
        for i, value in enumerate(columns):
            if value is not None:
                new_values[i] = value  # Update changed values
                
        # Write all values to tail record
        for i, value in enumerate(new_values):
            current_tail_pages[i + 4].write(value)
            
        # Update index if key changed
        if columns[self.key] is not None:
            self.index.update_index(self.key, current_record.columns[self.key], columns[self.key], rid)
        
        # Update base record to point to this new tail record
        base_pages[INDIRECTION_COLUMN].write(tail_rid)
        base_pages[SCHEMA_ENCODING_COLUMN].write(new_schema)

        base_range.tail_pages[-1] = current_tail_pages
        
        # Increment number of records in the page range
        base_range.num_records += 1
        
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
