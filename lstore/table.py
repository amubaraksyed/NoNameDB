from lstore.index import Index
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
        self.indirection = 0  # Points to the latest version (tail record) if updated
        self.timestamp = int(time())  # Creation/last update timestamp
        self.schema_encoding = '0' * len(columns)  # Bit string for tracking updated columns

    def get_indirection(self):
        return self.indirection

    def set_indirection(self, rid):
        self.indirection = rid

    def get_schema_encoding(self):
        return self.schema_encoding

    def set_schema_encoding(self, encoding):
        self.schema_encoding = encoding

    def get_timestamp(self):
        return self.timestamp

    def update_timestamp(self):
        self.timestamp = int(time())

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
        self.page_directory = {}  # Maps RID to (page_range_index, base/tail_page_index, offset)
        self.index = Index(self)
        
        # Metadata columns + data columns
        self.total_columns = num_columns + 4  # RID, Indirection, Timestamp, Schema Encoding
        
        # Page ranges - each containing base pages and tail pages
        self.page_ranges = []  # List of page ranges
        self.current_page_range_index = 0
        
        # Initialize first page range
        self.create_new_page_range()
        
        # Record tracking
        self.num_records = 0
        self.next_rid = 0

    def create_new_page_range(self):
        """Creates a new page range with initial base and tail pages"""
        page_range = {
            'base_pages': [[] for _ in range(self.total_columns)],  # One list per column
            'tail_pages': [[] for _ in range(self.total_columns)],  # One list per column
            'num_base_records': 0,
            'num_tail_records': 0
        }
        self.page_ranges.append(page_range)
        return len(self.page_ranges) - 1

    def get_page_range(self, rid):
        """Get the page range containing the record with given RID"""
        if rid in self.page_directory:
            page_range_index = self.page_directory[rid][0]
            return self.page_ranges[page_range_index]
        return None

    def __merge(self):
        """Merge tail records into base records for the current page range"""
        if not self.page_ranges:
            return
            
        current_range = self.page_ranges[self.current_page_range_index]
        # TODO: Implement merge logic in milestone 2
        print("merge is happening")
        pass

    def insert_record(self, *columns):
        """Insert a new base record into the table"""
        if len(columns) != self.num_columns:
            return False

        rid = self.next_rid
        self.next_rid += 1
        
        # Get current page range
        if not self.page_ranges or self.page_ranges[self.current_page_range_index]['num_base_records'] >= 16384:  # 16K records per range
            self.current_page_range_index = self.create_new_page_range()
            
        current_range = self.page_ranges[self.current_page_range_index]
        offset = current_range['num_base_records']
        
        # Insert metadata
        current_range['base_pages'][INDIRECTION_COLUMN].append(0)  # No updates yet
        current_range['base_pages'][RID_COLUMN].append(rid)
        current_range['base_pages'][TIMESTAMP_COLUMN].append(int(time()))
        current_range['base_pages'][SCHEMA_ENCODING_COLUMN].append('0' * self.num_columns)
        
        # Insert actual column values
        for i, value in enumerate(columns):
            current_range['base_pages'][i + 4].append(value)
            
        # Update page directory and record count
        self.page_directory[rid] = (self.current_page_range_index, 'base', offset)
        current_range['num_base_records'] += 1
        self.num_records += 1
        
        # Update index
        self.index.indices[self.key] = self.index.indices[self.key] or {}
        key_value = columns[self.key]
        if key_value not in self.index.indices[self.key]:
            self.index.indices[self.key][key_value] = []
        self.index.indices[self.key][key_value].append(rid)
        
        return True

    def update_record(self, key, *columns):
        """Update a record, creating a new tail record"""
        # Find the base record
        base_rid = None
        if self.index.indices[self.key] and key in self.index.indices[self.key]:
            base_rid = self.index.indices[self.key][key][0]
        
        if base_rid is None or base_rid not in self.page_directory:
            return False
            
        # Get the base record location
        page_range_index, _, base_offset = self.page_directory[base_rid]
        page_range = self.page_ranges[page_range_index]
        
        # Create new tail record
        tail_rid = self.next_rid
        self.next_rid += 1
        tail_offset = page_range['num_tail_records']
        
        # Get current values
        current_values = []
        for i in range(self.num_columns):
            current_values.append(page_range['base_pages'][i + 4][base_offset])
            
        # Update values with new ones where provided
        for i, value in enumerate(columns):
            if value is not None:
                current_values[i] = value
                
        # Insert tail record
        page_range['tail_pages'][INDIRECTION_COLUMN].append(page_range['base_pages'][INDIRECTION_COLUMN][base_offset])
        page_range['tail_pages'][RID_COLUMN].append(tail_rid)
        page_range['tail_pages'][TIMESTAMP_COLUMN].append(int(time()))
        
        # Update schema encoding
        schema_encoding = list(page_range['base_pages'][SCHEMA_ENCODING_COLUMN][base_offset])
        for i, value in enumerate(columns):
            if value is not None:
                schema_encoding[i] = '1'
        page_range['tail_pages'][SCHEMA_ENCODING_COLUMN].append(''.join(schema_encoding))
        
        # Insert updated values
        for value in current_values:
            page_range['tail_pages'][4:].append(value)
            
        # Update base record indirection
        page_range['base_pages'][INDIRECTION_COLUMN][base_offset] = tail_rid
        
        # Update page directory
        self.page_directory[tail_rid] = (page_range_index, 'tail', tail_offset)
        page_range['num_tail_records'] += 1
        
        return True

    def delete_record(self, key):
        """Delete a record (mark as deleted)"""
        if self.index.indices[self.key] and key in self.index.indices[self.key]:
            rid = self.index.indices[self.key][key][0]
            if rid in self.page_directory:
                page_range_index, record_type, offset = self.page_directory[rid]
                page_range = self.page_ranges[page_range_index]
                
                # Mark as deleted by setting RID to special value
                if record_type == 'base':
                    page_range['base_pages'][RID_COLUMN][offset] = -1
                else:
                    page_range['tail_pages'][RID_COLUMN][offset] = -1
                    
                # Remove from index
                self.index.indices[self.key][key].remove(rid)
                if not self.index.indices[self.key][key]:
                    del self.index.indices[self.key][key]
                    
                # Remove from page directory
                del self.page_directory[rid]
                self.num_records -= 1
                return True
        return False

    def get_record(self, rid):
        """Retrieve a record by RID"""
        if rid not in self.page_directory:
            return None
            
        page_range_index, record_type, offset = self.page_directory[rid]
        page_range = self.page_ranges[page_range_index]
        
        # Get the record data
        record_data = []
        pages = page_range['base_pages'] if record_type == 'base' else page_range['tail_pages']
        
        for i in range(self.total_columns):
            record_data.append(pages[i][offset])
            
        return Record(record_data[RID_COLUMN], 
                     record_data[self.key + 4],  # Add 4 to skip metadata columns
                     record_data[4:])  # Skip metadata columns
 
