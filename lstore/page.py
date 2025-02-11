import struct
from lstore.config import *

class Page:
    """
    :param page_size: int     #Size of the page in bytes (default 4096)
    A page stores values as 64-bit integers in a bytearray
    """
    def __init__(self):
        self.num_records = 0
        self.page_size = PAGE_SIZE
        self.data = bytearray(self.page_size)
    
    def has_capacity(self):
        """
        Returns true if the page has capacity to store another value
        """
        # Each value is 8 bytes (64-bit integer)
        return (self.num_records * RECORD_SIZE) < self.page_size
    
    def write(self, offset, value):
        """
        Writes a 64-bit integer value to the page at the given offset
        Returns True if successful, False if offset is invalid
        """
        if offset >= (self.page_size // RECORD_SIZE):
            return False
            
        # Pack the 64-bit integer into 8 bytes
        packed_value = struct.pack('q', value)
        
        # Write the bytes to the page at the specified offset
        start_pos = offset * RECORD_SIZE
        self.data[start_pos:start_pos + RECORD_SIZE] = packed_value
        
        # Update num_records if we're writing beyond current count
        if offset >= self.num_records:
            self.num_records = offset + 1
        return True
    
    def read(self, offset):
        """
        Reads the value at the given offset
        Returns None if offset is invalid
        """
        if offset >= self.num_records:
            return None
            
        # Read 8 bytes starting at offset * 8
        start_pos = offset * RECORD_SIZE
        value_bytes = self.data[start_pos:start_pos + RECORD_SIZE]
        
        # Unpack the bytes into a 64-bit integer
        value = struct.unpack('q', value_bytes)[0]
        return value

class PageRange:
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = []
        self.tail_pages = []
        self.num_records = 0
        
        # Initialize base pages (one per column including metadata columns)
        for _ in range(BASE_PAGES_PER_RANGE):
            column_pages = []
            for _ in range(num_columns + 4):  # +4 for metadata columns
                column_pages.append(Page())
            self.base_pages.append(column_pages)
            
        # Initialize first tail page
        self.create_new_tail_page()
    
    def create_new_tail_page(self):
        column_pages = []
        for _ in range(self.num_columns + 4):  # +4 for metadata columns
            column_pages.append(Page())
        self.tail_pages.append(column_pages)
    
    def has_capacity(self):
        return self.num_records < (BASE_PAGES_PER_RANGE * RECORDS_PER_PAGE)

