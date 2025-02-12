import lstore.config as config
from time import time

class Page:
    """
    A page stores table records and their metadata
    """
    def __init__(self):
        self.num_records = 0                    # Number of records currently stored
        self.data = bytearray(config.PAGE_SIZE) # Physical memory allocated for this page

    def has_capacity(self):
        """
        Returns true if the page has capacity to store another record
        """
        return self.num_records < config.RECORDS_PER_PAGE

    def write(self, value):
        """
        Writes an integer value to the page
        """
        if not self.has_capacity():
            return False
        
        # Calculate offset for new record
        offset = self.num_records * 8
        
        # Convert integer to bytes and write to page
        value_bytes = value.to_bytes(8, byteorder='big', signed=True)
        self.data[offset:offset+8] = value_bytes
        self.num_records += 1
        return True

    def read(self, index):
        """
        Reads an integer value from the page at the given index
        """
        if index >= self.num_records:
            return None
            
        # Calculate offset and convert bytes back to integer
        offset = index * 8
        value_bytes = self.data[offset:offset+8]
        return int.from_bytes(value_bytes, byteorder='big', signed=True)

class PageRange:
    """
    A page range contains multiple pages for storing base and tail records
    :param num_columns: int     #Number of columns in the table
    """
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = []                # List of base pages for each column
        self.tail_pages = []                # List of tail pages for each column
        self.num_tail_records = 0           # Number of tail records
        self.num_base_records = 0           # Number of base records
        
        # Initialize pages for each column (including metadata columns)
        for _ in range(num_columns + config.METADATA_COLUMNS):
            self.base_pages.append([Page()])
            self.tail_pages.append([Page()])

    def has_base_page_capacity(self):
        """
        Returns true if the page range can store another base record
        """
        return self.base_pages[0][-1].has_capacity()

    def has_tail_page_capacity(self):
        """
        Returns true if the page range can store another tail record
        """
        return self.tail_pages[0][-1].has_capacity()

    def add_base_page(self):
        """
        Adds a new base page for each column when current ones are full
        """
        for column in self.base_pages:
            column.append(Page())

    def add_tail_page(self):
        """
        Adds a new tail page for each column when current ones are full
        """
        for column in self.tail_pages:
            column.append(Page())

    def write_base_record(self, record):
        """
        Writes a base record across all columns
        :param record: list     #Values for all columns including metadata
        :return: tuple         #Location (page_index, slot) where record was written
        """
        if len(record) != self.num_columns + config.METADATA_COLUMNS:
            raise ValueError(f"Expected {self.num_columns + config.METADATA_COLUMNS} columns, got {len(record)}")
        
        if not self.has_base_page_capacity():
            self.add_base_page()
            
        page_index = len(self.base_pages[0]) - 1
        slot = self.base_pages[0][page_index].num_records
        
        # Write each column value to its respective page
        for i, value in enumerate(record):
            self.base_pages[i][page_index].write(value)
            
        self.num_base_records += 1
        return (page_index, slot)

    def write_tail_record(self, record):
        """
        Writes a tail record across all columns
        :param record: list     #Values for all columns including metadata
        :return: tuple         #Location (page_index, slot) where record was written
        """
        if len(record) != self.num_columns + config.METADATA_COLUMNS:
            raise ValueError(f"Expected {self.num_columns + config.METADATA_COLUMNS} columns, got {len(record)}")
        
        if not self.has_tail_page_capacity():
            self.add_tail_page()
            
        page_index = len(self.tail_pages[0]) - 1
        slot = self.tail_pages[0][page_index].num_records
        
        # Write each column value to its respective page
        for i, value in enumerate(record):
            self.tail_pages[i][page_index].write(value)
            
        self.num_tail_records += 1
        return (page_index, slot)

    def read_base_record(self, page_index, slot, projected_columns_index):
        """
        Reads a base record's values for specified columns
        :param page_index: int     #Index of the page containing the record
        :param slot: int           #Slot number within the page
        :param projected_columns_index: list     #Boolean list indicating which columns to read
        :return: list             #Values for requested columns
        """
        if page_index >= len(self.base_pages[0]) or slot >= self.base_pages[0][page_index].num_records:
            raise IndexError("Invalid page_index or slot")
            
        record = []
        # Read metadata columns (always included)
        for i in range(config.METADATA_COLUMNS):
            record.append(self.base_pages[i][page_index].read(slot))
            
        # Read data columns based on projection
        for i in range(self.num_columns):
            if projected_columns_index[i]:
                value = self.base_pages[i + config.METADATA_COLUMNS][page_index].read(slot)
                record.append(value)
            else:
                record.append(None)
                
        return record

    def read_tail_record(self, page_index, slot, projected_columns_index):
        """
        Reads a tail record's values for specified columns
        :param page_index: int     #Index of the page containing the record
        :param slot: int           #Slot number within the page
        :param projected_columns_index: list     #Boolean list indicating which columns to read
        :return: list             #Values for requested columns
        """
        if page_index >= len(self.tail_pages[0]) or slot >= self.tail_pages[0][page_index].num_records:
            raise IndexError("Invalid page_index or slot")
            
        record = []
        # Read metadata columns (always included)
        for i in range(config.METADATA_COLUMNS):
            record.append(self.tail_pages[i][page_index].read(slot))
            
        # Read data columns based on projection
        for i in range(self.num_columns):
            if projected_columns_index[i]:
                value = self.tail_pages[i + config.METADATA_COLUMNS][page_index].read(slot)
                record.append(value)
            else:
                record.append(None)
                
        return record

    def update_base_record_column(self, page_index, slot, column, value):
        """
        Updates a single column's value in a base record
        :param page_index: int     #Index of the page containing the record
        :param slot: int           #Slot number within the page
        :param column: int         #Column index to update
        :param value: int          #New value to write
        """
        if page_index >= len(self.base_pages[0]) or slot >= self.base_pages[0][page_index].num_records:
            raise IndexError("Invalid page_index or slot")
            
        if column < 0 or column >= self.num_columns + config.METADATA_COLUMNS:
            raise ValueError("Invalid column index")
            
        # Overwrite the old value with the new one
        page = self.base_pages[column][page_index]
        old_value = page.read(slot)
        if old_value is not None:
            page.data[slot * 8:(slot + 1) * 8] = value.to_bytes(8, byteorder='big', signed=True)

    def update_tail_record_column(self, page_index, slot, column, value):
        """
        Updates a single column's value in a tail record
        :param page_index: int     #Index of the page containing the record
        :param slot: int           #Slot number within the page
        :param column: int         #Column index to update
        :param value: int          #New value to write
        """
        if page_index >= len(self.tail_pages[0]) or slot >= self.tail_pages[0][page_index].num_records:
            raise IndexError("Invalid page_index or slot")
            
        if column < 0 or column >= self.num_columns + config.METADATA_COLUMNS:
            raise ValueError("Invalid column index")
            
        # Overwrite the old value with the new one
        page = self.tail_pages[column][page_index]
        old_value = page.read(slot)
        if old_value is not None:
            page.data[slot * 8:(slot + 1) * 8] = value.to_bytes(8, byteorder='big', signed=True)
