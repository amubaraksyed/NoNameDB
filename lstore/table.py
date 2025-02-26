from lstore.index import Index
from time import time
from lstore.page import Page
from lstore.bufferpool import BufferPoolManager
import json, os
from lstore.config import MERGE_TRIGGER_COUNT, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN

class Record:
    def __init__(self, rid: int, key: int, columns: int):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

class Table:
    """
    Represents a table in the database
    """
    def __init__(self, name, num_columns: int, key_col: int, currentpath: str, bufferpool: BufferPoolManager):
        self.name = name
        self.path = os.path.join(currentpath, name)
        os.makedirs(self.path, exist_ok=True)
        os.makedirs(os.path.join(self.path, "data"), exist_ok=True)
        
        self.num_columns = num_columns
        self.key_col = key_col
        self.versions = []
        self.page_directory = []
        self.page_range = []
        self.bufferpool = bufferpool
        
        # Initialize metadata columns
        self.metadata_columns = 4  # Number of metadata columns
        self.total_columns = self.num_columns + self.metadata_columns
        
        # Initialize page ranges
        self.base_pages_per_range = 16
        self.current_page_range = 0
        self.page_ranges = []  # List of page ranges
        
        # Initialize page ranges for both data and metadata columns
        for i in range(self.total_columns):
            self.page_range.append(dict())
            self.page_directory.append(dict())
            
        # Initialize index
        self.index = Index(self)
        
        self.is_history = False
        self.used = False
        self.update_count = 0  # Track number of updates
        self.last_page_number = self.total_columns * 16 + self.total_columns

    def _get_page(self, col: int, page_num: int) -> Page:
        """
        Gets a page from the bufferpool
        """
        # Get the page from the bufferpool, which will load it from disk if needed
        page = self.bufferpool.get_page(self.path, page_num, col)
        if page is None:
            return None
        return page

    def create_meta_data(self):
        """
        Create metadata for the table
        """
        self.used = True
        self.last_page_number = self.total_columns * 16 + self.total_columns
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.join(self.path, "data"), exist_ok=True)
        
        # Initialize base pages for each column
        for col in range(self.total_columns):
            for i in range(1, 17):  # 16 base pages per column
                page = self._get_page(col, i)
                self.page_range[col][i] = page
                self.bufferpool.unpin_page(page.path, page.page_num)
            
            # Create initial tail page for each column
            tail_page = self._get_page(col, 17)  # First tail page after base pages
            self.page_range[col][17] = tail_page
            self.bufferpool.unpin_page(tail_page.path, tail_page.page_num)

    def write(self, columns: list):
        """
        Write a new record to the table
        """
        rid = columns[self.key_col]
        if rid in self.page_directory[self.key_col]:
            return None
            
        # Prepare metadata
        metadata = [
            0,              # INDIRECTION_COLUMN (no updates yet)
            rid,            # RID_COLUMN
            int(time()),    # TIMESTAMP_COLUMN
            0              # SCHEMA_ENCODING_COLUMN (no updates yet)
        ]
        
        # Write metadata columns first
        for i in range(self.metadata_columns):
            page = None
            index = None
            page_num = None
            
            # Find page with capacity
            for k, v in self.page_range[i].items():
                curr_page = self._get_page(i, k)
                if curr_page.has_capacity():
                    page_num = k
                    page = curr_page
                    index = curr_page.num_records()
                    break
                self.bufferpool.unpin_page(curr_page.path, curr_page.page_num)
                    
            # Create new page if needed
            if page_num is None or index is None or page is None:
                index = 0
                self.last_page_number += 1
                page_num = self.last_page_number
                page = self._get_page(i, page_num)
                self.page_range[i][page_num] = page
            
            # Write metadata
            if page.write(metadata[i]):
                self.bufferpool.mark_dirty(page.path, page.page_num)
                self.page_directory[i][rid] = [page_num, index]
            
            self.bufferpool.unpin_page(page.path, page.page_num)
            
        # Write actual data columns
        for i in range(self.num_columns):
            page = None
            index = None
            page_num = None
            
            # Find page with capacity
            for k, v in self.page_range[i + self.metadata_columns].items():
                curr_page = self._get_page(i + self.metadata_columns, k)
                if curr_page.has_capacity():
                    page_num = k
                    page = curr_page
                    index = curr_page.num_records()
                    break
                self.bufferpool.unpin_page(curr_page.path, curr_page.page_num)
                    
            # Create new page if needed
            if page_num is None or index is None or page is None:
                index = 0
                self.last_page_number += 1
                page_num = self.last_page_number
                page = self._get_page(i + self.metadata_columns, page_num)
                self.page_range[i + self.metadata_columns][page_num] = page
            
            # Write data and update indices
            if page.write(columns[i]):
                self.bufferpool.mark_dirty(page.path, page.page_num)
                self.index.add_or_move_record_by_col(i + self.metadata_columns, rid, columns[i])
                self.page_directory[i + self.metadata_columns][rid] = [page_num, index]
            
            self.bufferpool.unpin_page(page.path, page.page_num)

    def update(self, columns: list):
        """
        Update existing record
        """
        rid = columns[self.key_col]
        updated = False
        schema_encoding = 0
        
        # Get current indirection value
        old_indirection = self.read_value(INDIRECTION_COLUMN, rid)
        
        # Create new tail record for updates
        tail_rid = self.last_page_number + 1
        
        # Prepare metadata for tail record
        metadata = [
            old_indirection,  # INDIRECTION_COLUMN (points to previous version)
            tail_rid,        # RID_COLUMN
            int(time()),     # TIMESTAMP_COLUMN
            0               # SCHEMA_ENCODING_COLUMN (will be set based on updates)
        ]
        
        # Store updated values and track schema encoding
        updated_values = []
        for i in range(self.num_columns):
            # Get the latest value for this column by following indirection chain
            latest_value = self.read_value(i + self.metadata_columns, rid)
            
            if columns[i] is not None:
                if latest_value != columns[i]:
                    updated = True
                    schema_encoding |= (1 << i)  # Mark column as updated in schema
                    updated_values.append(columns[i])
                else:
                    updated_values.append(latest_value)
            else:
                updated_values.append(latest_value)
        
        if updated:
            # Update schema encoding in metadata
            metadata[SCHEMA_ENCODING_COLUMN] = schema_encoding
            
            # Write metadata for tail record
            for i in range(self.metadata_columns):
                page = None
                index = None
                page_num = None
                
                # Find page with capacity
                for k, v in self.page_range[i].items():
                    curr_page = self._get_page(i, k)
                    if curr_page.has_capacity():
                        page_num = k
                        page = curr_page
                        index = curr_page.num_records()
                        break
                    self.bufferpool.unpin_page(curr_page.path, curr_page.page_num)
                        
                # Create new page if needed
                if page_num is None or index is None or page is None:
                    index = 0
                    self.last_page_number += 1
                    page_num = self.last_page_number
                    page = self._get_page(i, page_num)
                    self.page_range[i][page_num] = page
                
                # Write metadata
                if page.write(metadata[i]):
                    self.bufferpool.mark_dirty(page.path, page.page_num)
                    self.page_directory[i][tail_rid] = [page_num, index]  # Use tail_rid instead of rid
                
                self.bufferpool.unpin_page(page.path, page.page_num)
            
            # Write updated values
            for i in range(self.num_columns):
                page = None
                index = None
                page_num = None
                
                # Find page with capacity
                for k, v in self.page_range[i + self.metadata_columns].items():
                    curr_page = self._get_page(i + self.metadata_columns, k)
                    if curr_page.has_capacity():
                        page_num = k
                        page = curr_page
                        index = curr_page.num_records()
                        break
                    self.bufferpool.unpin_page(curr_page.path, curr_page.page_num)
                        
                # Create new page if needed
                if page_num is None or index is None or page is None:
                    index = 0
                    self.last_page_number += 1
                    page_num = self.last_page_number
                    page = self._get_page(i + self.metadata_columns, page_num)
                    self.page_range[i + self.metadata_columns][page_num] = page
                
                # Write value and update indices
                if page.write(updated_values[i]):
                    self.bufferpool.mark_dirty(page.path, page.page_num)
                    # Update index with tail_rid instead of base rid
                    self.index.add_or_move_record_by_col(i + self.metadata_columns, tail_rid, updated_values[i])
                    self.page_directory[i + self.metadata_columns][tail_rid] = [page_num, index]  # Use tail_rid instead of rid
                
                self.bufferpool.unpin_page(page.path, page.page_num)
            
            # Update indirection pointer of base record
            base_indirection_info = self.page_directory[INDIRECTION_COLUMN][rid]
            base_indirection_page = self._get_page(INDIRECTION_COLUMN, base_indirection_info[0])
            base_indirection_page.update(base_indirection_info[1], tail_rid)  # Use update instead of write
            self.bufferpool.mark_dirty(base_indirection_page.path, base_indirection_page.page_num)
            self.bufferpool.unpin_page(base_indirection_page.path, base_indirection_page.page_num)
            
            # Update indices for all columns to point to the latest values
            for i in range(self.num_columns):
                self.index.add_or_move_record_by_col(i + self.metadata_columns, rid, updated_values[i])
            
            # Increment update count and check if merge needed
            self.update_count += 1
            if self.update_count >= MERGE_TRIGGER_COUNT:
                self.merge()
                self.update_count = 0

    def read_records(self, col_num: int, search_key: int, proj_col: list) -> list[Record]:
        """
        Read records based on search criteria
        """
        records = []
        rids = []
        
        # Adjust column number to account for metadata columns
        actual_col = col_num + self.metadata_columns if col_num != self.key_col else self.key_col + self.metadata_columns
        
        if col_num == self.key_col:
            rids.append(search_key)
        else:
            # Only use index for data columns and when index exists
            if not self.is_history and actual_col < len(self.index.indices) and self.index.indices[actual_col] is not None:
                rids = self.index.get_rid_in_col_by_value(actual_col, search_key)
            else:
                for k, v in self.page_directory[actual_col].items():
                    page = self._get_page(actual_col, v[0])
                    value = page.read(v[1])
                    self.bufferpool.unpin_page(page.path, page.page_num)
                    if value == search_key:
                        rids.append(k)
                        
        for rid in rids:
            if rid in self.page_directory[self.metadata_columns + self.key_col]:  # Check in data columns
                col = []
                for cnt in range(self.num_columns):
                    if proj_col[cnt] == 1:
                        # Read from data columns (offset by metadata columns)
                        col.append(self.read_value(cnt + self.metadata_columns, rid))
                    else:
                        col.append(None)
                records.append(Record(rid, col[self.key_col], col))
                
        return records

    def read_value(self, col: int, rid: int) -> int:
        """
        Read single value, following the version chain if necessary
        """
        if rid not in self.page_directory[col]:
            return None
            
        # Only use index for data columns, not metadata
        if col >= self.metadata_columns and not self.is_history and col < len(self.index.indices) and self.index.indices[col] is not None:
            return self.index.get_value_in_col_by_rid(col, rid)
            
        # Get the initial page details
        page_details = self.page_directory[col][rid]
        current_rid = rid
        
        # If this is a data column and we're not in history mode, follow indirection chain
        if col >= self.metadata_columns and not self.is_history:
            # Keep track of visited RIDs to prevent infinite loops
            visited_rids = set([current_rid])
            
            # Follow indirection chain until we reach the latest version
            while True:
                # Check if current_rid exists in indirection column
                if current_rid not in self.page_directory[INDIRECTION_COLUMN]:
                    break
                    
                # Get indirection value
                ind_details = self.page_directory[INDIRECTION_COLUMN][current_rid]
                ind_page = self._get_page(INDIRECTION_COLUMN, ind_details[0])
                if ind_page is None:
                    break
                    
                next_rid = ind_page.read(ind_details[1])
                self.bufferpool.unpin_page(self.path, ind_page.page_num, INDIRECTION_COLUMN)
                
                # If no more updates (indirection is 0) or we've seen this RID before, break
                if next_rid == 0 or next_rid in visited_rids:
                    break
                    
                # Check if next_rid exists in the target column
                if next_rid not in self.page_directory[col]:
                    break
                    
                # Update current RID and page details
                current_rid = next_rid
                visited_rids.add(current_rid)
                page_details = self.page_directory[col][current_rid]
        
        # Read the value from the final page
        page = self._get_page(col, page_details[0])
        if page is None:
            return None
            
        value = page.read(page_details[1])
        self.bufferpool.unpin_page(self.path, page.page_num, col)
        return value

    def read_page(self, page_num: int, index: int, col: int = None) -> int:
        """
        Read value from specific page
        """
        if col is not None:
            page = self._get_page(col, page_num)
            value = page.read(index)
            self.bufferpool.unpin_page(page.path, page.page_num)
            return value
            
        for i in range(self.num_columns):
            page = self._get_page(i, page_num)
            value = page.read(index)
            self.bufferpool.unpin_page(page.path, page.page_num)
            if value is not None:
                return value
        return None

    def delete(self, rid: int):
        """
        Delete record
        """
        for i in range(self.num_columns):
            self.page_directory[i].pop(rid)
            self.index.delete_record(i, rid)

    def make_ver_copy(self):
        """
        Create version snapshot
        """
        self.versions.append([{k:[v[0], v[1]] 
                             for k,v in col.items()} 
                            for col in self.page_directory])

    def save(self):
        """
        Save table metadata and ensure all pages are flushed to disk
        """
        # First, flush all dirty pages in the bufferpool for this table
        for col in range(self.total_columns):
            for page_num in self.page_range[col].keys():
                page = self.page_range[col][page_num]
                if page.is_dirty:
                    page.flush_to_disk()
                    self.bufferpool.unpin_page(self.path, page_num, col)

        # Save the page directory
        with open(os.path.join(self.path, 'page_directory.json'), "w") as file:
            # Convert all keys and values to strings for JSON serialization
            page_dir_json = [{str(k): [v[0], v[1]] 
                            for k, v in col.items()} for col in self.page_directory]
            file.write(json.dumps(page_dir_json))

        # Save the page range
        value = []
        for col in self.page_range:
            value.append([str(k) for k in col.keys()])
        with open(os.path.join(self.path, 'page_range.json'), "w") as file:
            file.write(json.dumps(value))

        # Save the versions
        with open(os.path.join(self.path, 'versions.json'), "w") as file:
            # Convert all keys to strings for JSON serialization
            versions_json = [[{str(k): [v[0], v[1]] 
                             for k, v in col.items()} for col in version] 
                           for version in self.versions]
            file.write(json.dumps(versions_json))

        # Save the metadata
        data = {
            "columns": self.num_columns, 
            "key_col": self.key_col,
            "update_count": self.update_count
        }
        with open(os.path.join(self.path, 'metadata.json'), "w") as file:
            file.write(json.dumps(data))

    def restart_table(self):
        """
        Restart table from disk
        """
        self.last_page_number = 0
        
        # Load metadata
        data = json.load(open(os.path.join(self.path, 'metadata.json')))
        self.num_columns = int(data["columns"])
        self.key_col = int(data["key_col"])
        self.update_count = int(data.get("update_count", 0))  # Default to 0 if not found
        
        # Update total columns
        self.total_columns = self.num_columns + self.metadata_columns
        
        # Reinitialize page ranges for both data and metadata columns
        self.page_directory = []
        self.page_range = []
        for i in range(self.total_columns):
            self.page_range.append(dict())
            self.page_directory.append(dict())
        
        # Load page directory
        page_dir = json.load(open(os.path.join(self.path, 'page_directory.json')))
        self.page_directory = [{int(k):[int(v[0]), int(v[1])] 
                              for k,v in col.items()} for col in page_dir]
        
        # Load page range and initialize pages
        page_ranges_data = json.load(open(os.path.join(self.path, 'page_range.json')))
        for i, page_range_nums in enumerate(page_ranges_data):
            for page_num in page_range_nums:
                page_num = int(page_num)
                # Create and load the page
                page = self._get_page(i, page_num)
                if page is not None:
                    self.page_range[i][page_num] = page
                    # Unpin after loading
                    self.bufferpool.unpin_page(self.path, page_num, i)
                    # Update last page number
                    if page_num > self.last_page_number:
                        self.last_page_number = page_num
        
        # Load versions
        versions = json.load(open(os.path.join(self.path, 'versions.json')))
        self.versions = [[{int(k):[int(v[0]), int(v[1])] 
                          for k,v in col.items()} for col in version] 
                        for version in versions]
        
        # Rebuild indices
        self.index = Index(self)
        # Create index for key column and rebuild it
        self.index.create_index(self.key_col + self.metadata_columns)
        self.index.restart_index_by_col(self.key_col + self.metadata_columns)

    def merge(self):
        """
        Merge operation to consolidate base and tail records
        """
        # Track which records have been updated
        updated_records = set()
        
        # For each column (including metadata)
        for i in range(self.total_columns):
            # Get all pages in the column's range
            pages = sorted(self.page_range[i].keys())
            base_pages = pages[:16]  # First 16 pages are base pages
            tail_pages = pages[16:]  # Remaining pages are tail pages
            
            # For each tail page
            for tail_page_num in tail_pages:
                tail_page = self._get_page(i, tail_page_num)
                
                # Read all records in tail page
                for idx in range(tail_page.num_records()):
                    value = tail_page.read(idx)
                    rid = None
                    
                    # Find the RID this tail record belongs to
                    for record_rid, location in self.page_directory[i].items():
                        if location[0] == tail_page_num and location[1] == idx:
                            rid = record_rid
                            break
                    
                    if rid is not None:
                        updated_records.add(rid)
                        
                # Unpin the tail page
                self.bufferpool.unpin_page(tail_page.path, tail_page.page_num)
                
        # Now consolidate records that have been updated
        for rid in updated_records:
            # Get the latest values for all columns (metadata + data)
            latest_values = []
            for i in range(self.total_columns):
                value = self.read_value(i, rid)
                latest_values.append(value)
            
            # Reset metadata for consolidated record
            latest_values[INDIRECTION_COLUMN] = 0  # No more updates
            latest_values[TIMESTAMP_COLUMN] = int(time())  # Current time
            latest_values[SCHEMA_ENCODING_COLUMN] = 0  # Reset schema encoding
            
            # Write consolidated record to base pages
            for i in range(self.total_columns):
                # Find the appropriate base page
                base_page_num = (rid % 16) + 1  # Distribute records across base pages
                base_page = self._get_page(i, base_page_num)
                
                # Write the value
                if base_page.has_capacity():
                    index = base_page.num_records()
                    if base_page.write(latest_values[i]):
                        self.bufferpool.mark_dirty(base_page.path, base_page.page_num)
                        self.page_directory[i][rid] = [base_page_num, index]
                        
                self.bufferpool.unpin_page(base_page.path, base_page.page_num)
            
            # Update indices (only for data columns)
            for i in range(self.metadata_columns, self.total_columns):
                self.index.add_or_move_record_by_col(i, rid, latest_values[i])
        
        # Clear tail pages after successful merge
        for i in range(self.total_columns):
            pages = sorted(self.page_range[i].keys())
            tail_pages = pages[16:]
            for tail_page_num in tail_pages:
                # Remove tail page from page range
                self.page_range[i].pop(tail_page_num)
                
        # Reset last page number
        self.last_page_number = self.total_columns * 16 + self.total_columns

    def get_page_range_for_rid(self, rid: int) -> int:
        """
        Determines which page range a RID belongs to
        """
        return rid // (self.base_pages_per_range * 512)  # 512 records per page

    def create_new_page_range(self):
        """
        Creates a new page range
        """
        page_range = {
            'base_pages': {},
            'tail_pages': {},
            'base_rid_count': 0
        }
        self.page_ranges.append(page_range)
        self.current_page_range = len(self.page_ranges) - 1
        return page_range

    def get_or_create_page_range(self, rid: int) -> dict:
        """
        Gets the appropriate page range for a RID, creating it if necessary
        """
        range_index = self.get_page_range_for_rid(rid)
        while range_index >= len(self.page_ranges):
            self.create_new_page_range()
        return self.page_ranges[range_index]

    def insert_record(self, *columns) -> int:
        """
        Insert a record with specified columns
        """
        if len(columns) != self.num_columns:
            raise ValueError(f"Expected {self.num_columns} columns but got {len(columns)}")

        # Get or create appropriate page range
        rid = self._get_next_rid()
        page_range = self.get_or_create_page_range(rid)
        
        # Initialize metadata
        schema_encoding = '0' * self.num_columns
        timestamp = int(time.time())
        indirection = 0  # No updates yet
        
        # Prepare all values (metadata + data)
        all_values = [indirection, rid, timestamp, schema_encoding] + list(columns)
        
        # Insert into each column
        for col_index, value in enumerate(all_values):
            # Get or create page for this column in the current page range
            page_num = self._get_or_create_page_for_column(col_index, rid, is_base=True)
            page = self._get_page(col_index, page_num)
            
            # Calculate offset within page
            offset = (rid % 512)  # 512 records per page
            
            # Write value and update page directory
            page.write(offset, value)
            self.page_directory[col_index][rid] = (page_num, offset)
            self.bufferpool.unpin_page(page.path, page.page_num)
            
        # Update page range record count
        page_range['base_rid_count'] += 1
        
        # Add to index if it exists
        if self.index.indices[self.key_col]:
            self.index.add_to_index(self.key_col, columns[self.key_col], rid)
            
        return rid

    def update_record(self, primary_key: int, *columns) -> None:
        """
        Update a record with specified columns
        """
        if len(columns) != self.num_columns:
            raise ValueError(f"Expected {self.num_columns} columns but got {len(columns)}")
            
        # Find the base RID using index
        base_rid = self.index.get_rid_from_key(self.key_col, primary_key)
        if base_rid is None:
            raise ValueError(f"Record with primary key {primary_key} not found")
            
        # Get the page range for this record
        page_range = self.get_or_create_page_range(base_rid)
        
        # Create new tail record
        new_rid = self._get_next_rid()
        
        # Get current schema encoding
        schema_encoding_details = self.page_directory[SCHEMA_ENCODING_COLUMN][base_rid]
        schema_page = self._get_page(SCHEMA_ENCODING_COLUMN, schema_encoding_details[0])
        current_schema = schema_page.read(schema_encoding_details[1])
        self.bufferpool.unpin_page(schema_page.path, schema_page.page_num)
        
        # Update schema encoding based on which columns are being updated
        new_schema = list(bin(current_schema)[2:].zfill(self.num_columns))
        for i, value in enumerate(columns):
            if value is not None:
                new_schema[i] = '1'
        new_schema = int(''.join(new_schema), 2)
        
        # Prepare metadata for tail record
        timestamp = int(time.time())
        
        # Get current indirection
        ind_details = self.page_directory[INDIRECTION_COLUMN][base_rid]
        ind_page = self._get_page(INDIRECTION_COLUMN, ind_details[0])
        current_indirection = ind_page.read(ind_details[1])
        self.bufferpool.unpin_page(ind_page.path, ind_page.page_num)
        
        # Prepare all values for tail record
        all_values = [current_indirection, new_rid, timestamp, new_schema]
        
        # Copy existing values and apply updates
        for i in range(self.num_columns):
            if columns[i] is not None:
                all_values.append(columns[i])
            else:
                # Get existing value from base record
                all_values.append(self.read_value(i + self.metadata_columns, base_rid))
        
        # Insert tail record
        for col_index, value in enumerate(all_values):
            # Get or create tail page for this column
            page_num = self._get_or_create_page_for_column(col_index, new_rid, is_base=False)
            page = self._get_page(col_index, page_num)
            
            # Calculate offset within page
            offset = (new_rid % 512)
            
            # Write value and update page directory
            page.write(offset, value)
            self.page_directory[col_index][new_rid] = (page_num, offset)
            self.bufferpool.unpin_page(page.path, page.page_num)
            
        # Update base record's indirection to point to new tail record
        ind_page = self._get_page(INDIRECTION_COLUMN, ind_details[0])
        ind_page.write(ind_details[1], new_rid)
        self.bufferpool.unpin_page(ind_page.path, ind_page.page_num)
        
        # Increment update count
        self.update_count += 1

    def _get_or_create_page_for_column(self, col_index: int, rid: int, is_base: bool = True) -> int:
        """
        Gets or creates a page for the specified column in the appropriate page range
        """
        page_range = self.get_or_create_page_range(rid)
        pages_dict = page_range['base_pages'] if is_base else page_range['tail_pages']
        
        if col_index not in pages_dict:
            pages_dict[col_index] = []
            
        # Calculate which page number within the range we need
        records_per_page = 512
        page_index = (rid % (records_per_page * self.base_pages_per_range)) // records_per_page
        
        # Create new pages if needed
        while len(pages_dict[col_index]) <= page_index:
            new_page_num = len(self.page_range[col_index])
            new_page = Page(new_page_num, os.path.join(self.path, "data", f"{col_index}_{new_page_num}.page"), col_index)
            self.page_range[col_index][new_page_num] = new_page
            pages_dict[col_index].append(new_page_num)
            
        return pages_dict[col_index][page_index]

    def select_version(self, version_num: int):
        """
        Select a specific version of the table
        """
        if not self.versions:
            return False
            
        # Handle negative version numbers (relative to current)
        if version_num < 0:
            version_num = len(self.versions) + version_num
            
        # Validate version number
        if version_num < 0 or version_num >= len(self.versions):
            return False
            
        # Set history mode
        self.is_history = True
        
        # Restore the selected version's page directory
        self.page_directory = [
            {k: list(v) for k, v in version_dict.items()}
            for version_dict in self.versions[version_num]
        ]
        
        return True