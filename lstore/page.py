import os
from typing import Optional, List

class Page:
    """
    Represents a page of data in the database.
    Handles both in-memory and disk operations.
    """
    def __init__(self, currentpath: str, pagenum: int, col: int = None):
        self.capacity = 4096
        self.page_num = pagenum
        self.col = col
        
        # Use column-specific path if column is provided
        if col is not None:
            # Ensure we use the table's path for column data
            self.path = os.path.join(currentpath, "data", f"{col}_{str(self.page_num)}.bin")
        else:
            self.path = os.path.join(currentpath, f"{str(self.page_num)}.bin")
            
        self.data: List[int] = []  # In-memory data
        self.is_dirty = False
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        
        # Load data from disk if exists
        if os.path.exists(self.path):
            self._load_from_disk()
        else:
            # Create empty file with proper header
            self.flush_to_disk()

    def _load_from_disk(self) -> None:
        """
        Loads page data from disk
        Format:
        - First 8 bytes: Number of records (64-bit integer)
        - Remaining bytes: Record data (each record is 8 bytes)
        """
        self.data = []
        try:
            with open(self.path, 'rb') as file:
                # Read number of records
                num_records_bytes = file.read(8)
                if len(num_records_bytes) != 8:
                    print(f"Warning: Invalid header in {self.path}")
                    return
                    
                num_records = int.from_bytes(num_records_bytes, byteorder='big')
                
                # Read each record
                for _ in range(num_records):
                    value_bytes = file.read(8)
                    if len(value_bytes) != 8:
                        print(f"Warning: Truncated record in {self.path}")
                        break
                    self.data.append(int.from_bytes(value_bytes, byteorder='big'))
                        
        except Exception as e:
            print(f"Error loading page {self.path}: {e}")
            self.data = []

    def flush_to_disk(self) -> None:
        """
        Writes page data to disk
        Format:
        - First 8 bytes: Number of records (64-bit integer)
        - Remaining bytes: Record data (each record is 8 bytes)
        """
        try:
            with open(self.path, 'wb') as file:
                # Write number of records
                file.write(len(self.data).to_bytes(8, byteorder='big'))
                
                # Write each record
                for value in self.data:
                    file.write(value.to_bytes(8, byteorder='big'))
            self.is_dirty = False
        except Exception as e:
            print(f"Error writing page {self.path}: {e}")

    def num_records(self) -> int:
        """
        Returns number of records in the page
        """
        return len(self.data)
    
    def has_capacity(self) -> bool:
        """
        Checks if page has capacity for more records
        Accounts for both record count header and record data
        """
        # 8 bytes for number of records + 8 bytes per record
        return (len(self.data) + 1) * 8 < self.capacity

    def write(self, value: int) -> bool:
        """
        Writes a value to the page
        """
        if not self.has_capacity():
            return False
        
        # Ensure value is an integer
        if not isinstance(value, int):
            try:
                value = int(value)
            except (ValueError, TypeError):
                print(f"Warning: Non-integer value {value} being written to {self.path}")
                value = 0
        
        self.data.append(value)
        self.is_dirty = True
        return True
            
    def read(self, index: int) -> Optional[int]:
        """
        Reads a value from the page
        """
        if index >= len(self.data):
            return None
        return self.data[index]

    def update(self, index: int, value: int) -> bool:
        """
        Updates a value in the page
        """
        if index >= len(self.data):
            return False
            
        # Ensure value is an integer
        if not isinstance(value, int):
            try:
                value = int(value)
            except (ValueError, TypeError):
                print(f"Warning: Non-integer value {value} being written to {self.path}")
                value = 0
                
        self.data[index] = value
        self.is_dirty = True
        return True