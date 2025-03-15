import os
from typing import Optional, List

class Page:
    """
    Represents a page of data in the database.
    Handles both in-memory and disk operations.
    """
    def __init__(self, currentpath: str, pagenum: int, col: int = None):
        self.capacity = 4096    # Page capacity
        self.page_num = pagenum # Page number
        self.col = col          # Column number
        
        # Use column-specific path if column is provided
        if col is not None: self.path = os.path.join(currentpath, "data", f"{col}_{str(self.page_num)}.bin")

        # Use page-specific path if column is not provided
        else: self.path = os.path.join(currentpath, f"{str(self.page_num)}.bin")

        # Initialize data
        self.data: List[int] = []; self.is_dirty = False
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        
        # Load data from disk if exists
        if os.path.exists(self.path): self._load_from_disk()

        # Create empty file if it doesn't exist
        else: self.flush_to_disk()

    def _load_from_disk(self) -> None:
        """
        Loads page data from disk
        Format:
        - First 8 bytes: Number of records (64-bit integer)
        - Remaining bytes: Record data (each record is 8 bytes)
        """

        # Initialize data
        self.data = []

        # Try to load data from disk
        try:
            with open(self.path, 'rb') as file:

                # Read number of records
                num_records_bytes = file.read(8); 

                # If invalid header, print warning and return
                if len(num_records_bytes) != 8: print(f"Warning: Invalid header in {self.path}"); return
                    
                # Get number of records
                num_records = int.from_bytes(num_records_bytes, byteorder='big')
                
                # Read each record
                for _ in range(num_records):
                    value_bytes = file.read(8)

                    # If truncated record, print warning and break
                    if len(value_bytes) != 8: print(f"Warning: Truncated record in {self.path}"); break

                    # Add record to data
                    self.data.append(int.from_bytes(value_bytes, byteorder='big'))

        # If error, print warning and return empty data
        except Exception as e: print(f"Error loading page {self.path}: {e}"); self.data = []

    def flush_to_disk(self) -> None:
        """
        Writes page data to disk
        Format:
        - First 8 bytes: Number of records (64-bit integer)
        - Remaining bytes: Record data (each record is 8 bytes)
        """

        # Try to write data to disk
        try:
            with open(self.path, 'wb') as file:

                # Write number of records
                file.write(len(self.data).to_bytes(8, byteorder='big'))
                
                # Write each record
                for value in self.data: file.write(value.to_bytes(8, byteorder='big'))

            # Set dirty flag to false
            self.is_dirty = False

        # If error, print warning
        except Exception as e: print(f"Error writing page {self.path}: {e}")

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

            # Try to convert value to integer
            try: value = int(value)

            # If error, print warning and set value to 0
            except (ValueError, TypeError): 
                print(f"Warning: Non-integer value {value} being written to {self.path}"); value = 0
        
        # Add value to data and set dirty flag to true
        self.data.append(value); self.is_dirty = True

        # Return true
        return True
            
    def read(self, index: int) -> Optional[int]:
        """
        Reads a value from the page
        """

        # If index is out of bounds, return None
        if index >= len(self.data): return None

        # Return value
        return self.data[index]

    def update(self, index: int, value: int) -> bool:
        """
        Updates a value in the page
        """
        if index >= len(self.data):
            return False
            
        # Ensure value is an integer
        if not isinstance(value, int):

            # Try to convert value to integer
            try: value = int(value)

            # If error, print warning and set value to 0
            except (ValueError, TypeError): 
                print(f"Warning: Non-integer value {value} being written to {self.path}"); value = 0

        # Update value and set dirty flag to true   
        self.data[index] = value; self.is_dirty = True

        # Return true
        return True