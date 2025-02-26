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
            # Create empty file
            open(self.path, 'ab').close()

    def _load_from_disk(self) -> None:
        """
        Loads page data from disk
        """
        self.data = []
        if os.path.exists(self.path):
            with open(self.path, 'rb') as file:
                while True:
                    try:
                        value_bytes = file.read(8)
                        if not value_bytes:
                            break
                        self.data.append(int.from_bytes(value_bytes, byteorder='big'))
                    except:
                        break

    def flush_to_disk(self) -> None:
        """
        Writes page data to disk
        """
        with open(self.path, 'wb') as file:
            for value in self.data:
                file.write(value.to_bytes(8, byteorder='big'))
        self.is_dirty = False

    def num_records(self) -> int:
        """
        Returns number of records in the page
        """
        return len(self.data)
    
    def has_capacity(self) -> bool:
        """
        Checks if page has capacity for more records
        """
        return len(self.data) * 8 < self.capacity

    def write(self, value: int) -> bool:
        """
        Writes a value to the page
        """
        if not self.has_capacity():
            return False
        
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
        self.data[index] = value
        self.is_dirty = True
        return True