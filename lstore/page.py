class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)  # 4KB page size
        self.max_records = 4096 // 8  # Each value is 8 bytes (64-bit integer)

    def has_capacity(self):
        """
        Returns true if the page has capacity to store another record
        """
        return self.num_records < self.max_records

    def write(self, value):
        """
        Writes an integer value to the page
        Returns the offset where the value was written
        """
        if not self.has_capacity():
            return None
            
        offset = self.num_records * 8
        # Convert integer to bytes and write to page
        value_bytes = value.to_bytes(8, 'big', signed=True)
        self.data[offset:offset+8] = value_bytes
        self.num_records += 1
        return offset

    def read(self, offset):
        """
        Reads the integer value at the given offset
        """
        value_bytes = self.data[offset:offset+8]
        return int.from_bytes(value_bytes, 'big', signed=True)

