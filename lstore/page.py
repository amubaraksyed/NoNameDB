import os

class Page:
    """
    Represents a page of data in the database.
    """
    # data written direction into disk on binary files
    def __init__(self, currentpath, pagenum):
        self.capacity = 4096
        self.page_num = pagenum
        self.path = os.path.join(currentpath,"data/page"+str(self.page_num)+".bin")
        open(self.path, 'ab')

    def num_records(self):
        """
        Get the number of records in the page
        """
        return (os.path.getsize(self.path)/8)-1
    
    def has_capacity(self):
        """
        Check if the page has capacity for a new record
        """
        return False if os.path.getsize(self.path) >= self.capacity else True

    def write(self, value):
        """
        Write a new record to the page
        """
        if self.has_capacity is False:
            return False
        else:
            with open(self.path, 'ab') as file:
                file.write(bytes(value.to_bytes(8, byteorder="big")))
            return True
            
    def read(self, index):
        """
        Read a record from the page
        """
        if index > self.num_records():
            return None
        else:
            with open(self.path, 'rb') as file:
                file.seek(int(index * 8), 1)
                read_array = list(file.read(8))
        return int.from_bytes(read_array, byteorder='big')