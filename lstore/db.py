from lstore.table import Table
from lstore.bufferpool import BufferPoolManager
import os, shutil

class Database:

    def __init__(self):
        self.tables = {}  # Initialize tables as a dictionary
        self.path = "./Lineage_DB/"
        # Initialize bufferpool with 1000 pages
        self.bufferpool = BufferPoolManager(1000)

    def open(self, currentpath):
        """
        Opens the database with a bufferpool of 1000 pages

        Args:
            currentpath (str): The path to the database

        Returns:
            None
        """
        self.path = currentpath
        # Clear and reinitialize bufferpool
        if self.bufferpool:
            self.bufferpool.clear()
        self.bufferpool = BufferPoolManager(1000)
        
        os.makedirs(self.path, exist_ok=True)
        for name in os.listdir(self.path):
            table_path = os.path.join(self.path, name)
            if os.path.isdir(table_path):
                # Check if metadata file exists
                metadata_path = os.path.join(table_path, 'metadata.json')
                if not os.path.exists(metadata_path):
                    continue
                    
                # Create table with temporary values
                table = Table(name, 0, 0, self.path, self.bufferpool)
                # Load actual values from disk
                table.restart_table()
                self.tables[name] = table

    def close(self):
        """
        Closes the database and ensures all dirty pages are written to disk

        Args:
            None

        Returns:
            None
        """
        for v in self.tables.values():
            if v.used:
                v.save()
        
        # Flush all dirty pages and clear bufferpool
        if self.bufferpool:
            self.bufferpool.flush_all()
            self.bufferpool.clear()

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table if it does exist. If it exists, it returns the existing table.

        Args:
            name (str): The name of the table
            num_columns (int): The number of columns in the table
            key_index (int): The index of the key column

        Returns:
            Table: The new table
        """
        table = self.tables.get(name, None)

        if table is None:
            table = Table(name, num_columns, key_index, self.path, self.bufferpool)
            table.create_meta_data()
            self.tables[name] = table

        return table

    def drop_table(self, name):
        """
        Deletes the specified table

        Args:
            name (str): The name of the table

        Returns:
            bool: True if the table was deleted, False otherwise
        """
        if name in self.tables:
            shutil.rmtree(self.path + name)
            del self.tables[name]
            return True
        else:
            return False

    def get_table(self, name):
        """
        Returns table with the passed name

        Args:
            name (str): The name of the table

        Returns:
            Table: The table with the passed name
        """
        table = self.tables.get(name, None)
        if table is not None:
            table.used = True
        return table
    
