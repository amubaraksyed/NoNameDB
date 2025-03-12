from lstore.table import Table
from lstore.bufferpool import BufferPoolManager
import os, shutil
import pickle

class Database:

    def __init__(self):
        self.tables = {}  # Initialize tables as a dictionary
        self.path = None
        self.bufferpool = None

    def open(self, currentpath):
        """
        Opens the database with a bufferpool of 1000 pages

        Args:
            currentpath (str): The path to the database

        Returns:
            None
        """
        self.path = currentpath
        # Initialize bufferpool
        self.bufferpool = BufferPoolManager(1000)
        
        # Create database directory if it doesn't exist
        os.makedirs(self.path, exist_ok=True)
        
        # Try to load existing tables
        pickle_path = os.path.join(self.path, "tables.pickle")
        if os.path.exists(pickle_path):
            try:
                with open(pickle_path, 'rb') as file:
                    # Load tables and update their bufferpool reference
                    self.tables = pickle.load(file)
                    for table in self.tables.values():
                        table._initialize_after_load(self.bufferpool)
            except Exception as e:
                print(f"Error loading database state: {e}")
                self.tables = {}

    def close(self):
        """
        Closes the database and ensures all dirty pages are written to disk
        """
        # Save all tables that have been used
        for table in self.tables.values():
            if table.used:
                # Save table state
                table.save()
                
                # Flush all dirty pages for this table
                for col in range(table.total_columns):
                    for page_num in table.page_range[col].keys():
                        page = table.page_range[col][page_num]
                        if page.is_dirty:
                            page.flush_to_disk()
                            self.bufferpool.unpin_page(self.path, page_num, col)
        
        # Save database state using pickle
        if self.path:
            try:
                # Ensure all tables are in a serializable state
                for table in self.tables.values():
                    # Clear unpicklable objects
                    table.bufferpool = None
                    table.version_lock = None
                    if hasattr(table.index, '_lock'):
                        table.index._lock = None
                
                # Save tables state
                with open(os.path.join(self.path, "tables.pickle"), 'wb') as file:
                    pickle.dump(self.tables, file)
            except Exception as e:
                print(f"Error saving database state: {e}")
        
        # Clear bufferpool
        if self.bufferpool:
            self.bufferpool.clear()
            self.bufferpool = None

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
        # If database hasn't been opened, initialize with default path
        if self.path is None:
            self.path = "ECS165"
            os.makedirs(self.path, exist_ok=True)
            
        # Initialize bufferpool if not already done
        if self.bufferpool is None:
            self.bufferpool = BufferPoolManager(1000)

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
    
