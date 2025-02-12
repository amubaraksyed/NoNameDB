from lstore.table import Table
import lstore.config as config

class Database:
    """
    Database class that manages all tables
    """
    def __init__(self):
        self.tables = {}        # Dictionary mapping table names to Table objects

    def open(self, path):
        """
        Opens a database from a file path
        :param path: string     #Path to the database file
        To be implemented in milestone 2
        """
        pass

    def close(self):
        """
        Closes the database and ensures all data is persisted
        To be implemented in milestone 2
        """
        pass

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table in the database
        :param name: string         #Table name
        :param num_columns: int     #Number of data columns (excluding metadata)
        :param key_index: int       #Index of the primary key column
        :return: Table             #The newly created table
        :raises: Exception if table with given name already exists
        """
        # Check if table exists; throw error if so
        if name in self.tables: raise Exception("ERROR: Table already exists")

        # Create table
        table = Table(name, num_columns, key_index)
        self.tables[name] = table

        # Return table
        return table

    def drop_table(self, name):
        """
        Removes a table from the database
        :param name: string     #Name of the table to drop
        :raises: Exception if table does not exist
        """
        # Check if table exists; throw error if not
        if name not in self.tables: raise Exception("ERROR: Table does not exist")

        # Delete table
        del self.tables[name]

    def get_table(self, name):
        """
        Retrieves a table from the database
        :param name: string     #Name of the table to retrieve
        :return: Table         #The requested table
        :raises: KeyError if table does not exist
        """
        return self.tables[name]
