from lstore.table import Table

import os, shutil

class Database:

    def __init__(self):
        self.tables = {}  # Initialize tables as a dictionary
        self.path = "./Lineage_DB/"

    def open(self, currentpath):
        """
        Opens the database

        Args:
            currentpath (str): The path to the database

        Returns:
            None
        """

        self.path = currentpath
        os.makedirs(self.path, exist_ok=True)
        for name in os.listdir(self.path):
            if os.path.isdir(os.path.join(self.path,name)):
                self.tables[name] = Table(name, 0, 0, self.path)
                self.tables[name].restart_table()

    def close(self):
        """
        Closes the database

        Args:
            None

        Returns:
            None
        """
        for v in self.tables.values():
            if v.used: v.save()

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table
        """
        if (table:=self.tables.get(name, None)) is None:
            table = Table(name, num_columns, key_index, self.path)
            table.create_meta_data()
            self.tables[name] = table
        return table

    def drop_table(self, name):
        """
        Deletes the specified table
        """
        if name in self.tables:
            shutil.rmtree(self.path+name)
            del self.tables[name]
            return True
        else:
            return False

    def get_table(self, name):
        """
        Returns table with the passed name
        """
        if table:=self.tables.get(name, None):
            table.used = True
        return table
    
