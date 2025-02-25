from lstore.index import Index
from time import time
from lstore.page import Page
import json, os

class Record:
    def __init__(self, rid: int, key: int, columns: int):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    Represents a table in the database
    """
    def __init__(self, name, num_columns: int, key_col: int, currentpath):
        self.name = name
        self.path = os.path.join(currentpath,name)
        os.makedirs(self.path, exist_ok=True)
        os.makedirs(os.path.join(self.path,"data"), exist_ok=True)
        self.num_columns = num_columns
        self.key_col = key_col  
        self.versions = []
        self.page_directory = [] 
        self.page_range = []
        for i in range(self.num_columns):
            self.page_range.append(dict()) 
            self.page_directory.append(dict())
        self.index = Index(self)
        self.is_history = False
        self.used = False
    
    def create_meta_data(self):
        """
        Create metadata for the table
        """
        # Set the table to used
        self.used = True

        # Set the last page number
        self.last_page_number = self.num_columns*16+self.num_columns

        # Create pages for the table
        cnt = 0
        for i in range(1, self.num_columns*16+1):
            self.page_range[cnt][i] = Page(self.path, i)
            if i%16==0: self.page_range[cnt][self.num_columns*16+1+cnt] = Page(self.path, i); cnt += 1

    def restart_table(self):
        """ 
        Restart the table
        """
        # Set the last page number
        self.last_page_number = 0

        # Load the metadata
        data = json.load(open(os.path.join(self.path,'metadata.json'),))
        self.num_columns = int(data["columns"])
        self.key_col = int(data["key_col"])

        # Load the page directory
        page_dir = json.load(open(os.path.join(self.path,'page_directory.json'),))
        self.page_directory = [{int(k):[int(v[0]), int(v[1])] for k,v in col.items()} for col in page_dir]

        # Load the page range
        page_range = json.load(open(os.path.join(self.path,'page_range.json'),))
        self.page_range = [{int(page_num): Page(self.path, int(page_num)) for page_num in range} for range in page_range]

        # Update the last page number
        for range in page_range:
            if (num:=max(range)) > self.last_page_number: self.last_page_number = num

        # Load the versions
        versions = json.load(open(os.path.join(self.path,'versions.json'),))
        self.versions = [[{int(k):[int(v[0]), int(v[1])] for k,v in col.items()} for col in version] for version in versions]

        # Restart the index
        self.index.restart_index()

    def save(self):
        """
        Save the table
        """
        # Save the page directory
        with open(os.path.join(self.path,'page_directory.json'),"w") as file:
            file.write(json.dumps(self.page_directory))

        # Save the page range
        value = []
        for col in self.page_range: value.append(list(col.keys()))
        with open(os.path.join(self.path,'page_range.json'),"w") as file:file.write(json.dumps(value))

        # Save the versions
        with open(os.path.join(self.path,'versions.json'),"w") as file: file.write(json.dumps(self.versions))

        # Save the metadata
        data = {"columns": self.num_columns, "key_col": self.key_col}
        with open(os.path.join(self.path,'metadata.json'),"w") as file: file.write(json.dumps(data))

    def write(self, columns: list):
        """
        Write a new record to the table
        """
        # Get the record ID
        rid = columns[self.key_col]

        # Check if the record ID is already in the table
        if rid in self.page_directory[self.key_col]: return None

        # Find a page to write to
        for i in range(self.num_columns):

            # Initialize the page, index, and page number
            page, index, page_num = None, None, None

            # Find a page to write to
            for k, v in self.page_range[i].items():
                if v.has_capacity():
                    page_num, page = k, v
                    index = page.num_records() if page.num_records() is not None else -1
                    break

            # If no page is found, create a new page
            if page_num == None or index == None or page == None:
                index = 0
                self.last_page_number+=1
                page_num = self.last_page_number
                page = Page(self.path, self.last_page_number)
                self.page_range[i][page_num] = page
            else: index += 1

            # Write the record to the page
            check = page.write(columns[i])
            if check:
                self.index.add_or_move_record_by_col(i, rid, columns[i])
                self.page_directory[i][rid] = [page_num, index]

    def update(self, columns: list):
        """
        Update a record in the table
        """
        # Get the record ID
        rid = columns[self.key_col]

        # Check if the record ID is in the table
        for i in range(self.num_columns):
            page = self.page_directory[i][rid]
            if not columns[i] == None and not self.read_page(page[0], page[1], i) == columns[i]:
                # Initialize the page, index, and page number
                page, index, page_num = None, None, None

                # Find a page to write to
                for k, v in self.page_range[i].items():
                    if v.has_capacity():
                        page_num, page = k, v
                        index = page.num_records() if page.num_records() is not None else -1
                        break

                # If no page is found, create a new page
                if page_num == None or index == None or page == None:
                    index = 0
                    self.last_page_number+=1
                    page_num = self.last_page_number
                    page = Page(self.path, self.last_page_number)
                    self.page_range[i][page_num] = page
                else: index += 1

                # Write the record to the page
                check = page.write(columns[i])
                if check:
                    self.index.add_or_move_record_by_col(i, rid, columns[i])
                    self.page_directory[i][rid] = [page_num, index]

    def read_records(self, col_num: int, search_key: int, proj_col: list) -> list[Record]:
        """
        Read records from the table
        """
        # Initialize the records and record IDs
        records, rids = [], []

        # Check if the column number is the key column
        if col_num == self.key_col:
            rids.append(search_key)
        else:
            # If the index is not in history mode and the index exists, get the record IDs
            if not self.is_history and self.index.indices[col_num]:
                rids = self.index.get_rid_in_col_by_value(col_num, search_key)
            else:
                # Get the record IDs from the page directory
                for k, v in self.page_directory[col_num].items():
                    if self.read_page(v[0], v[1])==search_key:
                        rids.append(k)

        # Get the records
        for rid in rids:
            if rid in self.page_directory[self.key_col]:
                col = []
                for cnt in range(self.num_columns):
                    if proj_col[cnt] == 1:
                        col.append(self.read_value(cnt, rid))
                    else: col.append(None)

                # Add the record to the list of records
                records.append(Record(rid, self.key_col, col))

        # Return the list of records
        return records
    
    def read_value(self, col: int, rid: int) -> int:
        """
        Read the value of a column for a specific record
        """
        # Check if the record ID is in the table
        if not rid in self.page_directory[col]: return None

        # Check if the index is in history mode and the index exists
        if not self.is_history and self.index.indices[col]:
            return self.index.get_value_in_col_by_rid(col, rid)

        # Get the page details
        page_details = self.page_directory[col][rid]

        # Read the value from the page
        return self.read_page(page_details[0], page_details[1])
            
    def read_page(self, page_num, index, col=None):
        """
        Read the value of a column for a specific record
        """
        # Check if the column number is provided and the page exists
        if col and (page:=self.page_range[col].get(page_num, None)): return page.read(index)

        # Check if the page exists in any range
        for range in self.page_range:
            if page:=range.get(page_num, None): return page.read(index)

        # Return None if the page does not exist
        return None
    
    def delete(self, rid: int):
        """
        Delete a record from the table
        """
        for i in range(self.num_columns):
            self.page_directory[i].pop(rid)
            self.index.delete_record(i, rid)

    def make_ver_copy(self):
        """
        Make a version copy of the table
        """
        self.versions.append([{k:[v[0], v[1]] for k,v in col.items()} for col in self.page_directory])