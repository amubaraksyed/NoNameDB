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
        self.used = True
        self.last_page_number = self.num_columns*16+self.num_columns
        cnt = 0
        for i in range(1, self.num_columns*16+1):
            self.page_range[cnt][i] = Page(self.path, i)
            if i%16==0:
                self.page_range[cnt][self.num_columns*16+1+cnt] = Page(self.path, i)
                cnt += 1

    def restart_table(self):
        self.last_page_number = 0
        data = json.load(open(os.path.join(self.path,'metadata.json'),))
        self.num_columns = int(data["columns"])
        self.key_col = int(data["key_col"])
        page_dir = json.load(open(os.path.join(self.path,'page_directory.json'),))
        self.page_directory = [{int(k):[int(v[0]), int(v[1])] for k,v in col.items()} for col in page_dir]
        page_range = json.load(open(os.path.join(self.path,'page_range.json'),))
        self.page_range = [{int(page_num): Page(self.path, int(page_num)) for page_num in range} for range in page_range]
        for range in page_range:
            if (num:=max(range)) > self.last_page_number:
                self.last_page_number = num
        versions = json.load(open(os.path.join(self.path,'versions.json'),))
        self.versions = [[{int(k):[int(v[0]), int(v[1])] for k,v in col.items()} for col in version] for version in versions]
        self.index.restart_index()

    def save(self):
        with open(os.path.join(self.path,'page_directory.json'),"w") as file:
            file.write(json.dumps(self.page_directory))

        value = []
        for col in self.page_range:
            value.append(list(col.keys()))
        with open(os.path.join(self.path,'page_range.json'),"w") as file:
            file.write(json.dumps(value))

        with open(os.path.join(self.path,'versions.json'),"w") as file:
            file.write(json.dumps(self.versions))

        data = {"columns": self.num_columns, "key_col": self.key_col}
        with open(os.path.join(self.path,'metadata.json'),"w") as file:
            file.write(json.dumps(data))

    def write(self, columns: list):
        rid = columns[self.key_col]
        if rid in self.page_directory[self.key_col]:
            return None
        for i in range(self.num_columns):
            page = None
            index = None
            page_num = None
            for k, v in self.page_range[i].items():
                if v.has_capacity():
                    page_num = k
                    page = v
                    index = page.num_records() if page.num_records() is not None else -1
                    break
            if page_num == None or index == None or page == None:
                index = 0
                self.last_page_number+=1
                page_num = self.last_page_number
                page = Page(self.path, self.last_page_number)
                self.page_range[i][page_num] = page
            else:
                index += 1
            check = page.write(columns[i])
            if check:
                self.index.add_or_move_record_by_col(i, rid, columns[i])
                self.page_directory[i][rid] = [page_num, index]

    def update(self, columns: list):
        rid = columns[self.key_col]
        for i in range(self.num_columns):
            page = self.page_directory[i][rid]
            if not columns[i] == None and not self.read_page(page[0], page[1], i) == columns[i]:
                page = None
                index = None
                page_num = None
                for k, v in self.page_range[i].items():
                    if v.has_capacity():
                        page_num = k
                        page = v
                        index = page.num_records() if page.num_records() is not None else -1
                        break
                if page_num == None or index == None or page == None:
                    index = 0
                    self.last_page_number+=1
                    page_num = self.last_page_number
                    page = Page(self.path, self.last_page_number)
                    self.page_range[i][page_num] = page
                else:
                    index += 1
                check = page.write(columns[i])
                if check:
                    self.index.add_or_move_record_by_col(i, rid, columns[i])
                    self.page_directory[i][rid] = [page_num, index]

    def read_records(self, col_num: int, search_key: int, proj_col: list) -> list[Record]:
        records = []
        rids = []
        cnt = 0
        if col_num == self.key_col:
            rids.append(search_key)
        else:
            if not self.is_history and self.index.indices[col_num]:
                rids = self.index.get_rid_in_col_by_value(col_num, search_key)
            else:
                for k, v in self.page_directory[col_num].items():
                    if self.read_page(v[0], v[1])==search_key:
                        rids.append(k)
        for rid in rids:
            if rid in self.page_directory[self.key_col]:
                col = []
                for cnt in range(self.num_columns):
                    if proj_col[cnt] == 1:
                        col.append(self.read_value(cnt, rid))
                    else:
                        col.append(None)
                records.append(Record(rid, self.key_col, col))
        return records
    
    def read_value(self, col: int, rid: int) -> int:
        if not rid in self.page_directory[col]:
            return None
        if not self.is_history and self.index.indices[col]:
            return self.index.get_value_in_col_by_rid(col, rid)
        page_details = self.page_directory[col][rid]
        return self.read_page(page_details[0], page_details[1])
            
    def read_page(self, page_num, index, col=None):
        if col and (page:=self.page_range[col].get(page_num, None)):
            return page.read(index)
        for range in self.page_range:
            if page:=range.get(page_num, None):
                return page.read(index)
    
    def delete(self, rid: int):
        for i in range(self.num_columns):
            self.page_directory[i].pop(rid)
            self.index.delete_record(i, rid)

    def make_ver_copy(self):
        self.versions.append([{k:[v[0], v[1]] for k,v in col.items()} for col in self.page_directory])