# from lstore.index import Index
from time import time
from lstore.page import Page, PageRange
from lstore.index import Index
from lstore.config import *
from datetime import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# BASE_RID_COLUMN = 4
# TPS_COLUMN = 5

NUM_OF_META_COLUMNS = 4


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def setRID(self):
        self.rid = self.columns[self.key]


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_column: int      #Index of table key in columns
    """

    def __init__(self, name, num_columns, key_column, buffer_pool):

        # Table Attributes
        self.name = name
        self.key_column = key_column
        self.num_columns = num_columns  # 4 is added for initial columns
        self.buffer_pool = buffer_pool
        self.table_file_name = 'table.info'

        # Page Range List
        self.page_range_index = 0
        self.page_range_list = [PageRange(self.name, self.page_range_index, buffer_pool)]
        for i in range(self.num_columns + NUM_OF_META_COLUMNS):
            self.page_range_list[self.page_range_index].add_page(i)

            # for tail pages
        self.tail_page_range_index = -1

        self.record_start_page_index = 0
        self.tail_record_start_page_index = 0

        # Indexing
        self.page_directory = {}
        self.tail_page_directory = {}

        # till index get ready, using array to store unique key values
        self.primary_key_values = {}
        self.rid = 0
        self.index = Index(self)
        for column_number in range(self.num_columns):
            if column_number != self.key_column:
                self.index.create_index(column_number)

    def write(self, record):

        # Placeholder Values for the Address
        page_range_index = 0
        page_index = 0
        data_index = 0

        # Check if Record Already Exists in the Table
        if record[self.key_column] in self.primary_key_values:
            print(f"Error: Record with Key {record[self.key_column]} is already in the Table.")
            return

        # For each column of data in the record
        for i, data in enumerate(record):
            # first 4 columns reserved for indirection, rid, timestamp and encoding

            # Get the Current Page
            page = self.getPage(i + NUM_OF_META_COLUMNS)

            # Write the Column Data to the Page
            page.write(data)
            # update the index
            self.index.insert_index_value(i, data, self.rid)

        # now write first 4 columns

        page_range = self.page_range_list[self.page_range_index]
        # write indirection column
        page = page_range.get_page(self.record_start_page_index + INDIRECTION_COLUMN)
        page.write(0)

        # write Rid column
        page = page_range.get_page(self.record_start_page_index + RID_COLUMN)
        page.write(self.rid)

        # write timestamp
        page = page_range.get_page(self.record_start_page_index + TIMESTAMP_COLUMN)
        page.write(int(round(datetime.now().timestamp())))

        # Schema encoding column
        page = page_range.get_page(self.record_start_page_index + SCHEMA_ENCODING_COLUMN)
        page.write(0)

        # Store the Record Location in the Page Directory
        # Get the Address of the Written Data
        page_range_index = self.page_range_index
        record_start_page_index = self.record_start_page_index
        data_index = page.get_current_index()

        self.page_directory[self.rid] = (page_range_index, record_start_page_index, data_index, True)
        self.primary_key_values[record[self.key_column]] = self.rid
        self.rid = self.rid + 1

    def getPage(self, columnIndex):
        page_range = self.page_range_list[self.page_range_index]
        page = page_range.get_page(self.record_start_page_index + columnIndex)

        # Start a New Page if the Current Page has Reached Maximum Capacity
        if not page.has_capacity():

            # Check if the Page Range Can Accomodate another Page
            if page_range.has_capacity() and (
                    page_range.num_pages + self.num_columns + NUM_OF_META_COLUMNS) < PAGES_PER_RANGE:
                for i in range(self.num_columns + NUM_OF_META_COLUMNS):
                    page_range.add_page(i)
                self.record_start_page_index += self.num_columns

            # Otherwise Start a New Page Range
            else:
                self.page_range_list.append(PageRange(self.name, len(self.page_range_list), self.buffer_pool))
                self.page_range_index += 1
                page_range = self.page_range_list[self.page_range_index]
                for i in range(self.num_columns + NUM_OF_META_COLUMNS):
                    page_range.add_page(i)
                self.record_start_page_index = 0

        return page_range.get_page(self.record_start_page_index + columnIndex)

    def addPages(self, page_range):
        for i in range(self.num_columns + NUM_OF_META_COLUMNS):
            page_range.add_page(i)

    def getTailPage(self, columnIndex):
        if self.tail_page_range_index == -1:
            self.page_range_list.append(PageRange(self.name, len(self.page_range_list), self.buffer_pool))
            self.addPages(self.page_range_list[len(self.page_range_list) - 1])
            self.tail_page_range_index = len(self.page_range_list) - 1

        page_range = self.page_range_list[self.tail_page_range_index]
        page = page_range.get_page(self.tail_record_start_page_index + columnIndex)

        # Start a New Page if the Current Page has Reached Maximum Capacity
        if not page.has_capacity():

            # Check if the Page Range Can Accomodate another Page
            if page_range.has_capacity() and (
                    page_range.num_pages + self.num_columns + NUM_OF_META_COLUMNS) < PAGES_PER_RANGE:
                for i in range(self.num_columns + NUM_OF_META_COLUMNS):
                    page_range.add_page(i)
                self.tail_record_start_page_index += self.num_columns

            # Otherwise Start a New Page Range
            else:
                self.page_range_list.append(PageRange(self.name, len(self.page_range_list), self.buffer_pool))
                self.tail_page_range_index = len(self.page_range_list) - 1
                page_range = self.page_range_list[self.tail_page_range_index]
                for i in range(self.num_columns + NUM_OF_META_COLUMNS):
                    page_range.add_page(i)
                self.tail_record_start_page_index = 0

        return page_range.get_page(self.tail_record_start_page_index + columnIndex)

    def delete(self, key_value):
        if key_value not in self.primary_key_values:
            print(f"Record with keyvalue {key_value} doesnt exist")
            return

        record_rid = self.primary_key_values[key_value]
        page_range_index, record_start_index, data_index, IsValid = self.page_directory[record_rid]
        if not IsValid:
            print(f"Record with keyvalue {key_value} already deleted")
            return

        # drop index values
        is_indirected = self.getRecordValue(page_range_index, record_start_index, data_index, INDIRECTION_COLUMN)
        if is_indirected == 1:
            record_rid = self.getRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN)
            page_range_index, record_start_index, data_index, IsValid = self.tail_page_directory[record_rid]
        for i in range(self.num_columns):
            value = self.getRecordValue(page_range_index, record_start_index, data_index, i + NUM_OF_META_COLUMNS)
            self.index.delete_index_value(i, value, record_rid)

        # drop the records now
        self.page_directory[record_rid] = (page_range_index, record_start_index, data_index, False)
        record_rid = self.getRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN)
        is_indirected = self.getRecordValue(page_range_index, record_start_index, data_index, INDIRECTION_COLUMN)
        if is_indirected == 1:
            while True:
                if record_rid == 0:
                    break
                page_range_index, record_start_index, data_index, IsValid = self.tail_page_directory[record_rid]
                if not IsValid:
                    break
                self.tail_page_directory[record_rid] = (page_range_index, record_start_index, data_index, False)
                record_rid = self.getRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN)

    def update(self, primary_key, record):

        # Get the Address of the Record with the Given Primary Key
        if primary_key not in self.primary_key_values:
            print(f"Error: Record with primary key {primary_key} was not found in the Table.")
        old_rid = record_rid = self.primary_key_values[primary_key]

        # Unpack the Address
        base_page_range_index, base_record_start_index, base_data_index, IsValid = self.page_directory[record_rid]

        # Update the Data in All Columns

        # get latest record
        page = self.page_range_list[base_page_range_index].get_page(base_record_start_index + INDIRECTION_COLUMN)
        indirect_value = page.read(base_data_index)

        # if indirect column value is non-zero that means record is updated
        if indirect_value != 0:
            page = self.page_range_list[base_page_range_index].get_page(base_record_start_index + RID_COLUMN)
            # read rid value of updated record
            record_rid = page.read(base_data_index)
            old_rid = record_rid
            # updated record physical address
            updated_record_page_range_index, updated_record_record_start_index, updated_record_data_index, IsValid = \
            self.tail_page_directory[record_rid]

        encoding_scheme = 0
        for i, value in enumerate(record):
            if indirect_value != 0:
                old_val = self.getRecordValue(updated_record_page_range_index, updated_record_record_start_index,
                                              updated_record_data_index, i + NUM_OF_META_COLUMNS)
            else:
                old_val = self.getRecordValue(base_page_range_index, base_record_start_index, base_data_index,
                                              i + NUM_OF_META_COLUMNS)

            if value is None:
                val = old_val
            else:
                val = value
                encoding_scheme |= 1 << i

            # first 4 columns reserved for indirection, rid, timestamp and encoding
            # Get the Current Page
            page = self.getTailPage(i + NUM_OF_META_COLUMNS)

            # Write the Column Data to the Page
            page.write(val)
            self.index.update_index_value(i, old_val, val, old_rid, self.rid)

        # now update values in records
        self.tail_page_directory[self.rid] = (
        self.tail_page_range_index, self.tail_record_start_page_index, page.get_current_index(), True)
        self.rid += 1

        # update base record rid
        self.updateRecordValue(base_page_range_index, base_record_start_index, base_data_index, RID_COLUMN,
                               self.rid - 1)
        self.updateRecordValue(base_page_range_index, base_record_start_index, base_data_index, INDIRECTION_COLUMN, 1)
        old_encoding_val = self.getRecordValue(base_page_range_index, base_record_start_index, base_data_index,
                                               SCHEMA_ENCODING_COLUMN)
        self.updateRecordValue(base_page_range_index, base_record_start_index, base_data_index, SCHEMA_ENCODING_COLUMN,
                               old_encoding_val | encoding_scheme)

        # update last updated record
        if indirect_value != 0:
            self.updateRecordValue(updated_record_page_range_index, updated_record_record_start_index,
                                   updated_record_data_index, RID_COLUMN, record_rid)
            self.updateRecordValue(updated_record_page_range_index, updated_record_record_start_index,
                                   updated_record_data_index, TIMESTAMP_COLUMN, int(round(datetime.now().timestamp())))

    def serialize_table(self):
        table_str = self.name + '_' + str(self.num_columns) + '_' + str(self.key_column)
        with open(self.buffer_pool.path + "\\" + self.table_file_name, 'w') as file:
            file.write(table_str)

    def getRecordValue(self, page_range_index, record_start_index, data_index, column_index):
        return self.page_range_list[page_range_index].get_page(record_start_index + column_index).read(data_index)

    def updateRecordValue(self, page_range_index, record_start_index, data_index, column_index, value):
        self.page_range_list[page_range_index].get_page(record_start_index + column_index).update(data_index, value)

    def select(self, search_key, search_column, select_columns):
        physical_addresses = []

        rids = self.index.locate(search_column, search_key)
        for rid_value in rids:
            if rid_value in self.page_directory:
                page_range_index, record_start_index, data_index, isValid = self.page_directory[rid_value];
                if isValid:
                    physical_addresses.append((rid_value, page_range_index, record_start_index, data_index))
            if rid_value in self.tail_page_directory:
                page_range_index, record_start_index, data_index, isValid = self.tail_page_directory[rid_value];
                if isValid:
                    physical_addresses.append((rid_value, page_range_index, record_start_index, data_index))

        records = []
        for rid_value, page_range_index, record_start_index, data_index in physical_addresses:
            columns = []
            for i, val in enumerate(select_columns):
                if val == 1:
                    columns.append(
                        self.getRecordValue(page_range_index, record_start_index, data_index, i + NUM_OF_META_COLUMNS))
                else:
                    columns.append(None)

            record = Record(rid_value, search_key, columns)
            records.append(record)
        return records

    def find_value(self, column_index, location):

        # Unpack the Address
        page_range_index, page_index, data_index = location

        # Locate the Data
        page_range = self.page_range_list[page_range_index][column_index]
        page = page_range.get_page(page_index)
        data = page.read(data_index)

        return data

    def sum(self, start_index, end_index, column):
        sumVal = 0
        for key_val in range(start_index, end_index + 1):
            rid = self.index.locate(self.key_column, key_val)
            if rid[0] is None:
                continue
            if rid[0] in self.page_directory:
                page_range_index, record_start_index, data_index, IsValid = self.page_directory[rid[0]]
            elif rid[0] in self.tail_page_directory:
                page_range_index, record_start_index, data_index, IsValid = self.tail_page_directory[rid[0]]

            if not IsValid:
                continue

            # check whether record is updated or not, then get updated record
            is_indirected = self.getRecordValue(page_range_index, record_start_index, data_index, INDIRECTION_COLUMN)
            if is_indirected == 1:
                rid = self.getRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN)
                page_range_index, record_start_index, data_index, IsValid = self.tail_page_directory[rid]

            if not IsValid:
                continue
            sumVal = sumVal + self.getRecordValue(page_range_index, record_start_index, data_index,
                                                  column + NUM_OF_META_COLUMNS)
        return sumVal

    def get_column_data(self, column_number):
        column_data = {}
        for rid_value in self.page_directory:
            page_range_index, record_start_index, data_index, isValid = self.page_directory[rid_value]
            if isValid:
                is_indirect = self.getRecordValue(page_range_index, record_start_index, data_index, INDIRECTION_COLUMN)
                if is_indirect == 1:
                    updated_rid_value = self.getRecordValue(page_range_index, record_start_index, data_index,
                                                            RID_COLUMN)
                    page_range_index, record_start_index, data_index, IsValid = self.tail_page_directory[
                        updated_rid_value]
                column_value = self.getRecordValue(page_range_index, record_start_index, data_index,
                                                   column_number + NUM_OF_META_COLUMNS)
                column_data[column_value] = rid_value
        return column_data

    #    def update_value(self, column_index, location, value):
    #
    #        # Unpack the Address
    #        page_range_index, page_index, data_index = location
    #
    #        # Locate the Data
    #        page_range = self.page_range_list[page_range_index][column+index]
    #        page = page_range.get_page(page_index)
    #        page.update(data_index, value)
    #
    #
    #    def find_record(self, rid):
    #
    #        # Retrieve the Location using the Record ID
    #        location = self.page_directory[rid]
    #
    #        # Add the Record Values at the given location index
    #        record = []
    #        for i in range(self.num_columns):
    #            record.append(self.find_value(i, location))
    #
    #        # Return the Record
    #        return record

    # write a merging function for contention free merging

    def contention_free_merge(self):
        for rid in self.page_directory:
            page_range_index, record_start_index, data_index, isValid = self.page_directory[rid]
            if not isValid:
                continue
            rid_value = self.getRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN)
            if rid_value == 0:
                continue
            if rid_value not in self.tail_page_directory:
                continue
            page_range_index, record_start_index, data_index, isValid = self.tail_page_directory[rid_value]
            self.update_base_record(rid, rid_value)
            while True:
                self.tail_page_directory.__delitem__(rid_value)
                prev_rid_value = self.getRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN)
                if prev_rid_value == 0:
                    break
                page_range_index, record_start_index, data_index, isValid = self.tail_page_directory[prev_rid_value]
                rid_value = prev_rid_value

    def update_base_record(self, base_record_rid, updated_record_rid):
        update_page_range_index, update_record_start_index, update_data_index, update_isValid = \
        self.tail_page_directory[updated_record_rid]
        page_range_index, record_start_index, data_index, isValid = self.tail_page_directory[updated_record_rid]
        for i in range(self.num_columns):
            val = self.getRecordValue(update_page_range_index, update_record_start_index, update_data_index,
                                      i + NUM_OF_META_COLUMNS)
            self.updateRecordValue(page_range_index, record_start_index, data_index, i + NUM_OF_META_COLUMNS, val)

        self.updateRecordValue(page_range_index, record_start_index, data_index, RID_COLUMN, 0)
        self.updateRecordValue(page_range_index, record_start_index, data_index, INDIRECTION_COLUMN, 0)
