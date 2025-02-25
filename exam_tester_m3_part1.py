from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from time import process_time
from random import choice, randint, sample, seed

# Start timing the entire test
total_start_time = process_time()

# Time database open operation
open_start_time = process_time()
db = Database()
db.open('./ECS165')
open_end_time = process_time()
print(f"Database open operation took: {open_end_time - open_start_time:.2f} seconds")

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
num_threads = 8

# create index on the non primary columns
try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())
    
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(insert_transactions[i])

# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()

# Check inserted records using select query in the main thread outside workers
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

# Time the select operations
select_start_time = process_time()
select_count = 0
for key in records:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    select_count += 1
select_end_time = process_time()
print(f"Select {select_count} records took: {select_end_time - select_start_time:.2f} seconds")

# Time the update operations
update_start_time = process_time()
updated_records = {}
update_count = 0
for key in records:
    updated_columns = [None, None, None, None, None]
    updated_records[key] = records[key].copy()
    for i in range(2, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        updated_records[key][i] = value
    query.update(key, *updated_columns)
    update_count += 1

    # Check version -1
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on version -1:', records[key], 'and', updated_columns, ':', record)

    # Check version -2
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on version -2:', records[key], 'and', updated_columns, ':', record)
    
    # Check version 0
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != updated_records[key][j]:
            error = True
    if error:
        print('update error on version 0:', records[key], 'and', updated_columns, ':', record)
update_end_time = process_time()
print(f"Update and version checks for {update_count} records took: {update_end_time - update_start_time:.2f} seconds")

# Time the aggregate operations
aggregate_start_time = process_time()
aggregate_count = 0
keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_records):
        r = sorted(sample(range(0, len(keys)), 2))
        
        # Version -1 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -1)
        if column_sum != result:
            print('sum error on version -1:', result, ', correct:', column_sum)
        
        # Version -2 sum
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -2)
        if column_sum != result:
            print('sum error on version -2:', result, ', correct:', column_sum)
        
        # Version 0 sum
        updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
        updated_result = query.sum_version(keys[r[0]], keys[r[1]], c, 0)
        if updated_column_sum != updated_result:
            print('sum error on version 0:', updated_result, ', correct:', updated_column_sum)
        
        aggregate_count += 3  # Count all three version checks
aggregate_end_time = process_time()
print(f"Aggregate operations ({aggregate_count} total version checks) took: {aggregate_end_time - aggregate_start_time:.2f} seconds")

# Time database close operation
close_start_time = process_time()
db.close()
close_end_time = process_time()
print(f"Database close operation took: {close_end_time - close_start_time:.2f} seconds")

# Print total time
total_end_time = process_time()
print(f"\nTotal test time: {total_end_time - total_start_time:.2f} seconds")
