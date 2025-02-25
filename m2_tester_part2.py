from lstore.db import Database
from lstore.query import Query
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

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 10

seed(3562901)
# Time record simulation
sim_start_time = process_time()
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

# Simulate updates
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            records[key][j] = value
sim_end_time = process_time()
print(f"Record simulation took: {sim_end_time - sim_start_time:.2f} seconds")

# Time the select operations
select_start_time = process_time()
select_count = 0
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    select_count += 1
select_end_time = process_time()
print(f"Select {select_count} records took: {select_end_time - select_start_time:.2f} seconds")

# Time the aggregate operations
aggregate_start_time = process_time()
aggregate_count = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    aggregate_count += 1
aggregate_end_time = process_time()
print(f"Aggregate {aggregate_count} operations took: {aggregate_end_time - aggregate_start_time:.2f} seconds")

# Time the delete operations
delete_start_time = process_time()
deleted_keys = sample(keys, 100)
for key in deleted_keys:
    query.delete(key)
    records.pop(key, None)
delete_end_time = process_time()
print(f"Delete 100 records took: {delete_end_time - delete_start_time:.2f} seconds")

# Time database close operation
close_start_time = process_time()
db.close()
close_end_time = process_time()
print(f"Database close operation took: {close_end_time - close_start_time:.2f} seconds")

# Print total time
total_end_time = process_time()
print(f"\nTotal test time: {total_end_time - total_start_time:.2f} seconds")
