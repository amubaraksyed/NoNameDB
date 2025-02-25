from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randint, sample, seed

# Start timing the entire test
total_start_time = process_time()

db = Database()
# Create a table  with 5 columns
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
records = {}

number_of_records = 1000
number_of_aggregates = 100
seed(3562901)

# Time the insert operations
insert_start_time = process_time()
for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)
    while key in records:
        key = 92106429 + randint(0, number_of_records)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
insert_end_time = process_time()
print(f"Insert {number_of_records} records took: {insert_end_time - insert_start_time:.2f} seconds")

# Time the select operations
select_start_time = process_time()
for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
select_end_time = process_time()
print(f"Select {number_of_records} records took: {select_end_time - select_start_time:.2f} seconds")

# Time the update operations
update_start_time = process_time()
update_count = 0
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(2, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
        update_count += 1
        updated_columns[i] = None
update_end_time = process_time()
print(f"Update {update_count} values took: {update_end_time - update_start_time:.2f} seconds")

keys = sorted(list(records.keys()))
# Time the aggregate operations
aggregate_start_time = process_time()
aggregate_count = 0
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        aggregate_count += 1
aggregate_end_time = process_time()
print(f"Aggregate {aggregate_count} operations took: {aggregate_end_time - aggregate_start_time:.2f} seconds")

# Print total time
total_end_time = process_time()
print(f"\nTotal test time: {total_end_time - total_start_time:.2f} seconds")
