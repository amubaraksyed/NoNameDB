from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

import threading

from random import choice, randint, sample, seed     

def correctness_tester1():
    db = Database()
    db.open('./CT1')

    record_num = 20
    thread_cnt = 3

    # creating grades table
    grades_table = db.create_table('CT1', 5, 0)
    query = Query(grades_table)

    for i in range(record_num):
        query.insert(i, 0, 0, 0, 0)

    transaction_workers = []
    for j in range(thread_cnt):
        transaction_workers.append(TransactionWorker())


    # create 60 transactions, each has 1 queries
    # each transaction_worker runs 20 transactions
    for j in range(thread_cnt):
        update_transactions = []
        for i in range(record_num):
            update_transactions.append(Transaction())
            update_transactions[i].add_query(query.update, grades_table, i, *[None, 2+j, 2+j, 2+j, 2+j])
            transaction_workers[j].add_transaction(update_transactions[i])
    
    for j in range(thread_cnt):
        transaction_workers[j].run()

    for j in range(thread_cnt):
        transaction_workers[j].join()

    for i in range(record_num):
        result = query.select(i, 0, [1, 1, 1, 1, 1])[0].columns
        if not (result[1] == result[2] and result[1] == result[3] and result[1] == result[4]):
            print("Wrong Result:", result)
            break

    db.close()

def correctness_tester2():
    db = Database()
    db.open('./CT2')

    record_num = 20
    thread_cnt = 3

    # creating grades table
    grades_table = db.create_table('CT2', 5, 0)
    query = Query(grades_table)

    for i in range(record_num):
        query.insert(i, 0, 0, 0, 0)

    transaction_workers = []
    for j in range(thread_cnt):
        transaction_workers.append(TransactionWorker())

    # create 3 transactions, each has 20 queries
    # each transaction_worker runs 1 transaction
    for j in range(thread_cnt):
        update_transaction = Transaction()
        for i in range(record_num):
            update_transaction.add_query(query.update, grades_table, i, *[None, 2+j, 2+j, 2+j, 2+j])
        transaction_workers[j].add_transaction(update_transaction)
    
    for j in range(thread_cnt):
        transaction_workers[j].run()

    for j in range(thread_cnt):
        transaction_workers[j].join()

    for i in range(record_num):
        result1 = query.select(i, 0, [1, 1, 1, 1, 1])[0].columns
        result2 = query.select((i+1)%record_num, 0, [1, 1, 1, 1, 1])[0].columns
        if not (result1[1] == result1[2] and result1[1] == result1[3] and result1[1] == result1[4]):
            print("Wrong Result:", result1)
            break
        if result1[1] != result2[1] :
            print("Wrong Results:", result1, result2)
            break

    db.close()

def deadlock_tester():
    ######################################
    # M3 2PL TESTER (using 2PL)
    ######################################
    tests = {}
    test_count = 0
    m3_count = 0
    try:
        db = Database()
        db.open('./2PL')
        table = db.create_table('2PL', 2, 0)
        query = Query(table)

        # Insert 2 initial record
        query.insert(1, 100)
        query.insert(2, 200)

        # Construct tx1: Update record 1 first, then update record 2 after a delay
        tx1 = Transaction()
        tx1.add_query(query.update, table, 1, 1, 101)
        # Deliberately delay some time with multiple select
        for i in range(0, 10000):
            tx1.add_query(query.select, table, 1, 0, [1, 1])
        tx1.add_query(query.update, table, 2, 2, 201)

        # Construct tx2: Update record 2 first, then update record 1 after a delay
        tx2 = Transaction()
        tx2.add_query(query.update, table, 2, 2, 202)
        # Deliberately delay some time with multiple select
        for i in range(0, 10000):
            tx2.add_query(query.select, table, 2, 0, [1, 1])
        tx2.add_query(query.update, table, 1, 1, 102)

        # Concurrently run 2 transactions
        worker1 = TransactionWorker()
        worker2 = TransactionWorker()
        worker1.add_transaction(tx1)
        worker2.add_transaction(tx2)
        thread1 = threading.Thread(target=lambda: (worker1.run(), worker1.join()))
        thread2 = threading.Thread(target=lambda: (worker2.run(), worker2.join()))
        thread1.start()
        thread2.start()
        thread1.join(timeout=10)
        thread2.join(timeout=10)

        # Check final result
        final_val_1 = query.select(1, 0, [1, 1])[0].columns
        final_val_2 = query.select(2, 0, [1, 1])[0].columns

        # Possible outcomes
        possible_outcomes = [
            (101, 201),
            (102, 202)
        ]
        if (final_val_1[1], final_val_2[1]) not in possible_outcomes:
            raise Exception("2PL tester: Unexpected final state. Atomicity Violation.")

        test_count += 1
        m3_count += 1
        tests["M3 2PL Test"] = {
            "status": "Passed",
            "message": "2PL Test passed."
        }
        print(tests)
        db.close()
    except Exception as e:
        print(e)

correctness_tester1()
correctness_tester2()
deadlock_tester()