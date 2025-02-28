import threading

class AbortTransactionException(Exception):
    pass

class LockManager:
    def __init__(self):
        self.lock_table = {}
        self.table_lock = threading.RLock()

    def acquire(self, transaction, resource, mode):
        with self.table_lock:
            entry = self.lock_table.setdefault(resource, {
                "lock_type": None, "holders": set(), "waiting": [], 
                "condition": threading.Condition(self.table_lock)
            })
            # transaction already holds the lock
            if transaction in entry["holders"]:
                if entry["lock_type"] == 'S' and mode == 'X':
                    # requesting upgrade from S to X
                    if len(entry["holders"]) == 1:  
                        # only this transaction holds it, upgrade immediately
                        entry["lock_type"] = 'X'
                        return True
                    for other in entry["holders"]:
                        if other is not transaction and other.timestamp < transaction.timestamp:
                            # current transaction is younger than an older holder – abort
                            return False
                    entry["waiting"].append((transaction, 'X'))  # add upgrade request to queue
                    while not (len(entry["holders"]) == 1 and transaction in entry["holders"]):
                        entry["condition"].wait() 
                        if transaction.aborted:  # check if aborted during wait
                            return False
                    # upgrade lock
                    entry["lock_type"] = 'X'
                    entry["waiting"] = [(tx,m) for (tx,m) in entry["waiting"] if tx is not transaction]
                    return True
                return True

            # no existing lock or compatible shared lock
            if entry["lock_type"] is None or (entry["lock_type"] == 'S' and mode == 'S'):
                entry["holders"].add(transaction)
                entry["lock_type"] = 'X' if mode == 'X' else 'S'
                return True

            # determine wait or abort
            for holder in entry["holders"]:
                if holder.timestamp < transaction.timestamp:
                    return False
            entry["waiting"].append((transaction, mode))
            while True:
                entry["condition"].wait()  # wait until notified
                if transaction.aborted:
                    return False
                # check if lock can be granted now
                if entry["lock_type"] is None or (mode == 'S' and entry["lock_type"] == 'S'):
                    # Grant lock
                    entry["holders"].add(transaction)
                    entry["lock_type"] = 'X' if mode == 'X' else ('S' if entry["lock_type"] is None else 'S')
                    # Remove from waiting queue
                    entry["waiting"] = [(tx,m) for (tx,m) in entry["waiting"] if tx is not transaction]
                    return True

    def release(self, transaction, resource):
        with self.table_lock:
            entry = self.lock_table.get(resource)
            if not entry or transaction not in entry["holders"]:
                return
            # remove holder
            entry["holders"].remove(transaction)
            if len(entry["holders"]) == 0:
                entry["lock_type"] = None  # free now
            elif entry["lock_type"] == 'X':
                entry["lock_type"] = None
            if entry["lock_type"] is None and entry["waiting"]:
                txn, mode = entry["waiting"][0]
                if mode == 'X':
                    # Grant exclusive to first in queue
                    entry["waiting"].pop(0)
                    entry["holders"].add(txn)
                    entry["lock_type"] = 'X'
                    entry["condition"].notify_all()  # wake up waiting threads
                    # Grant all waiting shared
                else:
                    for _ in range(len(entry["waiting"])):
                        if entry["waiting"] and entry["waiting"][0][1] == 'S':
                            txn, mode = entry["waiting"].pop(0)
                            entry["holders"].add(txn)
                    entry["lock_type"] = 'S'
                    entry["condition"].notify_all()  # wake all waiting
            else:
                entry["condition"].notify_all()