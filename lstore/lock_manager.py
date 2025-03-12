from threading import Lock
from typing import Dict, Set, Tuple
from enum import Enum

class LockType(Enum):
    SHARED = 0      # For reads
    EXCLUSIVE = 1   # For writes

class LockManager:
    def __init__(self):
        self._lock_dict: Dict[Tuple[str, int], Dict[int, LockType]] = {}  # (table_name, rid) -> {transaction_id: lock_type}
        self._manager_lock = Lock()  # For thread-safe lock dictionary access

    def acquire_lock(self, table_name: str, rid: int, transaction_id: int, lock_type: LockType) -> bool:
        """
        Attempts to acquire a lock for a transaction. Returns immediately if lock cannot be granted (No Wait policy).
        Returns True if lock acquired, False if should abort.
        """
        with self._manager_lock:
            key = (table_name, rid)
            
            # If record has no locks yet, create new entry
            if key not in self._lock_dict:
                self._lock_dict[key] = {transaction_id: lock_type}
                return True

            current_locks = self._lock_dict[key]

            # If transaction already has the lock, check for upgrade
            if transaction_id in current_locks:
                if current_locks[transaction_id] == lock_type:
                    return True  # Already has the requested lock
                if current_locks[transaction_id] == LockType.SHARED and lock_type == LockType.EXCLUSIVE:
                    # Can only upgrade if no other transactions hold shared locks
                    if len(current_locks) == 1:
                        current_locks[transaction_id] = LockType.EXCLUSIVE
                        return True
                    return False  # Must abort - can't upgrade with other shared locks

            # Check if lock can be granted based on current locks
            if lock_type == LockType.SHARED:
                # Can get shared lock if no exclusive locks exist
                if any(lt == LockType.EXCLUSIVE for lt in current_locks.values()):
                    return False
                current_locks[transaction_id] = LockType.SHARED
                return True
            else:  # Exclusive lock
                # Can only get exclusive lock if no other locks exist
                if len(current_locks) > 0:
                    return False
                current_locks[transaction_id] = LockType.EXCLUSIVE
                return True

    def release_lock(self, table_name: str, rid: int, transaction_id: int) -> None:
        """
        Releases all locks held by transaction_id on the specified record.
        """
        with self._manager_lock:
            key = (table_name, rid)
            if key in self._lock_dict and transaction_id in self._lock_dict[key]:
                del self._lock_dict[key][transaction_id]
                if not self._lock_dict[key]:  # If no more locks on this record
                    del self._lock_dict[key]

    def release_all_locks(self, transaction_id: int) -> None:
        """
        Releases all locks held by a transaction (used during abort/commit).
        """
        with self._manager_lock:
            keys_to_delete = []
            for key in self._lock_dict:
                if transaction_id in self._lock_dict[key]:
                    del self._lock_dict[key][transaction_id]
                    if not self._lock_dict[key]:
                        keys_to_delete.append(key)
            
            for key in keys_to_delete:
                del self._lock_dict[key] 