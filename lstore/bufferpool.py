from collections import OrderedDict
from typing import Optional, Dict, Tuple
import os
from threading import Lock, RLock
from .page import Page

class BufferPoolManager:
    """
    Manages the bufferpool for the database.
    Implements LRU replacement policy and handles page pinning/unpinning.
    Thread-safe implementation for concurrent access.
    """
    
    def __init__(self, pool_size: int):
        self.pool_size = pool_size
        self.pages: OrderedDict[Tuple[str, int, int], Page] = OrderedDict()  # (path, page_num, col) -> Page
        self.pin_counts: Dict[Tuple[str, int, int], int] = {}  # (path, page_num, col) -> pin_count
        self.dirty_pages: set[Tuple[str, int, int]] = set()  # Set of (path, page_num, col) for dirty pages
        self._lock = RLock()  # Use RLock to allow reentrant locking from same thread
        
    def get_page(self, path: str, page_num: int, col: int = None) -> Optional[Page]:
        """
        Gets a page from the bufferpool. If not in pool, loads from disk.
        Automatically pins the page. Thread-safe.
        """
        with self._lock:

            # Check if page is already in pool
            page_id = (path, page_num, col)
            
            # If page in pool, move to end (most recently used) and return
            if page_id in self.pages:
                self.pages.move_to_end(page_id)
                self.pin_counts[page_id] += 1
                return self.pages[page_id]
                
            # If pool is full, try to evict pages
            attempts = 0
            while len(self.pages) >= self.pool_size and attempts < 3:
                if not self._evict_page():
                    # If eviction failed, unpin all pages with pin count > 1
                    for pid in self.pin_counts:
                        if self.pin_counts[pid] > 1: self.pin_counts[pid] = 1
                attempts += 1
                
            # If still full after attempts, force evict least recently used page
            if len(self.pages) >= self.pool_size: self._force_evict_page()
                
            # Load page from disk
            page = Page(path, page_num, col)
            
            # Add to pool only if page exists or was created successfully
            if page is not None:
                self.pages[page_id] = page
                self.pin_counts[page_id] = 1
                return page
            
            # If page is not found, return None
            return None
            
    def _evict_page(self) -> bool:
        """
        Evicts the least recently used unpinned page.
        Returns True if successful, False if no pages can be evicted.
        Must be called with self._lock held.
        """

        # Iterate over pages in reverse order (most recently used first)
        for page_id in list(self.pages.keys()):

            # If page is unpinned, evict it
            if self.pin_counts[page_id] == 0:

                # If dirty, write back to disk
                if page_id in self.dirty_pages:
                    self.flush_page(page_id[0], page_id[1], page_id[2])
                    self.dirty_pages.remove(page_id)
                
                # Remove from pool
                self.pages.pop(page_id)
                self.pin_counts.pop(page_id)
                return True
                
        # If no pages can be evicted, return False
        return False
        
    def _force_evict_page(self) -> None:
        """
        Forces eviction of the least recently used page regardless of pin count.
        Must be called with self._lock held.
        """

        # If no pages in pool, return
        if not self.pages: return
            
        # Get least recently used page
        page_id = next(iter(self.pages))
        
        # If dirty, write back to disk
        if page_id in self.dirty_pages:
            self.flush_page(page_id[0], page_id[1], page_id[2])
            self.dirty_pages.remove(page_id)
        
        # Remove from pool
        self.pages.pop(page_id)
        self.pin_counts.pop(page_id)
        
    def pin_page(self, path: str, page_num: int, col: int = None) -> None:
        """
        Pins a page in memory. Thread-safe.
        """

        # Lock the bufferpool
        with self._lock:

            # Check if page is in pool and pinned
            page_id = (path, page_num, col)
            if page_id in self.pin_counts: self.pin_counts[page_id] += 1
                
    def unpin_page(self, path: str, page_num: int, col: int = None) -> None:
        """
        Unpins a page in memory. Thread-safe.
        """

        # Lock the bufferpool
        with self._lock:

            # Check if page is in pool and pinned
            page_id = (path, page_num, col)
            if page_id in self.pin_counts and self.pin_counts[page_id] > 0:

                # Decrement pin count
                self.pin_counts[page_id] -= 1
                
    def mark_dirty(self, path: str, page_num: int, col: int = None) -> None:
        """
        Marks a page as dirty. Thread-safe.
        """

        # Lock the bufferpool
        with self._lock:

            # Add page to dirty pages
            self.dirty_pages.add((path, page_num, col))
        
    def flush_page(self, path: str, page_num: int, col: int = None) -> None:
        """
        Writes a page back to disk. Thread-safe.
        """

        # Lock the bufferpool
        with self._lock:

            # Check if page is in pool
            page_id = (path, page_num, col)
            if page_id in self.pages:

                # Get page
                page = self.pages[page_id]

                # Ensure directory exists
                os.makedirs(os.path.dirname(page.path), exist_ok=True)

                # Write page content to disk
                page.flush_to_disk()
                
    def flush_all(self) -> None:
        """
        Writes all dirty pages back to disk. Thread-safe.
        """

        # Lock the bufferpool
        with self._lock:

            # Flush all dirty pages
            for page_id in self.dirty_pages.copy():
                self.flush_page(page_id[0], page_id[1], page_id[2])
                self.dirty_pages.remove(page_id)
                
    def clear(self) -> None:
        """
        Clears the bufferpool after flushing dirty pages. Thread-safe.
        """

        # Lock the bufferpool
        with self._lock:

            # Flush all dirty pages
            self.flush_all()

            # Clear the bufferpool
            self.pages.clear()
            self.pin_counts.clear()
            self.dirty_pages.clear() 