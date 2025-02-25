from collections import OrderedDict
from typing import Optional, Dict, Tuple
import os
from .page import Page

class BufferPoolManager:
    """
    Manages the bufferpool for the database.
    Implements LRU replacement policy and handles page pinning/unpinning.
    """
    def __init__(self, pool_size: int):
        self.pool_size = pool_size
        self.pages: OrderedDict[Tuple[str, int], Page] = OrderedDict()  # (path, page_num) -> Page
        self.pin_counts: Dict[Tuple[str, int], int] = {}  # (path, page_num) -> pin_count
        self.dirty_pages: set[Tuple[str, int]] = set()  # Set of (path, page_num) for dirty pages
        
    def get_page(self, path: str, page_num: int) -> Optional[Page]:
        """
        Gets a page from the bufferpool. If not in pool, loads from disk.
        Automatically pins the page.
        """
        page_id = (path, page_num)
        
        # If page in pool, move to end (most recently used) and return
        if page_id in self.pages:
            self.pages.move_to_end(page_id)
            self.pin_counts[page_id] += 1
            return self.pages[page_id]
            
        # If pool is full, evict pages until we have space
        while len(self.pages) >= self.pool_size:
            self._evict_page()
            
        # Load page from disk
        page = Page(os.path.dirname(path), page_num)
        
        # Add to pool
        self.pages[page_id] = page
        self.pin_counts[page_id] = 1
        
        return page
        
    def _evict_page(self) -> bool:
        """
        Evicts the least recently used unpinned page.
        Returns True if successful, False if no pages can be evicted.
        """
        for page_id in self.pages:
            if self.pin_counts[page_id] == 0:
                # If dirty, write back to disk
                if page_id in self.dirty_pages:
                    # Write page content to disk
                    self.flush_page(page_id[0], page_id[1])
                    self.dirty_pages.remove(page_id)
                
                # Remove from pool
                self.pages.pop(page_id)
                self.pin_counts.pop(page_id)
                return True
                
        return False
        
    def pin_page(self, path: str, page_num: int) -> None:
        """
        Pins a page in memory
        """
        page_id = (path, page_num)
        if page_id in self.pin_counts:
            self.pin_counts[page_id] += 1
            
    def unpin_page(self, path: str, page_num: int) -> None:
        """
        Unpins a page in memory
        """
        page_id = (path, page_num)
        if page_id in self.pin_counts and self.pin_counts[page_id] > 0:
            self.pin_counts[page_id] -= 1
            
    def mark_dirty(self, path: str, page_num: int) -> None:
        """
        Marks a page as dirty
        """
        self.dirty_pages.add((path, page_num))
        
    def flush_page(self, path: str, page_num: int) -> None:
        """
        Writes a page back to disk
        """
        page_id = (path, page_num)
        if page_id in self.pages:
            page = self.pages[page_id]
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # Write page content to disk - this will be implemented in Page class
            page.flush_to_disk()
            
    def flush_all(self) -> None:
        """
        Writes all dirty pages back to disk
        """
        for page_id in self.dirty_pages.copy():
            self.flush_page(page_id[0], page_id[1])
            self.dirty_pages.remove(page_id)
            
    def clear(self) -> None:
        """
        Clears the bufferpool after flushing dirty pages
        """
        self.flush_all()
        self.pages.clear()
        self.pin_counts.clear()
        self.dirty_pages.clear() 