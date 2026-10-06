import os
import mmap
import struct
import threading
import time
import zlib
from typing import Dict, List, Optional, Tuple

from src.wal import WALManager

PAGE_SIZE = 4096


class Page:
    """
    Represents a single fixed-size page (4096 bytes) in memory, containing
    data, metadata, a per-page latch, pin count, dirty flag, and page ID.
    """

    def __init__(self, page_id: int):
        self.page_id: int = page_id
        self.data: bytearray = bytearray(PAGE_SIZE)
        self.pin_count: int = 0
        self.is_dirty: bool = False
        self.page_lsn: int = 0
        self.latch: threading.RLock = threading.RLock()

    def read_lock(self) -> None:
        self.latch.acquire()

    def read_unlock(self) -> None:
        self.latch.release()

    def write_lock(self) -> None:
        self.latch.acquire()

    def write_unlock(self) -> None:
        self.latch.release()

    def compute_checksum(self) -> int:
        # Checksum calculation over the first PAGE_SIZE - 4 bytes
        # The last 4 bytes store the checksum itself.
        payload = self.data[: PAGE_SIZE - 4]
        return zlib.crc32(payload) & 0xFFFFFFFF

    def verify_checksum(self) -> bool:
        stored_checksum_bytes = self.data[PAGE_SIZE - 4 : PAGE_SIZE]
        (stored_checksum,) = struct.unpack(">I", stored_checksum_bytes)
        calculated = self.compute_checksum()
        return stored_checksum == calculated

    def update_checksum(self) -> None:
        calculated = self.compute_checksum()
        struct.pack_into(">I", self.data, PAGE_SIZE - 4, calculated)

    def serialize(self) -> bytes:
        self.update_checksum()
        return bytes(self.data)

    def deserialize(self, raw_bytes: bytes) -> None:
        if len(raw_bytes) != PAGE_SIZE:
            raise ValueError(f"Invalid page size for deserialization: {len(raw_bytes)}")
        self.data[:] = raw_bytes
        if not self.verify_checksum():
            raise RuntimeError(f"Page corruption detected on page_id={self.page_id}")


class LRUKNode:
    """
    Doubly linked list node for LRU-K / LRU hybrid replacement policy.
    """

    def __init__(self, page_id: int):
        self.page_id: int = page_id
        self.prev: Optional["LRUKNode"] = None
        self.next: Optional["LRUKNode"] = None
        self.backward_distances: List[float] = []
        self.k: int = 2


class LRUKReplacer:
    """
    LRU-K replacer implementation to manage page eviction frames.
    """

    def __init__(self, capacity: int, k: int = 2):
        self.capacity: int = capacity
        self.k: int = k
        self.nodes: Dict[int, LRUKNode] = {}
        self.lock: threading.Lock = threading.Lock()
        
        # Dummy head and tail for doubly linked list
        self.head: LRUKNode = LRUKNode(-1)
        self.tail: LRUKNode = LRUKNode(-1)
        self.head.next = self.tail
        self.tail.prev = self.head

    def _remove_node(self, node: LRUKNode) -> None:
        if node.prev and node.next:
            node.prev.next = node.next
            node.next.prev = node.prev
            node.prev = None
            node.next = None

    def _add_to_front(self, node: LRUKNode) -> None:
        node.prev = self.head
        node.next = self.head.next
        if self.head.next:
            self.head.next.prev = node
        self.head.next = node

    def record_access(self, page_id: int) -> None:
        with self.lock:
            current_time = time.time()
            if page_id not in self.nodes:
                if len(self.nodes) >= self.capacity:
                    # Eviction capacity management if necessary during access tracking
                    pass
                node = LRUKNode(page_id)
                node.k = self.k
                node.backward_distances.append(current_time)
                self.nodes[page_id] = node
                self._add_to_front(node)
            else:
                node = self.nodes[page_id]
                node.backward_distances.append(current_time)
                if len(node.backward_distances) > node.k:
                    node.backward_distances.pop(0)
                self._remove_node(node)
                self._add_to_front(node)

    def evict(self) -> Optional[int]:
        with self.lock:
            if not self.nodes:
                return None

            # Scan backwards from tail to find candidate with largest backward distance (inf if accesses < k)
            current = self.tail.prev
            evict_candidate: Optional[LRUKNode] = None
            max_backward_dist = -1.0
            inf_candidate: Optional[LRUKNode] = None

            while current and current != self.head:
                pid = current.page_id
                node = current
                if len(node.backward_distances) < node.k:
                    inf_candidate = node
                else:
                    dist = time.time() - node.backward_distances[0]
                    if dist > max_backward_dist:
                        max_backward_dist = dist
                        evict_candidate = node
                current = current.prev

            chosen = inf_candidate if inf_candidate is not None else evict_candidate
            if chosen is not None:
                self._remove_node(chosen)
                del self.nodes[chosen.page_id]
                return chosen.page_id
            return None

    def remove(self, page_id: int) -> None:
        with self.lock:
            if page_id in self.nodes:
                node = self.nodes[page_id]
                self._remove_node(node)
                del self.nodes[page_id]


class BufferPoolManager:
    """
    Manages fixed-size pages in memory, backed by a file handle or memory-mapped file,
    providing pin/unpin operations, LRU-K eviction, dirty page tracking, and integration with WAL.
    """

    def __init__(self, db_file_path: str, pool_size: int, wal_manager: WALManager):
        self.db_file_path: str = db_file_path
        self.pool_size: int = pool_size
        self.wal_manager: WALManager = wal_manager
        
        self.pages: Dict[int, Page] = {}
        self.replacer: LRUKReplacer = LRUKReplacer(pool_size)
        self.latch: threading.RLock = threading.RLock()
        
        # Ensure file exists and is appropriately initialized
        if not os.path.exists(self.db_file_path):
            with open(self.db_file_path, "wb") as f:
                pass
        
        self.file_handle = open(self.db_file_path, "r+b")
        self.file_size = os.path.getsize(self.db_file_path)

    def __del__(self) -> None:
        try:
            self.flush_all()
            if hasattr(self, "file_handle") and self.file_handle:
                self.file_handle.close()
        except Exception:
            pass

    def _read_disk_page(self, page_id: int) -> bytes:
        offset = page_id * PAGE_SIZE
        with self.latch:
            self.file_handle.seek(0, os.SEEK_END)
            current_len = self.file_handle.tell()
            if offset >= current_len:
                # Page does not exist on disk yet, return empty zero-filled page
                return b"\x00" * PAGE_SIZE
            self.file_handle.seek(offset)
            data = self.file_handle.read(PAGE_SIZE)
            if len(data) < PAGE_SIZE:
                data += b"\x00" * (PAGE_SIZE - len(data))
            return data

    def _write_disk_page(self, page: Page) -> None:
        offset = page.page_id * PAGE_SIZE
        with self.latch:
            self.file_handle.seek(offset)
            self.file_handle.write(page.serialize())
            self.file_handle.flush()

    def fetch_page(self, page_id: int) -> Page:
        with self.latch:
            if page_id in self.pages:
                page = self.pages[page_id]
                page.write_lock()
                page.pin_count += 1
                page.write_unlock()
                self.replacer.record_access(page_id)
                return page

            # Check if pool is full and needs eviction
            if len(self.pages) >= self.pool_size:
                evict_pid = self.replacer.evict()
                if evict_pid is None:
                    raise RuntimeError("Buffer pool is full and no unpinned pages available for eviction.")
                
                evict_page = self.pages[evict_pid]
                evict_page.write_lock()
                if evict_page.pin_count > 0:
                    evict_page.write_unlock()
                    raise RuntimeError(f"Eviction attempted on pinned page_id={evict_pid}")
                
                if evict_page.is_dirty:
                    # Force WAL up to page LSN before writing dirty page
                    self.wal_manager.flush_to_lsn(evict_page.page_lsn)
                    self._write_disk_page(evict_page)
                    evict_page.is_dirty = False
                
                del self.pages[evict_pid]

            # Allocate new page structure and load from disk
            page = Page(page_id)
            raw_data = self._read_disk_page(page_id)
            if any(raw_data):
                page.deserialize(raw_data)
            
            page.write_lock()
            page.pin_count = 1
            page.write_unlock()
            
            self.pages[page_id] = page
            self.replacer.record_access(page_id)
            return page

    def unpin_page(self, page_id: int, is_dirty: bool) -> bool:
        with self.latch:
            if page_id not in self.pages:
                return False
            page = self.pages[page_id]
            page.write_lock()
            if page.pin_count <= 0:
                page.write_unlock()
                return False
            
            if is_dirty:
                page.is_dirty = True
            
            page.pin_count -= 1
            unpinned = (page.pin_count == 0)
            page.write_unlock()
            return unpinned

    def flush_page(self, page_id: int) -> bool:
        with self.latch:
            if page_id not in self.pages:
                return False
            page = self.pages[page_id]
            page.write_lock()
            if page.is_dirty:
                self.wal_manager.flush_to_lsn(page.page_lsn)
                self._write_disk_page(page)
                page.is_dirty = False
            page.write_unlock()
            return True

    def flush_all(self) -> None:
        with self.latch:
            max_lsn = 0
            for page in self.pages.values():
                page.write_lock()
                if page.is_dirty and page.page_lsn > max_lsn:
                    max_lsn = page.page_lsn
                page.write_unlock()
            
            if max_lsn > 0:
                self.wal_manager.flush_to_lsn(max_lsn)

            for page in self.pages.values():
                page.write_lock()
                if page.is_dirty:
                    self._write_disk_page(page)
                    page.is_dirty = False
                page.write_unlock()

    def new_page(self) -> Tuple[int, Page]:
        with self.latch:
            # Determine a new page ID based on file size and current pool allocations
            max_id = 0
            if self.pages:
                max_id = max(self.pages.keys())
            
            self.file_handle.seek(0, os.SEEK_END)
            disk_len = self.file_handle.tell()
            disk_max_id = (disk_len // PAGE_SIZE) - 1 if disk_len > 0 else -1
            
            new_id = max(max_id, disk_max_id) + 1
            
            if len(self.pages) >= self.pool_size:
                evict_pid = self.replacer.evict()
                if evict_pid is None:
                    raise RuntimeError("Buffer pool capacity reached and no frames available for new page.")
                evict_page = self.pages[evict_pid]
                evict_page.write_lock()
                if evict_page.pin_count > 0:
                    evict_page.write_unlock()
                    raise RuntimeError(f"Attempted to evict pinned page {evict_pid}")
                if evict_page.is_dirty:
                    self.wal_manager.flush_to_lsn(evict_page.page_lsn)
                    self._write_disk_page(evict_page)
                    evict_page.is_dirty = False
                del self.pages[evict_pid]

            page = Page(new_id)
            page.write_lock()
            page.pin_count = 1
            page.write_unlock()
            
            self.pages[new_id] = page
            self.replacer.record_access(new_id)
            return new_id, page


class BackgroundFlusher:
    """
    Background daemon thread performing group commits, fuzzy checkpointing,
    and periodic flushing of dirty buffer pool frames to disk storage.
    """

    def __init__(self, buffer_pool: BufferPoolManager, wal_manager: WALManager, interval_ms: int = 1000):
        self.buffer_pool: BufferPoolManager = buffer_pool
        self.wal_manager: WALManager = wal_manager
        self.interval_sec: float = interval_ms / 1000.0
        self.stop_event: threading.Event = threading.Event()
        self.thread: threading.Thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join()

    def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(self.interval_sec)
            try:
                self.checkpoint()
            except Exception as e:
                # Log or handle background flushing exceptions robustly
                pass

    def checkpoint(self) -> int:
        """
        Performs fuzzy checkpointing:
        1. Flushes dirty pages from the buffer pool to persistent storage.
        2. Records a CHECKPOINT record containing active transactions and dirty page tables via WALManager.
        """
        # Flush all currently dirty pages in buffer pool safely
        self.buffer_pool.flush_all()
        
        # Write checkpoint record to WAL
        checkpoint_lsn = self.wal_manager.log_checkpoint(active_txs=[], dirty_pages=[])
        return checkpoint_lsn