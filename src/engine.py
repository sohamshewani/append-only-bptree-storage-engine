import os
import sys
import mmap
import struct
import threading
import time
import zlib
from typing import Dict, List, Tuple, Optional, Set

PAGE_SIZE = 4096
MAGIC_NUMBER = 0x42505452  # 'BPTR'

# WAL Record Types
LOG_BEGIN = 1
LOG_COMMIT = 2
LOG_ABORT = 3
LOG_INSERT = 4
LOG_SPLIT = 5
LOG_PAGE_WRITE = 6
LOG_CHECKPOINT = 7

# Lock Types
LOCK_SHARED = 1
LOCK_EXCLUSIVE = 2

class StorageEngineError(Exception):
    pass

class PageCorruptionError(StorageEngineError):
    pass

class LatchManager:
    def __init__(self):
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.readers = 0
        self.writer = False
        self.waiting_writers = 0

    def acquire_read(self):
        with self.lock:
            while self.writer or self.waiting_writers > 0:
                self.cond.wait()
            self.readers += 1

    def release_read(self):
        with self.lock:
            self.readers -= 1
            if self.readers == 0:
                self.cond.notify_all()

    def acquire_write(self):
        with self.lock:
            self.waiting_writers += 1
            try:
                while self.writer or self.readers > 0:
                    self.cond.wait()
                self.writer = True
            finally:
                self.waiting_writers -= 1

    def release_write(self):
        with self.lock:
            self.writer = False
            self.cond.notify_all()

class DiskManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        exists = os.path.exists(db_path)
        self.file = open(db_path, "a+b")
        if not exists:
            self.file.seek(0)
            self.file.write(b'\x00' * PAGE_SIZE)
            self.file.flush()
        self.file.seek(0, os.SEEK_END)
        self.file_size = self.file.tell()
        self.mutex = threading.Lock()

    def allocate_page(self) -> int:
        with self.mutex:
            self.file.seek(0, os.SEEK_END)
            page_id = self.file_size // PAGE_SIZE
            self.file.write(b'\x00' * PAGE_SIZE)
            self.file.flush()
            self.file_size += PAGE_SIZE
            return page_id

    def read_page(self, page_id: int) -> bytearray:
        with self.mutex:
            offset = page_id * PAGE_SIZE
            self.file.seek(offset)
            data = self.file.read(PAGE_SIZE)
            if len(data) < PAGE_SIZE:
                data += b'\x00' * (PAGE_SIZE - len(data))
            return bytearray(data)

    def write_page(self, page_id: int, page_data: bytes):
        with self.mutex:
            offset = page_id * PAGE_SIZE
            self.file.seek(offset)
            self.file.write(page_data)
            self.file.flush()

    def close(self):
        with self.mutex:
            self.file.close()

class WALManager:
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.file = open(wal_path, "a+b")
        self.mutex = threading.Lock()
        self.lsn_counter = 0
        self.file.seek(0, os.SEEK_END)
        if self.file.tell() > 0:
            self._recover_lsn_counter()

    def _recover_lsn_counter(self):
        self.file.seek(0)
        max_lsn = 0
        while True:
            header = self.file.read(4)
            if not header or len(header) < 4:
                break
            length = struct.unpack(">I", header)[0]
            record_data = self.file.read(length)
            if len(record_data) < 16:
                break
            lsn = struct.unpack(">Q", record_data[0:8])[0]
            if lsn > max_lsn:
                max_lsn = lsn
        self.lsn_counter = max_lsn

    def append(self, tx_id: int, record_type: int, payload: bytes) -> int:
        with self.mutex:
            self.lsn_counter += 1
            lsn = self.lsn_counter
            # Framing: [Length: u32][LSN: u64][TxID: u64][Type: u8][Payload]
            inner_payload = struct.pack(">QQB", lsn, tx_id, record_type) + payload
            length = len(inner_payload)
            frame = struct.pack(">I", length) + inner_payload
            self.file.write(frame)
            self.file.flush()
            os.fsync(self.file.fileno())
            return lsn

    def read_all(self) -> List[Tuple[int, int, int, bytes]]:
        with self.mutex:
            self.file.seek(0)
            records = []
            while True:
                header = self.file.read(4)
                if not header or len(header) < 4:
                    break
                length = struct.unpack(">I", header)[0]
                record_data = self.file.read(length)
                if len(record_data) < 17:
                    break
                lsn = struct.unpack(">Q", record_data[0:8])[0]
                tx_id = struct.unpack(">Q", record_data[8:16])[0]
                rtype = record_data[16]
                payload = record_data[17:]
                records.append((lsn, tx_id, rtype, payload))
            return records

    def close(self):
        with self.mutex:
            self.file.close()

class Frame:
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.data = bytearray(PAGE_SIZE)
        self.pin_count = 0
        self.is_dirty = False
        self.latch = LatchManager()
        self.rec_lsn = 0
        self.last_access_time = time.time()

class BufferPoolManager:
    def __init__(self, disk_manager: DiskManager, wal_manager: WALManager, capacity: int = 1024):
        self.disk_manager = disk_manager
        self.wal_manager = wal_manager
        self.capacity = capacity
        self.frames: Dict[int, Frame] = {}
        self.mutex = threading.Lock()

    def fetch_page(self, page_id: int) -> Frame:
        with self.mutex:
            if page_id in self.frames:
                frame = self.frames[page_id]
                frame.pin_count += 1
                frame.last_access_time = time.time()
                return frame

            if len(self.frames) >= self.capacity:
                self._evict_page()

            data = self.disk_manager.read_page(page_id)
            if len(data) == PAGE_SIZE:
                # Verify checksum if valid page
                stored_checksum = struct.unpack(">I", data[PAGE_SIZE-4:])[0]
                calculated_checksum = zlib.crc32(data[:PAGE_SIZE-4]) & 0xFFFFFFFF
                if stored_checksum != 0 and stored_checksum != calculated_checksum:
                    raise PageCorruptionError(f"Page {page_id} checksum mismatch!")

            frame = Frame(page_id)
            frame.data = data
            frame.pin_count = 1
            frame.last_access_time = time.time()
            self.frames[page_id] = frame
            return frame

    def unpin_page(self, page_id: int, is_dirty: bool):
        with self.mutex:
            if page_id in self.frames:
                frame = self.frames[page_id]
                if is_dirty:
                    frame.is_dirty = True
                frame.pin_count = max(0, frame.pin_count - 1)

    def flush_page(self, frame: Frame):
        # Calculate checksum
        checksum = zlib.crc32(frame.data[:PAGE_SIZE-4]) & 0xFFFFFFFF
        struct.pack_into(">I", frame.data, PAGE_SIZE-4, checksum)
        self.disk_manager.write_page(frame.page_id, frame.data)
        frame.is_dirty = False

    def flush_all(self):
        with self.mutex:
            for frame in self.frames.values():
                frame.latch.acquire_write()
                try:
                    if frame.is_dirty:
                        self.flush_page(frame)
                finally:
                    frame.latch.release_write()

    def _evict_page(self):
        # LRU eviction policy among unpinned pages
        candidate_page_id = None
        oldest_time = float('inf')
        for pid, frame in self.frames.items():
            if frame.pin_count == 0:
                if frame.last_access_time < oldest_time:
                    oldest_time = frame.last_access_time
                    candidate_page_id = pid

        if candidate_page_id is None:
            raise StorageEngineError("Buffer pool overflow: all pages are pinned!")

        frame = self.frames[candidate_page_id]
        frame.latch.acquire_write()
        try:
            if frame.is_dirty:
                self.flush_page(frame)
            del self.frames[candidate_page_id]
        finally:
            frame.latch.release_write()

class BackgroundFlusher:
    def __init__(self, bpm: BufferPoolManager, wal_manager: WALManager, interval_ms: int = 1000):
        self.bpm = bpm
        self.wal_manager = wal_manager
        self.interval = interval_ms / 1000.0
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            time.sleep(self.interval)
            try:
                self.checkpoint()
            except Exception:
                pass

    def checkpoint(self):
        self.bpm.flush_all()
        # Write checkpoint WAL record
        self.wal_manager.append(0, LOG_CHECKPOINT, b"CHECKPOINT")

    def stop(self):
        self.running = False
        self.thread.join()

class LockManager:
    def __init__(self):
        self.mutex = threading.Lock()
        self.locks: Dict[str, Dict[int, Set[int]]] = {} # key -> {lock_type: set of tx_ids}

    def acquire(self, tx_id: int, resource: str, lock_type: int) -> bool:
        with self.mutex:
            if resource not in self.locks:
                self.locks[resource] = {LOCK_SHARED: set(), LOCK_EXCLUSIVE: set()}
            
            res = self.locks[resource]
            if lock_type == LOCK_SHARED:
                if len(res[LOCK_EXCLUSIVE]) == 0 or (len(res[LOCK_EXCLUSIVE]) == 1 and tx_id in res[LOCK_EXCLUSIVE]):
                    res[LOCK_SHARED].add(tx_id)
                    return True
                return False
            else: # Exclusive
                if len(res[LOCK_SHARED]) == 0 and (len(res[LOCK_EXCLUSIVE]) == 0 or (len(res[LOCK_EXCLUSIVE]) == 1 and tx_id in res[LOCK_EXCLUSIVE])):
                    res[LOCK_EXCLUSIVE].add(tx_id)
                    return True
                return False

    def release(self, tx_id: int, resource: str):
        with self.mutex:
            if resource in self.locks:
                res = self.locks[resource]
                if tx_id in res[LOCK_SHARED]:
                    res[LOCK_SHARED].remove(tx_id)
                if tx_id in res[LOCK_EXCLUSIVE]:
                    res[LOCK_EXCLUSIVE].remove(tx_id)
                if not res[LOCK_SHARED] and not res[LOCK_EXCLUSIVE]:
                    del self.locks[resource]

class BPlusTree:
    def __init__(self, bpm: BufferPoolManager, wal_manager: WALManager, root_page_id: int):
        self.bpm = bpm
        self.wal_manager = wal_manager
        self.root_page_id = root_page_id
        self.mutex = threading.Lock()

    def _init_page(self, data: bytearray, is_leaf: bool):
        # Page layout: [IsLeaf: u8][NumKeys: u16][PrevPage: u32][NextPage: u32][Payloads...]
        data[0] = 1 if is_leaf else 0
        struct.pack_into(">H", data, 1, 0)
        struct.pack_into(">I", data, 3, 0)
        struct.pack_into(">I", data, 7, 0)

    def search(self, key: bytes) -> Optional[bytes]:
        with self.mutex:
            current_id = self.root_page_id
            while True:
                frame = self.bpm.fetch_page(current_id)
                frame.latch.acquire_read()
                try:
                    data = frame.data
                    is_leaf = (data[0] == 1)
                    num_keys = struct.unpack(">H", data[1:3])[0]
                    
                    if is_leaf:
                        offset = 11
                        found_val = None
                        for _ in range(num_keys):
                            k_len = struct.unpack(">H", data[offset:offset+2])[0]
                            offset += 2
                            k = bytes(data[offset:offset+k_len])
                            offset += k_len
                            v_len = struct.unpack(">H", data[offset:offset+2])[0]
                            offset += 2
                            v = bytes(data[offset:offset+v_len])
                            offset += v_len
                            if k == key:
                                found_val = v
                                break
                        self.bpm.unpin_page(current_id, False)
                        return found_val
                    else:
                        offset = 11
                        next_pid = 0
                        child_pids = []
                        keys = []
                        for _ in range(num_keys):
                            k_len = struct.unpack(">H", data[offset:offset+2])[0]
                            offset += 2
                            k = bytes(data[offset:offset+k_len])
                            offset += k_len
                            keys.append(k)
                        for _ in range(num_keys + 1):
                            cp = struct.unpack(">I", data[offset:offset+4])[0]
                            offset += 4
                            child_pids.append(cp)

                        idx = 0
                        while idx < len(keys) and key >= keys[idx]:
                            idx += 1
                        
                        next_pid = child_pids[idx]
                        self.bpm.unpin_page(current_id, False)
                        current_id = next_pid
                finally:
                    frame.latch.release_read()

    def insert(self, tx_id: int, key: bytes, value: bytes) -> bool:
        with self.mutex:
            # Simple point insertion implementation for demo compliance
            frame = self.bpm.fetch_page(self.root_page_id)
            frame.latch.acquire_write()
            try:
                data = frame.data
                is_leaf = (data[0] == 1)
                num_keys = struct.unpack(">H", data[1:3])[0]
                
                # Check space
                needed = 2 + len(key) + 2 + len(value)
                # Calculate current used size