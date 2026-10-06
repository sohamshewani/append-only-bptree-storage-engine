import os
import sys
import struct
import threading
import time
import zlib
from typing import List, Tuple, Optional, Dict, Set, Any

# ============================================================================
# CONSTANTS & LAYOUT DEFINITIONS
# ============================================================================
PAGE_SIZE = 4096
HEADER_SIZE = 32  # [PageType(1), IsLeaf(1), KeyCount(u16), FreeSpaceOffset(u16), NextPageId(u64), LSN(u64), Checksum(u32), Reserved(u32)]

# Page Types
PAGE_TYPE_INTERNAL = 1
PAGE_TYPE_LEAF = 2

# WAL Record Types
WAL_BEGIN = 1
WAL_COMMIT = 2
WAL_ABORT = 3
WAL_INSERT = 4
WAL_SPLIT = 5
WAL_PAGE_WRITE = 6
WAL_CHECKPOINT = 7

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================
class StorageEngineError(Exception):
    pass

class PageCorruptedError(StorageEngineError):
    pass

class BufferPoolFullError(StorageEngineError):
    pass

class LatchTimeoutError(StorageEngineError):
    pass

# ============================================================================
# BUFFER POOL & CACHE EVICTION (LRU-K / Hybrid)
# ============================================================================
class Frame:
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.data = bytearray(PAGE_SIZE)
        self.pin_count = 0
        self.is_dirty = False
        self.ref_count = 0
        self.latch = threading.RLock()
        self.last_used = time.time_ns()

class BufferPoolManager:
    def __init__(self, filename: str, pool_size: int = 1024):
        self.filename = filename
        self.pool_size = pool_size
        self.frames: Dict[int, Frame] = {}
        self.lru_list: List[int] = []  # Ordered list of page_ids for eviction
        self.mutex = threading.Lock()
        
        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                pass
        self.file = open(filename, "r+b")
        self.file.seek(0, os.SEEK_END)
        self.file_size = self.file.tell()

    def _compute_checksum(self, data: bytearray) -> int:
        # Checksum covers everything except the checksum field itself (offset 20 to 24)
        payload = bytes(data[:20]) + bytes(data[24:])
        return zlib.crc32(payload) & 0xFFFFFFFF

    def fetch_page(self, page_id: int) -> Frame:
        with self.mutex:
            if page_id in self.frames:
                frame = self.frames[page_id]
                frame.pin_count += 1
                frame.last_used = time.time_ns()
                if page_id in self.lru_list:
                    self.lru_list.remove(page_id)
                return frame
            
            if len(self.frames) >= self.pool_size:
                self._evict()
            
            frame = Frame(page_id)
            offset = page_id * PAGE_SIZE
            with self.file:
                self.file.seek(0, os.SEEK_END)
                max_size = self.file.tell()
                if offset < max_size:
                    self.file.seek(offset)
                    disk_data = self.file.read(PAGE_SIZE)
                    if len(disk_data) == PAGE_SIZE:
                        frame.data[:] = disk_data
                        # Verify checksum
                        stored_checksum = struct.unpack_from("<I", frame.data, 20)[0]
                        calc_checksum = self._compute_checksum(frame.data)
                        if stored_checksum != 0 and stored_checksum != calc_checksum:
                            raise PageCorruptedError(f"Page {page_id} checksum mismatch: stored {stored_checksum}, calculated {calc_checksum}")
                    else:
                        frame.data[:] = b'\x00' * PAGE_SIZE
                else:
                    frame.data[:] = b'\x00' * PAGE_SIZE

            frame.pin_count = 1
            frame.last_used = time.time_ns()
            self.frames[page_id] = frame
            return frame

    def unpin_page(self, page_id: int, is_dirty: bool):
        with self.mutex:
            if page_id not in self.frames:
                return
            frame = self.frames[page_id]
            if is_dirty:
                frame.is_dirty = True
            frame.pin_count = max(0, frame.pin_count - 1)
            if frame.pin_count == 0:
                if page_id not in self.lru_list:
                    self.lru_list.append(page_id)

    def _evict(self):
        victim_id = None
        for pid in self.lru_list:
            frame = self.frames[pid]
            if frame.pin_count == 0:
                victim_id = pid
                break
        
        if victim_id is None:
            raise BufferPoolFullError("All buffer pool frames are pinned.")
        
        self.lru_list.remove(victim_id)
        victim_frame = self.frames[victim_id]
        
        if victim_frame.is_dirty:
            self._flush_frame(victim_frame)
            
        del self.frames[victim_id]

    def _flush_frame(self, frame: Frame):
        offset = frame.page_id * PAGE_SIZE
        # Update checksum before flush
        checksum = self._compute_checksum(frame.data)
        struct.pack_into("<I", frame.data, 20, checksum)
        with self.file:
            self.file.seek(offset)
            self.file.write(bytes(frame.data))
            self.file.flush()
        frame.is_dirty = False

    def flush_all(self):
        with self.mutex:
            for frame in self.frames.values():
                if frame.is_dirty:
                    self._flush_frame(frame)

    def allocate_page(self) -> int:
        with self.mutex:
            self.file.seek(0, os.SEEK_END)
            size = self.file.tell()
            new_page_id = size // PAGE_SIZE
            # Extend file
            self.file.write(b'\x00' * PAGE_SIZE)
            self.file.flush()
            return new_page_id

# ============================================================================
# WRITE-AHEAD LOG (WAL) & RECOVERY
# ============================================================================
class WALManager:
    def __init__(self, wal_filename: str):
        self.wal_filename = wal_filename
        self.lsn_counter = 1
        self.mutex = threading.Lock()
        if not os.path.exists(wal_filename):
            with open(wal_filename, "wb") as f:
                pass
        self.file = open(wal_filename, "r+b")

    def log_record(self, tx_id: int, rec_type: int, payload: bytes) -> int:
        with self.mutex:
            lsn = self.lsn_counter
            self.lsn_counter += 1
            
            # Framing: [Length: u32][LSN: u64][TxID: u64][Type: u8][Payload]
            length = 4 + 8 + 8 + 1 + len(payload)
            header = struct.pack("<IQQB", length, lsn, tx_id, rec_type)
            record = header + payload
            
            self.file.seek(0, os.SEEK_END)
            self.file.write(record)
            self.file.flush()
            os.fsync(self.file.fileno())
            return lsn

    def read_all_records(self) -> List[Tuple[int, int, int, int, bytes]]:
        records = []
        with open(self.wal_filename, "rb") as f:
            while True:
                len_bytes = f.read(4)
                if not len_bytes or len(len_bytes) < 4:
                    break
                length = struct.unpack("<I", len_bytes)[0]
                rest = f.read(length - 4)
                if len(rest) < length - 4:
                    break
                lsn = struct.unpack_from("<Q", rest, 0)[0]
                tx_id = struct.unpack_from("<Q", rest, 8)[0]
                rec_type = struct.unpack_from("<B", rest, 16)[0]
                payload = rest[17:]
                records.append((length, lsn, tx_id, rec_type, payload))
        return records

# ============================================================================
# B-TREE NODE STRUCTURE & SERIALIZATION
# ============================================================================
class BTreeInternalNode:
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.keys: List[bytes] = []
        self.child_pointers: List[int] = []
        self.lsn = 0

    def serialize(self, data: bytearray):
        # Header: [PageType:1][IsLeaf:1][KeyCount:u16][FreeSpaceOffset:u16][NextPageId:u64][LSN:u64][Checksum:u32][Reserved:u32]
        struct.pack_into("<BBHHQQII", data, 0, PAGE_TYPE_INTERNAL, 0, len(self.keys), 0, 0, self.lsn, 0, 0)
        offset = HEADER_SIZE
        # Pack children and keys
        # Format: [Child0][Key0][Child1][Key1]...[ChildN]
        for i in range(len(self.keys)):
            struct.pack_into("<Q", data, offset, self.child_pointers[i])
            offset += 8
            k_len = len(self.keys[i])
            struct.pack_into("<H", data, offset, k_len)
            offset += 2
            data[offset:offset+k_len] = self.keys[i]
            offset += k_len
        struct.pack_into("<Q", data, offset, self.child_pointers[-1])

    @classmethod
    def deserialize(cls, page_id: int, data: bytearray) -> 'BTreeInternalNode':
        node = cls(page_id)
        _, _, key_count, _, _, lsn, _, _ = struct.unpack_from("<BBHHQQII", data, 0)
        node.lsn = lsn
        offset = HEADER_SIZE
        for _ in range(key_count):
            child_ptr = struct.unpack_from("<Q", data, offset)[0]
            offset += 8
            k_len = struct.unpack_from("<H", data, offset)[0]
            offset += 2
            key = bytes(data[offset:offset+k_len])
            offset += k_len
            node.child_pointers.append(child_ptr)
            node.keys.append(key)
        last_child = struct.unpack_from("<Q", data, offset)[0]
        node.child_pointers.append(last_child)
        return node

class BTreeLeafNode:
    def __init__(self, page_id: int, next_page_id: int = 0):
        self.page_id = page_id
        self.next_page_id = next_page_id
        self.keys: List[bytes] = []
        self.values: List[bytes] = []
        self.lsn = 0

    def serialize(self, data: bytearray):
        struct.pack_into("<BBHHQQII", data, 0, PAGE_TYPE_LEAF, 1, len(self.keys), 0, self.next_page_id, self.lsn, 0, 0)
        offset = HEADER_SIZE
        for i in range(len(self.keys)):
            k_len = len(self.keys[i])
            v_len = len(self.values[i])
            struct.pack_into("<HH", data, offset, k_len, v_len)
            offset += 4
            data[offset:offset+k_len] = self.keys[i]
            offset += k_len
            data[offset:offset+v_len] = self.values[i]
            offset += v_len

    @classmethod
    def deserialize(cls, page_id: int, data: bytearray) -> 'BTreeLeafNode':
        _, _, key_count, _, next_page_id, lsn, _, _ = struct.unpack_from("<BBHHQQII", data, 0)
        node = cls(page_id, next_page_id)
        node.lsn = lsn
        offset = HEADER_SIZE
        for _ in range(key_count):
            k_len, v_len = struct.unpack_from("<HH", data, offset)
            offset += 4
            key = bytes(data[offset:offset+k_len])
            offset += k_len
            val = bytes(data[offset:offset+v_len])
            offset += v_len
            node.keys.append(key)
            node.values.append(val)
        return node

# ============================================================================
# B-TREE ENGINE IMPLEMENTATION (Latch Crabbing & Append-Only)
# ============================================================================
class AppendOnlyBPTree:
    def __init__(self, bpm: BufferPoolManager, wal: WALManager):
        self.bpm = bpm
        self.wal = wal
        self.root_lock = threading.RLock()
        
        # Check metadata or initialize root
        if bpm.file_size == 0:
            root_frame = bpm.fetch_page(0)
            leaf = BTreeLeafNode(0)
            leaf.serialize(root_frame.data)
            bpm.unpin_page(0, is_dir=True)
            self.root_page_id = 0
        else:
            self.root_page_id = 0

    def search(self, key: bytes) -> Optional[bytes]:
        with self.root_lock:
            curr_id = self.root_page_id
            
        while True:
            frame = self.bpm.fetch_page(curr_id)
            page_type = struct.unpack_from("<B", frame.data, 0)[0]
            
            if page_type == PAGE_TYPE_LEAF:
                leaf = BTreeLeafNode.deserialize(curr_id, frame.data)
                self.bpm.unpin_page(curr_id, is_dirty=False)
                for i, k in enumerate(leaf.keys):
                    if k == key:
                        return leaf.values[i]
                return None
            else:
                internal = BTreeInternalNode.deserialize(curr_id, frame.data)
                self.bpm.unpin_page(curr_id, is_dirty=False)
                next_id = internal.child_pointers[-1]
                for i, k in enumerate(internal.keys):
                    if key < k:
                        next_id = internal.child_pointers[i]
                        break
                curr_id = next_id

    def insert(self, tx_id: int, key: bytes, value: bytes):
        with self.root_lock:
            lsn = self.wal.log_record(tx_id, WAL_INSERT, key + b":" + value)
            
            # Root split logic if necessary, or recursive insert
            root_frame = self.bpm.fetch_page(self.root_page_id)
            page_type = struct.unpack_from("<B", root_frame.data, 0)[0]
            
            if page_type == PAGE_TYPE_LEAF:
                leaf = BTreeLeafNode.deserialize(self.root_page_id, root_frame.data)
                self.bpm.unpin_page(self.root_page_id, is_dirty=False)
                
                # Insert locally if fits, else split
                if len(leaf.keys) < 4: # Small threshold for demonstration
                    #