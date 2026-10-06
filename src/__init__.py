"""
append-only-bptree-storage-engine
src/__init__.py

Production-grade entry point and core orchestrator for the Append-Only B+Tree
Storage Engine. Exposes the complete public API (StorageEngine, Transaction,
Config, etc.) while unifying the WAL, Buffer Pool, B+Tree, Concurrency, and
ARIES Recovery subsystems.
"""

import os
import sys
import threading
import struct
import zlib
import time
import logging
from typing import Dict, List, Tuple, Optional, Set, Any, Generator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s (%(threadName)s): %(message)s"
)
logger = logging.getLogger("BPTreeEngine")


# ============================================================================
# CONSTANTS & CONFIGURATION
# ============================================================================

PAGE_SIZE = 4096
MAGIC_NUMBER = 0x4150504250545245  # "APPBPTR"
HEADER_PAGE_ID = 0

# WAL Record Types
WAL_TYPE_BEGIN = 1
WAL_TYPE_COMMIT = 2
WAL_TYPE_ABORT = 3
WAL_TYPE_INSERT = 4
WAL_TYPE_SPLIT = 5
WAL_TYPE_PAGE_WRITE = 6
WAL_TYPE_CHECKPOINT = 7
WAL_TYPE_CLR = 8

# Lock Modes
LOCK_SHARED = 1
LOCK_EXCLUSIVE = 2


class Config:
    """System-wide configuration parameters for the storage engine."""
    def __init__(
        self,
        db_path: str,
        wal_path: str,
        buffer_pool_size: int = 1024,
        flush_interval_sec: float = 1.0,
        checkpoint_interval_sec: float = 30.0
    ):
        self.db_path = db_path
        self.wal_path = wal_path
        self.buffer_pool_size = buffer_pool_size
        self.flush_interval_sec = flush_interval_sec
        self.checkpoint_interval_sec = checkpoint_interval_sec


# ============================================================================
# EXCEPTION HIERARCHY
# ============================================================================

class StorageEngineError(Exception):
    """Base exception for all storage engine errors."""
    pass


class PageCorruptionError(StorageEngineError):
    """Raised when page checksum verification fails."""
    pass


class BufferPoolFullError(StorageEngineError):
    """Raised when the buffer pool cannot evict any frames."""
    pass


class TransactionAbortedError(StorageEngineError):
    """Raised when a transaction has been aborted or conflicts."""
    pass


class LockTimeoutError(StorageEngineError):
    """Raised when acquiring a lock times out."""
    pass


# ============================================================================
# SUB-SYSTEM 1: WAL & LOGGING
# ============================================================================

class WriteAheadLog:
    """
    Manages structured binary WAL framing, monotonic LSNs, flushing, and
    recovery scanning.
    Binary Frame: [Length: u32][LSN: u64][TxID: u64][Type: u8][Payload][CRC32: u32]
    """
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self._lock = threading.Lock()
        self._next_lsn = 1
        self._flushed_lsn = 0
        
        # Ensure directory exists
        dirname = os.path.dirname(os.path.abspath(wal_path))
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
            
        self._file = open(self.wal_path, "a+b")
        self._recover_next_lsn()

    def _recover_next_lsn(self) -> None:
        """Scan existing WAL to determine the next monotonic LSN."""
        with self._lock:
            self._file.seek(0, os.SEEK_END)
            file_size = self._file.tell()
            self._file.seek(0, os.SEEK_SET)
            
            max_lsn = 0
            while True:
                length_bytes = self._file.read(4)
                if not length_bytes or len(length_bytes) < 4:
                    break
                length = struct.unpack(">I", length_bytes)[0]
                record_data = self._file.read(length)
                if len(record_data) < length:
                    logger.warning("Incomplete WAL record detected at end of file. Truncating.")
                    break
                
                # Unpack: [LSN: u64][TxID: u64][Type: u8][Payload...][CRC: u32]
                if len(record_data) < 17:
                    break
                lsn = struct.unpack(">Q", record_data[0:8])[0]
                if lsn > max_lsn:
                    max_lsn = lsn
            
            self._next_lsn = max_lsn + 1
            self._flushed_lsn = max_lsn
            self._file.seek(0, os.SEEK_END)

    def append(self, tx_id: int, record_type: int, payload: bytes) -> int:
        """Append a record to the WAL buffer and return its assigned LSN."""
        with self._lock:
            lsn = self._next_lsn
            self._next_lsn += 1
            
            # Construct frame: LSN (8), TxID (8), Type (1), Payload
            header = struct.pack(">QQB", lsn, tx_id, record_type)
            body = header + payload
            
            # Compute CRC32 over length, lsn, txid, type, payload
            # Format length as u32
            length = len(body) + 4  # include crc32 size itself in payload or outside
            # Let's standardize: [Length (4)] [LSN (8)] [TxID (8)] [Type (1)] [Payload] [CRC32 (4)]
            # Length = sizeof(LSN) + sizeof(TxID) + sizeof(Type) + len(Payload) + sizeof(CRC32)
            total_payload_len = 8 + 8 + 1 + len(payload) + 4
            
            checksum_data = struct.pack(">IQQB", total_payload_len, lsn, tx_id, record_type) + payload
            checksum = zlib.crc32(checksum_data) & 0xFFFFFFFF
            
            frame = struct.pack(">IQQBI", total_payload_len, lsn, tx_id, record_type) + payload + struct.pack(">I", checksum)
            self._file.write(frame)
            return lsn

    def flush(self, up_to_lsn: Optional[int] = None) -> int:
        """fsync WAL up to specified LSN or current tip."""
        with self._lock:
            self._file.flush()
            os.fsync(self._file.fileno())
            if up_to_lsn is not None:
                self._flushed_lsn = max(self._flushed_lsn, up_to_lsn)
            else:
                self._flushed_lsn = self._next_lsn - 1
            return self._flushed_lsn

    def read_records(self) -> Generator[Tuple[int, int, int, bytes], None, None]:
        """Generator yielding (lsn, tx_id, record_type, payload) from WAL."""
        with self._lock:
            self._file.seek(0, os.SEEK_SET)
            while True:
                length_bytes = self._file.read(4)
                if not length_bytes or len(length_bytes) < 4:
                    break
                length = struct.unpack(">I", length_bytes)[0]
                record_data = self._file.read(length)
                if len(record_data) < length:
                    break
                
                # Parse
                lsn = struct.unpack(">Q", record_data[0:8])[0]
                tx_id = struct.unpack(">Q", record_data[8:16])[0]
                rec_type = record_data[16]
                payload = record_data[17:-4]
                stored_crc = struct.unpack(">I", record_data[-4:])[0]
                
                # Verify CRC
                check_data = struct.pack(">IQQB", length, lsn, tx_id, rec_type) + payload
                computed_crc = zlib.crc32(check_data) & 0xFFFFFFFF
                if computed_crc != stored_crc:
                    raise StorageEngineError(f"WAL corruption detected at LSN {lsn}!")
                
                yield lsn, tx_id, rec_type, payload

    def close(self) -> None:
        with self._lock:
            self._file.close()


# ============================================================================
# SUB-SYSTEM 2: BUFFER POOL & DISK MANAGER
# ============================================================================

class DiskManager:
    """Manages low-level read/write operations of fixed-size 4096-byte pages."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        dirname = os.path.dirname(os.path.abspath(db_path))
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
            
        if not os.path.exists(self.db_path):
            with open(self.db_path, "wb") as f:
                # Initialize with a header page
                header_data = bytearray(PAGE_SIZE)
                struct.pack_into(">Q", header_data, 0, MAGIC_NUMBER)
                f.write(header_data)
        
        self._file = open(self.db_path, "r+b")
        self._lock = threading.Lock()

    def read_page(self, page_id: int) -> bytearray:
        with self._lock:
            self._file.seek(page_id * PAGE_SIZE)
            data = self._file.read(PAGE_SIZE)
            if len(data) < PAGE_SIZE:
                # Extend page with zeros if out of bounds
                extended = bytearray(PAGE_SIZE)
                extended[:len(data)] = data
                return extended
            return bytearray(data)

    def write_page(self, page_id: int, page_data: bytearray) -> None:
        with self._lock:
            self._file.seek(page_id * PAGE_SIZE)
            # Compute and inject CRC32 trailing checksum before writing
            # Page layout: [Data (4092 bytes)][CRC32 (4 bytes)]
            content = page_data[:PAGE_SIZE - 4]
            checksum = zlib.crc32(content) & 0xFFFFFFFF
            struct.pack_into(">I", page_data, PAGE_SIZE - 4, checksum)
            
            self._file.write(page_data)
            self._file.flush()
            os.fsync(self._file.fileno())

    def allocate_page(self) -> int:
        with self._lock:
            self._file.seek(0, os.SEEK_END)
            size = self._file.tell()
            page_id = size // PAGE_SIZE
            empty_page = bytearray(PAGE_SIZE)
            self._file.write(empty_page)
            self._file.flush()
            return page_id

    def close(self) -> None:
        with self._lock:
            self._file.close()


class Frame:
    """Represents a single buffer pool frame caching a disk page."""
    def __init__(self, frame_id: int):
        self.frame_id = frame_id
        self.page_id = -1
        self.pin_count = 0
        self.is_dirty = False
        self.page_lsn = 0
        self.data = bytearray(PAGE_SIZE)
        self.latch = threading.RLock()


class BufferPoolManager:
    """LRU-K/LRU hybrid buffer pool manager supporting pin/unpin and stealing."""
    def __init__(self, disk_manager: DiskManager, wal: WriteAheadLog, pool_size: int = 1024):
        self.disk_manager = disk_manager
        self.wal = wal
        self.pool_size = pool_size
        self.frames = [Frame(i) for i in range(pool_size)]
        self.page_table: Dict[int, int] = {}  # page_id -> frame_id
        self.lru_list: List[int] = list(range(pool_size))  # frame_ids in LRU order
        self._lock = threading.RLock()

    def _verify_checksum(self, data: bytearray) -> None:
        content = data[:PAGE_SIZE - 4]
        stored_crc = struct.unpack(">I", data[PAGE_SIZE - 4:])[0]
        # If stored_crc is 0, it might be a brand new unwritten page
        if stored_crc == 0:
            return
        computed_crc = zlib.crc32(content) & 0xFFFFFFFF
        if computed_crc != stored_crc:
            raise PageCorruptionError(f"Page CRC32 mismatch! Computed {computed_crc:#x}, Stored {stored_crc:#x}")

    def fetch_page(self, page_id: int) -> Frame:
        """Fetch page into buffer pool, pinning it."""
        with self._lock:
            if page_id in self.page_table:
                frame_id = self.page_table[page_id]
                frame = self.frames[frame_id]
                frame.pin_count += 1
                self._update_lru(frame_id)
                return frame
            
            # Evict a frame
            frame_id = self._evict()
            frame = self.frames[frame_id]
            
            # Flush victim if dirty
            if frame.page_id != -1 and frame.is_dirty:
                self._flush_frame(frame)
                del self.page_table[frame.page_id]
            
            # Read from disk
            disk_data = self.disk_manager.read_page(page_id)
            self._verify_checksum(disk_data)
            
            frame.data = disk_data
            frame.page_id = page_id
            frame.pin_count = 1
            frame.is_dirty = False
            # Parse page_lsn from page header if applicable
            if PAGE_SIZE >= 16:
                frame.page_lsn = struct.unpack(">Q", disk_data[8:16])[0]
            else:
                frame.page_lsn = 0
                
            self.page_table[page_id] = frame_id
            self._update_lru(frame_id)
            return frame

    def new_page(self) -> Tuple[int, Frame]:
        """Allocate a brand new page on disk and pin it in the buffer pool."""
        with self._lock:
            page_id = self.disk_manager.allocate_page()
            frame_id = self._evict()
            frame = self.frames[frame_id]
            
            if frame.page_id != -1 and frame.is_dirty:
                self._flush_frame(frame)
                del self.page_table[frame.page_id]
                
            frame.data = bytearray(PAGE_SIZE)
            frame.page_id = page_id
            frame.pin_count = 1
            frame.is_dirty = True
            frame.page_lsn = 0
            
            self.page_table[page_id] = frame_id
            self._update_lru(frame_id)
            return page_id, frame

    def unpin_page(self, page_id: int, is_dirty: bool) -> None:
        with self._lock:
            if page_id not in self.page_table:
                return
            frame_id = self.page_table[page_id]
            frame = self.frames[frame_id]
            if is_dirty:
                frame.is_dirty = True
            frame.pin_count = max(0, frame.pin_count - 1)

    def _evict(self) -> int:
        """Find an unpinned frame using LRU eviction."""