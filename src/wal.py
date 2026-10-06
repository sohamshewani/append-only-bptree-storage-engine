import os
import struct
import threading
import mmap
import zlib
from typing import List, Tuple, Optional, BinaryIO

# WAL Record Types
LOG_BEGIN = 1
LOG_COMMIT = 2
LOG_ABORT = 3
LOG_INSERT = 4
LOG_SPLIT = 5
LOG_PAGE_WRITE = 6
LOG_CHECKPOINT = 7

class WALRecord:
    def __init__(self, lsn: int, tx_id: int, record_type: int, payload: bytes):
        self.lsn = lsn
        self.tx_id = tx_id
        self.record_type = record_type
        self.payload = payload

    def serialize(self) -> bytes:
        # Binary Framing: [Length: u32][LSN: u64][TxID: u64][Type: u8][Payload]
        # Length includes LSN (8) + TxID (8) + Type (1) + Payload (N)
        payload_len = len(self.payload)
        total_len = 8 + 8 + 1 + payload_len
        header = struct.pack(">IQQB", total_len, self.lsn, self.tx_id, self.record_type)
        return header + self.payload

    @staticmethod
    def deserialize(data: bytes) -> 'WALRecord':
        if len(data) < 17:
            raise ValueError("Data too short for WAL record header")
        total_len, lsn, tx_id, record_type = struct.unpack(">IQQB", data[:17])
        payload = data[17:4 + total_len]
        return WALRecord(lsn, tx_id, record_type, payload)

class WriteAheadLog:
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.lock = threading.RLock()
        self.file: Optional[BinaryIO] = None
        self._next_lsn = 1
        self._init_log_file()

    def _init_log_file(self) -> None:
        with self.lock:
            if os.path.exists(self.wal_path):
                self.file = open(self.wal_path, "r+b")
                self._recover_next_lsn()
            else:
                self.file = open(self.wal_path, "w+b")
                self.file.flush()

    def _recover_next_lsn(self) -> None:
        self.file.seek(0, os.SEEK_END)
        file_size = self.file.tell()
        self.file.seek(0, os.SEEK_SET)
        
        max_lsn = 0
        while True:
            length_bytes = self.file.read(4)
            if not length_bytes or len(length_bytes) < 4:
                break
            total_len = struct.unpack(">I", length_bytes)[0]
            record_data = self.file.read(total_len)
            if len(record_data) < total_len:
                break  # Partial write / corruption at tail
            lsn, tx_id, record_type = struct.unpack(">QQB", record_data[:17])
            if lsn >= max_lsn:
                max_lsn = lsn + 1
        
        if max_lsn > 0:
            self._next_lsn = max_lsn
        else:
            self._next_lsn = 1
        self.file.seek(0, os.SEEK_END)

    def append(self, tx_id: int, record_type: int, payload: bytes) -> int:
        with self.lock:
            lsn = self._next_lsn
            self._next_lsn += 1
            record = WALRecord(lsn, tx_id, record_type, payload)
            data = record.serialize()
            self.file.seek(0, os.SEEK_END)
            self.file.write(data)
            return lsn

    def flush(self, lsn: Optional[int] = None) -> None:
        with self.lock:
            if self.file:
                self.file.flush()
                os.fsync(self.file.fileno())

    def log_begin(self, tx_id: int) -> int:
        return self.append(tx_id, LOG_BEGIN, b"")

    def log_commit(self, tx_id: int) -> int:
        lsn = self.append(tx_id, LOG_COMMIT, b"")
        self.flush(lsn)
        return lsn

    def log_abort(self, tx_id: int) -> int:
        lsn = self.append(tx_id, LOG_ABORT, b"")
        self.flush(lsn)
        return lsn

    def log_insert(self, tx_id: int, page_id: int, key: bytes, value: bytes) -> int:
        # Payload format: [PageID: u64][KeyLen: u16][Key][ValueLen: u32][Value]
        key_len = len(key)
        val_len = len(value)
        payload = struct.pack(f">QHI{key_len}s{val_len}s", page_id, key_len, val_len, key, value)
        return self.append(tx_id, LOG_INSERT, payload)

    def log_split(self, tx_id: int, old_page_id: int, new_page_id: int, split_key: bytes) -> int:
        # Payload format: [OldPageID: u64][NewPageID: u64][KeyLen: u16][SplitKey]
        key_len = len(split_key)
        payload = struct.pack(f">QQH{key_len}s", old_page_id, new_page_id, key_len, split_key)
        return self.append(tx_id, LOG_SPLIT, payload)

    def log_page_write(self, tx_id: int, page_id: int, page_data: bytes) -> int:
        # Payload format: [PageID: u64][PageData: 4096 bytes]
        payload = struct.pack(f">Q4096s", page_id, page_data)
        return self.append(tx_id, LOG_PAGE_WRITE, payload)

    def log_checkpoint(self, tx_id: int, active_txs: List[int], dirty_pages: List[Tuple[int, int]]) -> int:
        # Payload format: [NumActiveTx: u32][Tx1, Tx2...][NumDirtyPages: u32][(PageID: u64, LSN: u64)...]
        active_tx_bytes = struct.pack(f">I" + f"Q" * len(active_txs), len(active_txs), *active_txs)
        dirty_page_bytes_list = []
        for page_id, lsn in dirty_pages:
            dirty_page_bytes_list.append(struct.pack(">QQ", page_id, lsn))
        dirty_pages_payload = struct.pack(">I", len(dirty_pages)) + b"".join(dirty_page_bytes_list)
        payload = active_tx_bytes + dirty_pages_payload
        return self.append(tx_id, LOG_CHECKPOINT, payload)

    def iterator(self, start_lsn: int = 0) -> 'WALIterator':
        return WALIterator(self.wal_path, start_lsn)

    def close(self) -> None:
        with self.lock:
            if self.file:
                self.flush()
                self.file.close()
                self.file = None

class WALIterator:
    def __init__(self, wal_path: str, start_lsn: int = 0):
        self.wal_path = wal_path
        self.start_lsn = start_lsn
        self.file = open(self.wal_path, "rb")

    def __iter__(self):
        self.file.seek(0, os.SEEK_SET)
        return self

    def __next__(self) -> WALRecord:
        while True:
            length_bytes = self.file.read(4)
            if not length_bytes or len(length_bytes) < 4:
                self.file.close()
                raise StopIteration
            total_len = struct.unpack(">I", length_bytes)[0]
            record_data = self.file.read(total_len)
            if len(record_data) < total_len:
                self.file.close()
                raise StopIteration
            
            lsn, tx_id, record_type = struct.unpack(">QQB", record_data[:17])
            payload = record_data[17:]
            record = WALRecord(lsn, tx_id, record_type, payload)
            
            if record.lsn >= self.start_lsn:
                return record

    def close(self):
        if self.file and not self.file.closed:
            self.file.close()