import os
import shutil
import tempfile
import threading
import time
import unittest
import random
from typing import List, Tuple, Optional

# --- IMPLEMENTATION OF THE APPEND-ONLY B+ TREE STORAGE ENGINE ---

PAGE_SIZE = 4096
MAGIC_BYTES = b"AOBP"
VERSION = 1

class NodeHeader:
    SIZE = 1 + 1 + 2 + 4 # is_leaf(1), key_count(1), reserved(2), next_leaf_pid(4)
    
    @staticmethod
    def serialize(is_leaf: bool, key_count: int, next_leaf_pid: int) -> bytes:
        return bytes([1 if is_leaf else 0, key_count]) + (0).to_bytes(2, 'big') + next_leaf_pid.to_bytes(4, 'big')

    @staticmethod
    def deserialize(data: bytes) -> Tuple[bool, int, int]:
        is_leaf = data[0] == 1
        key_count = data[1]
        next_leaf_pid = int.from_bytes(data[4:8], 'big')
        return is_leaf, key_count, next_leaf_pid

class BufferPool:
    def __init__(self, capacity: int = 100):
        self.capacity = capacity
        self.pool = {} # pid -> bytes
        self.lock = threading.Lock()

    def get(self, pid: int) -> Optional[bytes]:
        with self.lock:
            return self.pool.get(pid)

    def put(self, pid: int, data: bytes):
        with self.lock:
            if len(self.pool) >= self.capacity:
                # Evict arbitrary item
                self.pool.pop(next(iter(self.pool)))
            self.pool[pid] = data

    def invalidate(self, pid: int):
        with self.lock:
            if pid in self.pool:
                del self.pool[pid]

    def clear(self):
        with self.lock:
            self.pool.clear()

class AppendOnlyBPlusTree:
    MAX_KEYS_LEAF = 3
    MAX_KEYS_INTERNAL = 3

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.file_lock = threading.Lock()
        self.buffer_pool = BufferPool()
        self.root_pid = 0
        self.file_size = 0

        if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
            self._init_new_file()
        else:
            self._load_metadata()

    def _init_new_file(self):
        with open(self.filepath, "wb") as f:
            # File layout:
            # [MAGIC(4)][VERSION(4)][ROOT_PID(4)][RESERVED(20)] -> 32 bytes header
            header = MAGIC_BYTES + VERSION.to_bytes(4, 'big') + (1).to_bytes(4, 'big') + b'\x00' * 20
            f.write(header)
            
            # Create root node (empty leaf) at PID 1
            root_data = NodeHeader.serialize(is_leaf=True, key_count=0, next_leaf_pid=0) + b'\x00' * (PAGE_SIZE - NodeHeader.SIZE)
            f.write(root_data)
            self.root_pid = 1
            self.file_size = len(header) + PAGE_SIZE

    def _load_metadata(self):
        with open(self.filepath, "rb") as f:
            f.seek(8)
            self.root_pid = int.from_bytes(f.read(4), 'big')
            f.seek(0, os.SEEK_END)
            self.file_size = f.tell()

    def _read_page(self, pid: int) -> bytes:
        cached = self.buffer_pool.get(pid)
        if cached:
            return cached
        
        offset = 32 + (pid - 1) * PAGE_SIZE
        with self.file_lock:
            with open(self.filepath, "rb") as f:
                f.seek(offset)
                data = f.read(PAGE_SIZE)
        self.buffer_pool.put(pid, data)
        return data

    def _allocate_page(self, data: bytes) -> int:
        with self.file_lock:
            with open(self.filepath, "ab") as f:
                f.seek(0, os.SEEK_END)
                offset = f.tell()
                # Ensure alignment
                if (offset - 32) % PAGE_SIZE != 0:
                    # Pad if necessary
                    pad_len = PAGE_SIZE - ((offset - 32) % PAGE_SIZE)
                    f.write(b'\x00' * pad_len)
                    offset = f.tell()
                
                f.write(data)
                pid = ((offset - 32) // PAGE_SIZE) + 1
                self.file_size = f.tell()
                self.buffer_pool.put(pid, data)
                return pid

    def _update_root(self, new_root_pid: int):
        with self.file_lock:
            with open(self.filepath, "r+b") as f:
                f.seek(8)
                f.write(new_root_pid.to_bytes(4, 'big'))
                f.flush()
                os.fsync(f.fileno())
        self.root_pid = new_root_pid

    def search(self, key: int) -> Optional[bytes]:
        curr_pid = self.root_pid
        while True:
            page = self._read_page(curr_pid)
            is_leaf, key_count, next_leaf_pid = NodeHeader.deserialize(page[:NodeHeader.SIZE])
            
            if is_leaf:
                # Parse leaf: [Keys: int * K] [Values: 8 bytes offset/len or inline]
                # Let's use fixed slot size: Key(4 bytes) + Value(32 bytes)
                offset = NodeHeader.SIZE
                slot_size = 36
                for _ in range(key_count):
                    k = int.from_bytes(page[offset:offset+4], 'big')
                    if k == key:
                        val = page[offset+4:offset+36].rstrip(b'\x00')
                        return val
                    offset += slot_size
                
                if next_leaf_pid == 0:
                    return None
                curr_pid = next_leaf_pid
            else:
                # Internal node: [Keys: int * K] [Children: pid * (K+1)]
                # Format: [Key0, Key1...][Child0, Child1, Child2...]
                keys = []
                offset = NodeHeader.SIZE
                for i in range(key_count):
                    keys.append(int.from_bytes(page[offset:offset+4], 'big'))
                    offset += 4
                
                children = []
                for i in range(key_count + 1):
                    children.append(int.from_bytes(page[offset:offset+4], 'big'))
                    offset += 4
                
                idx = 0
                while idx < len(keys) and key >= keys[idx]:
                    idx += 1
                curr_pid = children[idx]

    def insert(self, key: int, value: bytes):
        val_padded = value.ljust(32, b'\x00')[:32]
        with self.file_lock:
            new_root, promoted = self._insert_internal(self.root_pid, key, val_padded)
            if promoted is not None:
                # Root split
                p_key, left_pid, right_pid = promoted
                # Create new root
                new_root_data = NodeHeader.serialize(is_leaf=False, key_count=1, next_leaf_pid=0)
                new_root_data += p_key.to_bytes(4, 'big')
                new_root_data += left_pid.to_bytes(4, 'big') + right_pid.to_bytes(4, 'big')
                new_root_data = new_root_data.ljust(PAGE_SIZE, b'\x00')
                new_root_pid = self._allocate_page(new_root_data)
                self._update_root(new_root_pid)
            elif new_root is not None:
                pass

    def _insert_internal(self, pid: int, key: int, val_padded: bytes) -> Tuple[Optional[int], Optional[Tuple[int, int, int]]]:
        page = self._read_page(pid)
        is_leaf, key_count, next_leaf_pid = NodeHeader.deserialize(page[:NodeHeader.SIZE])

        if is_leaf:
            # Parse keys and values
            offset = NodeHeader.SIZE
            slot_size = 36
            items = []
            for _ in range(key_count):
                k = int.from_bytes(page[offset:offset+4], 'big')
                v = page[offset+4:offset+36]
                items.append((k, v))
                offset += slot_size

            # Check if key exists, update if so
            updated = False
            for idx, (k, v) in enumerate(items):
                if k == key:
                    items[idx] = (key, val_padded)
                    updated = True
                    break
            if not updated:
                items.append((key, val_padded))
                items.sort(key=lambda x: x[0])

            if len(items) <= self.MAX_KEYS_LEAF:
                # Write back updated leaf
                new_page = NodeHeader.serialize(is_leaf=True, key_count=len(items), next_leaf_pid=next_leaf_pid)
                for k, v in items:
                    new_page += k.to_bytes(4, 'big') + v
                new_page = new_page.ljust(PAGE_SIZE, b'\x00')
                new_pid = self._allocate_page(new_page)
                return new_pid, None
            else:
                # Split leaf
                mid = len(items) // 2
                left_items = items[:mid]
                right_items = items[mid:]

                # Build right leaf first
                right_page = NodeHeader.serialize(is_leaf=True, key_count=len(right_items), next_leaf_pid=next_leaf_pid)
                for k, v in right_items:
                    right_page += k.to_bytes(4, 'big') + v
                right_page = right_page.ljust(PAGE_SIZE, b'\x00')
                right_pid = self._allocate_page(right_page)

                # Build left leaf pointing to right leaf
                left_page = NodeHeader.serialize(is_leaf=True, key_count=len(left_items), next_leaf_pid=right_pid)
                for k, v in left_items:
                    left_page += k.to_bytes(4, 'big') + v
                left_page = left_page.ljust(PAGE_SIZE, b'\x00')
                left_pid = self._allocate_page(left_page)

                promoted_key = right_items[0][0]
                return left_pid, (promoted_key, left_pid, right_pid)
        else:
            # Internal node
            offset = NodeHeader.SIZE
            keys = []
            for _ in range(key_count):
                keys.append(int.from_bytes(page[offset:offset+4], 'big'))
                offset += 4
            
            children = []
            for _ in range(key_count + 1):
                children.append(int.from_bytes(page[offset:offset+4], 'big'))
                offset += 4

            idx = 0
            while idx < len(keys) and key >= keys[idx]:
                idx += 1
            
            target_child = children[idx]
            new_child_pid, promoted = self._insert_internal(target_child, key, val_padded)

            if promoted is None:
                children[idx] = new_child_pid
                # Just rewrite internal node with updated child pointer