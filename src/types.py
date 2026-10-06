from enum import IntEnum, auto
from typing import NamedTuple, Optional, List, Dict, Any


class WALType(IntEnum):
    BEGIN = 1
    COMMIT = 2
    ABORT = 3
    INSERT = 4
    SPLIT = 5
    PAGE_WRITE = 6
    CHECKPOINT = 7


class LockMode(IntEnum):
    SHARED = auto()
    EXCLUSIVE = auto()


class LSN(NamedTuple):
    partition: int
    sequence: int

    def __lt__(self, other: "LSN") -> bool:
        if self.partition != other.partition:
            return self.partition < other.partition
        return self.sequence < other.sequence

    def __le__(self, other: "LSN") -> bool:
        return self < other or self == other

    def __gt__(self, other: "LSN") -> bool:
        return not self <= other

    def __ge__(self, other: "LSN") -> bool:
        return not self < other

    def to_bytes(self) -> bytes:
        return self.partition.to_bytes(4, byteorder="big", signed=False) + \
               self.sequence.to_bytes(8, byteorder="big", signed=False)

    @classmethod
    def from_bytes(cls, data: bytes) -> "LSN":
        if len(data) < 12:
            raise ValueError("Data too short to decode LSN (requires 12 bytes)")
        partition = int.from_bytes(data[0:4], byteorder="big", signed=False)
        sequence = int.from_bytes(data[4:12], byteorder="big", signed=False)
        return cls(partition=partition, sequence=sequence)


class TxID(NamedTuple):
    id: int

    def to_bytes(self) -> bytes:
        return self.id.to_bytes(8, byteorder="big", signed=False)

    @classmethod
    def from_bytes(cls, data: bytes) -> "TxID":
        if len(data) < 8:
            raise ValueError("Data too short to decode TxID (requires 8 bytes)")
        return cls(id=int.from_bytes(data[0:8], byteorder="big", signed=False))


class PageID(NamedTuple):
    id: int

    def to_bytes(self) -> bytes:
        return self.id.to_bytes(8, byteorder="big", signed=False)

    @classmethod
    def from_bytes(cls, data: bytes) -> "PageID":
        if len(data) < 8:
            raise ValueError("Data too short to decode PageID (requires 8 bytes)")
        return cls(id=int.from_bytes(data[0:8], byteorder="big", signed=False))


class WALRecord(NamedTuple):
    length: int
    lsn: LSN
    tx_id: TxID
    record_type: WALType
    payload: bytes

    def serialize(self) -> bytes:
        type_byte = bytes([int(self.record_type)])
        lsn_bytes = self.lsn.to_bytes()
        tx_bytes = self.tx_id.to_bytes()
        body = lsn_bytes + tx_bytes + type_byte + self.payload
        total_length = 4 + len(body)
        length_bytes = total_length.to_bytes(4, byteorder="big", signed=False)
        return length_bytes + body

    @classmethod
    def deserialize(cls, data: bytes) -> "WALRecord":
        if len(data) < 4:
            raise ValueError("Data too short for WALRecord length prefix")
        length = int.from_bytes(data[0:4], byteorder="big", signed=False)
        if len(data) < length:
            raise ValueError(f"Incomplete WAL record: expected {length} bytes, got {len(data)}")
        
        body = data[4:length]
        if len(body) < 21:  # 12 (LSN) + 8 (TxID) + 1 (Type)
            raise ValueError("WALRecord body too short")
        
        lsn = LSN.from_bytes(body[0:12])
        tx_id = TxID.from_bytes(body[12:20])
        record_type = WALType(body[20])
        payload = body[21:]
        
        return cls(
            length=length,
            lsn=lsn,
            tx_id=tx_id,
            record_type=record_type,
            payload=payload
        )


class PageHeader(NamedTuple):
    page_id: PageID
    lsn: LSN
    is_leaf: bool
    item_count: int
    free_space_pointer: int
    checksum: int

    def serialize(self) -> bytes:
        pid_b = self.page_id.to_bytes()
        lsn_b = self.lsn.to_bytes()
        leaf_b = b'\x01' if self.is_leaf else b'\x00'
        count_b = self.item_count.to_bytes(4, byteorder="big", signed=False)
        fsp_b = self.free_space_pointer.to_bytes(4, byteorder="big", signed=False)
        chk_b = self.checksum.to_bytes(4, byteorder="big", signed=False)
        return pid_b + lsn_b + leaf_b + count_b + fsp_b + chk_b

    @classmethod
    def deserialize(cls, data: bytes) -> "PageHeader":
        if len(data) < 33:
            raise ValueError("Data too short for PageHeader (requires 33 bytes)")
        page_id = PageID.from_bytes(data[0:8])
        lsn = LSN.from_bytes(data[8:20])
        is_leaf = bool(data[20])
        item_count = int.from_bytes(data[21:25], byteorder="big", signed=False)
        free_space_pointer = int.from_bytes(data[25:29], byteorder="big", signed=False)
        checksum = int.from_bytes(data[29:33], byteorder="big", signed=False)
        return cls(
            page_id=page_id,
            lsn=lsn,
            is_leaf=is_leaf,
            item_count=item_count,
            free_space_pointer=free_space_pointer,
            checksum=checksum
        )


class DiskPage(NamedTuple):
    header: PageHeader
    data: bytes

    @property
    def capacity(self) -> int:
        return 4096

    def serialize(self) -> bytes:
        header_bytes = self.header.serialize()
        padding_len = self.capacity - len(header_bytes) - len(self.data)
        if padding_len < 0:
            raise ValueError("Page payload exceeds fixed size of 4096 bytes")
        return header_bytes + self.data + (b'\x00' * padding_len)

    @classmethod
    def deserialize(cls, data: bytes, expected_capacity: int = 4096) -> "DiskPage":
        if len(data) != expected_capacity:
            raise ValueError(f"Invalid disk page size: expected {expected_capacity}, got {len(data)}")
        header = PageHeader.deserialize(data[0:33])
        payload_end = 33 + (expected_capacity - 33)  # Remaining buffer allocated for slots and data
        page_data = data[33:payload_end]
        return cls(header=header, data=page_data)