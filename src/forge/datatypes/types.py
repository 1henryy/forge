from enum import Enum, auto

import pyarrow as pa


class DataType(Enum):
    Int8 = auto()
    Int16 = auto()
    Int32 = auto()
    Int64 = auto()
    Float32 = auto()
    Float64 = auto()
    Utf8 = auto()
    Boolean = auto()


_TO_ARROW: dict[DataType, pa.DataType] = {
    DataType.Int8: pa.int8(),
    DataType.Int16: pa.int16(),
    DataType.Int32: pa.int32(),
    DataType.Int64: pa.int64(),
    DataType.Float32: pa.float32(),
    DataType.Float64: pa.float64(),
    DataType.Utf8: pa.utf8(),
    DataType.Boolean: pa.bool_(),
}

_FROM_ARROW: dict[pa.DataType, DataType] = {v: k for k, v in _TO_ARROW.items()}


def to_arrow_type(dt: DataType) -> pa.DataType:
    return _TO_ARROW[dt]


def from_arrow_type(arrow_type: pa.DataType) -> DataType:
    if arrow_type not in _FROM_ARROW:
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")
    return _FROM_ARROW[arrow_type]
