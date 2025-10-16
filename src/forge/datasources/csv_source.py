from typing import Iterator

import pyarrow as pa
import pyarrow.csv as pcsv

from forge.datatypes import Schema, Field, RecordBatch, ArrowVector, from_arrow_type
from .datasource import DataSource


_CANDIDATE_DELIMITERS = [",", "\t", "|", ";"]


class CsvSource(DataSource):
    def __init__(self, path: str, batch_size: int = 4096, delimiter: str | None = None) -> None:
        self._path = path
        self._batch_size = batch_size
        self._delimiter = delimiter or self._detect_delimiter()

    def _detect_delimiter(self) -> str:
        with open(self._path, "r") as f:
            first_line = f.readline()
        best = ","
        best_count = 0
        for d in _CANDIDATE_DELIMITERS:
            count = first_line.count(d)
            if count > best_count:
                best_count = count
                best = d
        return best

    def schema(self) -> Schema:
        parse_options = pcsv.ParseOptions(delimiter=self._delimiter)
        reader = pcsv.open_csv(self._path, parse_options=parse_options)
        arrow_schema = reader.schema
        fields = [
            Field(arrow_schema.field(i).name, from_arrow_type(arrow_schema.field(i).type))
            for i in range(len(arrow_schema))
        ]
        return Schema(fields)

    def scan(self, projection: list[int] | None = None) -> Iterator[RecordBatch]:
        parse_options = pcsv.ParseOptions(delimiter=self._delimiter)
        read_options = pcsv.ReadOptions(block_size=self._batch_size * 1024)
        reader = pcsv.open_csv(
            self._path,
            parse_options=parse_options,
            read_options=read_options,
        )
        for arrow_batch in reader:
            if projection is not None:
                arrow_batch = pa.RecordBatch.from_arrays(
                    [arrow_batch.column(i) for i in projection],
                    names=[arrow_batch.schema.field(i).name for i in projection],
                )
            yield RecordBatch.from_arrow(arrow_batch)
