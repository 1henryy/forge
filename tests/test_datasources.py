import tempfile
import os

import pytest

from forge.datatypes import DataType, Field, Schema
from forge.datasources.memory_source import MemorySource
from forge.datasources.csv_source import CsvSource


class TestMemorySource:
    def test_from_pydict(self):
        src = MemorySource.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        schema = src.schema()
        assert len(schema) == 2
        assert schema[0].name == "a"
        assert schema[1].name == "b"

    def test_schema_types(self):
        src = MemorySource.from_pydict({
            "ints": [1, 2],
            "floats": [1.0, 2.0],
            "strs": ["a", "b"],
        })
        schema = src.schema()
        assert schema[0].data_type == DataType.Int64
        assert schema[1].data_type == DataType.Float64
        assert schema[2].data_type == DataType.Utf8

    def test_scan_all(self):
        src = MemorySource.from_pydict({"x": [10, 20], "y": [30, 40]})
        batches = list(src.scan())
        assert len(batches) == 1
        batch = batches[0]
        assert batch.row_count == 2
        assert batch.column_count == 2
        assert batch.column(0).get_value(0) == 10
        assert batch.column(1).get_value(1) == 40

    def test_scan_with_projection(self):
        src = MemorySource.from_pydict({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        batches = list(src.scan(projection=[0, 2]))
        assert len(batches) == 1
        batch = batches[0]
        assert batch.column_count == 2
        assert batch.schema[0].name == "a"
        assert batch.schema[1].name == "c"
        assert batch.column(0).get_value(0) == 1
        assert batch.column(1).get_value(0) == 5

    def test_scan_single_projection(self):
        src = MemorySource.from_pydict({"a": [1], "b": [2], "c": [3]})
        batches = list(src.scan(projection=[1]))
        batch = batches[0]
        assert batch.column_count == 1
        assert batch.schema[0].name == "b"
        assert batch.column(0).get_value(0) == 2


class TestCsvSource:
    def test_read_csv(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,value\n1,alice,10.5\n2,bob,20.3\n")
        src = CsvSource(str(csv_file))
        schema = src.schema()
        assert len(schema) == 3
        assert schema[0].name == "id"
        assert schema[1].name == "name"
        assert schema[2].name == "value"

    def test_csv_scan(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,hello\n2,world\n")
        src = CsvSource(str(csv_file))
        batches = list(src.scan())
        total_rows = sum(b.row_count for b in batches)
        assert total_rows == 2

    def test_csv_scan_with_projection(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("x,y,z\n1,2,3\n4,5,6\n")
        src = CsvSource(str(csv_file))
        batches = list(src.scan(projection=[0, 2]))
        batch = batches[0]
        assert batch.column_count == 2
        names = [batch.schema[i].name for i in range(batch.column_count)]
        assert "x" in names
        assert "z" in names

    def test_csv_delimiter_detection_tab(self, tmp_path):
        csv_file = tmp_path / "test.tsv"
        csv_file.write_text("a\tb\n1\t2\n3\t4\n")
        src = CsvSource(str(csv_file))
        schema = src.schema()
        assert len(schema) == 2
        assert schema[0].name == "a"
        assert schema[1].name == "b"

    def test_csv_explicit_delimiter(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a|b\n1|2\n")
        src = CsvSource(str(csv_file), delimiter="|")
        schema = src.schema()
        assert len(schema) == 2
