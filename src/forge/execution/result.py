from __future__ import annotations

import pyarrow as pa

from forge.datatypes import RecordBatch


class QueryResult:
    def __init__(self, batches: list[RecordBatch]) -> None:
        self._batches = batches

    @property
    def batches(self) -> list[RecordBatch]:
        return self._batches

    def to_arrow(self) -> pa.Table:
        if not self._batches:
            return pa.table({})
        arrow_batches = [b.to_arrow() for b in self._batches]
        return pa.Table.from_batches(arrow_batches)

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_csv(self, path: str) -> None:
        from pyarrow import csv as pa_csv
        pa_csv.write_csv(self.to_arrow(), path)

    def to_markdown(self) -> str:
        if not self._batches:
            return "(empty)"
        table = self.to_arrow()
        headers = table.column_names
        rows = table.to_pydict()

        col_widths = [len(h) for h in headers]
        str_rows: list[list[str]] = []
        n = table.num_rows
        for i in range(n):
            row = []
            for j, h in enumerate(headers):
                val = str(rows[h][i])
                col_widths[j] = max(col_widths[j], len(val))
                row.append(val)
            str_rows.append(row)

        lines: list[str] = []
        header_line = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
        sep_line = "|" + "|".join("-" * (w + 2) for w in col_widths) + "|"
        lines.append(header_line)
        lines.append(sep_line)
        for row in str_rows:
            line = "| " + " | ".join(row[i].ljust(col_widths[i]) for i in range(len(headers))) + " |"
            lines.append(line)
        return "\n".join(lines)

    def show(self, max_rows: int = 20) -> None:
        if not self._batches:
            print("(empty result)")
            return
        table = self.to_arrow()
        headers = table.column_names
        rows = table.to_pydict()
        n = min(table.num_rows, max_rows)

        col_widths = [len(h) for h in headers]
        str_rows: list[list[str]] = []
        for i in range(n):
            row = []
            for j, h in enumerate(headers):
                val = str(rows[h][i])
                col_widths[j] = max(col_widths[j], len(val))
                row.append(val)
            str_rows.append(row)

        header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        sep_line = "-+-".join("-" * w for w in col_widths)
        print(header_line)
        print(sep_line)
        for row in str_rows:
            print(" | ".join(row[i].ljust(col_widths[i]) for i in range(len(headers))))
        if table.num_rows > max_rows:
            print(f"... ({table.num_rows - max_rows} more rows)")
        print(f"\n({table.num_rows} rows)")

    @property
    def row_count(self) -> int:
        return sum(b.row_count for b in self._batches)

    def __repr__(self) -> str:
        return f"QueryResult(rows={self.row_count})"
