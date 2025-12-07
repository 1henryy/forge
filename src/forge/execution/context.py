# __MARKER_2__
from __future__ import annotations

from forge.datasources import CsvSource, DataSource, MemorySource, ParquetSource
from forge.logicalplan.dataframe import DataFrame
from forge.logicalplan.plan import LogicalPlan, Scan
from forge.optimizer.optimizer import Optimizer
from forge.queryplanner.planner import QueryPlanner
from forge.sql.parser import Parser
from forge.sql.planner import SqlPlanner
from forge.sql.tokenizer import Tokenizer

from .result import QueryResult


class ExecutionContext:
    def __init__(self) -> None:
        self._tables: dict[str, DataSource] = {}
        self._config: dict[str, object] = {
            "parallelism": 1,
        }
        self._optimizer = Optimizer.default()
        self._query_planner = QueryPlanner()

    @property
    def config(self) -> dict[str, object]:
        return self._config

    def set_parallelism(self, n: int) -> None:
        self._config["parallelism"] = n

    def register_table(self, name: str, source: DataSource) -> None:
        self._tables[name] = source

    def register_csv(self, name: str, path: str, **kwargs) -> None:
        self._tables[name] = CsvSource(path, **kwargs)

    def register_parquet(self, name: str, path: str) -> None:
        self._tables[name] = ParquetSource(path)

    def register_memory(self, name: str, data: dict[str, list]) -> None:
        self._tables[name] = MemorySource.from_pydict(data)

    def tables(self) -> list[str]:
        return list(self._tables.keys())

    def table_schema(self, name: str):
        source = self._tables.get(name)
        if source is None:
            raise ValueError(f"Unknown table: {name}")
        return source.schema()

    def csv(self, path: str) -> DataFrame:
        source = CsvSource(path)
        plan = Scan(source, path)
        return DataFrame(plan)

    def parquet(self, path: str) -> DataFrame:
        source = ParquetSource(path)
        plan = Scan(source, path)
        return DataFrame(plan)

    def sql(self, sql_text: str) -> QueryResult:
        tokens = Tokenizer().tokenize(sql_text)
        statement = Parser(tokens).parse()

        if statement.is_explain:
            planner = SqlPlanner(self._tables)
            logical = planner.create_plan(statement)
            optimized = self._optimizer.optimize(logical)
            physical = self._query_planner.create_physical_plan(optimized)

            from forge.datatypes import DataType, Field, Schema, RecordBatch as RB
            from forge.datatypes import ArrowVector
            import pyarrow as pa

            plan_text = physical.format()
            schema = Schema([Field("plan", DataType.Utf8)])
            col = ArrowVector(pa.array([plan_text]))
            batch = RB(schema, [col])
            return QueryResult([batch])

        planner = SqlPlanner(self._tables)
        logical = planner.create_plan(statement)
        optimized = self._optimizer.optimize(logical)
        physical = self._query_planner.create_physical_plan(optimized)

        batches = list(physical.execute())
        return QueryResult(batches)

    def execute(self, df: DataFrame) -> QueryResult:
        logical = df.logical_plan()
        optimized = self._optimizer.optimize(logical)
        physical = self._query_planner.create_physical_plan(optimized)
        batches = list(physical.execute())
        return QueryResult(batches)

    def execute_plan(self, plan: LogicalPlan) -> QueryResult:
        optimized = self._optimizer.optimize(plan)
        physical = self._query_planner.create_physical_plan(optimized)
        batches = list(physical.execute())
        return QueryResult(batches)
