from __future__ import annotations

import sys
import time

from forge.execution.context import ExecutionContext


def _get_prompt_session():
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import FileHistory
        from prompt_toolkit.lexers import PygmentsLexer
        import os

        history_path = os.path.expanduser("~/.forge_history")

        try:
            from pygments.lexers.sql import SqlLexer
            lexer = PygmentsLexer(SqlLexer)
        except ImportError:
            lexer = None

        return PromptSession(
            history=FileHistory(history_path),
            lexer=lexer,
            multiline=False,
        )
    except ImportError:
        return None


def main() -> None:
    ctx = ExecutionContext()
    session = _get_prompt_session()

    print("Forge SQL Engine v0.1.0")
    print("Type .help for commands, .quit to exit\n")

    buffer: list[str] = []

    while True:
        try:
            if session:
                prompt = "forge> " if not buffer else "   ... "
                line = session.prompt(prompt)
            else:
                prompt = "forge> " if not buffer else "   ... "
                line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        stripped = line.strip()

        if not buffer and stripped.startswith("."):
            _handle_command(ctx, stripped)
            continue

        buffer.append(line)
        full = " ".join(buffer).strip()

        if not full.endswith(";") and not full.upper().startswith("EXPLAIN"):
            if full.upper().startswith("EXPLAIN") or full.endswith(";"):
                pass
            else:
                continue

        sql = full.rstrip(";").strip()
        buffer.clear()

        if not sql:
            continue

        try:
            start = time.perf_counter()
            result = ctx.sql(sql)
            elapsed = time.perf_counter() - start
            result.show()
            print(f"Time: {elapsed:.3f}s\n")
        except Exception as e:
            print(f"Error: {e}\n")


def _handle_command(ctx: ExecutionContext, cmd: str) -> None:
    parts = cmd.split()
    name = parts[0].lower()

    if name == ".quit" or name == ".exit":
        print("Bye!")
        sys.exit(0)

    elif name == ".help":
        print("Commands:")
        print("  .tables              List registered tables")
        print("  .schema <table>      Show table schema")
        print("  .load <name> <path>  Load CSV file as table")
        print("  .loadpq <name> <path> Load Parquet file as table")
        print("  .quit                Exit the REPL")
        print()
        print("SQL:")
        print("  SELECT ... FROM ... WHERE ... ;")
        print("  EXPLAIN SELECT ... ;")
        print()

    elif name == ".tables":
        tables = ctx.tables()
        if tables:
            for t in tables:
                print(f"  {t}")
        else:
            print("  (no tables registered)")
        print()

    elif name == ".schema":
        if len(parts) < 2:
            print("Usage: .schema <table>")
            return
        table = parts[1]
        try:
            schema = ctx.table_schema(table)
            for field in schema:
                print(f"  {field.name}: {field.data_type.name}")
        except ValueError as e:
            print(f"Error: {e}")
        print()

    elif name == ".load":
        if len(parts) < 3:
            print("Usage: .load <name> <path>")
            return
        table_name = parts[1]
        path = parts[2]
        try:
            ctx.register_csv(table_name, path)
            print(f"Loaded '{path}' as '{table_name}'")
        except Exception as e:
            print(f"Error: {e}")
        print()

    elif name == ".loadpq":
        if len(parts) < 3:
            print("Usage: .loadpq <name> <path>")
            return
        table_name = parts[1]
        path = parts[2]
        try:
            ctx.register_parquet(table_name, path)
            print(f"Loaded '{path}' as '{table_name}'")
        except Exception as e:
            print(f"Error: {e}")
        print()

    else:
        print(f"Unknown command: {name}. Type .help for available commands.")
        print()


if __name__ == "__main__":
    main()
