from __future__ import annotations

from forge.logicalplan.plan import LogicalPlan
from forge.physicalplan.plan import PhysicalPlan


def format_logical_plan(plan: LogicalPlan) -> str:
    return plan.format()


def format_physical_plan(plan: PhysicalPlan) -> str:
    return plan.format()


def format_plan_tree(plan: LogicalPlan | PhysicalPlan) -> str:
    lines: list[str] = []
    _build_tree(plan, lines, "", True)
    return "\n".join(lines)


def _build_tree(
    node: LogicalPlan | PhysicalPlan,
    lines: list[str],
    prefix: str,
    is_last: bool,
) -> None:
    connector = "└── " if is_last else "├── "
    lines.append(prefix + connector + str(node))

    children = node.children()
    child_prefix = prefix + ("    " if is_last else "│   ")
    for i, child in enumerate(children):
        _build_tree(child, lines, child_prefix, i == len(children) - 1)
