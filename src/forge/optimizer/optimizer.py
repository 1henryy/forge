from __future__ import annotations

from abc import ABC, abstractmethod

from forge.logicalplan.plan import LogicalPlan


class OptimizerRule(ABC):
    @abstractmethod
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        ...


class Optimizer:
    def __init__(self) -> None:
        self.rules: list[OptimizerRule] = []

    def add_rule(self, rule: OptimizerRule) -> None:
        self.rules.append(rule)

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        result = plan
        for rule in self.rules:
            result = rule.optimize(result)
        return result

    @classmethod
    def default(cls) -> Optimizer:
        opt = cls()
        from .projection_pushdown import ProjectionPushdown
        from .filter_pushdown import FilterPushdown
        from .constant_folding import ConstantFolding
        opt.add_rule(ProjectionPushdown())
        opt.add_rule(FilterPushdown())
        opt.add_rule(ConstantFolding())
        return opt
