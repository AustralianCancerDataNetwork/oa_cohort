import sqlalchemy as sa
from sqlalchemy.sql import Select, CompoundSelect
from sqlalchemy.engine import Row as SARow
from typing import TypeAlias, Any
from ..core import RuleCombination

Row = SARow[Any]

SQLQuery: TypeAlias = Select | CompoundSelect

COMBINATION_SQL = {
    RuleCombination.rule_or: sa.union_all,
    RuleCombination.rule_and: sa.intersect_all,
    RuleCombination.rule_except: sa.except_all,
}