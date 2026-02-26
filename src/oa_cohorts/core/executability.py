from enum import Enum
from dataclasses import dataclass

class ExecStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"

@dataclass
class MeasureExecCheck:
    status: ExecStatus
    ok_variants: list[str]
    failed_variants: dict[str, str]  

@dataclass
class IndicatorExecCheck:
    status: ExecStatus
    numerator: MeasureExecCheck
    denominator: MeasureExecCheck