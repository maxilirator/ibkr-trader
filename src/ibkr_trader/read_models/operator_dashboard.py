from __future__ import annotations

from typing import Any

from ibkr_trader.read_models import operator_dashboard_builders as _builders
from ibkr_trader.read_models.operator_dashboard_common import OperatorAccountDayPerformance
from ibkr_trader.read_models.operator_dashboard_common import OperatorAccountPerformancePoint
from ibkr_trader.read_models.operator_dashboard_common import OperatorAccountSnapshot
from ibkr_trader.read_models.operator_dashboard_common import OperatorBrokerAttention
from ibkr_trader.read_models.operator_dashboard_common import OperatorDashboardSnapshot
from ibkr_trader.read_models.operator_dashboard_common import OperatorExecutionFill
from ibkr_trader.read_models.operator_dashboard_common import OperatorKillSwitch
from ibkr_trader.read_models.operator_dashboard_common import OperatorOpenOrder
from ibkr_trader.read_models.operator_dashboard_common import OperatorPositionSnapshot
from ibkr_trader.read_models.operator_dashboard_common import OperatorReconciliationIssue
from ibkr_trader.read_models.operator_dashboard_common import OperatorReconciliationRun
from ibkr_trader.read_models.operator_dashboard_common import serialize_operator_dashboard_snapshot
from ibkr_trader.read_models.operator_dashboard_common import utc_now


def build_operator_dashboard_snapshot(*args: Any, **kwargs: Any) -> OperatorDashboardSnapshot:
    previous_utc_now = _builders.utc_now
    _builders.utc_now = utc_now
    try:
        return _builders.build_operator_dashboard_snapshot(*args, **kwargs)
    finally:
        _builders.utc_now = previous_utc_now

__all__ = [
    "OperatorAccountDayPerformance",
    "OperatorAccountPerformancePoint",
    "OperatorAccountSnapshot",
    "OperatorBrokerAttention",
    "OperatorDashboardSnapshot",
    "OperatorExecutionFill",
    "OperatorKillSwitch",
    "OperatorOpenOrder",
    "OperatorPositionSnapshot",
    "OperatorReconciliationIssue",
    "OperatorReconciliationRun",
    "build_operator_dashboard_snapshot",
    "serialize_operator_dashboard_snapshot",
    "utc_now",
]
