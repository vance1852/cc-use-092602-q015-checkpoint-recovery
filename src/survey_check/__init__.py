"""自然资源测绘校核的可恢复分片执行组件。"""

from .compute import ALGORITHM_VERSION, area_stats, boundary_compare, run_shard, summary
from .contracts import JobDefinition, ParcelEntry, Rule, ShardSpec, ValidationError, validate_result
from .service import SurveyCheckService

__all__ = [
    "ALGORITHM_VERSION",
    "JobDefinition",
    "ParcelEntry",
    "Rule",
    "ShardSpec",
    "SurveyCheckService",
    "ValidationError",
    "area_stats",
    "boundary_compare",
    "run_shard",
    "summary",
    "validate_result",
]

__version__ = "0.1.0"
