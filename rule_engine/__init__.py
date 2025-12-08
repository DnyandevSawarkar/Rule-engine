"""
PLB (Performance-Based Loyalty) Rule Engine

A production-ready rule engine for processing airline coupon data against
multiple contracts and generating detailed analysis reports.
"""

__version__ = "1.0.0"
__author__ = "Rule Engine Team"

from .core import RuleEngine
from .models import CouponData, ContractAnalysis, ProcessingResult
from .exceptions import RuleEngineError, ValidationError, ContractError

__all__ = [
    "RuleEngine",
    "CouponData", 
    "ContractAnalysis",
    "ProcessingResult",
    "RuleEngineError",
    "ValidationError", 
    "ContractError"
]
