"""
Custom exceptions for the Rule Engine
"""


class RuleEngineError(Exception):
    """Base exception for all rule engine errors"""
    pass


class ValidationError(RuleEngineError):
    """Raised when input data validation fails"""
    pass


class ContractError(RuleEngineError):
    """Raised when contract processing fails"""
    pass


class ComputationError(RuleEngineError):
    """Raised when computation fails"""
    pass


class EligibilityError(RuleEngineError):
    """Raised when eligibility checking fails"""
    pass
