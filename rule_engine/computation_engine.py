"""
Computation engine for trigger and payout calculations
"""

import re
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Any, Union
from loguru import logger

from .models import CouponData, ContractData
from .exceptions import ComputationError
from .formula_parser import FormulaParser


class ComputationEngine:
    """
    Handles all computation logic for trigger and payout calculations
    """
    
    def __init__(self):
        """Initialize computation engine"""
        self.logger = logger
        self.precision = 4  # Decimal precision for calculations
        self.formula_parser = FormulaParser()

    def _safe_decimal(self, value: Any, default: str = "0") -> Decimal:
        """
        Safely convert a value to Decimal.
        
        Any invalid or non‑numeric input is logged and replaced by the
        provided default (as a string) to avoid Decimal(ConversionSyntax)
        bubbling up and breaking processing.
        """
        try:
            return Decimal(str(value))
        except Exception as e:
            self.logger.warning(f"Invalid decimal value '{value}' – using default {default}. Error: {e}")
            return Decimal(default)
    
    def compute_trigger(self, coupon: CouponData, contract: ContractData) -> Decimal:
        """
        Compute trigger value using contract formula
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Calculated trigger value
        """
        try:
            # Check if contract has a specific trigger formula
            if contract.trigger_formula and self._is_mathematical_formula(contract.trigger_formula):
                # Use formula parser to evaluate the trigger formula
                trigger_value = self.formula_parser.evaluate_formula(
                    contract.trigger_formula, coupon, contract
                )
                self.logger.debug(f"Trigger value calculated using formula: {trigger_value}")
            else:
                # Fallback to component-based calculation
                components = contract.trigger_components
                considered_revenue = self._calculate_considered_revenue(coupon, components)
                trigger_value = self._apply_trigger_formula(considered_revenue, contract)
                self.logger.debug(f"Trigger value calculated from components: {trigger_value}")
            
            # Apply capping if enabled
            if hasattr(contract, 'trigger_capping') and contract.trigger_capping:
                trigger_value = self._apply_capping(trigger_value, contract.trigger_capping_value)
            
            return trigger_value.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            
        except Exception as e:
            self.logger.error(f"Error computing trigger: {str(e)}")
            raise ComputationError(f"Trigger computation failed: {str(e)}")
    
    def compute_payout(self, coupon: CouponData, contract: ContractData) -> Decimal:
        """
        Compute payout value using contract formula
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Calculated payout value
        """
        try:
            # Check if contract has a specific payout formula
            if contract.payout_formula and self._is_mathematical_formula(contract.payout_formula):
                # Use formula parser to evaluate the payout formula
                payout_value = self.formula_parser.evaluate_formula(
                    contract.payout_formula, coupon, contract
                )
                self.logger.debug(f"Payout value calculated using formula: {payout_value}")
            else:
                # Fallback to type-based calculation
                components = contract.payout_components
                considered_revenue = self._calculate_considered_revenue(coupon, components)
                
                if contract.payout_type == "PERCENTAGE":
                    payout_value = self._apply_percentage_payout(considered_revenue, contract)
                elif contract.payout_type == "AMOUNT":
                    payout_value = self._apply_fixed_payout(considered_revenue, contract)
                else:
                    payout_value = self._apply_tiered_payout(considered_revenue, contract)
                
                self.logger.debug(f"Payout value calculated from type: {payout_value}")
            
            # Apply capping if enabled
            if hasattr(contract, 'payout_capping') and contract.payout_capping:
                payout_value = self._apply_capping(payout_value, contract.payout_capping_value)
            
            return payout_value.quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            
        except Exception as e:
            self.logger.error(f"Error computing payout: {str(e)}")
            raise ComputationError(f"Payout computation failed: {str(e)}")
    
    def _calculate_considered_revenue(self, coupon: CouponData, components: List[str]) -> Decimal:
        """
        Calculate considered revenue based on specified components
        
        Args:
            coupon: Coupon data
            components: List of revenue components to include
            
        Returns:
            Sum of considered revenue components
        """
        considered_revenue = Decimal('0')
        
        # Handle NONE components - return 0 for contracts that don't use revenue components
        if 'NONE' in components:
            self.logger.debug("NONE component detected - using total revenue as fallback")
            return coupon.cpn_total_revenue
        
        # Map component names to coupon fields
        component_mapping = {
            'BASE': coupon.cpn_revenue_base,
            'YQ': coupon.cpn_revenue_yq,
            'YR': coupon.cpn_revenue_yr,
            'XT': coupon.cpn_revenue_xt
        }
        
        for component in components:
            if component in component_mapping:
                considered_revenue += component_mapping[component]
                self.logger.debug(f"Added {component}: {component_mapping[component]}")
            else:
                self.logger.warning(f"Unknown component: {component}")
        
        return considered_revenue
    
    def _apply_trigger_formula(self, considered_revenue: Decimal, contract: ContractData) -> Decimal:
        """
        Apply trigger formula to considered revenue
        
        Args:
            considered_revenue: Revenue amount to apply formula to
            contract: Contract data with formula information
            
        Returns:
            Calculated trigger value
        """
        # For now, trigger value is the same as considered revenue
        # This can be extended to support more complex formulas
        return considered_revenue
    
    def _apply_percentage_payout(self, considered_revenue: Decimal, contract: ContractData) -> Decimal:
        """
        Apply percentage-based payout calculation
        
        Args:
            considered_revenue: Revenue amount to calculate payout for
            contract: Contract data with payout information
            
        Returns:
            Calculated payout value
        """
        if not contract.payout_percentage:
            self.logger.warning("No payout percentage specified, returning 0")
            return Decimal('0')
        
        payout_percentage = contract.payout_percentage 
        payout_value = considered_revenue * payout_percentage
        
        self.logger.debug(f"Percentage payout: {considered_revenue} * {payout_percentage} = {payout_value}")
        return payout_value
    
    def _apply_fixed_payout(self, considered_revenue: Decimal, contract: ContractData) -> Decimal:
        """
        Apply fixed amount payout calculation
        
        Args:
            considered_revenue: Revenue amount (not used for fixed payout)
            contract: Contract data with payout information
            
        Returns:
            Fixed payout value
        """
        # For fixed payout, return the fixed amount regardless of revenue
        fixed_amount = getattr(contract, 'payout_fixed_amount', Decimal('0'))
        self.logger.debug(f"Fixed payout: {fixed_amount}")
        return fixed_amount
    
    def _apply_tiered_payout(self, considered_revenue: Decimal, contract: ContractData) -> Decimal:
        """
        Apply tiered payout calculation based on revenue tiers
        
        Args:
            considered_revenue: Revenue amount to calculate payout for
            contract: Contract data with tier information
            
        Returns:
            Calculated tiered payout value
        """
        if not contract.tiers:
            self.logger.warning("No tiers specified for tiered payout, returning 0")
            return Decimal('0')
        
        # Find the appropriate tier for the revenue amount
        applicable_tier = self._find_applicable_tier(considered_revenue, contract.tiers)
        
        if not applicable_tier:
            self.logger.debug(f"No applicable tier found for revenue: {considered_revenue}")
            return Decimal('0')
        
        # Calculate payout based on tier
        payout_value = self._calculate_tier_payout(considered_revenue, applicable_tier)
        
        self.logger.debug(f"Tiered payout: {considered_revenue} -> tier {applicable_tier} -> {payout_value}")
        return payout_value
    
    def _find_applicable_tier(self, revenue: Decimal, tiers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find the applicable tier for a given revenue amount
        
        Args:
            revenue: Revenue amount
            tiers: List of tier definitions
            
        Returns:
            Applicable tier or None
        """
        for tier in tiers:
            rows = tier.get('rows', [])
            for row in rows:
                # Handle both old format (target_min/target_max) and new format (target.min/target.max)
                if 'target' in row and isinstance(row['target'], dict):
                    # New format
                    target = row['target']
                    min_value = self._safe_decimal(target.get('min', 0))
                    max_raw = target.get('max', float('inf'))
                    max_value = self._safe_decimal(max_raw) if max_raw != float('inf') else Decimal(str(float('inf')))
                else:
                    # Old format
                    min_value = self._safe_decimal(row.get('target_min', 0))
                    max_raw = row.get('target_max', float('inf'))
                    max_value = self._safe_decimal(max_raw) if max_raw != float('inf') else Decimal(str(float('inf')))
                
                if min_value <= revenue <= max_value:
                    return row
        
        return None
    
    def _calculate_tier_payout(self, revenue: Decimal, tier: Dict[str, Any]) -> Decimal:
        """
        Calculate payout for a specific tier
        
        Args:
            revenue: Revenue amount
            tier: Tier definition
            
        Returns:
            Calculated payout value
        """
        # Handle both old format (payout_value/payout_unit) and new format (payout.value/payout.unit)
        if 'payout' in tier and isinstance(tier['payout'], dict):
            # New format
            payout = tier['payout']
            payout_value = self._safe_decimal(payout.get('value', 0))
            payout_unit = payout.get('unit', 'PERCENT')
        else:
            # Old format
            payout_value = self._safe_decimal(tier.get('payout_value', 0))
            payout_unit = tier.get('payout_unit', 'PERCENT')
        
        if payout_unit == 'PERCENT':
            # Apply percentage to revenue (convert percentage to decimal)
            percentage = payout_value / Decimal('100')
            return revenue * percentage
        elif payout_unit == 'AMOUNT':
            # Fixed amount per tier
            return payout_value
        else:
            # Default to percentage
            percentage = payout_value / Decimal('100')
            return revenue * percentage
    
    def _apply_capping(self, value: Decimal, cap_value: Union[Decimal, float, int]) -> Decimal:
        """
        Apply capping to a value
        
        Args:
            value: Value to cap
            cap_value: Maximum allowed value
            
        Returns:
            Capped value
        """
        cap_decimal = Decimal(str(cap_value))
        
        if value > cap_decimal:
            self.logger.debug(f"Value {value} capped to {cap_decimal}")
            return cap_decimal
        
        return value
    
    def compute_tier_progression(self, revenue: Decimal, tiers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compute tier progression analysis for a given revenue amount
        
        Args:
            revenue: Revenue amount
            tiers: List of tier definitions
            
        Returns:
            Tier progression analysis
        """
        try:
            analysis = {
                'current_revenue': float(revenue),
                'current_tier': None,
                'next_tier': None,
                'tier_progression': [],
                'payout_breakdown': []
            }
            
            for tier in tiers:
                rows = tier.get('rows', [])
                for i, row in enumerate(rows):
                    # Handle both old format and new format
                    if 'target' in row and isinstance(row['target'], dict):
                        # New format
                        target = row['target']
                        min_value = self._safe_decimal(target.get('min', 0))
                        max_raw = target.get('max', float('inf'))
                        max_value = self._safe_decimal(max_raw) if max_raw != float('inf') else Decimal(str(float('inf')))
                    else:
                        # Old format
                        min_value = self._safe_decimal(row.get('target_min', 0))
                        max_raw = row.get('target_max', float('inf'))
                        max_value = self._safe_decimal(max_raw) if max_raw != float('inf') else Decimal(str(float('inf')))
                    
                    if 'payout' in row and isinstance(row['payout'], dict):
                        # New format
                        payout = row['payout']
                        payout_value = self._safe_decimal(payout.get('value', 0))
                        payout_unit = payout.get('unit', 'PERCENT')
                    else:
                        # Old format
                        payout_value = self._safe_decimal(row.get('payout_value', 0))
                        payout_unit = row.get('payout_unit', 'PERCENT')
                    
                    tier_info = {
                        'tier_index': i,
                        'min_revenue': float(min_value),
                        'max_revenue': float(max_value) if max_value != float('inf') else None,
                        'payout_value': float(payout_value),
                        'payout_unit': payout_unit,
                        'is_current': min_value <= revenue <= max_value,
                        'is_next': revenue < min_value and (i == 0 or Decimal(str(rows[i-1].get('target_max', 0))) < revenue)
                    }
                    
                    analysis['tier_progression'].append(tier_info)
                    
                    if tier_info['is_current']:
                        analysis['current_tier'] = tier_info
                        
                        # Calculate payout for current tier
                        if payout_unit == 'PERCENT':
                            payout_amount = revenue * (payout_value / Decimal('100'))
                        else:
                            payout_amount = payout_value
                        
                        analysis['payout_breakdown'].append({
                            'tier': i,
                            'revenue_used': float(revenue),
                            'payout_rate': float(payout_value),
                            'payout_amount': float(payout_amount)
                        })
                    
                    if tier_info['is_next']:
                        analysis['next_tier'] = tier_info
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error computing tier progression: {str(e)}")
            raise ComputationError(f"Tier progression computation failed: {str(e)}")
    
    def validate_computation_parameters(self, contract: ContractData) -> Dict[str, Any]:
        """
        Validate computation parameters for a contract
        
        Args:
            contract: Contract data to validate
            
        Returns:
            Validation results
        """
        try:
            validation_results = {
                'valid': True,
                'warnings': [],
                'errors': []
            }
            
            # Validate trigger components
            if not contract.trigger_components:
                validation_results['errors'].append("No trigger components specified")
                validation_results['valid'] = False
            
            # Validate payout components
            if not contract.payout_components:
                validation_results['errors'].append("No payout components specified")
                validation_results['valid'] = False
            
            # Validate payout percentage for percentage-based contracts
            if contract.payout_type == "PERCENTAGE" and not contract.payout_percentage:
                validation_results['warnings'].append("No payout percentage specified for percentage-based contract")
            
            # Validate tiers for tiered contracts
            if contract.payout_type == "TIERED" and not contract.tiers:
                validation_results['errors'].append("No tiers specified for tiered contract")
                validation_results['valid'] = False
            
            # Validate tier progression
            if contract.tiers:
                tier_validation = self._validate_tier_progression(contract.tiers)
                validation_results['warnings'].extend(tier_validation.get('warnings', []))
                if not tier_validation.get('valid', True):
                    validation_results['errors'].extend(tier_validation.get('errors', []))
                    validation_results['valid'] = False
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating computation parameters: {str(e)}")
            return {
                'valid': False,
                'warnings': [],
                'errors': [f"Validation error: {str(e)}"]
            }
    
    def compute_with_formula(self, formula: str, coupon: CouponData, contract: ContractData, 
                           additional_params: Optional[Dict[str, Any]] = None) -> Decimal:
        """
        Compute a value using a specific formula
        
        Args:
            formula: Formula string to evaluate
            coupon: Coupon data
            contract: Contract data
            additional_params: Additional parameters for the formula
            
        Returns:
            Calculated result
        """
        try:
            result = self.formula_parser.evaluate_formula(
                formula, coupon, contract, additional_params
            )
            self.logger.debug(f"Formula '{formula}' computed: {result}")
            return result.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
        except Exception as e:
            self.logger.error(f"Error computing formula '{formula}': {str(e)}")
            raise ComputationError(f"Formula computation failed: {str(e)}")
    
    def extract_and_compute_formulas(self, rule_data: Dict[str, Any], coupon: CouponData, 
                                   contract: ContractData) -> Dict[str, Decimal]:
        """
        Extract all formulas from rule data and compute their values
        
        Args:
            rule_data: Rule JSON data
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Dictionary of formula names and their computed values
        """
        try:
            # Extract formulas from rule
            formulas = self.formula_parser.extract_formulas_from_rule(rule_data)
            
            # Compute each formula
            results = {}
            for formula_name, formula_string in formulas.items():
                try:
                    result = self.formula_parser.evaluate_formula(
                        formula_string, coupon, contract
                    )
                    results[formula_name] = result
                    self.logger.debug(f"Formula '{formula_name}': {result}")
                except Exception as e:
                    self.logger.warning(f"Failed to compute formula '{formula_name}': {str(e)}")
                    results[formula_name] = Decimal('0')
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error extracting and computing formulas: {str(e)}")
            return {}
    
    def validate_formula_parameters(self, formula: str, coupon: CouponData, 
                                 contract: ContractData) -> Dict[str, Any]:
        """
        Validate formula parameters against available data
        
        Args:
            formula: Formula string to validate
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Validation results
        """
        return self.formula_parser.validate_formula(formula, coupon, contract)
    
    def _is_mathematical_formula(self, formula: str) -> bool:
        """
        Check if a formula string is a mathematical expression
        
        Args:
            formula: Formula string to check
            
        Returns:
            True if it's a mathematical formula, False otherwise
        """
        if not formula:
            return False
        
        # Check for mathematical operators
        math_operators = ['=', '+', '-', '*', '/', '(', ')', '^', '**']
        has_operators = any(op in formula for op in math_operators)
        
        # Check for parameter references (BASE, YQ, etc.)
        parameter_pattern = r'\b(BASE|YQ|YR|XT|TOTAL|slab_percent|tier_percent)\b'
        has_parameters = bool(re.search(parameter_pattern, formula))
        
        # Check if it's just descriptive text (contains common descriptive words)
        descriptive_words = ['excluding', 'including', 'revenue', 'flown', 'commissions', 'refunds', 'taxes', 
                            'nrf', 'ticketed', 'operated', 'on', 'program', 'incentive']
        is_descriptive = any(word in formula.lower() for word in descriptive_words)
        
        # It's a mathematical formula if it has operators and parameters, and is not just descriptive
        return has_operators and has_parameters and not is_descriptive
    
    def _validate_tier_progression(self, tiers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate tier progression logic
        
        Args:
            tiers: List of tier definitions
            
        Returns:
            Validation results
        """
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        for tier in tiers:
            rows = tier.get('rows', [])
            if not rows:
                validation_results['warnings'].append(f"Tier {tier.get('table_label', 'Unknown')} has no rows")
                continue
            
            # Check for overlapping tiers
            for i in range(len(rows) - 1):
                current_max = Decimal(str(rows[i].get('target_max', 0)))
                next_min = Decimal(str(rows[i + 1].get('target_min', 0)))
                
                if current_max >= next_min:
                    validation_results['warnings'].append(f"Overlapping tiers: tier {i} max {current_max} >= tier {i+1} min {next_min}")
            
            # Check for gaps in tiers
            for i in range(len(rows) - 1):
                current_max = Decimal(str(rows[i].get('target_max', 0)))
                next_min = Decimal(str(rows[i + 1].get('target_min', 0)))
                
                if current_max + Decimal('0.01') < next_min:
                    validation_results['warnings'].append(f"Gap in tiers: tier {i} max {current_max} < tier {i+1} min {next_min}")
        
        return validation_results
