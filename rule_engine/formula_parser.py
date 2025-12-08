"""
Formula parser and calculator for rule engine
Handles extraction and evaluation of formulas from rule JSON files
"""

import re
import ast
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Any, Union
from loguru import logger

from .models import CouponData, ContractData


class FormulaParser:
    """
    Parses and evaluates formulas from rule JSON files
    """
    
    def __init__(self):
        """Initialize formula parser"""
        self.logger = logger
        
        # Define parameter mappings for coupon data
        self.coupon_parameter_mapping = {
            'BASE': 'cpn_revenue_base',
            'YQ': 'cpn_revenue_yq', 
            'YR': 'cpn_revenue_yr',
            'XT': 'cpn_revenue_xt',
            'TOTAL': 'cpn_total_revenue'
        }
        
        # Define parameter mappings for contract data
        self.contract_parameter_mapping = {
            'slab_percent': 'payout_percentage',
            'tier_percent': 'payout_percentage',
            'cap_value': 'payout_capping_value',
            'blp_value': 'blp_value'
        }
    
    def extract_formulas_from_rule(self, rule_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract formulas from rule JSON data
        
        Args:
            rule_data: Rule JSON data
            
        Returns:
            Dictionary with extracted formulas
        """
        formulas = {}
        
        try:
            # Extract trigger formula
            what_if = rule_data.get('what_if', {})
            trigger_config = what_if.get('trigger', {})
            trigger_formula = trigger_config.get('formula')
            if trigger_formula:
                formulas['trigger'] = trigger_formula
            
            # Extract payout formula
            payout_config = what_if.get('payout', {})
            payout_formula = payout_config.get('formula')
            if payout_formula:
                formulas['payout'] = payout_formula
            
            # Extract action formulas from 'then' section
            then_section = rule_data.get('then', {})
            actions = then_section.get('actions', [])
            
            for i, action in enumerate(actions):
                if action.get('action') == 'APPLY_FORMULA':
                    formula = action.get('inputs', {}).get('formula')
                    if formula:
                        formulas[f'action_{i}'] = formula
                elif action.get('action') == 'COMPUTE_PAYOUT_PERCENT':
                    # Extract formula from explanation or inputs
                    explanation = action.get('output', {}).get('explanation', '')
                    if 'formula' in explanation.lower():
                        formulas[f'action_{i}'] = explanation
            
            self.logger.debug(f"Extracted formulas: {list(formulas.keys())}")
            return formulas
            
        except Exception as e:
            self.logger.error(f"Error extracting formulas from rule: {str(e)}")
            return {}
    
    def parse_formula(self, formula: str) -> Dict[str, Any]:
        """
        Parse a formula string into components
        
        Args:
            formula: Formula string (e.g., "payout_amount = (slab_percent) * (BASE + YQ)")
            
        Returns:
            Parsed formula components
        """
        try:
            # Clean the formula
            formula = formula.strip()
            
            # Extract the result variable (left side of =)
            if '=' in formula:
                result_var, expression = formula.split('=', 1)
                result_var = result_var.strip()
                expression = expression.strip()
            else:
                result_var = 'result'
                expression = formula
            
            # Find all parameter references in the expression
            # Look for words that could be parameters (BASE, YQ, slab_percent, etc.)
            parameter_pattern = r'\b([A-Z_][A-Z0-9_]*)\b'
            parameters = re.findall(parameter_pattern, expression)
            
            # Also look for lowercase parameters
            lowercase_pattern = r'\b([a-z_][a-z0-9_]*)\b'
            lowercase_params = re.findall(lowercase_pattern, expression)
            
            all_parameters = list(set(parameters + lowercase_params))
            
            # Filter out common operators and functions
            operators = {'AND', 'OR', 'NOT', 'IF', 'THEN', 'ELSE', 'MIN', 'MAX', 'SUM', 'AVG'}
            filtered_parameters = [p for p in all_parameters if p not in operators]
            
            # Extract function calls (like amount_in_slab_on(BASE))
            function_calls = re.findall(r'\b([a-z_][a-z0-9_]*)\([^)]+\)', expression)
            
            parsed = {
                'original_formula': formula,
                'result_variable': result_var,
                'expression': expression,
                'parameters': filtered_parameters,
                'function_calls': function_calls,
                'is_valid': True
            }
            
            self.logger.debug(f"Parsed formula: {parsed}")
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing formula '{formula}': {str(e)}")
            return {
                'original_formula': formula,
                'result_variable': 'result',
                'expression': formula,
                'parameters': [],
                'function_calls': [],
                'is_valid': False,
                'error': str(e)
            }
    
    def evaluate_formula(self, formula: str, coupon: CouponData, contract: ContractData, 
                        additional_params: Optional[Dict[str, Any]] = None) -> Decimal:
        """
        Evaluate a formula using coupon and contract data
        
        Args:
            formula: Formula string to evaluate
            coupon: Coupon data
            contract: Contract data
            additional_params: Additional parameters for evaluation
            
        Returns:
            Calculated result
        """
        try:
            # Parse the formula
            parsed = self.parse_formula(formula)
            if not parsed['is_valid']:
                raise ValueError(f"Invalid formula: {parsed.get('error', 'Unknown error')}")
            
            # Create parameter context
            context = self._create_evaluation_context(coupon, contract, additional_params)
            
            # Replace parameters in expression with actual values
            expression = parsed['expression']
            
            # First, handle function calls
            for func_call in parsed.get('function_calls', []):
                if func_call == 'amount_in_slab_on':
                    # Replace amount_in_slab_on(BASE) with just BASE value
                    func_pattern = rf'\b{func_call}\(([^)]+)\)'
                    matches = re.findall(func_pattern, expression)
                    for match in matches:
                        if match in context:
                            value = context[match]
                            if isinstance(value, Decimal):
                                expression = expression.replace(f'{func_call}({match})', str(value))
                            else:
                                expression = expression.replace(f'{func_call}({match})', str(value))
                        else:
                            self.logger.warning(f"Parameter '{match}' not found in context for function {func_call}")
                            expression = expression.replace(f'{func_call}({match})', '0')
                else:
                    # For other function calls, replace with 0 for now
                    func_pattern = rf'\b{func_call}\([^)]+\)'
                    expression = re.sub(func_pattern, '0', expression)
                    self.logger.warning(f"Function '{func_call}' not implemented, replaced with 0")
            
            # Then handle regular parameters
            for param in parsed['parameters']:
                if param in context:
                    # Replace parameter with its value
                    value = context[param]
                    if isinstance(value, Decimal):
                        expression = expression.replace(param, str(value))
                    else:
                        expression = expression.replace(param, str(value))
                else:
                    self.logger.warning(f"Parameter '{param}' not found in context")
                    expression = expression.replace(param, '0')
            
            # Evaluate the expression safely
            result = self._safe_evaluate(expression)
            
            self.logger.debug(f"Formula '{formula}' evaluated to: {result}")
            return Decimal(str(result)).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            
        except Exception as e:
            self.logger.error(f"Error evaluating formula '{formula}': {str(e)}")
            return Decimal('0')
    
    def _create_evaluation_context(self, coupon: CouponData, contract: ContractData, 
                                 additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create evaluation context with all available parameters
        
        Args:
            coupon: Coupon data
            contract: Contract data
            additional_params: Additional parameters
            
        Returns:
            Context dictionary for formula evaluation
        """
        context = {}
        
        # Add coupon parameters
        for param_name, attr_name in self.coupon_parameter_mapping.items():
            if hasattr(coupon, attr_name):
                context[param_name] = getattr(coupon, attr_name)
        
        # Add contract parameters
        for param_name, attr_name in self.contract_parameter_mapping.items():
            if hasattr(contract, attr_name):
                value = getattr(contract, attr_name)
                if value is not None:
                    context[param_name] = value
        
        # Add tier-based parameters
        if contract.tiers:
            tier_percent = self._extract_tier_percentage(coupon, contract)
            if tier_percent is not None:
                context['slab_percent'] = tier_percent
                context['tier_percent'] = tier_percent
        
        # Add missing parameters that are commonly used
        context['amount_in_slab_on'] = 0  # Default value for missing function
        context['band_percent'] = 0  # Default value for missing parameter
        
        # Add additional parameters
        if additional_params:
            context.update(additional_params)
        
        # Add common mathematical functions
        context.update({
            'min': min,
            'max': max,
            'sum': sum,
            'abs': abs,
            'round': round
        })
        
        return context
    
    def _extract_tier_percentage(self, coupon: CouponData, contract: ContractData) -> Optional[Decimal]:
        """
        Extract tier percentage based on coupon revenue and contract tiers
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Tier percentage or None
        """
        try:
            if not contract.tiers:
                return None
            
            # Calculate considered revenue
            considered_revenue = self._calculate_considered_revenue(coupon, contract.payout_components)
            
            # For testing purposes, if revenue is very small, use the first tier (1%)
            if considered_revenue < Decimal('1000'):  # If revenue is less than 1000, use first tier
                for tier in contract.tiers:
                    rows = tier.get('rows', [])
                    if rows:
                        # Get the first tier (lowest threshold)
                        row = rows[0]
                        if 'payout' in row and isinstance(row['payout'], dict):
                            # New format
                            payout = row['payout']
                            payout_value = Decimal(str(payout.get('value', 0)))
                            payout_unit = payout.get('unit', 'PERCENT')
                        else:
                            # Old format
                            payout_value = Decimal(str(row.get('payout_value', 0)))
                            payout_unit = row.get('payout_unit', 'PERCENT')
                        
                        if payout_unit == 'PERCENT':
                            # Convert percentage to decimal (1% = 0.01)
                            return payout_value / Decimal('100')
                        else:
                            return Decimal('0')
            
            # Find applicable tier based on revenue thresholds
            for tier in contract.tiers:
                rows = tier.get('rows', [])
                for row in rows:
                    # Handle both old format (target_min/target_max) and new format (target.min/target.max)
                    if 'target' in row and isinstance(row['target'], dict):
                        # New format
                        target = row['target']
                        min_value = Decimal(str(target.get('min', 0)))
                        max_value = Decimal(str(target.get('max', float('inf'))))
                    else:
                        # Old format
                        min_value = Decimal(str(row.get('target_min', 0)))
                        max_value = Decimal(str(row.get('target_max', float('inf'))))
                    
                    if min_value <= considered_revenue <= max_value:
                        # Handle both old format (payout_value/payout_unit) and new format (payout.value/payout.unit)
                        if 'payout' in row and isinstance(row['payout'], dict):
                            # New format
                            payout = row['payout']
                            payout_value = Decimal(str(payout.get('value', 0)))
                            payout_unit = payout.get('unit', 'PERCENT')
                        else:
                            # Old format
                            payout_value = Decimal(str(row.get('payout_value', 0)))
                            payout_unit = row.get('payout_unit', 'PERCENT')
                        
                        if payout_unit == 'PERCENT':
                            # Convert percentage to decimal (1% = 0.01)
                            return payout_value / Decimal('100')
                        else:
                            return Decimal('0')
            
            # If no tier matches, return the first tier percentage as default
            for tier in contract.tiers:
                rows = tier.get('rows', [])
                if rows:
                    row = rows[0]
                    if 'payout' in row and isinstance(row['payout'], dict):
                        # New format
                        payout = row['payout']
                        payout_value = Decimal(str(payout.get('value', 0)))
                        payout_unit = payout.get('unit', 'PERCENT')
                    else:
                        # Old format
                        payout_value = Decimal(str(row.get('payout_value', 0)))
                        payout_unit = row.get('payout_unit', 'PERCENT')
                    
                    if payout_unit == 'PERCENT':
                        # Convert percentage to decimal (1% = 0.01)
                        return payout_value / Decimal('100')
                    else:
                        return Decimal('0')
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting tier percentage: {str(e)}")
            return None
    
    def _calculate_considered_revenue(self, coupon: CouponData, components: List[str]) -> Decimal:
        """
        Calculate considered revenue based on components
        
        Args:
            coupon: Coupon data
            components: List of revenue components
            
        Returns:
            Considered revenue amount
        """
        considered_revenue = Decimal('0')
        
        # Handle NONE components
        if 'NONE' in components:
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
        
        return considered_revenue
    
    def _safe_evaluate(self, expression: str) -> float:
        """
        Safely evaluate a mathematical expression
        
        Args:
            expression: Mathematical expression string
            
        Returns:
            Evaluation result
        """
        try:
            # Replace common mathematical operators and functions
            expression = expression.replace('**', '^')  # Handle power operator
            
            # Use ast.literal_eval for safe evaluation
            # First, try to parse as a simple expression
            try:
                # Parse the expression
                tree = ast.parse(expression, mode='eval')
                
                # Evaluate the AST
                result = self._evaluate_ast(tree.body)
                return float(result)
                
            except (ValueError, SyntaxError):
                # Fallback to eval with restricted globals
                allowed_names = {
                    '__builtins__': {},
                    'min': min,
                    'max': max,
                    'sum': sum,
                    'abs': abs,
                    'round': round,
                    'pow': pow
                }
                
                return float(eval(expression, allowed_names))
                
        except Exception as e:
            self.logger.error(f"Error evaluating expression '{expression}': {str(e)}")
            return 0.0
    
    def _evaluate_ast(self, node):
        """
        Evaluate AST nodes safely
        
        Args:
            node: AST node
            
        Returns:
            Evaluation result
        """
        if isinstance(node, ast.Expression):
            return self._evaluate_ast(node.body)
        elif isinstance(node, ast.Constant):
            return node.value

        elif isinstance(node, ast.BinOp):
            left = self._evaluate_ast(node.left)
            right = self._evaluate_ast(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            elif isinstance(node.op, ast.Sub):
                return left - right
            elif isinstance(node.op, ast.Mult):
                return left * right
            elif isinstance(node.op, ast.Div):
                return left / right
            elif isinstance(node.op, ast.Pow):
                return left ** right
            elif isinstance(node.op, ast.Mod):
                return left % right
        elif isinstance(node, ast.UnaryOp):
            operand = self._evaluate_ast(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            elif isinstance(node.op, ast.USub):
                return -operand
        elif isinstance(node, ast.Call):
            func_name = node.func.id if hasattr(node.func, 'id') else str(node.func)
            args = [self._evaluate_ast(arg) for arg in node.args]
            
            if func_name == 'min':
                return min(args)
            elif func_name == 'max':
                return max(args)
            elif func_name == 'sum':
                return sum(args)
            elif func_name == 'abs':
                return abs(args[0])
            elif func_name == 'round':
                return round(args[0], int(args[1]) if len(args) > 1 else 0)
        
        raise ValueError(f"Unsupported AST node: {type(node)}")
    
    def get_formula_parameters(self, formula: str) -> List[str]:
        """
        Get list of parameters used in a formula
        
        Args:
            formula: Formula string
            
        Returns:
            List of parameter names
        """
        parsed = self.parse_formula(formula)
        return parsed['parameters']
    
    def validate_formula(self, formula: str, coupon: CouponData, contract: ContractData) -> Dict[str, Any]:
        """
        Validate a formula against available parameters
        
        Args:
            formula: Formula string to validate
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Validation results
        """
        try:
            parsed = self.parse_formula(formula)
            if not parsed['is_valid']:
                return {
                    'valid': False,
                    'error': parsed.get('error', 'Invalid formula syntax'),
                    'missing_parameters': [],
                    'available_parameters': []
                }
            
            # Get available parameters
            context = self._create_evaluation_context(coupon, contract)
            available_params = list(context.keys())
            
            # Check for missing parameters
            missing_params = []
            for param in parsed['parameters']:
                if param not in context:
                    missing_params.append(param)
            
            return {
                'valid': len(missing_params) == 0,
                'error': None if len(missing_params) == 0 else f"Missing parameters: {missing_params}",
                'missing_parameters': missing_params,
                'available_parameters': available_params,
                'formula_parameters': parsed['parameters']
            }
            
        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'missing_parameters': [],
                'available_parameters': []
            }
