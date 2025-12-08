"""
Addon Rule Case Processor for handling special eligibility overrides
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from loguru import logger

from .models import CouponData, ContractData, ContractAnalysis
from .exceptions import RuleEngineError


class AddonRuleProcessor:
    """
    Handles addon rule cases that can override initial eligibility decisions
    """
    
    def __init__(self):
        """Initialize addon rule processor"""
        self.logger = logger
    
    def process_addon_rules(self, coupon: CouponData, contract: ContractData, 
                           initial_trigger_eligible: bool, initial_payout_eligible: bool,
                           contract_analysis: ContractAnalysis) -> Dict[str, Any]:
        """
        Process addon rule cases for a coupon and contract
        
        Args:
            coupon: Coupon data
            contract: Contract data
            initial_trigger_eligible: Initial trigger eligibility result
            initial_payout_eligible: Initial payout eligibility result
            contract_analysis: Current contract analysis
            
        Returns:
            Dictionary with updated eligibility results and addon processing details
        """
        try:
            # Check if contract has addon rules
            addon_rules = getattr(contract, 'addon_rule_cases', [])
            if not addon_rules:
                self.logger.debug(f"No addon rules found for contract {contract.contract_id}")
                return {
                    'trigger_eligible': initial_trigger_eligible,
                    'payout_eligible': initial_payout_eligible,
                    'addon_applied': False,
                    'addon_details': []
                }
            
            self.logger.info(f"Processing {len(addon_rules)} addon rules for contract {contract.contract_id}")
            
            # Process each addon rule
            final_trigger_eligible = initial_trigger_eligible
            final_payout_eligible = initial_payout_eligible
            addon_details = []
            
            for addon_rule in addon_rules:
                addon_result = self._process_single_addon_rule(
                    coupon, contract, addon_rule, 
                    final_trigger_eligible, final_payout_eligible
                )
                
                if addon_result['applied']:
                    final_trigger_eligible = addon_result['trigger_eligible']
                    final_payout_eligible = addon_result['payout_eligible']
                    self.logger.info(f"Addon rule {addon_rule.get('addon_rule_id', 'Unknown')} applied successfully")
                
                addon_details.append(addon_result)
            
            return {
                'trigger_eligible': final_trigger_eligible,
                'payout_eligible': final_payout_eligible,
                'addon_applied': any(detail['applied'] for detail in addon_details),
                'addon_details': addon_details
            }
            
        except Exception as e:
            self.logger.error(f"Error processing addon rules: {str(e)}")
            return {
                'trigger_eligible': initial_trigger_eligible,
                'payout_eligible': initial_payout_eligible,
                'addon_applied': False,
                'addon_details': [],
                'error': str(e)
            }
    
    def _process_single_addon_rule(self, coupon: CouponData, contract: ContractData,
                                  addon_rule: Dict[str, Any], current_trigger_eligible: bool,
                                  current_payout_eligible: bool) -> Dict[str, Any]:
        """
        Process a single addon rule case
        
        Args:
            coupon: Coupon data
            contract: Contract data
            addon_rule: Addon rule configuration
            current_trigger_eligible: Current trigger eligibility
            current_payout_eligible: Current payout eligibility
            
        Returns:
            Dictionary with addon rule processing result
        """
        try:
            addon_rule_id = addon_rule.get('addon_rule_id', 'Unknown')
            addon_name = addon_rule.get('name', 'Unknown Addon Rule')
            when_to_apply = addon_rule.get('when_to_apply', '')
            override_logic = addon_rule.get('override_logic', '')
            mappings = addon_rule.get('mappings', [])
            exclusions = addon_rule.get('exclusions_still_applicable', [])
            
            self.logger.debug(f"Processing addon rule: {addon_rule_id} - {addon_name}")
            
            # Check if this addon rule should be applied
            should_apply = self._should_apply_addon_rule(
                coupon, addon_rule, current_trigger_eligible, current_payout_eligible
            )
            
            if not should_apply:
                return {
                    'addon_rule_id': addon_rule_id,
                    'addon_name': addon_name,
                    'applied': False,
                    'reason': 'Addon rule conditions not met',
                    'trigger_eligible': current_trigger_eligible,
                    'payout_eligible': current_payout_eligible
                }
            
            # Check if coupon matches any mapping in the addon rule
            matching_mapping = self._find_matching_mapping(coupon, mappings)
            
            if not matching_mapping:
                return {
                    'addon_rule_id': addon_rule_id,
                    'addon_name': addon_name,
                    'applied': False,
                    'reason': 'No matching mapping found',
                    'trigger_eligible': current_trigger_eligible,
                    'payout_eligible': current_payout_eligible
                }
            
            # Check exclusions that still apply
            excluded_by_exclusions = self._check_exclusions(coupon, exclusions)
            
            if excluded_by_exclusions:
                return {
                    'addon_rule_id': addon_rule_id,
                    'addon_name': addon_name,
                    'applied': False,
                    'reason': f'Excluded by: {excluded_by_exclusions}',
                    'trigger_eligible': current_trigger_eligible,
                    'payout_eligible': current_payout_eligible
                }
            
            # Apply the addon rule override
            new_trigger_eligible = True
            new_payout_eligible = True
            
            return {
                'addon_rule_id': addon_rule_id,
                'addon_name': addon_name,
                'applied': True,
                'reason': f'Override applied - matched mapping: {matching_mapping}',
                'matching_mapping': matching_mapping,
                'trigger_eligible': new_trigger_eligible,
                'payout_eligible': new_payout_eligible,
                'override_logic': override_logic
            }
            
        except Exception as e:
            self.logger.error(f"Error processing addon rule {addon_rule.get('addon_rule_id', 'Unknown')}: {str(e)}")
            return {
                'addon_rule_id': addon_rule.get('addon_rule_id', 'Unknown'),
                'addon_name': addon_rule.get('name', 'Unknown'),
                'applied': False,
                'reason': f'Processing error: {str(e)}',
                'trigger_eligible': current_trigger_eligible,
                'payout_eligible': current_payout_eligible
            }
    
    def _should_apply_addon_rule(self, coupon: CouponData, addon_rule: Dict[str, Any],
                                current_trigger_eligible: bool, current_payout_eligible: bool) -> bool:
        """
        Determine if an addon rule should be applied based on when_to_apply criteria
        
        Args:
            coupon: Coupon data
            addon_rule: Addon rule configuration
            current_trigger_eligible: Current trigger eligibility
            current_payout_eligible: Current payout eligibility
            
        Returns:
            True if addon rule should be applied
        """
        try:
            when_to_apply = addon_rule.get('when_to_apply', '').lower()
            
            # Common patterns for when to apply addon rules
            if 'after base in/out filtering' in when_to_apply:
                # Apply if coupon was rejected due to base filtering
                if not current_trigger_eligible or not current_payout_eligible:
                    return True
            
            if 'only for coupons rejected' in when_to_apply:
                # Apply only if coupon was initially rejected
                if not current_trigger_eligible or not current_payout_eligible:
                    return True
            
            if 'operating carrier' in when_to_apply or 'codeshare' in when_to_apply:
                # Apply if coupon has codeshare or operating carrier issues
                if coupon.code_share or (coupon.operating_airline and coupon.operating_airline != coupon.marketing_airline):
                    return True
            
            if 'route/flight constraints' in when_to_apply:
                # Apply if there might be route/flight constraint issues
                return True
            
            # Default: apply if coupon was initially ineligible
            return not current_trigger_eligible or not current_payout_eligible
            
        except Exception as e:
            self.logger.error(f"Error determining if addon rule should apply: {str(e)}")
            return False
    
    def _find_matching_mapping(self, coupon: CouponData, mappings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find a mapping that matches the coupon data
        
        Args:
            coupon: Coupon data
            mappings: List of addon rule mappings
            
        Returns:
            Matching mapping or None
        """
        try:
            for mapping in mappings:
                if self._coupon_matches_mapping(coupon, mapping):
                    return mapping
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding matching mapping: {str(e)}")
            return None
    
    def _coupon_matches_mapping(self, coupon: CouponData, mapping: Dict[str, Any]) -> bool:
        """
        Check if coupon matches a specific mapping
        
        Args:
            coupon: Coupon data
            mapping: Mapping configuration
            
        Returns:
            True if coupon matches mapping
        """
        try:
            # Check marketing flight
            marketing_flight = mapping.get('marketing_flight', '')
            if marketing_flight:
                # Compare with flight number or marketing airline + flight number
                coupon_flight = str(coupon.flight_number) if coupon.flight_number else ''
                if marketing_flight != coupon_flight:
                    # Try combining marketing airline with flight number
                    combined_flight = f"{coupon.marketing_airline}{coupon_flight}" if coupon.marketing_airline else coupon_flight
                    if marketing_flight != combined_flight:
                        return False
            
            # Check operating airline
            operating_airline = mapping.get('operating_airline', '')
            if operating_airline:
                if operating_airline != coupon.operating_airline:
                    return False
            
            # Check route
            route = mapping.get('route', '')
            if route:
                # Build route from origin and destination
                coupon_route = f"{coupon.cpn_origin}-{coupon.cpn_destination}"
                if route != coupon_route:
                    return False
            
            # Check effective dates
            effective_from = mapping.get('effective_from')
            effective_to = mapping.get('effective_to')
            
            if effective_from or effective_to:
                # Use travel date for effective date checking
                travel_date = coupon.cpn_flown_date
                
                if effective_from:
                    try:
                        from_date = datetime.strptime(effective_from, '%Y-%m-%d').date()
                        if travel_date < from_date:
                            return False
                    except ValueError:
                        self.logger.warning(f"Invalid effective_from date format: {effective_from}")
                
                if effective_to:
                    try:
                        to_date = datetime.strptime(effective_to, '%Y-%m-%d').date()
                        if travel_date > to_date:
                            return False
                    except ValueError:
                        self.logger.warning(f"Invalid effective_to date format: {effective_to}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking coupon mapping match: {str(e)}")
            return False
    
    def _check_exclusions(self, coupon: CouponData, exclusions: List[str]) -> Optional[str]:
        """
        Check if coupon is excluded by any of the exclusion criteria
        
        Args:
            coupon: Coupon data
            exclusions: List of exclusion criteria
            
        Returns:
            Exclusion reason if excluded, None otherwise
        """
        try:
            for exclusion in exclusions:
                if self._is_excluded_by_criterion(coupon, exclusion):
                    return exclusion
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error checking exclusions: {str(e)}")
            return None
    
    def _is_excluded_by_criterion(self, coupon: CouponData, exclusion: str) -> bool:
        """
        Check if coupon is excluded by a specific criterion
        
        Args:
            coupon: Coupon data
            exclusion: Exclusion criterion
            
        Returns:
            True if excluded
        """
        try:
            exclusion_lower = exclusion.lower()
            
            # Check for OTADOC (Other Airline Document)
            if 'otadoc' in exclusion_lower or 'other airline document' in exclusion_lower:
                # This would need to be determined from coupon data
                # For now, assume not excluded unless we have specific logic
                return False
            
            # Check for disallowed RBDs
            if 'disallowed rbd' in exclusion_lower or 'rbd' in exclusion_lower:
                # Check if coupon RBD is in disallowed list
                # This would need to be checked against the specific RBD exclusions
                # For now, assume not excluded unless we have specific logic
                return False
            
            # Check for disallowed deal/discount families
            if 'deal' in exclusion_lower or 'discount' in exclusion_lower:
                # Check fare type or other deal-related fields
                # This would need to be checked against specific deal exclusions
                return False
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking exclusion criterion: {str(e)}")
            return False
    
    def update_contract_analysis_with_addon(self, contract_analysis: ContractAnalysis,
                                          addon_result: Dict[str, Any]) -> ContractAnalysis:
        """
        Update contract analysis with addon rule results
        
        Args:
            contract_analysis: Original contract analysis
            addon_result: Addon rule processing result
            
        Returns:
            Updated contract analysis
        """
        try:
            # Update eligibility flags
            contract_analysis.trigger_eligibility = addon_result['trigger_eligible']
            contract_analysis.payout_eligibility = addon_result['payout_eligible']
            
            # Update eligibility reasons
            if addon_result['addon_applied']:
                addon_details = addon_result['addon_details']
                applied_addons = [detail for detail in addon_details if detail['applied']]
                
                if applied_addons:
                    addon_names = [addon['addon_name'] for addon in applied_addons]
                    addon_reason = f"Addon rules applied: {', '.join(addon_names)}"
                    
                    # Append to existing reasons
                    if contract_analysis.trigger_eligibility_reason:
                        contract_analysis.trigger_eligibility_reason += f"; {addon_reason}"
                    else:
                        contract_analysis.trigger_eligibility_reason = addon_reason
                    
                    if contract_analysis.payout_eligibility_reason:
                        contract_analysis.payout_eligibility_reason += f"; {addon_reason}"
                    else:
                        contract_analysis.payout_eligibility_reason = addon_reason
            
            return contract_analysis
            
        except Exception as e:
            self.logger.error(f"Error updating contract analysis with addon results: {str(e)}")
            return contract_analysis
