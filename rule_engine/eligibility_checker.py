"""
Eligibility checking functionality for all criteria
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from loguru import logger

from .models import CouponData, ContractData
from .exceptions import EligibilityError


class EligibilityChecker:
    """
    Handles all eligibility checking criteria for contracts
    """
    
    def __init__(self):
        """Initialize eligibility checker"""
        self.logger = logger
    
    def check_airline_eligibility(self, coupon: CouponData, contract: ContractData) -> bool:
        """
        Check if coupon's airline is eligible for the contract
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            True if airline is eligible
        """
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            out_criteria = trigger_criteria.get('OUT', {})
            
            # Use cpn_airline_code as fallback for empty airline fields
            marketing_airline = coupon.marketing_airline if coupon.marketing_airline not in ["", "Unknown", None] else coupon.cpn_airline_code
            operating_airline = coupon.operating_airline if coupon.operating_airline not in ["", "Unknown", None] else coupon.cpn_airline_code
            ticketing_airline = coupon.ticketing_airline if coupon.ticketing_airline not in ["", "Unknown", None] else coupon.cpn_airline_code
            
            # Check marketing airline (try both underscore and space versions)
            marketing_airlines_in = in_criteria.get('Marketing_Airline', []) or in_criteria.get('Marketing Airline', [])
            marketing_airlines_out = out_criteria.get('Marketing_Airline', []) or out_criteria.get('Marketing Airline', [])
            if not self._check_list_criteria(marketing_airline, marketing_airlines_in, marketing_airlines_out, 'Marketing Airline'):
                return False
            
            # Check operating airline (try both underscore and space versions)
            operating_airlines_in = in_criteria.get('Operating_Airline', []) or in_criteria.get('Operating Airline', [])
            operating_airlines_out = out_criteria.get('Operating_Airline', []) or out_criteria.get('Operating Airline', [])
            if not self._check_list_criteria(operating_airline, operating_airlines_in, operating_airlines_out, 'Operating Airline'):
                return False
            
            # Check ticketing airline (try both underscore and space versions)
            ticketing_airlines_in = in_criteria.get('Ticketing_Airline', []) or in_criteria.get('Ticketing Airline', [])
            ticketing_airlines_out = out_criteria.get('Ticketing_Airline', []) or out_criteria.get('Ticketing Airline', [])
            if not self._check_list_criteria(ticketing_airline, ticketing_airlines_in, ticketing_airlines_out, 'Ticketing Airline'):
                return False
            
            # Check flight numbers (if available)
            if hasattr(coupon, 'flight_numbers'):
                if not self._check_list_criteria(coupon.flight_numbers, in_criteria.get('Flight_Nos', []), out_criteria.get('Flight_Nos', []), 'Flight_Nos'):
                    return False
            
            self.logger.debug(f"Airline eligibility passed for {coupon.cpn_airline_code}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking airline eligibility: {str(e)}")
            raise EligibilityError(f"Airline eligibility check failed: {str(e)}")
    
    def check_date_range_eligibility(self, coupon: CouponData, contract: ContractData) -> bool:
        """
        Check if coupon date is within contract validity period and date criteria
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            True if date is eligible
        """
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            
            # Check contract validity period first
            if contract.trigger_type == "FLOWN":
                coupon_date = coupon.cpn_flown_date
            else:  # SALES or other
                coupon_date = coupon.cpn_sales_date
            
            # Check if date is within contract window
            if coupon_date < contract.start_date or coupon_date > contract.end_date:
                self.logger.debug(f"Coupon date {coupon_date} outside contract window {contract.start_date} to {contract.end_date}")
                return False
            
            # Check specific date criteria from contract
            sales_date_range = in_criteria.get('Sales_Date', {})
            travel_date_range = in_criteria.get('Travel_Date', {})
            
            # Check sales date range
            if sales_date_range and not self._check_date_range_criteria(coupon.cpn_sales_date, sales_date_range.get('start', ''), sales_date_range.get('end', ''), 'Sales_Date'):
                return False
            
            # Check travel date range
            if travel_date_range and not self._check_date_range_criteria(coupon.cpn_flown_date, travel_date_range.get('start', ''), travel_date_range.get('end', ''), 'Travel_Date'):
                return False
            
            self.logger.debug(f"Date eligibility passed for {coupon_date}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking date eligibility: {str(e)}")
            raise EligibilityError(f"Date eligibility check failed: {str(e)}")
    
    def check_geographic_eligibility(self, coupon: CouponData, contract: ContractData) -> bool:
        """
        Check geographic eligibility (IATA codes, countries, routes, city codes, etc.)
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            True if geographically eligible
        """
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            out_criteria = trigger_criteria.get('OUT', {})
            
            # Check IATA codes
            if contract.iata_codes:
                # Convert both to strings for comparison, handling float precision
                coupon_iata_str = str(int(float(coupon.iata))) if coupon.iata else str(coupon.iata)
                contract_iata_strs = [str(iata).strip() for iata in contract.iata_codes]
                if coupon_iata_str not in contract_iata_strs:
                    self.logger.debug(f"IATA code {coupon_iata_str} not in eligible list: {contract_iata_strs}")
                    return False
            
            # Check countries (if we had country mapping)
            if contract.countries:
                # This would require a city-to-country mapping
                # For now, we'll assume it passes if IATA codes match
                pass
            
            # Check routes
            if not self._check_list_criteria(coupon.route, in_criteria.get('Route', []), out_criteria.get('Route', []), 'Route'):
                return False
            
            # Check city codes
            if not self._check_list_criteria(coupon.city_codes, in_criteria.get('City_Codes', []), out_criteria.get('City_Codes', []), 'City_Codes'):
                return False
            
            # Check OnD (Origin and Destination)
            if hasattr(coupon, 'ond'):
                if not self._check_list_criteria(coupon.ond, in_criteria.get('OnD', []), out_criteria.get('OnD', []), 'OnD'):
                    return False
            
            # Check POS (Point of Sale)
            if not self._check_list_criteria(coupon.pcc, in_criteria.get('POS', []), out_criteria.get('POS', []), 'POS'):
                return False
            
            # Check POO (Point of Origin) - if available
            if hasattr(coupon, 'poo'):
                if not self._check_list_criteria(coupon.poo, in_criteria.get('POO', []), out_criteria.get('POO', []), 'POO'):
                    return False
            
            self.logger.debug(f"Geographic eligibility passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking geographic eligibility: {str(e)}")
            raise EligibilityError(f"Geographic eligibility check failed: {str(e)}")
    
    def check_booking_eligibility(self, coupon: CouponData, contract: ContractData) -> bool:
        """
        Check booking-related eligibility (RBD, cabin, fare type, corporate code, etc.)
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            True if booking eligible
        """
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            out_criteria = trigger_criteria.get('OUT', {})
            
            # Handle QR-AO contracts that have list format for IN criteria
            if isinstance(in_criteria, list):
                # For QR-AO contracts, IN criteria is a list of fare types
                # Skip booking eligibility checks as they don't apply
                self.logger.debug(f"QR-AO contract format detected, skipping booking eligibility checks")
                return True
            
            # Check RBD eligibility
            if not self._check_list_criteria(coupon.cpn_RBD, in_criteria.get('RBD', []), out_criteria.get('RBD', []), 'RBD'):
                return False
            
            # Check cabin class
            if not self._check_list_criteria(coupon.cabin, in_criteria.get('Cabin', []), out_criteria.get('Cabin', []), 'Cabin'):
                return False
            
            # Check fare type
            if not self._check_list_criteria(coupon.fare_type, in_criteria.get('Fare_Type', []), out_criteria.get('Fare_Type', []), 'Fare_Type'):
                return False
            
            # Check corporate code
            if not self._check_list_criteria(coupon.corporate_code, in_criteria.get('Corporate_Code', []), out_criteria.get('Corporate_Code', []), 'Corporate_Code'):
                return False
            
            # Check tour codes
            if not self._check_list_criteria(coupon.tour_codes, in_criteria.get('Tour_Code', []), out_criteria.get('Tour_Code', []), 'Tour_Code'):
                return False
            
            # Check fare basis patterns (if available)
            if hasattr(coupon, 'fare_basis_patterns'):
                if not self._check_list_criteria(coupon.fare_basis_patterns, in_criteria.get('Fare_Basis_Patterns', []), out_criteria.get('Fare_Basis_Patterns', []), 'Fare_Basis_Patterns'):
                    return False
            
            # Check DomIntl (Domestic/International) criteria
            if not self._check_boolean_criteria(coupon.cpn_is_international, in_criteria.get('DomIntl'), out_criteria.get('DomIntl'), 'DomIntl'):
                return False
            
            self.logger.debug(f"Booking eligibility passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking booking eligibility: {str(e)}")
            raise EligibilityError(f"Booking eligibility check failed: {str(e)}")
    
    def check_technical_eligibility(self, coupon: CouponData, contract: ContractData) -> bool:
        """
        Check technical eligibility (NDC, code share, interline, alliance, etc.)
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            True if technically eligible
        """
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            out_criteria = trigger_criteria.get('OUT', {})
            
            # Check NDC eligibility
            if not self._check_boolean_criteria(coupon.ndc, in_criteria.get('NDC'), out_criteria.get('NDC'), 'NDC'):
                return False
            
            # Check code share (OUT = True means exclude code share)
            if not self._check_boolean_criteria(coupon.code_share, in_criteria.get('Code_Share'), out_criteria.get('Code_Share'), 'Code_Share'):
                return False
            
            # Check interline
            if not self._check_boolean_criteria(coupon.interline, in_criteria.get('Interline'), out_criteria.get('Interline'), 'Interline'):
                return False
            
            # Check alliance (if available)
            if hasattr(coupon, 'alliance'):
                if not self._check_list_criteria(coupon.alliance, in_criteria.get('Alliance', []), out_criteria.get('Alliance', []), 'Alliance'):
                    return False
            
            # Check SITI/SOTO/SITO/SOTI (if available)
            if hasattr(coupon, 'siti_soto_sito_soti'):
                if not self._check_list_criteria(coupon.siti_soto_sito_soti, in_criteria.get('SITI_SOTO_SITO_SOTI', []), out_criteria.get('SITI_SOTO_SITO_SOTI', []), 'SITI_SOTO_SITO_SOTI'):
                    return False
            
            self.logger.debug(f"Technical eligibility passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking technical eligibility: {str(e)}")
            raise EligibilityError(f"Technical eligibility check failed: {str(e)}")
    
    def check_payout_eligibility(self, coupon: CouponData, contract: ContractData) -> bool:
        """
        Check payout-specific eligibility criteria (separate from trigger eligibility)
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            True if payout eligible
        """
        try:
            payout_criteria = contract.payout_eligibility_criteria
            
            # If no separate payout criteria defined, use trigger criteria
            if not payout_criteria or payout_criteria == {}:
                self.logger.debug("No separate payout criteria defined, using trigger criteria")
                return self.check_technical_eligibility(coupon, contract)
            
            in_criteria = payout_criteria.get('IN', {})
            out_criteria = payout_criteria.get('OUT', {})
            
            # Check NDC eligibility for payout
            if not self._check_boolean_criteria(coupon.ndc, in_criteria.get('NDC'), out_criteria.get('NDC'), 'NDC_Payout'):
                return False
            
            # Check code share for payout (separate from trigger)
            if not self._check_boolean_criteria(coupon.code_share, in_criteria.get('Code_Share'), out_criteria.get('Code_Share'), 'Code_Share_Payout'):
                return False
            
            # Check interline for payout
            if not self._check_boolean_criteria(coupon.interline, in_criteria.get('Interline'), out_criteria.get('Interline'), 'Interline_Payout'):
                return False
            
            # Check alliance for payout (if available)
            if hasattr(coupon, 'alliance'):
                if not self._check_list_criteria(coupon.alliance, in_criteria.get('Alliance', []), out_criteria.get('Alliance', []), 'Alliance_Payout'):
                    return False
            
            # Check SITI/SOTO/SITO/SOTI for payout (if available)
            if hasattr(coupon, 'siti_soto_sito_soti'):
                if not self._check_list_criteria(coupon.siti_soto_sito_soti, in_criteria.get('SITI_SOTO_SITO_SOTI', []), out_criteria.get('SITI_SOTO_SITO_SOTI', []), 'SITI_SOTO_SITO_SOTI_Payout'):
                    return False
            
            # Check other payout-specific criteria
            # Check RBD eligibility for payout
            if not self._check_list_criteria(coupon.cpn_RBD, in_criteria.get('RBD', []), out_criteria.get('RBD', []), 'RBD_Payout'):
                return False
            
            # Check cabin class for payout
            if not self._check_list_criteria(coupon.cabin, in_criteria.get('Cabin', []), out_criteria.get('Cabin', []), 'Cabin_Payout'):
                return False
            
            # Check fare type for payout
            if not self._check_list_criteria(coupon.fare_type, in_criteria.get('Fare_Type', []), out_criteria.get('Fare_Type', []), 'Fare_Type_Payout'):
                return False
            
            # Check DomIntl for payout
            if not self._check_boolean_criteria(coupon.cpn_is_international, in_criteria.get('DomIntl'), out_criteria.get('DomIntl'), 'DomIntl_Payout'):
                return False
            
            self.logger.debug(f"Payout eligibility passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking payout eligibility: {str(e)}")
            raise EligibilityError(f"Payout eligibility check failed: {str(e)}")
    
    def check_all_eligibility_criteria(self, coupon: CouponData, contract: ContractData) -> Dict[str, Any]:
        """
        Check all eligibility criteria and return detailed results
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Dictionary with eligibility results for each criterion
        """
        try:
            # Check individual eligibility components
            airline_eligible = self.check_airline_eligibility(coupon, contract)
            date_eligible = self.check_date_range_eligibility(coupon, contract)
            geo_eligible = self.check_geographic_eligibility(coupon, contract)
            booking_eligible = self.check_booking_eligibility(coupon, contract)
            technical_eligible = self.check_technical_eligibility(coupon, contract)
            
            # Calculate trigger eligibility (all components must pass)
            trigger_eligible = airline_eligible and date_eligible and geo_eligible and booking_eligible and technical_eligible
            
            # Check payout eligibility
            payout_eligible = self.check_payout_eligibility(coupon, contract)
            
            # FIX: If trigger and payout criteria are identical, payout should fail if trigger fails
            trigger_criteria = contract.trigger_eligibility_criteria
            payout_criteria = contract.payout_eligibility_criteria
            
            # Check if trigger and payout criteria are identical
            if trigger_criteria == payout_criteria:
                self.logger.debug("Trigger and payout criteria are identical - payout eligibility should match trigger eligibility")
                payout_eligible = trigger_eligible
            
            # Build results with detailed reasons
            results = {
                'airline_eligibility': airline_eligible,
                'date_eligibility': date_eligible,
                'geographic_eligibility': geo_eligible,
                'booking_eligibility': booking_eligible,
                'technical_eligibility': technical_eligible,
                'trigger_eligibility': trigger_eligible,
                'payout_eligibility': payout_eligible,
                'trigger_eligibility_reason': self._build_eligibility_reason(airline_eligible, date_eligible, geo_eligible, booking_eligible, technical_eligible),
                'payout_eligibility_reason': self._build_payout_eligibility_reason(payout_eligible, trigger_criteria, payout_criteria)
            }
            
            self.logger.debug(f"Eligibility results: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error checking all eligibility criteria: {str(e)}")
            raise EligibilityError(f"Eligibility check failed: {str(e)}")
    
    def _build_eligibility_reason(self, airline_eligible: bool, date_eligible: bool, geo_eligible: bool, 
                                 booking_eligible: bool, technical_eligible: bool) -> str:
        """Build detailed eligibility reason string"""
        reasons = []
        
        if not airline_eligible:
            reasons.append("Airline criteria not met")
        if not date_eligible:
            reasons.append("Date criteria not met")
        if not geo_eligible:
            reasons.append("Geographic criteria not met")
        if not booking_eligible:
            reasons.append("Booking criteria not met")
        if not technical_eligible:
            reasons.append("Technical criteria not met")
        
        if not reasons:
            return "All criteria met"
        else:
            return "; ".join(reasons)
    
    def _build_payout_eligibility_reason(self, payout_eligible: bool, trigger_criteria: Dict, payout_criteria: Dict) -> str:
        """Build detailed payout eligibility reason string"""
        if trigger_criteria == payout_criteria:
            if payout_eligible:
                return "All payout criteria met (identical to trigger criteria)"
            else:
                return "Payout criteria not met (identical to trigger criteria)"
        else:
            if payout_eligible:
                return "All payout criteria met"
            else:
                return "Payout criteria not met"
    
    def check_silent_criteria(self, coupon: CouponData, contract: ContractData) -> Dict[str, bool]:
        """
        Check silent criteria (criteria that don't affect eligibility but are tracked)
        
        Args:
            coupon: Coupon data
            contract: Contract data
            
        Returns:
            Dictionary with silent criteria results
        """
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            silent_criteria = trigger_criteria.get('SILENT', {})
            
            results = {}
            
            # Check each silent criterion
            for criterion, expected_values in silent_criteria.items():
                if expected_values == ["SILENT"]:
                    # This criterion is silent (not checked)
                    results[criterion] = True
                else:
                    # This criterion has specific values to check
                    coupon_value = getattr(coupon, criterion.lower().replace(' ', '_'), None)
                    if coupon_value is not None:
                        results[criterion] = coupon_value in expected_values
                    else:
                        results[criterion] = True  # Default to True if value not found
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error checking silent criteria: {str(e)}")
            return {}
    
    def validate_eligibility_criteria(self, contract: ContractData) -> Dict[str, Any]:
        """
        Validate that contract eligibility criteria are properly configured
        
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
            
            trigger_criteria = contract.trigger_eligibility_criteria
            payout_criteria = contract.payout_eligibility_criteria
            
            # Check for required sections
            if 'IN' not in trigger_criteria:
                validation_results['errors'].append("Missing IN criteria in trigger eligibility")
                validation_results['valid'] = False
            
            if 'OUT' not in trigger_criteria:
                validation_results['warnings'].append("Missing OUT criteria in trigger eligibility")
            
            if 'SILENT' not in trigger_criteria:
                validation_results['warnings'].append("Missing SILENT criteria in trigger eligibility")
            
            # Check for conflicting criteria
            in_rbds = trigger_criteria.get('IN', {}).get('RBD', [])
            out_rbds = trigger_criteria.get('OUT', {}).get('RBD', [])
            
            conflicting_rbds = set(in_rbds) & set(out_rbds)
            if conflicting_rbds:
                validation_results['warnings'].append(f"Conflicting RBDs in IN and OUT: {conflicting_rbds}")
            
            # Check date ranges
            if contract.start_date >= contract.end_date:
                validation_results['errors'].append("Contract start date must be before end date")
                validation_results['valid'] = False
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating eligibility criteria: {str(e)}")
            return {
                'valid': False,
                'warnings': [],
                'errors': [f"Validation error: {str(e)}"]
            }
    
    def _check_list_criteria(self, coupon_value: Any, in_list: List[Any], out_list: List[Any], criteria_name: str) -> bool:
        """
        Check list-based criteria with IN/OUT logic according to specifications:
        
        Specifications:
        1. "Filter included for trigger and payout" → Any value acceptable for both
        2. "Filter included for trigger, excluded for payout" → Any value for trigger, only excluded values for payout  
        3. "Filter excluded for trigger and payout" → Only non-excluded values acceptable for both
        
        Args:
            coupon_value: Value from coupon data
            in_list: List of values that must be included (IN criteria)
            out_list: List of values that must be excluded (OUT criteria)
            criteria_name: Name of the criteria for logging
            
        Returns:
            True if criteria passes
        """
        try:
            # Handle empty or None values
            if coupon_value is None or coupon_value == "":
                coupon_value = ""
            
            # Convert lists to proper format (handle None cases)
            in_list = in_list if in_list is not None else []
            out_list = out_list if out_list is not None else []
            
            # Apply the corrected logic based on specifications:
            
            # If both IN and OUT criteria are specified, this is an invalid configuration
            if in_list and out_list:
                self.logger.warning(f"{criteria_name} has both IN and OUT criteria specified - this is invalid")
                return False
            
            # If only OUT criteria is specified (excluded)
            if out_list and not in_list:
                # OUT: ["B", "C"] means these values are NOT acceptable
                # So if coupon_value is in out_list, it's NOT eligible
                if coupon_value in out_list:
                    self.logger.debug(f"{criteria_name} {coupon_value} is in excluded list: {out_list}")
                    return False
                else:
                    self.logger.debug(f"{criteria_name} {coupon_value} is not in excluded list: {out_list} - eligible")
                    return True
            
            # If only IN criteria is specified (included)
            if in_list and not out_list:
                # IN: ["B", "C"] means only these values are acceptable
                # Handle special cases first
                if "ALL" in in_list or "SILENT" in in_list:
                    self.logger.debug(f"{criteria_name} has ALL/SILENT in IN list - any value acceptable")
                    return True
                
                # Check if coupon value is in the included list
                if coupon_value not in in_list:
                    self.logger.debug(f"{criteria_name} {coupon_value} not in eligible list: {in_list}")
                    return False
                else:
                    self.logger.debug(f"{criteria_name} {coupon_value} is in eligible list: {in_list} - eligible")
                    return True
            
            # If neither IN nor OUT criteria is specified, any value is acceptable
            if not in_list and not out_list:
                self.logger.debug(f"{criteria_name} no criteria specified - any value acceptable")
                return True
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking {criteria_name} criteria: {str(e)}")
            return False
    
    def _check_boolean_criteria(self, coupon_value: Any, in_value: Any, out_value: Any, criteria_name: str) -> bool:
        """
        Check boolean-based criteria with IN/OUT logic according to specifications:
        
        Specifications:
        1. "Filter included for trigger and payout" → Any value acceptable for both
        2. "Filter included for trigger, excluded for payout" → Any value for trigger, only FALSE for payout  
        3. "Filter excluded for trigger and payout" → Only FALSE acceptable for both
        
        Args:
            coupon_value: Value from coupon data
            in_value: Value that must be included (IN criteria) - if coupon matches this, it's eligible
            out_value: Value that must be excluded (OUT criteria) - if coupon matches this, it's NOT eligible
            criteria_name: Name of the criteria for logging
            
        Returns:
            True if criteria passes
        """
        try:
            # Handle empty or None values
            if coupon_value is None or coupon_value == "":
                coupon_value = False
            
            # Convert to boolean if needed
            if isinstance(coupon_value, str):
                coupon_value = coupon_value.lower() in ['true', '1', 'yes', 'y']
            
            # Convert criteria values to boolean if needed
            if in_value is not None and isinstance(in_value, str):
                in_value = in_value.lower() in ['true', '1', 'yes', 'y']
            if out_value is not None and isinstance(out_value, str):
                out_value = out_value.lower() in ['true', '1', 'yes', 'y']
            
            # Apply the corrected logic based on specifications:
            
            # If both IN and OUT criteria are specified, this is an invalid configuration
            if in_value is not None and out_value is not None:
                self.logger.warning(f"{criteria_name} has both IN and OUT criteria specified - this is invalid")
                return False
            
            # If only OUT criteria is specified (excluded)
            if out_value is not None and in_value is None:
                # OUT: {"Code_Share": true} means only FALSE values are acceptable
                # So if coupon_value == out_value, it's NOT eligible
                if coupon_value == out_value:
                    self.logger.debug(f"{criteria_name} {coupon_value} matches excluded value: {out_value}")
                    return False
                else:
                    self.logger.debug(f"{criteria_name} {coupon_value} does not match excluded value: {out_value} - eligible")
                    return True
            
            # If only IN criteria is specified (included)
            if in_value is not None and out_value is None:
                # IN: {"Code_Share": true} means only TRUE values are acceptable
                # So if coupon_value != in_value, it's NOT eligible
                if coupon_value != in_value:
                    self.logger.debug(f"{criteria_name} {coupon_value} does not match required value: {in_value}")
                    return False
                else:
                    self.logger.debug(f"{criteria_name} {coupon_value} matches required value: {in_value} - eligible")
                    return True
            
            # If neither IN nor OUT criteria is specified, any value is acceptable
            if in_value is None and out_value is None:
                self.logger.debug(f"{criteria_name} no criteria specified - any value acceptable")
                return True
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking {criteria_name} boolean criteria: {str(e)}")
            return False
    
    def _check_date_range_criteria(self, coupon_date: date, start_date: str, end_date: str, criteria_name: str) -> bool:
        """
        Check date range criteria
        
        Args:
            coupon_date: Date from coupon data
            start_date: Start date string
            end_date: End date string
            criteria_name: Name of the criteria for logging
            
        Returns:
            True if date is within range
        """
        try:
            if not start_date and not end_date:
                return True  # No date restrictions
            
            if start_date:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                if coupon_date < start_dt:
                    self.logger.debug(f"{criteria_name} date {coupon_date} before start date: {start_dt}")
                    return False
            
            if end_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                if coupon_date > end_dt:
                    self.logger.debug(f"{criteria_name} date {coupon_date} after end date: {end_dt}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking {criteria_name} date range: {str(e)}")
            return False
