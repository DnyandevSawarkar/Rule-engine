"""
Refactored eligibility checking functionality with separated evaluation phases

This module implements a 3-phase eligibility evaluation model:
1. Sector Eligibility - Geographic, booking, airline/flight, contract window
2. Trigger Eligibility - Technical criteria for triggering calculations
3. Payout Eligibility - Technical criteria for receiving payouts (independent from trigger)
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union, Tuple
from loguru import logger

from .models import CouponData, ContractData
from .exceptions import EligibilityError
from .field_mapper import FieldMapper


class EligibilityCheckerV2:
    """
    Refactored eligibility checker with separated evaluation phases
    """
    
    def __init__(self, field_mapper: Optional[FieldMapper] = None):
        """
        Initialize eligibility checker
        
        Args:
            field_mapper: FieldMapper instance. If None, creates a new one.
        """
        self.logger = logger
        self.field_mapper = field_mapper or FieldMapper()
        
        # Mapping from contract field names to rule field names
        self.contract_field_to_rule_field = {
            'POS': 'pos',
            'Route': 'route',
            'Routes': 'route',
            'O&D Area': 'ond',
            'OnD': 'ond',
            'City_Codes': 'cityCode',
            'Marketing_Airline': 'marketingAirline',
            'Marketing Airline': 'marketingAirline',
            'Operating_Airline': 'operatingAirline',
            'Operating Airline': 'operatingAirline',
            'Ticketing_Airline': 'ticketingAirline',
            'Ticketing Airline': 'ticketingAirline',
            'Flight_Nos': 'flightNumber',
            'RBD': 'rbd',
            'Cabin': 'cabin',
            'Fare_Type': 'fareType',
            'Fare Type': 'fareType',
            'Corporate_Code': 'corporateCode',
            'Tour_Code': 'tourCode',
            'NDC': 'ndc',
            'Code_Share': 'codeShare',
            'Interline': 'interline',
            'DomIntl': 'domIntl',
            'SITI_SOTO_SITO_SOTI': 'sitiSotoSitoSoti',
            'SITI/SOTO/SITO/SOTI': 'sitiSotoSitoSoti',
            'Alliance': 'alliance',
            'Sales_Date': 'salesDate',
            'Travel_Date': 'travelDate',
            'IATA': 'iataCode'
        }
        
        self.sector_criteria_groups = {'geographic', 'booking', 'airline_flight'}

    def check_sector_eligibility(self, coupon: CouponData, contract: ContractData) -> Tuple[bool, List[str]]:
        """
        Check sector eligibility based on Geographic + Booking + Airline/Flight + Contract Window.
        Iterates through contract.trigger_eligibility_criteria but ONLY evaluates fields 
        belonging to sector-relevant groups.
        """
        try:
            reasons = []
            
            # 0. SECTOR AIRLINE CODE CHECK (FIRST AND STRICTEST)
            # Use ONLY contract.airline_codes from rule metadata vs coupon.cpn_airline_code
            # Marketing/Ticketing/Operating Airline are for TRIGGER/PAYOUT filters, NOT sector eligibility
            contract_airline_codes = getattr(contract, 'airline_codes', []) or []
            
            if contract_airline_codes:
                # For sector: compare ONLY cpn_airline_code (sector airline) against contract.airline_codes
                coupon_sector_airline = (coupon.cpn_airline_code or "").strip().upper()
                contract_airline_codes_upper = {str(c).strip().upper() for c in contract_airline_codes if c}
                
                # If coupon sector airline doesn't match contract airline codes, sector is INELIGIBLE
                if coupon_sector_airline not in contract_airline_codes_upper:
                    reason = (
                        f"Sector airline mismatch: coupon sector airline '{coupon_sector_airline}' "
                        f"does not match contract airline codes {sorted(contract_airline_codes_upper)}"
                    )
                    return False, [reason]  # Fail immediately - no need to check other criteria
            
            # 1. Contract Window Check
            window_passed, window_reason = self._check_contract_window(coupon, contract)
            if not window_passed:
                reasons.append(window_reason)
                # Fail immediately if date outside window? User said "If date not in contract window... sectorEligible=false"
                # We can continue to collect other reasons or return now. 
                # User prompted: "It must not be possible to have... Date not in window... and still show sectorEligible=true"
                # Safest to return False immediately OR ensure final result is False.
                # Let's collect all reasons but ensure result is False.
            
            # 2. IATA Code Check (Explicit field in contract)
            if contract.iata_codes:
                iata_passed, iata_reason = self._check_criterion_explicit(
                    coupon, 'iataCode', contract.iata_codes, [], 'geographic'
                )
                if not iata_passed:
                    reasons.append(iata_reason)

            # 3. Iterate Trigger Criteria but Filter for Sector Groups
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            out_criteria = trigger_criteria.get('OUT', {})

            # Helper to check a dictionary of criteria
            def check_criteria_dict(criteria_dict, is_exclusion=False):
                local_reasons = []
                for contract_field, rules in criteria_dict.items():
                    rule_field = self._resolve_rule_field(contract_field)
                    group = self.field_mapper.get_criteria_group(rule_field)
                    
                    if group in self.sector_criteria_groups:
                        # This is a sector creation field
                        in_vals = [] if is_exclusion else rules
                        out_vals = rules if is_exclusion else []
                        
                        passed, reason = self._check_criterion(
                            coupon, contract_field, in_vals, out_vals, group
                        )
                        if not passed:
                            local_reasons.append(reason)
                return local_reasons

            reasons.extend(check_criteria_dict(in_criteria, is_exclusion=False))
            reasons.extend(check_criteria_dict(out_criteria, is_exclusion=True))

            is_eligible = len(reasons) == 0
            return is_eligible, reasons

        except Exception as e:
            self.logger.error(f"Error checking sector eligibility: {str(e)}")
            raise EligibilityError(f"Sector eligibility check failed: {str(e)}")

    def check_trigger_eligibility(self, coupon: CouponData, contract: ContractData, 
                                  sector_eligible: bool) -> Tuple[bool, List[str]]:
        """
        Check trigger eligibility based on criteria NOT checked in sector eligibility.
        """
        try:
            if not sector_eligible:
                return False, ["Sector not eligible - trigger skipped"]
            
            reasons = []
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            out_criteria = trigger_criteria.get('OUT', {})
            
            all_passed = True

            def check_remaining_criteria(criteria_dict, is_exclusion=False):
                local_reasons = []
                local_passed = True
                for contract_field, rules in criteria_dict.items():
                    rule_field = self._resolve_rule_field(contract_field)
                    group = self.field_mapper.get_criteria_group(rule_field)
                    
                    # Skip sector groups as they were already checked
                    if group in self.sector_criteria_groups:
                        continue
                        
                    # Evaluate other groups (e.g. 'other', 'trigger')
                    in_vals = [] if is_exclusion else rules
                    out_vals = rules if is_exclusion else []
                    
                    passed, reason = self._check_criterion(
                        coupon, contract_field, in_vals, out_vals, group or 'unknown'
                    )
                    if not passed:
                        local_passed = False
                        local_reasons.append(reason)
                return local_passed, local_reasons

            passed_in, reasons_in = check_remaining_criteria(in_criteria, is_exclusion=False)
            passed_out, reasons_out = check_remaining_criteria(out_criteria, is_exclusion=True)
            
            if not passed_in or not passed_out:
                all_passed = False
                reasons.extend(reasons_in)
                reasons.extend(reasons_out)
            
            if all_passed:
                reasons = ["All trigger criteria met"]
                
            return all_passed, reasons

        except Exception as e:
            self.logger.error(f"Error checking trigger eligibility: {str(e)}")
            raise EligibilityError(f"Trigger eligibility check failed: {str(e)}")
            
    def check_payout_eligibility(self, coupon: CouponData, contract: ContractData,
                                sector_eligible: bool) -> Tuple[bool, List[str]]:
        """
        Check payout eligibility. 
        Uses 'payout_eligibility_criteria' if present.
        If empty, uses 'trigger_eligibility_criteria' (fallback).
        """
        try:
            if not sector_eligible:
                return False, ["Sector not eligible - payout skipped"]
            
            payout_criteria = contract.payout_eligibility_criteria
            
            # Fallback logic: if no payout criteria, use trigger criteria
            # User said: "If no separate payout criteria defined, use trigger criteria"
            # We assume this means re-evaluate trigger criteria as the payout check?
            # Or assume true? Usually implies same conditions apply for payout.
            if not payout_criteria or (not payout_criteria.get('IN') and not payout_criteria.get('OUT')):
                self.logger.debug("No separate payout criteria - using trigger criteria for payout")
                source_criteria = contract.trigger_eligibility_criteria
            else:
                source_criteria = payout_criteria
                
            reasons = []
            in_criteria = source_criteria.get('IN', {})
            out_criteria = source_criteria.get('OUT', {})
            
            all_passed = True
            
            # Evaluate ALL fields in the payout definition (even if they overlap with sector fields)
            # because payout criteria might be stricter or different (e.g. different RBD list).
            
            def check_all_criteria(criteria_dict, is_exclusion=False):
                local_reasons = []
                local_passed = True
                for contract_field, rules in criteria_dict.items():
                    rule_field = self._resolve_rule_field(contract_field)
                    # We do not filter by group here. Payout criteria are explicit.
                    
                    in_vals = [] if is_exclusion else rules
                    out_vals = rules if is_exclusion else []
                    
                    # Determine group for logging (optional)
                    group = self.field_mapper.get_criteria_group(rule_field)
                    
                    passed, reason = self._check_criterion(
                        coupon, contract_field, in_vals, out_vals, group or 'payout'
                    )
                    if not passed:
                        local_passed = False
                        local_reasons.append(reason)
                return local_passed, local_reasons

            passed_in, reasons_in = check_all_criteria(in_criteria, is_exclusion=False)
            passed_out, reasons_out = check_all_criteria(out_criteria, is_exclusion=True)
            
            if not passed_in or not passed_out:
                all_passed = False
                reasons.extend(reasons_in)
                reasons.extend(reasons_out)
                
            if all_passed:
                reasons = ["All payout criteria met"]
                
            return all_passed, reasons

        except Exception as e:
            self.logger.error(f"Error checking payout eligibility: {str(e)}")
            raise EligibilityError(f"Payout eligibility check failed: {str(e)}")

    def _resolve_rule_field(self, contract_field: str) -> str:
        """Resolve contract field name to internal rule field name"""
        return self.contract_field_to_rule_field.get(contract_field, contract_field)

    def _check_contract_window(self, coupon: CouponData, contract: ContractData) -> Tuple[bool, Optional[str]]:
        """Check if coupon date is within contract validity period"""
        # We need to map 'Sales_Date' or 'Travel_Date' to actual fields.
        # But this check is usually fundamental to the contract object itself (start_date/end_date).
        # We assume standard contract window logic applies here.
        if contract.trigger_type == "FLOWN":
            coupon_date = coupon.cpn_flown_date
        else:  # SALES or other
            coupon_date = coupon.cpn_sales_date
        
        if coupon_date < contract.start_date or coupon_date > contract.end_date:
            reason = (f"Date not in contract window: {coupon_date} outside "
                     f"{contract.start_date} to {contract.end_date}")
            return False, reason
        
        return True, None

    def _check_criterion_explicit(self, coupon: CouponData, rule_field: str, 
                                 in_values: List[Any], out_values: List[Any],
                                 criteria_type: str) -> Tuple[bool, Optional[str]]:
        """Check a specific rule field (already resolved)"""
        # Use FieldMapper to check
        mapping = self.field_mapper.get_mapping(rule_field)
        if not mapping:
            # If no mapping, we can't check it. 
            # If strict fail? User says "skip_and_allow" if null/unknown usually.
            self.logger.debug(f"No mapping for {rule_field}, skipping")
            return True, None
            
        contract_field = rule_field # Just for logging
        return self._check_criterion_logic(coupon, rule_field, contract_field, in_values, out_values)

    def _check_criterion(self, coupon: CouponData, contract_field: str, 
                        in_values: List[Any], out_values: List[Any],
                        criteria_type: str) -> Tuple[bool, Optional[str]]:
        """Generic criterion check from contract field"""
        rule_field = self._resolve_rule_field(contract_field)
        return self._check_criterion_logic(coupon, rule_field, contract_field, in_values, out_values)

    def _check_criterion_logic(self, coupon: CouponData, rule_field: str, log_field_name: str,
                              in_values: List[Any], out_values: List[Any]) -> Tuple[bool, Optional[str]]:
        """Core logic separated from resolution"""
        
        mapping = self.field_mapper.get_mapping(rule_field)
        if not mapping:
            self.logger.debug(f"No mapping found for {rule_field}, skipping")
            return True, None
            
        # [NEW] Check for ignoreCriteria flag - logic update
        if mapping.get('ignoreCriteria', False):
            self.logger.debug(f"{rule_field} check ignored by configuration (ignoreCriteria=True)")
            return True, None
            
        # [NEW] Normalize rule values (IN/OUT) to match input format (e.g. truncate IATA to 7 chars)
        # This fixes bugs where rule has 8 chars and input has 7 chars (or vice versa) and they don't match
        if in_values:
            in_values = self.field_mapper.normalize_rule_values(rule_field, in_values)
        if out_values:
            out_values = self.field_mapper.normalize_rule_values(rule_field, out_values)
            
        # 1. Get Value
        coupon_value = self.field_mapper.get_field_value(coupon, rule_field)
        
        # [NEW] Check for 'Unknown' value - if present, treat as wildcard match
        # This handles cases where input data has explicit 'Unknown' string which should pass filters
        is_unknown = False
        if isinstance(coupon_value, str) and coupon_value.strip().lower() == 'unknown':
            is_unknown = True
        elif isinstance(coupon_value, list):
            # If any value in the input list is Unknown, consider it a wildcard pass
            if any(isinstance(v, str) and v.strip().lower() == 'unknown' for v in coupon_value):
                is_unknown = True
                
        if is_unknown:
            return True, None
        
        # 2. Check Null/Unknown
        null_handling = mapping.get('nullHandling', 'skip_and_allow')
        null_result, null_reason = self.field_mapper.check_null_value(coupon_value, null_handling, log_field_name)
        if null_result is not None:
            return null_result, null_reason if not null_result else None
            
        # 3. Check Logic (Boolean vs List)
        data_type = mapping.get('dataType', 'string')
        
        if data_type == 'boolean':
            # Boolean logic
            return self._check_boolean_value(coupon_value, in_values, out_values, log_field_name)
        elif data_type == 'date':
             # Date logic (usually ranges)
             return self._check_date_range(coupon_value, in_values, out_values, log_field_name)
        else:
            # List/String logic with match mode
            match_mode = mapping.get('matchMode', 'any') # Default to 'any'
            return self._check_list_value(coupon_value, in_values, out_values, log_field_name, match_mode)

    def _check_boolean_value(self, coupon_value: Any, in_value: Any, out_value: Any, 
                            field_name: str) -> Tuple[bool, Optional[str]]:
        """Check boolean logic"""
        # Helper to convert to bool
        def to_bool(v):
            if isinstance(v, str): return v.lower() in ('true', '1', 'yes', 'y')
            return bool(v)

        c_bool = to_bool(coupon_value)

        # In/Out are usually lists from JSON, but might be scalar boolean in parsing
        # Try to handle list wrapper
        if isinstance(in_value, list) and in_value: in_value = in_value[0]
        if isinstance(out_value, list) and out_value: out_value = out_value[0]
        
        if in_value is not None and in_value != []:
             req = to_bool(in_value)
             if c_bool != req:
                 return False, f"{field_name} {c_bool} != {req}"
        
        if out_value is not None and out_value != []:
             exc = to_bool(out_value)
             if c_bool == exc:
                 return False, f"{field_name} {c_bool} is excluded"
                 
        return True, None

    def _check_list_value(self, coupon_value: Any, in_values: List[Any], out_values: List[Any],
                         field_name: str, match_mode: str) -> Tuple[bool, Optional[str]]:
        """Check list match logic"""
        
        # Special case: Dates passed as list criteria? Handled in _check_date_range usually.
        # But if string matching:
        
        if out_values:
            # For exclusion, if 'arrayMatchMode' is 'any' (default), and ANY input matches exclusion list, FAIL.
            # Usually exclusions mean "If my value is in this list, I am bad".
            if self.field_mapper.check_array_match(coupon_value, out_values, 'any'): 
                 return False, f"{field_name} value {coupon_value} excluded"

        if in_values:
            # Parse special keywords
            normalized_in = self.field_mapper.normalize_to_list(in_values)
            if "ALL" in normalized_in or "ANY" in normalized_in:
                return True, None
                
            if not self.field_mapper.check_array_match(coupon_value, in_values, match_mode):
                 return False, f"{field_name} value {coupon_value} not in eligible list"
                 
        return True, None

    def _check_date_range(self, coupon_date: date, in_values: Any, out_values: Any, field_name: str) -> Tuple[bool, Optional[str]]:
        """Check date range logic"""
        # Expecting in_values to be a dict or list of dicts with start/end
        # or list of single dates?
        # Standard format in this codebase seems to be dict with 'start', 'end' keys
        
        ranges = []
        if isinstance(in_values, dict):
            ranges.append(in_values)
        elif isinstance(in_values, list):
            for v in in_values:
                if isinstance(v, dict): ranges.append(v)
        
        if not ranges:
            return True, None # No range specified
            
        # If multiple ranges, typically OR logic? 
        # Check if date fits in ANY of the ranges
        in_any = False
        for rng in ranges:
            start = rng.get('start')
            end = rng.get('end')
            
            valid_start = True
            valid_end = True
            
            if start:
                 dt = datetime.strptime(start, '%Y-%m-%d').date()
                 if coupon_date < dt: valid_start = False
            
            if end:
                 dt = datetime.strptime(end, '%Y-%m-%d').date()
                 if coupon_date > dt: valid_end = False
            
            if valid_start and valid_end:
                in_any = True
                break
        
        if not in_any:
            return False, f"{field_name} {coupon_date} not in specified date ranges"
            
        return True, None
