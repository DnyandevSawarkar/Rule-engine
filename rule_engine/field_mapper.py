"""
Field mapping utilities for the rule engine

This module provides the FieldMapper class which handles:
- Loading field mapping configuration from JSON
- Extracting field values from coupon data
- Applying normalization functions
- Handling composite fields (e.g., route = origin + destination)
- Managing null/unknown values
- Supporting array vs scalar handling
"""

import json
import os
import re
from typing import Any, Dict, List, Optional, Union
from loguru import logger

from .models import CouponData


class FieldMapper:
    """
    Handles field mapping and value extraction based on configuration
    """
    
    def __init__(self, mapping_config_path: Optional[str] = None):
        """
        Initialize the field mapper
        
        Args:
            mapping_config_path: Path to the field mapping JSON configuration.
                                If None, uses default path in rule_engine directory.
        """
        self.logger = logger  # Initialize logger first
        
        if mapping_config_path is None:
            # Check for local field_mapping.json first (User preference)
            local_mapping = os.path.join(
                os.path.dirname(__file__),
                'field_mapping.json'
            )
            if os.path.exists(local_mapping):
                mapping_config_path = local_mapping
            else:
                # Fallback to root mapping.json
                mapping_config_path = os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    'mapping.json'
                )
        
        self.config_path = mapping_config_path
        self.config = self._load_config()
        self.mappings = {m['ruleField']: m for m in self.config['mappings']}
    
    def _load_config(self) -> Dict:
        """Load the field mapping configuration from JSON"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.logger.info(f"Loaded field mapping configuration from {self.config_path}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Field mapping configuration not found: {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in field mapping configuration: {e}")
            raise
    
    def get_field_value(self, coupon: CouponData, rule_field: str) -> Optional[Any]:
        """
        Get the value of a field from coupon data, handling multiple sources
        """
        if rule_field not in self.mappings:
            self.logger.warning(f"Rule field '{rule_field}' not found in mapping configuration")
            return None
        
        mapping = self.mappings[rule_field]
        
        # Collect values from all paths (primary + alternative)
        collected_values = []
        
        # 1. Primary Input Path
        primary_val = self._resolve_path_value(coupon, mapping, mapping['inputPath'])
        if primary_val is not None and primary_val != "":
             if isinstance(primary_val, list):
                 collected_values.extend(primary_val)
             else:
                 collected_values.append(primary_val)
                 
        # 2. Alternative Paths (New Feature)
        # Check for enable flag (default to True if alternativePaths exists)
        if 'alternativePaths' in mapping and mapping.get('enableAlternativePaths', True):
            for alt_path in mapping['alternativePaths']:
                alt_val = self._resolve_path_value(coupon, mapping, alt_path)
                if alt_val is not None and alt_val != "":
                    if isinstance(alt_val, list):
                        collected_values.extend(alt_val)
                    else:
                        collected_values.append(str(alt_val))
        
        # 3. Fallback (only if we still have nothing)
        if not collected_values and 'fallbackPath' in mapping:
             fallback_val = self._get_value_from_path(coupon, mapping['fallbackPath'])
             if fallback_val is not None and fallback_val != "":
                 collected_values.append(fallback_val)

        # Handle missing field
        if not collected_values:
            if mapping.get('skipIfMissing', True):
                self.logger.debug(f"Field {rule_field} not found in coupon data, skipping")
                return None
            return None # Or default?

        # Apply normalization to all collected values
        normalize_funcs = mapping.get('normalize', [])
        normalized_values = []
        for v in collected_values:
            norm_v = self._apply_normalization(v, normalize_funcs)
            if isinstance(norm_v, list):
                normalized_values.extend(norm_v)
            else:
                normalized_values.append(norm_v)
        
        # If the original field expects a scalar but we have multiple values (e.g. from array merge),
        # return list. The EligibilityChecker _check_criterion logic (via _check_list_value)
        # handles lists transparently if allowArrayOrScalar is true or implicitly via "any" logic.
        # However, for strictly scalar fields, we might need to take the first one?
        # User request specifies "if any value matches", so returning a List is correct.
        
        if len(normalized_values) == 1 and not mapping.get('forceList', False):
            return normalized_values[0]
            
        return normalized_values

    def normalize_rule_values(self, rule_field: str, values: List[Any]) -> List[Any]:
        """
        Normalize values from a rule definition using the field's configuration
        
        Args:
            rule_field: Rule field name
            values: List of values from the rule (in_values or out_values)
            
        Returns:
            List of normalized values
        """
        if rule_field not in self.mappings:
            return values
            
        mapping = self.mappings[rule_field]
        normalize_funcs = mapping.get('normalize', [])
        
        if not normalize_funcs:
            return values
            
        normalized = []
        for v in values:
            norm_v = self._apply_normalization(v, normalize_funcs)
            if isinstance(norm_v, list):
                normalized.extend(norm_v)
            else:
                normalized.append(norm_v)
                
        return normalized

    def _resolve_path_value(self, coupon, mapping, path_config):
        # Handle composite vs simple path
        if isinstance(path_config, list):
             return self._get_composite_value(coupon, mapping, override_path=path_config)
        return self._get_value_from_path(coupon, path_config)
    
    def _get_value_from_path(self, coupon: CouponData, path: str) -> Optional[Any]:
        """
        Get value from coupon using a field path
        
        Args:
            coupon: Coupon data object
            path: Field path (e.g., 'cpn_origin', 'marketing_airline')
        
        Returns:
            Field value or None if not found
        """
        try:
            return getattr(coupon, path, None)
        except Exception as e:
            self.logger.debug(f"Error getting value from path '{path}': {e}")
            return None
    
    def _get_composite_value(self, coupon: CouponData, mapping: Dict, override_path: Optional[List[str]] = None) -> Optional[str]:
        """
        Build a composite value from multiple input paths
        
        Args:
            coupon: Coupon data object
            mapping: Field mapping configuration
            override_path: Optional path list to use instead of mapping default
        
        Returns:
            Composite value (e.g., "CAI-DOH" from origin and destination)
        """
        input_paths = override_path if override_path else mapping['inputPath']
        values = []
        
        for path in input_paths:
            value = self._get_value_from_path(coupon, path)
            if value is None or value == "":
                # If any component is missing, composite value is None
                return None
            values.append(str(value))
        
        # Apply composite format if specified
        if 'compositeFormat' in mapping:
            format_str = mapping['compositeFormat']
            try:
                return format_str.format(*values)
            except Exception as e:
                self.logger.error(f"Error formatting composite value: {e}")
                return None
        else:
            # Default: join with hyphen
            return "-".join(values)
    
    def _apply_normalization(self, value: Any, normalize_funcs: List[str]) -> Any:
        """
        Apply normalization functions to a value
        
        Args:
            value: Value to normalize
            normalize_funcs: List of normalization function names
        
        Returns:
            Normalized value
        """
        if value is None:
            return None
        
        # Handle arrays
        if isinstance(value, list):
            return [self._apply_normalization(v, normalize_funcs) for v in value]
        
        # Convert to string for normalization
        result = str(value) if not isinstance(value, str) else value
        
        # Apply each normalization function in order
        for func_name in normalize_funcs:
            result = self._apply_single_normalization(result, func_name)
        
        return result
    
    def _apply_single_normalization(self, value: str, func_name: str) -> str:
        """
        Apply a single normalization function
        
        Args:
            value: String value to normalize
            func_name: Name of the normalization function
        
        Returns:
            Normalized string
        """
        if func_name == 'trim':
            return value.strip()
        elif func_name == 'upper':
            return value.upper()
        elif func_name == 'lower':
            return value.lower()
        elif func_name == 'stripAirlinePrefix':
            return self._strip_airline_prefix(value)
        elif func_name == 'trimLeadingZeros':
            return value.lstrip('0') or '0'  # Keep at least one zero
        elif func_name == 'removeSpaces':
            return value.replace(' ', '')
        elif func_name == 'first7':
            return value[:7]
        elif func_name == 'generateODPairs':
            return self._generate_od_pairs(value)
        else:
            self.logger.warning(f"Unknown normalization function: {func_name}")
            return value
    
    def _strip_airline_prefix(self, flight_number: str) -> str:
        """
        Normalize flight number for comparison (bidirectional matching)
        
        This function enables matching whether the rule or input has airline prefix:
        - Rule: QR1234, Input: 1234 → Both normalize to 1234 → Match ✓
        - Rule: 1234, Input: QR1234 → Both normalize to 1234 → Match ✓
        - Rule: QR1234, Input: QR1234 → Both normalize to 1234 → Match ✓
        - Rule: 1234, Input: 1234 → Both normalize to 1234 → Match ✓
        
        Args:
            flight_number: Flight number (with or without prefix)
        
        Returns:
            Flight number without airline prefix (numeric part only)
        """
        pattern = r'^[A-Z]{2}(\d+)$'
        match = re.match(pattern, flight_number.strip())
        if match:
            # Has prefix like QR1234 → return 1234
            return match.group(1)
        # No prefix, already numeric like 1234 → return as is
        return flight_number.strip()
    
    def _generate_od_pairs(self, itinerary: str) -> List[str]:
        """
        Generate O&D pairs from an itinerary string
        
        Args:
            itinerary: String like "CAI-DOH-CGK-DOH-CAI"
            
        Returns:
            List of pairs: ["CAI-DOH", "DOH-CGK", "CGK-DOH", "DOH-CAI"]
        """
        if not itinerary or '-' not in itinerary:
            return [itinerary] if itinerary else []
            
        segments = [s.strip() for s in itinerary.split('-') if s.strip()]
        if len(segments) < 2:
            return [itinerary]
            
        pairs = []
        for i in range(len(segments) - 1):
            pairs.append(f"{segments[i]}-{segments[i+1]}")
            
        return pairs
    
    def normalize_flight_number_for_comparison(self, flight_number: str, airline_code: Optional[str] = None) -> List[str]:
        """
        Generate all possible normalized forms of a flight number for comparison
        
        This allows matching between:
        - "1234" ↔ "QR1234" 
        - "QR1234" ↔ "1234"
        
        Args:
            flight_number: Flight number (with or without prefix)
            airline_code: Optional airline code to add prefix (e.g., "QR")
        
        Returns:
            List of normalized forms: [numeric_only, with_prefix] if airline_code provided,
            otherwise just [numeric_only]
        """
        # Get numeric-only version
        numeric = self._strip_airline_prefix(flight_number)
        
        # Generate both forms for comparison
        forms = [numeric]
        
        # If airline code provided, also include prefixed version
        if airline_code:
            prefixed = f"{airline_code.upper()}{numeric}"
            forms.append(prefixed)
        
        return forms
    
    def normalize_to_list(self, value: Any) -> List[Any]:
        """
        Normalize a value to a list for uniform processing
        
        Args:
            value: Scalar or list value
        
        Returns:
            List representation of the value
        """
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
    
    def check_null_value(self, value: Any, null_handling: str, field_name: str) -> tuple[Optional[bool], Optional[str]]:
        """
        Handle null/unknown values according to configuration
        
        Args:
            value: Value to check
            null_handling: 'skip_and_allow' or 'strict_fail'
            field_name: Name of the field for logging
        
        Returns:
            Tuple of (pass_result, reason) or (None, None) if not a null value
        """
        if value is None or value == "" or value == "UNKNOWN":
            if null_handling == "skip_and_allow":
                self.logger.debug(f"{field_name} is null/empty - skipping (allow)")
                return True, "skipped"
            else:  # strict_fail
                self.logger.debug(f"{field_name} is null/empty - strict fail")
                return False, f"{field_name} is null or unknown"
        
        return None, None
    
    def check_array_match(self, input_values: Any, rule_values: List[Any], match_mode: str) -> bool:
        """
        Check if input values match rule values according to match mode
        
        Args:
            input_values: Input value(s) - can be scalar or list
            rule_values: Rule value(s) to match against
            match_mode: 'any', 'all', or 'none'
        
        Returns:
            True if match succeeds according to mode
        """
        input_list = self.normalize_to_list(input_values)
        rule_list = self.normalize_to_list(rule_values)
        
        if not input_list:
            return True  # Empty input matches everything (no restriction)
        
        if not rule_list:
            return True  # No rule restrictions
        
        if match_mode == 'any':
            # Pass if any input value matches any rule value
            return any(iv in rule_list for iv in input_list)
        elif match_mode == 'all':
            # Pass if all input values match at least one rule value
            return all(iv in rule_list for iv in input_list)
        elif match_mode == 'none':
            # Pass if no input values match any rule value
            return not any(iv in rule_list for iv in input_list)
        else:
            self.logger.warning(f"Unknown match mode: {match_mode}")
            return False
    
    def get_mapping(self, rule_field: str) -> Optional[Dict]:
        """
        Get the mapping configuration for a rule field
        
        Args:
            rule_field: Rule field name
        
        Returns:
            Mapping dictionary or None if not found
        """
        return self.mappings.get(rule_field)
    
    def get_criteria_group(self, rule_field: str) -> Optional[str]:
        """
        Get the criteria group for a rule field
        
        Args:
            rule_field: Rule field name
        
        Returns:
            Criteria group name or None if not found
        """
        mapping = self.get_mapping(rule_field)
        return mapping['criteriaGroup'] if mapping else None
