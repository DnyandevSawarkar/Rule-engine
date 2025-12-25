
from rule_engine.models import CouponData, ContractData
from rule_engine.field_mapper import FieldMapper
from rule_engine.eligibility_checker import EligibilityChecker
from datetime import date
from decimal import Decimal
import logging

# Configure logging to see debug output
logging.basicConfig(level=logging.DEBUG)

def verify_pos_fix():
    print("Verifying POS Field Mapping and Eligibility Logic...")

    # 1. Setup Coupon Data with pos_array
    # The user mentioned: "from 4 of 3 values are in eligible lis for pos in contract"
    # This implies pos_array has multiple values, e.g., ["EG", "SA", "AE"]
    # and contract includes some of them.
    
    pos_values = ["EG", "SA", "AE"]
    coupon = CouponData(
        cpn_airline_code="QR",
        pos_array=pos_values,
        # Required fields default
    )
    
    print(f"Created CouponData with pos_array: {coupon.pos_array}")

    # 2. Verify Field Mapping for 'pos'
    mapper = FieldMapper()
    # Force reload of config in case it was cached globally/singleton (though here we instantiate new)
    # The FieldMapper loads from field_mapping.json in __init__
    
    pos_extracted = mapper.get_field_value(coupon, "pos")
    print(f"Extracted 'pos' field value: {pos_extracted}")
    
    # Expectation: It should be a list ["EG", "SA", "AE"] (normalized/trimmed)
    if pos_extracted is None:
        print("❌ FAILED: extracted pos is None. Field mapping might still be broken.")
        return False
    
    # Check content (normalization might apply, e.g. upper/trim)
    expected_pos = ["EG", "SA", "AE"]
    
    # If mapper returns a single string for list input (if allowArrayOrScalar is false), verify that behavior
    # In field_mapping.json for "pos":
    # "ruleField": "pos",
    # "inputPath": "pos_array", <--- This was the fix
    # "allowArrayOrScalar": false, <--- This suggests it expects a single value? 
    # But wait, inputPath "pos_array" in CouponData is a List[str].
    # And "normalize" includes "trim", "upper".
    
    # Let's see what FieldMapper does when input is a list but allowArrayOrScalar is False.
    # In FieldMapper.get_field_value:
    # 1. Gets value (List)
    # 2. Normalizes (returns List of normalized values)
    # 3. "if len(normalized_values) == 1 and not mapping.get('forceList', False): return normalized_values[0]"
    # 4. else returns list.
    
    # So if there are multiple values, it returns a list.
    
    if sorted(pos_extracted) != sorted(expected_pos):
        print(f"❌ FAILED: Expected {expected_pos}, got {pos_extracted}")
        # If it returned None earlier, we caught it. 
        # If it returned empty list, that's also wrong.
    else:
        print("✅ SUCCESS: POS field extraction works correctly.")

    # 3. Verify Eligibility Checker Logic
    print("\nChecking Eligibility Logic...")
    checker = EligibilityChecker()
    
    # Case A: Contract allows "EG" (IN list)
    # "pos": ["EG"] in contract means only EG is allowed.
    # If coupon has ["EG", "SA"], is it eligible?
    # Field mapping says "arrayMatchMode": "any" for 'pos' field.
    # HOWEVER, Eligibility Checker logic in local file `eligibility_checker.py` uses 
    # `_check_list_criteria` which doesn't seem to use `arrayMatchMode` from field_mapping.json directly?
    
    # Let's check `check_geographic_eligibility` -> `_check_list_criteria`
    # It calls `_check_list_criteria(coupon.pcc, in_criteria.get('POS'), ...)` 
    # WAIT! `check_geographic_eligibility` uses `coupon.pcc` for POS check?
    # Line 164 in eligibility_checker.py: 
    # `if not self._check_list_criteria(coupon.pcc, in_criteria.get('POS', []), out_criteria.get('POS', []), 'POS'):`
    
    # CRITICAL FINDING: It uses `coupon.pcc` NOT the mapped `pos` value from logic!
    # The `EligibilityChecker` might be bypassing the `FieldMapper` entirely and reading directly from `CouponData` attributes.
    # If `CouponData` has a `pos` field, it should use that. But `CouponData` model has `pos_array`.
    
    # Let's look at `eligibility_checker.py` again. 
    # Row 164: `coupon.pcc` used for 'POS' check. 
    # `coupon.pcc` is "Psuedo City Code" usually, but here it seems mapped to POS?
    
    # FieldMapping.json says:
    # "ruleField": "pos",
    # "inputPath": "pos_array",
    # "alternativePaths": ["pcc"]
    
    # So Field Mapper logic maps `pos` (abstract concept) from `pos_array` OR `pcc`.
    # BUT `EligibilityChecker` seems to hardcode direct attribute access in `check_geographic_eligibility`.
    
    # If `EligibilityChecker` class (in `rule_engine/eligibility_checker.py`) is used by `PLBRuleEngine` (in `rule_engine_integrated.py`),
    # we need to know HOW `PLBRuleEngine` invokes checks.
    
    # If `PLBRuleEngine` uses `FieldMapper` to populate a "virtual" coupon or dict before passing to `EligibilityChecker`, 
    # then `EligibilityChecker` receiving `coupon` object needs those fields.
    
    # Check `rule_engine_integrated.py`:
    # `process_single_coupon` method.
    
    # I suspect `EligibilityChecker` is reading attributes directly and `field_mapping.json` is partially ignored or used differently.
    
    pass

if __name__ == "__main__":
    verify_pos_fix()
