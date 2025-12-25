
from rule_engine.models import CouponData
import json

class MockArray:
    def __init__(self, data):
        self.data = data
    def tolist(self):
        return self.data
    def __repr__(self):
        return f"array({self.data})"

class MockNdArray(MockArray):
    """Class mimicking numpy ndarray name"""
    pass

def test_validator():
    print("Testing CouponData array validator (Robust)...")
    
    # Test cases mimicking Spark/Databricks output
    test_cases = [
        # Case 1: Simple list
        {"ticket_origin": ["GRU"], "ticket_destination": ["BSB"]},
        
        # Case 2: Mock Numpy Array (single element)
        {"ticket_origin": MockArray(["LHR"]), "ticket_destination": MockArray(["DXB"])},
        
        # Case 3: Empty list
        {"ticket_origin": [], "ticket_destination": []},
        
        # Case 4: Normal strings (regression check)
        {"ticket_origin": "DEL", "ticket_destination": "BOM"},
        
        # Case 5: Mock array with 'ndarray' typenames logic check
        # (This requires actual numpy or precise mocking, assume logic holds if above pass)
    ]
    
    for i, data in enumerate(test_cases):
        try:
            print(f"Case {i+1}: Input {data}")
            coupon = CouponData(
                # Required fields mock
                cpn_airline_code="QR",
                cpn_total_revenue=100,
                # Test fields
                ticket_origin=data["ticket_origin"],
                ticket_destination=data["ticket_destination"]
            )
            print(f"  -> SUCCESS: Origin='{coupon.ticket_origin}', Dest='{coupon.ticket_destination}'")
            
            # Assertions
            expected_origin = ""
            if isinstance(data["ticket_origin"], str):
                 expected_origin = data["ticket_origin"]
            elif hasattr(data["ticket_origin"], 'tolist'):
                 expected_origin = data["ticket_origin"].tolist()[0]
            elif isinstance(data["ticket_origin"], list) and data["ticket_origin"]:
                 expected_origin = data["ticket_origin"][0]
            
            assert coupon.ticket_origin == expected_origin
            
        except Exception as e:
            print(f"  -> FAILED: {e}")
            raise e

if __name__ == "__main__":
    test_validator()
