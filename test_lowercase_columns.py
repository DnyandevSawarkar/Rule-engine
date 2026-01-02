"""
Quick test to verify output columns are lowercase while input columns remain unchanged
"""

import pandas as pd
from rule_engine_integrated import PLBRuleEngine

# Create a simple test DataFrame with mixed case input columns
test_data = pd.DataFrame({
    'Ticket_Number': ['1234567890'],
    'cpn_airline_code': ['QR'],
    'cpn_sales_date': ['2025-04-15'],
    'cpn_revenue_base': [1000.0],
    'CPN_REVENUE_YQ': [200.0],  # Uppercase input column
    'Flight_Number': ['QR123']
})

print("Input DataFrame columns:")
print(test_data.columns.tolist())
print()

# Initialize engine
engine = PLBRuleEngine(contracts_dir="contracts")

# Process DataFrame
try:
    result_df = engine.process_dataframe(test_data)
    
    print("Output DataFrame columns:")
    print(result_df.columns.tolist())
    print()
    
    # Check that input columns remain unchanged
    input_cols = test_data.columns.tolist()
    output_cols = result_df.columns.tolist()
    
    print("Verification:")
    all_input_preserved = all(col in output_cols for col in input_cols)
    print(f"  All input columns preserved: {all_input_preserved}")
    
    # Check that output columns (not in input) are lowercase
    output_only_cols = [col for col in output_cols if col not in input_cols]
    all_lowercase = all(col == col.lower() for col in output_only_cols)
    print(f"  All output-only columns are lowercase: {all_lowercase}")
    
    if all_input_preserved and all_lowercase:
        print("\n[PASS] All checks passed!")
    else:
        print("\n[FAIL] Some checks failed")
        if not all_input_preserved:
            missing = [col for col in input_cols if col not in output_cols]
            print(f"  Missing input columns: {missing}")
        if not all_lowercase:
            uppercase_output = [col for col in output_only_cols if col != col.lower()]
            print(f"  Uppercase output columns: {uppercase_output}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()


