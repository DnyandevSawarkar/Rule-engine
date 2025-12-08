
import pandas as pd
import sys
import os
from rule_engine_integrated import PLBRuleEngine

def verify():
    input_file = "input_sample.csv"
    output_file = "output_actual.csv"
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file)
    
    print("Initializing Rule Engine...")
    engine = PLBRuleEngine(contracts_dir="contracts")
    
    print("Processing DataFrame...")
    try:
        result_df = engine.process_dataframe(df)
        
        print(f"Processing complete. Input rows: {len(df)}, Output rows: {len(result_df)}")
        
        result_df.to_csv(output_file, index=False)
        print(f"Output saved to {output_file}")
        
        # Basic verification
        required_cols = ['Sector_Airline_Eligibility', 'Contract_Status', 'Tier1_percent']
        missing = [col for col in required_cols if col not in result_df.columns]
        
        if missing:
            print(f"FAILED: Missing columns in output: {missing}")
        else:
            print("SUCCESS: Output contains expected columns.")
            
        # Check if row explosion happened (if applicable)
        # Note: If input sample has 1 row and matches 1 rule, output is 1. If matches 2, output is 2.
        # We can't know for sure without knowing the rules, but successful execution is key.
            
    except Exception as e:
        print(f"FAILED: Processing error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify()
