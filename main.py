
import pandas as pd
import os
import time
from rule_engine_integrated import PLBRuleEngine

def main():
    input_file = r"input/inc.csv"
    output_file = r"output/inc.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        return

    print(f"Loading data from {input_file}...")
    start_time = time.time()
    try:
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} rows.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Initialize Rule Engine
    print("Initializing Rule Engine...")
    try:
        engine = PLBRuleEngine(contracts_dir="contracts")
    except Exception as e:
        print(f"Error initializing engine: {e}")
        return

    # Process DataFrame
    print("Processing DataFrame...")
    try:
        processing_start = time.time()
        result_df = engine.process_dataframe(df)
        processing_time = time.time() - processing_start
        print(f"Processing complete in {processing_time:.2f} seconds.")
        print(f"Output contains {len(result_df)} rows.")
    except Exception as e:
        print(f"Error during processing: {e}")
        return

    # Save Output
    print(f"Saving output to {output_file}...")
    try:
        result_df.to_csv(output_file, index=False)
        print("Successfully saved output CSV.")
    except Exception as e:
        print(f"Error saving output: {e}")

    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
