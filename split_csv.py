import pandas as pd
import os
import math

def split_csv(input_file, output_folder, chunk_size=50000):
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: File '{input_file}' not found.")
        return

    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created directory: {output_folder}")

    print(f"Reading {input_file}...")
    
    # Get total rows for progress estimation (optional but helpful)
    # This might take a moment for huge files, strictly optional
    # num_lines = sum(1 for _ in open(input_file, encoding='utf-8')) - 1
    # print(f"Total rows approx: {num_lines}")

    # Read and save in chunks
    try:
        chunk_iterator = pd.read_csv(input_file, chunksize=chunk_size, low_memory=False)
        
        part_num = 1
        for chunk in chunk_iterator:
            # Clean newlines from string columns to prevent row splitting issues
            for col in chunk.select_dtypes(include=['object']):
                chunk[col] = chunk[col].astype(str).str.replace(r'[\r\n]+', ' ', regex=True)

            output_file = os.path.join(output_folder, f"mir_output_part_{part_num}.csv")
            chunk.to_csv(output_file, index=False)
            print(f"Saved {output_file} ({len(chunk)} rows)")
            part_num += 1
            
        print("Done! Split complete.")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Configuration
    INPUT_FILE = r"e:\ProDT\rule-engine\mir_output.csv"
    OUTPUT_FOLDER = r"e:\ProDT\rule-engine\MIR output"
    CHUNK_SIZE = 50000

    split_csv(INPUT_FILE, OUTPUT_FOLDER, CHUNK_SIZE)
