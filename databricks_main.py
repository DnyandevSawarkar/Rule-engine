import pandas as pd
import os
import time
from rule_engine_integrated import PLBRuleEngine

df = spark.table("megatron.silver.silver_view").limit(2000)
display(df.limit(5))

from pyspark.sql.functions import lit

df = df.withColumn("batch_id", lit(1))
display(df.limit(5))

def main():
    output_file = "processed_output.csv"

    start_time = time.time()
    
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
        
        # Convert Spark DataFrame to Pandas DataFrame
        print("Converting Spark DataFrame to Pandas...")
        df_pandas = df.toPandas()
        print(f"Converted {len(df_pandas)} rows.")
        
        result_df = engine.process_dataframe(df_pandas) 
        processing_time = time.time() - processing_start
        print(f"Processing complete in {processing_time:.2f} seconds.")
        # Use len() for Pandas DataFrame
        print(f"Output contains {len(result_df)} rows.")
    except Exception as e:
        print(f"Error during processing: {e}")
        return

    # Save Output
    print(f"Saving output to {output_file}...")
    try:
        # Result is already Pandas DataFrame
        result_df.to_csv(output_file, index=False)
        print("Successfully saved output CSV.")
    except Exception as e:
        print(f"Error saving output: {e}")

    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()