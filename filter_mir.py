import pandas as pd
import sys

def filter_csv(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        print(f"Columns: {list(df.columns)}")
        
        target_docs = [
            "Emirates EG Apr25-Mar26.pdf",
            "QR EG Apr-Sep25 PLB.pdf",
            "Air Cairo EG Jan-Dec25 PLB.pdf"
        ]
        
        # Check if columns exist
        if 'Contract_Document_Name' not in df.columns:
            print("Error: 'Contract_Document_Name' column not found.")
            return
        if 'Sector_Airline_Eligibility' not in df.columns:
            print("Error: 'Sector_Airline_Eligibility' column not found.")
            return

        # Filter
        # Normalize eligibility to boolean if it's string
        if df['Sector_Airline_Eligibility'].dtype == 'object':
             # Handle "True", "true", "TRUE", etc.
             df['Sector_Airline_Eligibility'] = df['Sector_Airline_Eligibility'].astype(str).str.lower() == 'true'
        
        filtered_df = df[
            (df['Contract_Document_Name'].isin(target_docs)) &
            (df['Sector_Airline_Eligibility'] == True)
        ]
        
        print(f"Filtered count: {len(filtered_df)}")
        
        if len(filtered_df) == 150:
            print("SUCCESS: Count matches expected 150.")
        else:
            print(f"WARNING: Count {len(filtered_df)} does not match expected 150.")
            
        filtered_df.to_csv(output_file, index=False)
        print(f"Saved to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    filter_csv("mir_output.csv", "mir_output_filtered.csv")
