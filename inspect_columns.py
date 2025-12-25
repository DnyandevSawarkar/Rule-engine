import pandas as pd

def check_columns(input_file):
    try:
        df = pd.read_csv(input_file)
        print("Columns found:")
        for col in df.columns:
            print(col)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_columns("mir_output.csv")
