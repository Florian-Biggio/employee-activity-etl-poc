import pandas as pd
import pyarrow.parquet as pq
import os
import glob
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def read_delta_as_parquet():
    """Read Delta table by reading the underlying Parquet files directly"""
    try:
        # Path to your Delta table directory
        delta_dir = "output/delta/employee_activities_enriched"
        
        logging.info(f"Reading Delta table from: {delta_dir}")
        
        # Check if directory exists
        if not os.path.exists(delta_dir):
            logging.error(f"Directory does not exist: {delta_dir}")
            return None
        
        # Find all parquet files in the directory
        parquet_files = glob.glob(os.path.join(delta_dir, "*.parquet"))
        logging.info(f"Found {len(parquet_files)} Parquet files")
        
        if not parquet_files:
            logging.error("No Parquet files found in the directory")
            return None
        
        # Read all parquet files into a list of DataFrames
        dfs = []
        for file_path in parquet_files:
            try:
                logging.info(f"Reading file: {os.path.basename(file_path)}")
                table = pq.read_table(file_path)
                df = table.to_pandas()
                dfs.append(df)
                logging.info(f"Successfully read {len(df)} records from {os.path.basename(file_path)}")
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")
                continue
        
        if not dfs:
            logging.error("No DataFrames were successfully read")
            return None
        
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        
        logging.info(f"Success! Combined DataFrame shape: {combined_df.shape}")
        
        # Display basic info
        print(f"\n=== DataFrame Info ===")
        print(f"Shape: {combined_df.shape}")
        print(f"Columns: {list(combined_df.columns)}")
        print(f"Total records: {len(combined_df)}")
        
        if not combined_df.empty:
            print(f"\n=== First 5 records ===")
            print(combined_df.head())
            
            print(f"\n=== Basic Statistics ===")
            print(f"Unique IDs: {combined_df['ID'].nunique()}")
            if 'ID_salarie' in combined_df.columns:
                print(f"Unique employees: {combined_df['ID_salarie'].nunique()}")
            if 'Sport_type' in combined_df.columns:
                print(f"Sport types: {combined_df['Sport_type'].unique()}")
        
        return combined_df
        
    except Exception as e:
        logging.error(f"Error reading Delta table: {e}")
        return None

if __name__ == '__main__':
    df = read_delta_as_parquet()
    if df is not None:
        print(f"\nSuccessfully loaded {len(df)} records!")
        # You can now work with the pandas DataFrame
        # Example: df.to_csv('output.csv', index=False)
    else:
        print("Failed to read Delta table")
