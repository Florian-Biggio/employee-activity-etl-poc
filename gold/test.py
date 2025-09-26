import pandas as pd
import logging
import os
from deltalake import DeltaTable
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('gold_processing.log')
    ]
)

def check_delta_table_exists(delta_path):
    """Check if Delta table exists and has data"""
    try:
        if not os.path.exists(delta_path):
            logging.warning(f"Delta table path does not exist: {delta_path}")
            return False
        
        # Check if _delta_log directory exists (essential for Delta tables)
        delta_log_path = os.path.join(delta_path, "_delta_log")
        if not os.path.exists(delta_log_path):
            logging.warning(f"Delta log directory not found: {delta_log_path}")
            return False
            
        # Try to read the table
        delta_table = DeltaTable(delta_path)
        df = delta_table.to_pandas()
        
        if len(df) == 0:
            logging.warning("Delta table exists but contains no data")
            return False
            
        return True
        
    except Exception as e:
        logging.warning(f"Delta table check failed: {e}")
        return False

def process_gold_layer():
    """Process bronze Delta data into gold layer by merging with XLSX reference data"""
    
    # Path configuration - FIXED: using correct XLSX path
    bronze_delta_path = "../bronze/output/delta/employee_activities"
    gold_output_path = "../gold/employee_activities_enriched.csv"
    xlsx1_reference_path = '../data/DonneesSportive.xlsx'
    xlsx2_reference_path = '../data/DonneesRH.xlsx'
    
    try:
        logging.info("Starting gold layer processing...")
        logging.info(f"Reading from Delta: {bronze_delta_path}")
        logging.info(f"Reference XLSX1: {xlsx1_reference_path}")
        logging.info(f"Reference XLSX2: {xlsx2_reference_path}")
        logging.info(f"Output CSV: {gold_output_path}")
        
        # Check if Delta table exists first
        if not check_delta_table_exists(bronze_delta_path):
            logging.error("Delta table not found or empty. Please run the bronze consumer first!")
            logging.info("To create the Delta table, run:")
            logging.info("cd bronze && python your_consumer_script.py")
            return False
        
        # Step 1: Read Delta Lake table
        logging.info("Reading Delta Lake table...")
        delta_table = DeltaTable(bronze_delta_path)
        df_delta = delta_table.to_pandas()
        
        logging.info(f"Read {len(df_delta)} records from Delta Lake")
        logging.info(f"Delta columns: {list(df_delta.columns)}")
        logging.info(f"First few records:\n{df_delta.head()}")
        
        # Convert timestamp columns back to datetime if needed
        timestamp_columns = ['Date_de_debut', 'Date_de_fin', 'processing_timestamp']
        for col in timestamp_columns:
            if col in df_delta.columns:
                if df_delta[col].dtype == 'object':
                    try:
                        df_delta[col] = pd.to_datetime(df_delta[col])
                        logging.info(f"Converted {col} to datetime")
                    except Exception as e:
                        logging.warning(f"Could not convert {col} to datetime: {e}")
        
        # Step 2: Read XLSX reference data
        logging.info("Reading first XLSX reference data...")
        try:
            # Read Excel file
            df_xlsx1 = pd.read_excel(xlsx1_reference_path)
            logging.info(f"Successfully read XLSX from {xlsx1_reference_path}")
            
        except Exception as e:
            logging.error(f"Error reading XLSX file {xlsx1_reference_path}: {e}")
            logging.info("Checking if file exists...")
            if not os.path.exists(xlsx1_reference_path):
                logging.error(f"XLSX file not found at: {xlsx1_reference_path}")
            else:
                logging.error("File exists but could not be read. Check file format.")
            return False
        
        logging.info(f"Read {len(df_xlsx1)} records from XLSX")
        logging.info(f"XLSX columns: {list(df_xlsx1.columns)}")
        logging.info(f"XLSX first few records:\n{df_xlsx1.head()}")

        logging.info("Reading second XLSX reference data...")
        try:
            # Read Excel file
            df_xlsx2 = pd.read_excel(xlsx2_reference_path)
            logging.info(f"Successfully read XLSX from {xlsx2_reference_path}")
            
        except Exception as e:
            logging.error(f"Error reading XLSX file {xlsx2_reference_path}: {e}")
            logging.info("Checking if file exists...")
            if not os.path.exists(xlsx2_reference_path):
                logging.error(f"XLSX file not found at: {xlsx2_reference_path}")
            else:
                logging.error("File exists but could not be read. Check file format.")
            return False

        logging.info(f"Read {len(df_xlsx2)} records from XLSX")
        logging.info(f"XLSX columns: {list(df_xlsx2.columns)}")
        logging.info(f"XLSX first few records:\n{df_xlsx2.head()}")
        
        # Step 3: Identify the ID column for merging
        delta_id_column = 'ID_salarie'
        xlsx_id_column = 'ID salarié'
        
        if delta_id_column not in df_delta.columns:
            logging.error(f"ID column '{delta_id_column}' not found in Delta data")
            logging.info(f"Available Delta columns: {list(df_delta.columns)}")
            # Try to find alternative ID columns
            for col in df_delta.columns:
                if 'id' in col.lower() or 'ID' in col:
                    logging.info(f"Possible alternative ID column: {col}")
            return False
            
        if xlsx_id_column not in df_xlsx1.columns:
            logging.error(f"ID column '{xlsx_id_column}' not found in XLSX1 data")
            logging.info(f"Available XLSX columns: {list(df_xlsx1.columns)}")
            # Try to find alternative ID columns
            for col in df_xlsx1.columns:
                if 'id' in col.lower() or 'ID' in col:
                    logging.info(f"Possible alternative ID column: {col}")
            return False

        if xlsx_id_column not in df_xlsx2.columns:
            logging.error(f"ID column '{xlsx_id_column}' not found in XLSX2 data")
            logging.info(f"Available XLSX columns: {list(df_xlsx2.columns)}")
            # Try to find alternative ID columns
            for col in df_xlsx2.columns:
                if 'id' in col.lower() or 'ID' in col:
                    logging.info(f"Possible alternative ID column: {col}")
            return False
        
        # Step 4: Merge the datasets
        logging.info("Merging Delta data with XLSX1 reference...")
        
        # Left join to keep all Delta records, even if no match in XLSX
        merged_df = pd.merge(
            df_delta,
            df_xlsx1,
            left_on=delta_id_column,
            right_on=xlsx_id_column,
            how='left',
            suffixes=('_delta', '_xlsx')
        )

        logging.info("Merging Delta data with XLSX2 reference...")
        
        # Left join to keep all Delta records, even if no match in XLSX
        merged_df2 = pd.merge(
            merged_df,
            df_xlsx2,
            left_on=delta_id_column,
            right_on=xlsx_id_column,
            how='left',
            suffixes=('_delta', '_xlsx')
        )
        
        logging.info(f"Merged dataset has {len(merged_df2)} records")
        
        # Step 5: Add processing metadata
        merged_df2['gold_processing_timestamp'] = datetime.now()
        merged_df2['gold_processing_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Step 6: Write to CSV in gold folder
        logging.info(f"Writing enriched data to {gold_output_path}...")
        
        # Ensure gold directory exists
        os.makedirs(os.path.dirname(gold_output_path), exist_ok=True)
        
        # Write to CSV
        merged_df2.to_csv(gold_output_path, index=False)
        
        logging.info(f"Successfully wrote {len(merged_df2)} records to gold layer")
        logging.info(f"Output file: {gold_output_path}")
        
        # Step 7: Show some statistics
        logging.info("\n=== Processing Statistics ===")
        logging.info(f"Total Delta records: {len(df_delta)}")
        logging.info(f"Total XLSX1 reference records: {len(df_xlsx1)}")
        logging.info(f"Total XLSX2 reference records: {len(df_xlsx2)}")
        logging.info(f"Total merged records: {len(merged_df2)}")
        
        # Check for unmatched records
        unmatched = merged_df2[xlsx_id_column].isna().sum()
        if unmatched > 0:
            logging.info(f"Records without XLSX match: {unmatched}")
        
        # Show column information
        logging.info(f"Merged dataset columns: {list(merged_df2.columns)}")
        
        # Show sample of merged data
        logging.info(f"Sample of merged data:\n{merged_df2.head()}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error in gold layer processing: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def main():
    """Main function with argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Gold Layer Processing: Merge Delta data with XLSX')
    
    parser.add_argument('--delta-path', type=str, default='../bronze/output/delta/employee_activities',
                       help='Path to Delta table (default: ../bronze/output/delta/employee_activities)')
    
    parser.add_argument('--xlsx1-path', type=str, default='../data/DonneesSportive.xlsx',
                       help='Path to XLSX reference file (default: ../data/DonneesSportive.xlsx)')
    
    parser.add_argument('--xlsx2-path', type=str, default='../data/DonneesRH.xlsx',
                       help='Path to XLSX reference file (default: ../data/DonneesRH.xlsx)')
    
    parser.add_argument('--output-path', type=str, default='../gold/employee_activities_enriched.csv',
                       help='Output CSV path (default: ../gold/employee_activities_enriched.csv)')
    
    parser.add_argument('--delta-id-column', type=str, default='ID_salarie',
                       help='ID column name in Delta data (default: ID_salarie)')
    
    parser.add_argument('--xlsx-id-column', type=str, default='ID salarié',
                       help='ID column name in XLSX data (default: ID)')
    
    args = parser.parse_args()
    
    logging.info("Gold Layer Processor started")
    logging.info(f"Delta path: {args.delta_path}")
    logging.info(f"XLSX path: {args.xlsx1_path}")
    logging.info(f"XLSX path: {args.xlsx2_path}")
    logging.info(f"Output path: {args.output_path}")
    
    success = process_gold_layer()
    
    if success:
        logging.info("Gold layer processing completed successfully!")
    else:
        logging.error("Gold layer processing failed!")
    
    return success

if __name__ == '__main__':
    main()
