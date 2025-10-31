import pandas as pd
import logging
import os
import shutil
from deltalake import DeltaTable, write_deltalake
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

def is_delta_table_healthy(delta_path):
    """Check if Delta table exists and is healthy"""
    try:
        if not os.path.exists(delta_path):
            return False
        
        # Check for essential Delta Lake structure
        delta_log_path = os.path.join(delta_path, "_delta_log")
        if not os.path.exists(delta_log_path):
            return False
        
        # Try to read the table
        delta_table = DeltaTable(delta_path)
        # Try a simple operation to verify table is healthy
        delta_table.schema()
        return True
        
    except Exception:
        return False

def get_last_processed_timestamp(delta_path):
    """Get the timestamp of the last processed record from gold layer"""
    try:
        if is_delta_table_healthy(delta_path):
            gold_table = DeltaTable(delta_path)
            gold_df = gold_table.to_pandas()
            if len(gold_df) > 0 and 'processing_timestamp' in gold_df.columns:
                last_timestamp = gold_df['processing_timestamp'].max()
                logging.info(f"Last processed timestamp: {last_timestamp}")
                return last_timestamp
    except Exception as e:
        logging.warning(f"Could not read last processed timestamp: {str(e)}")
    return None

def repair_delta_table(delta_path):
    """Repair a corrupted Delta table by recreating it"""
    try:
        if os.path.exists(delta_path):
            logging.warning(f"Delta table at {delta_path} is corrupted, repairing...")
            # Backup the corrupted table
            backup_path = delta_path + "_corrupted_backup"
            if os.path.exists(backup_path):
                shutil.rmtree(backup_path)
            shutil.move(delta_path, backup_path)
            logging.info(f"Backed up corrupted table to: {backup_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to repair Delta table: {str(e)}")
        return False

def process_gold_layer_incremental():
    """Process only NEW bronze Delta data into gold layer"""
    
    # Path configuration
    bronze_delta_path = "../bronze/output/delta/employee_activities"
    gold_delta_output_path = "output/delta/employee_activities_enriched" 
    xlsx1_reference_path = '../data/DonneesSportive.xlsx'
    xlsx2_reference_path = '../data/DonneesRH.xlsx'
    
    try:
        logging.info("Starting INCREMENTAL gold layer processing...")
        
        # Check if bronze Delta table exists and is healthy
        if not is_delta_table_healthy(bronze_delta_path):
            logging.error("Bronze Delta table not found or corrupted!")
            return False
        
        # Step 1: Check if gold table needs repair
        if os.path.exists(gold_delta_output_path) and not is_delta_table_healthy(gold_delta_output_path):
            if not repair_delta_table(gold_delta_output_path):
                logging.error("Failed to repair gold Delta table!")
                return False
        
        # Step 2: Get last processed timestamp from gold layer
        last_timestamp = get_last_processed_timestamp(gold_delta_output_path)
        
        # Step 3: Read data from bronze Delta
        logging.info("Reading Delta Lake table...")
        bronze_table = DeltaTable(bronze_delta_path)
        df_bronze = bronze_table.to_pandas()
        
        # Convert timestamp columns
        if 'processing_timestamp' in df_bronze.columns:
            df_bronze['processing_timestamp'] = pd.to_datetime(df_bronze['processing_timestamp'])
        
        if last_timestamp:
            # Filter for records newer than last processed timestamp
            df_new = df_bronze[df_bronze['processing_timestamp'] > last_timestamp]
            logging.info(f"Found {len(df_new)} NEW records since last processing")
        else:
            # First time processing - take all data
            df_new = df_bronze
            logging.info(f"First run - processing all {len(df_new)} records")
        
        if len(df_new) == 0:
            logging.info("No new records to process")
            return True
        
        # Step 4: Read XLSX reference data
        logging.info("Reading XLSX reference data...")
        df_xlsx1 = pd.read_excel(xlsx1_reference_path)
        df_xlsx2 = pd.read_excel(xlsx2_reference_path)
        
        # Step 5: Merge with reference data
        delta_id_column = 'ID_salarie'
        xlsx_id_column = 'ID salarié'
        
        # Merge with first XLSX
        merged_df = pd.merge(
            df_new,
            df_xlsx1,
            left_on=delta_id_column,
            right_on=xlsx_id_column,
            how='left'
        )
        if xlsx_id_column in merged_df.columns:
            merged_df = merged_df.drop(columns=[xlsx_id_column])
        
        # Merge with second XLSX
        merged_df = pd.merge(
            merged_df,
            df_xlsx2,
            left_on=delta_id_column,
            right_on=xlsx_id_column,
            how='left'
        )
        if xlsx_id_column in merged_df.columns:
            merged_df = merged_df.drop(columns=[xlsx_id_column])
        
        # Step 6: Clean and standardize data types
        logging.info("Standardizing data types...")
        
        # Convert potential problematic columns to string
        for col in merged_df.columns:
            if merged_df[col].dtype == 'object':
                merged_df[col] = merged_df[col].fillna('').astype(str)
        
        # Handle timestamp columns
        timestamp_cols = ['Date_de_debut', 'Date_de_fin', 'processing_timestamp']
        for col in timestamp_cols:
            if col in merged_df.columns:
                merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce')
        
        # Step 7: Add gold layer metadata
        merged_df['gold_processing_timestamp'] = datetime.now()
        merged_df['gold_processing_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Step 8: Write to Delta Lake
        logging.info(f"Writing {len(merged_df)} records to gold Delta table...")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(gold_delta_output_path), exist_ok=True)
        
        if is_delta_table_healthy(gold_delta_output_path):
            # Append to existing table
            write_deltalake(
                gold_delta_output_path, 
                merged_df, 
                mode='append',
                schema_mode='merge'
            )
            logging.info("Successfully appended to existing gold Delta table")
        else:
            # Create new table
            write_deltalake(
                gold_delta_output_path, 
                merged_df, 
                mode='overwrite'
            )
            logging.info("Successfully created new gold Delta table")
        
        # Step 9: Update CSV
        gold_csv_output_path = "../gold/employee_activities_enriched.csv"
        os.makedirs(os.path.dirname(gold_csv_output_path), exist_ok=True)
        
        # Read the entire gold table for CSV
        gold_table = DeltaTable(gold_delta_output_path)
        gold_df = gold_table.to_pandas()
        gold_df.to_csv(gold_csv_output_path, index=False)
        logging.info(f"Updated CSV with {len(gold_df)} total records")
        
        logging.info(f"Successfully processed {len(merged_df)} new records")
        return True
        
    except Exception as e:
        logging.error(f"Error in incremental gold processing: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def main():
    """Main function for incremental processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='INCREMENTAL Gold Layer Processing')
    
    parser.add_argument('--force-repair', action='store_true',
                       help='Force repair of gold Delta table')
    
    parser.add_argument('--full-refresh', action='store_true',
                       help='Force full refresh instead of incremental')
    
    args = parser.parse_args()
    
    logging.info("INCREMENTAL Gold Layer Processor started")
    
    if args.force_repair:
        gold_delta_path = "output/delta/employee_activities_enriched"
        if os.path.exists(gold_delta_path):
            repair_delta_table(gold_delta_path)
    
    success = process_gold_layer_incremental()
    
    if success:
        logging.info("Incremental gold processing completed successfully!")
    else:
        logging.error("Incremental gold processing failed!")
    
    return success

if __name__ == '__main__':
    main()
