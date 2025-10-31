import pandas as pd
import logging
import os
from deltalake import write_deltalake
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('commute_processing.log')
    ]
)

def calculate_sport_prime(row):
    """Calculate Prime_Sportive boolean"""
    moyen_deplacement = str(row['Moyen de déplacement']).strip().lower()
    
    sport_transport = [
        'marche/running',
        'vélo/trottinette/autres',
        'velo/trottinette/autres'  # variant without accent
    ]
    
    return moyen_deplacement in sport_transport

def calculate_commute_long(row):
    """Calculate Commute_long boolean"""
    moyen_deplacement = str(row['Moyen de déplacement']).strip().lower()
    distance_km = row['Distance_km_typical']
    
    # Handle potential missing values
    if pd.isna(distance_km):
        return False
    
    # Walking/Running case
    if moyen_deplacement == 'marche/running':
        return distance_km > 15
    
    # Bike/Scooter case
    elif moyen_deplacement in ['vélo/trottinette/autres', 'velo/trottinette/autres']:
        return distance_km > 25
    
    # Other transport methods
    return False

def process_employee_commute_data():
    """Process the CSV file into a Delta table with calculated fields"""
    
    # Path configuration
    csv_file_path = "../google_map/employee_commutes.csv"
    gold_delta_output_path = "output/delta/employee_commute_analytics"
    
    try:
        logging.info("Starting employee commute data processing...")
        logging.info(f"Reading from CSV: {csv_file_path}")
        logging.info(f"Output Delta: {gold_delta_output_path}")
        
        # Step 1: Read CSV file
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found: {csv_file_path}")
            return False
            
        logging.info("Reading CSV file...")
        
        # Try different approaches to read the CSV
        df = None
        read_success = False
        
        # Approach 1: Try with comma delimiter (most common)
        try:
            df = pd.read_csv(csv_file_path, delimiter=',', encoding='utf-8')
            if len(df.columns) > 1:
                read_success = True
                logging.info("Successfully read CSV with comma delimiter")
        except Exception as e:
            logging.warning(f"Failed to read with comma delimiter: {e}")
        
        # Approach 2: Try with semicolon delimiter (common in European CSVs)
        if not read_success:
            try:
                df = pd.read_csv(csv_file_path, delimiter=';', encoding='utf-8')
                if len(df.columns) > 1:
                    read_success = True
                    logging.info("Successfully read CSV with semicolon delimiter")
            except Exception as e:
                logging.warning(f"Failed to read with semicolon delimiter: {e}")
        
        # Approach 3: Try auto-detecting delimiter
        if not read_success:
            try:
                df = pd.read_csv(csv_file_path, sep=None, engine='python', encoding='utf-8')
                if len(df.columns) > 1:
                    read_success = True
                    logging.info("Successfully read CSV with auto-detected delimiter")
            except Exception as e:
                logging.warning(f"Failed to read with auto-detection: {e}")
        
        # Approach 4: Try different encoding
        if not read_success:
            try:
                df = pd.read_csv(csv_file_path, encoding='latin-1')
                if len(df.columns) > 1:
                    read_success = True
                    logging.info("Successfully read CSV with latin-1 encoding")
            except Exception as e:
                logging.warning(f"Failed to read with latin-1 encoding: {e}")
        
        if not read_success or df is None:
            logging.error("Could not read CSV file with any method")
            return False
        
        logging.info(f"Read {len(df)} records from CSV")
        logging.info(f"CSV columns: {list(df.columns)}")
        
        # Debug: Show first few rows to verify structure
        logging.info(f"First few rows:\n{df.head(2)}")
        
        # Step 2: Data cleaning and type conversion
        # Convert date columns
        date_columns = ['Date de naissance', 'Date d\'embauche']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logging.info(f"Converted {col} to datetime")
            else:
                logging.warning(f"Date column '{col}' not found in DataFrame")
        
        # Convert numeric columns
        numeric_columns = ['Salaire brut', 'Nombre de jours de CP', 'Distance_km_typical']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logging.info(f"Converted {col} to numeric")
            else:
                logging.warning(f"Numeric column '{col}' not found in DataFrame")
        
        # Verify required columns exist before calculations
        required_columns = ['Moyen de déplacement', 'Distance_km_typical']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            logging.error(f"Available columns: {list(df.columns)}")
            return False
        
        # Step 3: Calculate new fields
        logging.info("Calculating Prime_Sportive...")
        df['Prime_Sportive'] = df.apply(calculate_sport_prime, axis=1)
        
        logging.info("Calculating Commute_long...")
        df['Commute_long'] = df.apply(calculate_commute_long, axis=1)
        
        # Step 4: Add processing metadata
        df['commute_processing_timestamp'] = datetime.now()
        df['commute_processing_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Step 5: Write to Delta Lake
        logging.info(f"Writing commute analytics data to Delta Lake: {gold_delta_output_path}...")
        
        # Ensure delta output directory exists
        os.makedirs(os.path.dirname(gold_delta_output_path), exist_ok=True)
        
        # Write to Delta Lake
        write_deltalake(gold_delta_output_path, df, mode='overwrite')
        
        logging.info(f"Successfully wrote {len(df)} records to commute Delta table")
        logging.info(f"Delta output: {gold_delta_output_path}")
        
        # Step 6: Show comprehensive statistics
        logging.info("\n" + "="*50)
        logging.info("FINAL PROCESSING SUMMARY")
        logging.info("="*50)
        
        total_records = len(df)
        prime_sportive_count = df['Prime_Sportive'].sum()
        commute_long_count = df['Commute_long'].sum()
        
        logging.info(f"Total records processed: {total_records}")
        logging.info(f"Employees qualifying for Prime_Sportive: {int(prime_sportive_count)} ({prime_sportive_count/total_records*100:.1f}%)")
        logging.info(f"Employees with Commute_long (over threshold): {int(commute_long_count)} ({commute_long_count/total_records*100:.1f}%)")
        
        # Breakdown by transport type
        logging.info("\n--- Breakdown by Transport Type ---")
        transport_summary = df.groupby('Moyen de déplacement').agg({
            'Prime_Sportive': 'sum',
            'Commute_long': 'sum',
            'ID salarié': 'count'
        }).rename(columns={'ID salarié': 'Total'})
        
        transport_summary['Prime_Sportive_Pct'] = (transport_summary['Prime_Sportive'] / transport_summary['Total'] * 100).round(1)
        transport_summary['Commute_long_Pct'] = (transport_summary['Commute_long'] / transport_summary['Total'] * 100).round(1)
        
        for transport_type, row in transport_summary.iterrows():
            logging.info(f"  {transport_type}:")
            logging.info(f"    - Total employees: {int(row['Total'])}")
            logging.info(f"    - Prime_Sportive: {int(row['Prime_Sportive'])} ({row['Prime_Sportive_Pct']}%)")
            logging.info(f"    - Commute_long: {int(row['Commute_long'])} ({row['Commute_long_Pct']}%)")
        
        # Show criteria summary
        logging.info("\n--- Criteria Applied ---")
        logging.info("Prime_Sportive = True for: Marche/running, Vélo/Trottinette/Autres")
        logging.info("Commute_long = True for:")
        logging.info("  - Marche/running AND Distance_km_typical > 15 km")
        logging.info("  - Vélo/Trottinette/Autres AND Distance_km_typical > 25 km")
        
        # Show sample of processed data
        sample_cols = ['ID salarié', 'Moyen de déplacement', 'Distance_km_typical', 'Prime_Sportive', 'Commute_long']
        available_cols = [col for col in sample_cols if col in df.columns]
        logging.info(f"\nSample of processed data:\n{df[available_cols].head(10)}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error in commute data processing: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def main():
    """Main function with argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Employee Commute Analytics: Process CSV to Delta Lake')
    
    parser.add_argument('--csv-path', type=str, default='../google_map/employee_commutes.csv',
                       help='Path to commute CSV file')
    
    parser.add_argument('--delta-output', type=str, default='output/delta/employee_commute_analytics',
                       help='Output Delta path for commute analytics')
    
    parser.add_argument('--delimiter', type=str, default='auto',
                       help='CSV delimiter (auto, comma, semicolon, tab)')
    
    args = parser.parse_args()
    
    logging.info("Employee Commute Analytics Processor started")
    logging.info(f"CSV path: {args.csv_path}")
    logging.info(f"Delta output: {args.delta_output}")
    
    success = process_employee_commute_data()
    
    if success:
        logging.info("Commute analytics processing completed successfully!")
    else:
        logging.error("Commute analytics processing failed!")
    
    return success

if __name__ == '__main__':
    main()
