import requests
import pandas as pd
import os
import time
from datetime import datetime

# --- Configuration ---
BLS_API_KEY = os.getenv('BLS_API_KEY')

if not BLS_API_KEY:
    raise ValueError("BLS_API_KEY environment variable not set.")
    
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES_MAP = {
    "LNS14000000": "Unemployment_Rate_SA",
    "CES0000000001": "Total_Nonfarm_Employment_SA",
    "CES0500000003": "Avg_Weekly_Hours_Private_SA",
    "PRS85006092": "Output_Per_Hour_NF",
    "CUUR0000SA0L1E": "CPI_U_Ex_Food_Energy_U",
    "EIUIR": "Imports_All_Commodities_U",
    "EIUIQ": "Exports_All_Commodities_U",
}

# Define the data ranges
FULL_HISTORY_YEARS = 5
UPDATE_WINDOW_YEARS = 3 

# Global variables for initial history
END_YEAR = datetime.now().year
START_YEAR = END_YEAR - FULL_HISTORY_YEARS
DATA_FILE_PATH = "data/bls_data.csv"

# --- BLS API Fetch Function ---
def get_bls_data(series_ids, start_year, end_year):
    """Fetches data from the BLS API for the given series and date range."""
    headers = {'Content-type': 'application/json'}
    data = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY,
        "catalog": False,
        "calculations": False,
        "annualaverage": False
    }

    try:
        response = requests.post(BLS_API_URL, headers=headers, json=data)
        response.raise_for_status()
        json_data = response.json()

        if json_data.get('status', '').strip() == 'REQUEST_SUCCEEDED':
            return json_data['Results']['series']
        else:
            print(f"BLS API Error: {json_data.get('message', 'Unknown Error')}. Status: {json_data.get('status')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during API request: {e}")
        return None

# --- Data Processing ---
def process_data(series_results):
    processed_data = {}

    for series in series_results:
        series_id = series['seriesID']
        column_name = SERIES_MAP.get(series_id, series_id)

        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']

            date_str = None
            if period.startswith('M'):
                month = int(period[1:])
                date_str = f"{year}-{month:02d}-01"
            elif period.startswith('Q'):
                # Map quarterly data to the last month of the quarter
                quarter_map = {'Q01': 3, 'Q02': 6, 'Q03': 9, 'Q04': 12}
                month = quarter_map.get(period, None)
                if month is not None:
                    date_str = f"{year}-{month:02d}-01"

            if date_str:
                if date_str not in processed_data:
                    processed_data[date_str] = {}

                try:
                    processed_data[date_str][column_name] = float(value)
                except ValueError:
                    processed_data[date_str][column_name] = None

    df = pd.DataFrame.from_dict(processed_data, orient='index')
    df.index.name = 'Date'
    df = df.reset_index()
    df['Date'] = pd.to_datetime(df['Date'])

    df = df.sort_values(by='Date').reset_index(drop=True)
    return df

# 1. Initial Data Collection (Generates the full file from scratch)
def initial_data_collection():
    """Fetches full history (5 years) and saves the initial CSV."""
    print(f"Starting initial data collection from {START_YEAR} to {END_YEAR}...")

    series_ids = list(SERIES_MAP.keys())
    series_data = get_bls_data(series_ids, START_YEAR, END_YEAR)

    if series_data:
        df_final = process_data(series_data)
        
        # Ensure data directory exists before saving
        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)

        df_final.to_csv(DATA_FILE_PATH, index=False)
        print(f"Successfully collected {len(df_final)} historical records and saved to {DATA_FILE_PATH}")
        return True # Indicate success
    else:
        print("Initial data collection failed.")
        return False # Indicate failure

# 2. Universal Data Update/Creation Logic
def update_data_and_save():
    
    # 1. Check for missing/empty/corrupt file (Handles the 'ValueError: Missing column' error)
    if not os.path.exists(DATA_FILE_PATH) or os.path.getsize(DATA_FILE_PATH) == 0:
        print("Initial run check: Data file does not exist or is empty. Attempting full historical collection.")
        if initial_data_collection():
            print("Full historical collection completed. Attempting one last read before exiting.")
            # If successful, we exit here to ensure the newly created file is committed.
            # The next action run will then follow the update path.
            return
        else:
            print("CRITICAL ERROR: Initial data collection failed to save the file. Cannot proceed.")
            return

    # 2. File exists and should have content. Read the existing data.
    try:
        # This is line 149, which has been consistently failing.
        df_existing = pd.read_csv(DATA_FILE_PATH, parse_dates=['Date'])
    
    except ValueError as e:
        # Handle the case where the file exists but is corrupted (i.e., missing the 'Date' header)
        if "Missing column provided to 'parse_dates'" in str(e):
            print("WARNING: Existing CSV file structure error (corrupted header). Deleting and running initial collection to rebuild.")
            os.remove(DATA_FILE_PATH)
            # Recursively call the function. It will now hit the 'if not os.path.exists' block above.
            update_data_and_save()
            return
        raise # Re-raise other unexpected ValueErrors

    # 3. Define update window years (CRITICAL: Needs to be defined here for the update path)
    current_year = datetime.now().year
    update_start_year = current_year - UPDATE_WINDOW_YEARS
    update_end_year = current_year
    
    latest_date = df_existing['Date'].max()
    print(f"Loaded existing data with {len(df_existing)} records. Latest date: {latest_date}.")
    print(f"Fetching data for update/revisions from {update_start_year} to {update_end_year}...")

    # 4. Fetch update data
    series_ids = list(SERIES_MAP.keys())
    series_data = get_bls_data(series_ids, update_start_year, update_end_year)

    if not series_data:
        print("Monthly update data collection failed.")
        return

    # 5. Process, combine, and save
    df_new = process_data(series_data)
    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['Date'], keep='last')
    df_combined = df_combined.sort_values(by='Date').reset_index(drop=True)

    new_records_count = len(df_combined) - len(df_existing)
    if new_records_count > 0:
        print(f"New data found! {new_records_count} new record(s) appended.")
    else:
        print("No new records found, assuming data was already up to date or revisions were applied.")

    df_combined.to_csv(DATA_FILE_PATH, index=False)
    print(f"Data updated successfully. Total records now: {len(df_combined)}")


# --- Main Execution Logic ---
if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)
    
    # Run the universal update/create logic
    update_data_and_save()
