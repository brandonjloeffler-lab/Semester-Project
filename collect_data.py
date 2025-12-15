import requests
import pandas as pd
import os
import time
from datetime import datetime

# --- Configuration ---
BLS_API_KEY = os.getenv("BLS_API_KEY", "88317efad228417fb8b93e6d0796cb8e")
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
UPDATE_WINDOW_YEARS = 3 # Fetch data for the last 3 years on monthly updates

END_YEAR = datetime.now().year
START_YEAR = END_YEAR - FULL_HISTORY_YEARS
DATA_FILE_PATH = "data/bls_data.csv"

# --- BLS API Fetch Function (No changes needed here) ---
def get_bls_data(series_ids, start_year, end_year):
    """Fetches data from the BLS API for the given series and date range."""
    '''
    # Check if API key is set before making the request
    if BLS_API_KEY == "88317efad228417fb8b93e6d0796cb8e" or not BLS_API_KEY:
        print("API Key not found. Please set the BLS_API_KEY environment variable.")
        return None
    '''
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
            print(f"BLS API Error: {json_data.get('message', 'Unknown Error')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during API request: {e}")
        return None

#  Data Processing
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

#  1. Initial Data Collection
def initial_data_collection():
    """Fetches full history (5 years) and saves the initial CSV."""
    print(f"Starting initial data collection from {START_YEAR} to {END_YEAR}...")

    series_ids = list(SERIES_MAP.keys())
    series_data = get_bls_data(series_ids, START_YEAR, END_YEAR)

    if series_data:
        df_final = process_data(series_data)

        os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)

        df_final.to_csv(DATA_FILE_PATH, index=False)
        print(f"Successfully collected {len(df_final)} historical records and saved to {DATA_FILE_PATH}")
    else:
        print("Initial data collection failed.")

# 2. Monthly Data Update (GitHub Action)
def update_data_and_save():
    """Fetches the latest data (last 3 years), merges it with existing data, and saves."""

    # 1. Determine the date range for the update
    update_start_year = datetime.now().year - UPDATE_WINDOW_YEARS
    update_end_year = datetime.now().year

    print(f"Fetching data for update from {update_start_year} to {update_end_year}...")

    series_ids = list(SERIES_MAP.keys())
    series_data = get_bls_data(series_ids, update_start_year, update_end_year)

    if not series_data:
        print("Monthly update data collection failed.")
        return

    # 2. Process the new data
    df_new = process_data(series_data)

    # 3. Read the existing data
    df_existing = pd.read_csv(DATA_FILE_PATH, parse_dates=['Date'])
    print(f"Loaded existing data with {len(df_existing)} records.")

    # 4. Combine data and drop duplicates (based on the Date column)

    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['Date'], keep='last')

    # 5. Final cleanup and save
    df_combined = df_combined.sort_values(by='Date').reset_index(drop=True)

    # Check if any new records were actually added/updated
    if len(df_combined) > len(df_existing):
        print(f"New data found! {len(df_combined) - len(df_existing)} new record(s) appended.")
    else:
        print("No new records found, but potential revisions were saved.")

    df_combined.to_csv(DATA_FILE_PATH, index=False)
    print(f"Data updated successfully. Total records now: {len(df_combined)}")


# Main Execution Logic
if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DATA_FILE_PATH), exist_ok=True)

    if not os.path.exists(DATA_FILE_PATH):
        # RUN 1: File doesn't exist, run initial historical collection
        initial_data_collection()
    else:
        # RUN 2+: File exists, run monthly update logic
        update_data_and_save()
     