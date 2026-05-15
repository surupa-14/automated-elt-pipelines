import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json

class NYC311Extractor:
    """Extract service requests from NYC 311 Open Data API"""
    
    BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    
    def __init__(self, app_token=None):
        self.app_token = app_token
        self.session = requests.Session()
        if app_token:
            self.session.headers.update({'X-App-Token': app_token})
    
    def extract_incremental(self, start_date, end_date, limit=50000):
        """
        Extract records created between start_date and end_date
        
        Args:
            start_date (str): Format 'YYYY-MM-DD'
            end_date (str): Format 'YYYY-MM-DD'
            limit (int): Records per API call (max 50,000)
        
        Returns:
            pd.DataFrame: Raw service request data
        """
        all_records = []
        offset = 0
        
        while True:
            params = {
                "$limit": limit,
                "$offset": offset,
                "$where": f"created_date >= '{start_date}T00:00:00' AND created_date < '{end_date}T23:59:59'",
                "$order": "created_date ASC"
            }
            
            print(f"Fetching records {offset} to {offset + limit}...")
            
            try:
                response = self.session.get(self.BASE_URL, params=params, timeout=60)
                response.raise_for_status()
                records = response.json()
                
                if not records:
                    print(f"No more records. Total extracted: {len(all_records)}")
                    break
                
                all_records.extend(records)
                offset += limit
                
                # Safety check: prevent infinite loops
                if offset > 500000:
                    print("WARNING: Offset exceeded 500k. Breaking loop.")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"API Error: {e}")
                break
        
        df = pd.DataFrame(all_records)
        return df
    
    def save_to_staging(self, df, staging_dir='data/staging'):
        """
        Save raw data to timestamped JSON file (data lake pattern)
        
        Args:
            df (pd.DataFrame): Raw data
            staging_dir (str): Directory path
        
        Returns:
            str: File path of saved data
        """
        os.makedirs(staging_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"nyc311_raw_{timestamp}.json"
        filepath = os.path.join(staging_dir, filename)
        
        df.to_json(filepath, orient='records', date_format='iso')
        print(f"Saved {len(df)} records to {filepath}")
        
        return filepath


# Example usage
if __name__ == "__main__":
    extractor = NYC311Extractor()
    
    # Extract last 7 days of data
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    df = extractor.extract_incremental(start_date, end_date)
    filepath = extractor.save_to_staging(df)
    
    print(f"\\nExtraction Summary:")
    print(f"Records extracted: {len(df)}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Columns: {list(df.columns)}")
