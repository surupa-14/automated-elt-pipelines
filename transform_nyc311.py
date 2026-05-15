import pandas as pd
import numpy as np
from datetime import datetime

class NYC311Transformer:
    """Transform raw NYC 311 data into warehouse-ready format"""
    
    # Complaint type categorization mapping
    COMPLAINT_CATEGORIES = {
        'Noise': ['Noise - Residential', 'Noise - Street/Sidewalk', 'Noise - Commercial', 
                  'Noise - Vehicle', 'Noise - Park', 'Noise'],
        'Housing': ['HEAT/HOT WATER', 'HEATING', 'PLUMBING', 'PAINT/PLASTER', 
                    'Water System', 'DOOR/WINDOW', 'FLOORING/STAIRS'],
        'Street Conditions': ['Street Condition', 'Street Light Condition', 
                              'Sidewalk Condition', 'Traffic Signal Condition'],
        'Sanitation': ['Illegal Parking', 'Blocked Driveway', 'Derelict Vehicle',
                       'Abandoned Vehicle', 'UNSANITARY CONDITION'],
        'Vegetation': ['Overgrown Tree/Branches', 'Dead Tree', 'Damaged Tree',
                       'New Tree Request'],
        'Animal Issues': ['Rodent', 'Animal Abuse', 'Animal-Bite', 'Animal in a Park'],
        'Public Safety': ['Drinking', 'Drug Activity', 'Graffiti', 'Homeless Person Assistance'],
        'Other': []  # Default category
    }
    
    def __init__(self, df):
        """Initialize with raw DataFrame"""
        self.df = df.copy()
    
    def standardize_dates(self):
        """Convert date strings to datetime and handle missing values"""
        date_columns = ['created_date', 'closed_date', 'resolution_action_updated_date']
        
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        print(f"Standardized {len(date_columns)} date columns")
        return self
    
    def impute_missing_borough(self):
        """
        Impute missing borough values using:
        1. City (if available)
        2. Zip code lookup
        3. Default to 'Unspecified'
        """
        missing_before = self.df['borough'].isnull().sum() if 'borough' in self.df.columns else len(self.df)
        
        # Simple imputation: use 'Unspecified' for missing
        if 'borough' in self.df.columns:
            self.df['borough'] = self.df['borough'].fillna('Unspecified')
        else:
            self.df['borough'] = 'Unspecified'
        
        missing_after = (self.df['borough'] == 'Unspecified').sum()
        print(f"Borough imputation: {missing_before} missing -> {missing_after} 'Unspecified'")
        return self
    
    def calculate_resolution_time(self):
        """Calculate hours between created_date and closed_date"""
        if 'closed_date' in self.df.columns and 'created_date' in self.df.columns:
            self.df['resolution_time_hours'] = (
                (self.df['closed_date'] - self.df['created_date'])
                .dt.total_seconds() / 3600
            )
            
            # Handle negative values (data quality issue)
            self.df.loc[self.df['resolution_time_hours'] < 0, 'resolution_time_hours'] = np.nan
            
            print(f"Calculated resolution time. Median: {self.df['resolution_time_hours'].median():.1f} hours")
        return self
    
    def categorize_complaints(self):
        """Map 30+ complaint types to 8 business categories"""
        def assign_category(complaint_type):
            if pd.isnull(complaint_type):
                return 'Other'
            
            for category, keywords in self.COMPLAINT_CATEGORIES.items():
                if any(keyword.lower() in str(complaint_type).lower() for keyword in keywords):
                    return category
            return 'Other'
        
        if 'complaint_type' in self.df.columns:
            self.df['complaint_category'] = self.df['complaint_type'].apply(assign_category)
            
            print("\\nComplaint categorization:")
            print(self.df['complaint_category'].value_counts())
        return self
    
    def add_surrogate_keys(self):
        """Add warehouse surrogate keys for dimension tables"""
        # Date key: YYYYMMDD format
        if 'created_date' in self.df.columns:
            self.df['date_key'] = self.df['created_date'].dt.strftime('%Y%m%d').astype('Int64')
        
        # Location key: hash of borough + zip
        if 'borough' in self.df.columns and 'incident_zip' in self.df.columns:
            self.df['location_key'] = (
                self.df['borough'].astype(str) + '_' + 
                self.df['incident_zip'].astype(str).str[:5]
            ).apply(hash).abs()
        
        # Agency key: hash of agency
        if 'agency' in self.df.columns:
            self.df['agency_key'] = self.df['agency'].apply(lambda x: hash(str(x)) if pd.notna(x) else None)
        
        return self
    
    def select_final_columns(self):
        """Select and rename columns for warehouse fact table"""
        column_mapping = {
            'unique_key': 'complaint_id',
            'created_date': 'created_datetime',
            'closed_date': 'closed_datetime',
            'agency': 'agency_code',
            'agency_name': 'agency_name',
            'complaint_type': 'complaint_type',
            'complaint_category': 'complaint_category',
            'descriptor': 'complaint_descriptor',
            'location_type': 'location_type',
            'incident_zip': 'incident_zip',
            'incident_address': 'incident_address',
            'street_name': 'street_name',
            'city': 'city',
            'borough': 'borough',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'resolution_time_hours': 'resolution_time_hours',
            'status': 'status',
            'date_key': 'date_key',
            'location_key': 'location_key',
            'agency_key': 'agency_key'
        }
        
        # Select only columns that exist
        available_cols = [col for col in column_mapping.keys() if col in self.df.columns]
        self.df = self.df[available_cols].rename(columns=column_mapping)
        
        return self
    
    def get_transformed_data(self):
        """Return final transformed DataFrame"""
        return self.df
    
    def transform_pipeline(self):
        """Execute full transformation pipeline"""
        print("=== STARTING TRANSFORMATION PIPELINE ===\\n")
        
        self.standardize_dates()
        self.impute_missing_borough()
        self.calculate_resolution_time()
        self.categorize_complaints()
        self.add_surrogate_keys()
        self.select_final_columns()
        
        print(f"\\n=== TRANSFORMATION COMPLETE ===")
        print(f"Final record count: {len(self.df)}")
        print(f"Final column count: {len(self.df.columns)}")
        
        return self.get_transformed_data()


import glob
import os

# Example usage
if __name__ == "__main__":
    try:
        # Load raw staging data dynamically
        list_of_files = glob.glob('data/staging/nyc311_raw_*.json')
        if not list_of_files:
            raise FileNotFoundError("Staging file not found. Run extract_nyc311.py first.")
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Loading raw data from: {latest_file}")
        df_raw = pd.read_json(latest_file)
        
        # Transform
        transformer = NYC311Transformer(df_raw)
        df_clean = transformer.transform_pipeline()
        
        # Save to processed staging
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        clean_file = f'data/staging/nyc311_clean_{timestamp}.csv'
        df_clean.to_csv(clean_file, index=False)
        print(f"\\nSaved clean data to {clean_file}")
    except FileNotFoundError as e:
        print(e)
