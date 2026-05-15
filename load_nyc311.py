import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

class NYC311Loader:
    """Load transformed data into PostgreSQL star schema warehouse"""
    
    def __init__(self, connection_string):
        """
        Args:
            connection_string (str): PostgreSQL connection string
                Example: 'postgresql://user:password@localhost:5432/nyc311_warehouse'
        """
        self.engine = create_engine(connection_string)
        self.connection_string = connection_string
    
    def load_dim_date(self, start_year=2020, end_year=2030):
        """
        Populate date dimension with date range
        
        Args:
            start_year (int): Start year
            end_year (int): End year
        """
        dates = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        
        dim_date = pd.DataFrame({
            'date_key': dates.strftime('%Y%m%d').astype(int),
            'full_date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'week_of_year': dates.isocalendar().week,
            'day_of_month': dates.day,
            'day_of_week': dates.dayofweek,
            'day_name': dates.strftime('%A'),
            'is_weekend': dates.dayofweek >= 5
        })
        
        # Upsert logic: insert only new dates
        with self.engine.connect() as conn:
            # Check existing dates
            existing_dates = pd.read_sql("SELECT date_key FROM warehouse.dim_date", conn)
            new_dates = dim_date[~dim_date['date_key'].isin(existing_dates['date_key'])]
            
            if len(new_dates) > 0:
                new_dates.to_sql('dim_date', conn, schema='warehouse', 
                                 if_exists='append', index=False)
                print(f"Loaded {len(new_dates)} new dates into dim_date")
            else:
                print("No new dates to load")
            conn.commit()
    
    def load_dim_location(self, df):
        """
        Load location dimension from fact data
        
        Args:
            df (pd.DataFrame): Transformed data with location fields
        """
        if 'location_key' not in df.columns:
            print("No location data to load.")
            return

        dim_location = df[['location_key', 'borough', 'incident_zip', 'city']].copy()
        dim_location['unique_location_desc'] = (
            dim_location['borough'].astype(str) + ', ' + 
            dim_location['city'].fillna('').astype(str) + ' ' + 
            dim_location['incident_zip'].fillna('').astype(str)
        )
        dim_location = dim_location.drop_duplicates(subset=['location_key'])
        
        # Upsert: ON CONFLICT DO UPDATE
        with self.engine.connect() as conn:
            for _, row in dim_location.iterrows():
                conn.execute(text("""
                    INSERT INTO warehouse.dim_location 
                        (location_key, borough, incident_zip, city, unique_location_desc)
                    VALUES 
                        (:location_key, :borough, :incident_zip, :city, :unique_location_desc)
                    ON CONFLICT (location_key) 
                    DO UPDATE SET 
                        borough = EXCLUDED.borough,
                        incident_zip = EXCLUDED.incident_zip,
                        city = EXCLUDED.city,
                        unique_location_desc = EXCLUDED.unique_location_desc,
                        updated_at = CURRENT_TIMESTAMP
                """), row.to_dict())
            conn.commit()
        
        print(f"Upserted {len(dim_location)} locations into dim_location")
    
    def load_dim_agency(self, df):
        """
        Load agency dimension from fact data
        
        Args:
            df (pd.DataFrame): Transformed data with agency fields
        """
        if 'agency_key' not in df.columns:
            print("No agency data to load.")
            return

        dim_agency = df[['agency_key', 'agency_code', 'agency_name']].copy()
        dim_agency = dim_agency.dropna(subset=['agency_key']).drop_duplicates(subset=['agency_key'])
        
        with self.engine.connect() as conn:
            for _, row in dim_agency.iterrows():
                conn.execute(text("""
                    INSERT INTO warehouse.dim_agency 
                        (agency_key, agency_code, agency_name)
                    VALUES 
                        (:agency_key, :agency_code, :agency_name)
                    ON CONFLICT (agency_key) 
                    DO UPDATE SET 
                        agency_code = EXCLUDED.agency_code,
                        agency_name = EXCLUDED.agency_name,
                        updated_at = CURRENT_TIMESTAMP
                """), row.to_dict())
            conn.commit()
        
        print(f"Upserted {len(dim_agency)} agencies into dim_agency")
    
    def load_fact_complaints(self, df):
        """
        Load fact table with idempotent upsert logic
        
        Args:
            df (pd.DataFrame): Transformed complaints data
        """
        # Select fact table columns
        fact_columns = [
            'complaint_id', 'date_key', 'location_key', 'agency_key',
            'created_datetime', 'closed_datetime',
            'complaint_type', 'complaint_category', 'complaint_descriptor', 'status',
            'location_type', 'incident_address', 'street_name',
            'latitude', 'longitude', 'resolution_time_hours'
        ]
        
        # Filter available columns
        available_cols = [col for col in fact_columns if col in df.columns]
        fact_df = df[available_cols].copy()
        
        # Prepare missing columns with None/NaN if needed, but since we are inserting, we need all columns matching the query.
        # Alternatively, dynamically generate query. We will use a standard query and supply None for missing columns.
        for col in fact_columns:
            if col not in fact_df.columns:
                fact_df[col] = None

        # Handle NaNs for DB insertion
        fact_df = fact_df.astype(object).where(pd.notnull(fact_df), None)

        # Upsert logic
        with self.engine.connect() as conn:
            inserted = 0
            updated = 0
            
            for _, row in fact_df.iterrows():
                result = conn.execute(text("""
                    INSERT INTO warehouse.fact_complaints 
                        (complaint_id, date_key, location_key, agency_key,
                         created_datetime, closed_datetime,
                         complaint_type, complaint_category, complaint_descriptor, status,
                         location_type, incident_address, street_name,
                         latitude, longitude, resolution_time_hours)
                    VALUES 
                        (:complaint_id, :date_key, :location_key, :agency_key,
                         :created_datetime, :closed_datetime,
                         :complaint_type, :complaint_category, :complaint_descriptor, :status,
                         :location_type, :incident_address, :street_name,
                         :latitude, :longitude, :resolution_time_hours)
                    ON CONFLICT (complaint_id) 
                    DO UPDATE SET 
                        closed_datetime = EXCLUDED.closed_datetime,
                        status = EXCLUDED.status,
                        resolution_time_hours = EXCLUDED.resolution_time_hours,
                        updated_at = CURRENT_TIMESTAMP
                """), row.to_dict())
                
                if result.rowcount > 0:
                    inserted += 1
                else:
                    updated += 1
            
            conn.commit()
        
        print(f"Loaded fact_complaints: {inserted} new, {updated} updated")
    
    def execute_full_load(self, df):
        """
        Execute complete ETL load sequence
        
        Args:
            df (pd.DataFrame): Transformed data
        """
        print("=== STARTING LOAD PROCESS ===\\n")
        
        self.load_dim_date()
        self.load_dim_location(df)
        self.load_dim_agency(df)
        self.load_fact_complaints(df)
        
        print("\\n=== LOAD COMPLETE ===")


import glob

# Example usage
if __name__ == "__main__":
    try:
        # Load transformed data dynamically
        list_of_files = glob.glob('data/staging/nyc311_clean_*.csv')
        if not list_of_files:
            raise FileNotFoundError("Clean file not found. Run transform_nyc311.py first.")
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Loading clean data from: {latest_file}")
        df = pd.read_csv(latest_file)
        
        # Database connection
        conn_string = "postgresql://postgres:yourpassword@127.0.0.1:5433/nyc311_warehouse"
        
        # Load to warehouse
        loader = NYC311Loader(conn_string)
        loader.execute_full_load(df)
    except FileNotFoundError as e:
         print(e)
