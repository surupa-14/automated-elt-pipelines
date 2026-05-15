-- ==========================================
-- NYC 311 DATA WAREHOUSE - STAR SCHEMA
-- ==========================================

-- Drop existing tables
DROP TABLE IF EXISTS warehouse.fact_complaints CASCADE;
DROP TABLE IF EXISTS warehouse.dim_date CASCADE;
DROP TABLE IF EXISTS warehouse.dim_location CASCADE;
DROP TABLE IF EXISTS warehouse.dim_agency CASCADE;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

-- DIM_DATE: Date dimension for time-series analysis
CREATE TABLE warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON warehouse.dim_date(full_date);

-- DIM_LOCATION: Geographic dimension
CREATE TABLE warehouse.dim_location (
    location_key BIGINT PRIMARY KEY,
    borough VARCHAR(50) NOT NULL,
    incident_zip VARCHAR(10),
    city VARCHAR(100),
    unique_location_desc VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_location_borough ON warehouse.dim_location(borough);

-- DIM_AGENCY: Agency dimension
CREATE TABLE warehouse.dim_agency (
    agency_key BIGINT PRIMARY KEY,
    agency_code VARCHAR(10) NOT NULL,
    agency_name VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_agency_code ON warehouse.dim_agency(agency_code);

-- ==========================================
-- FACT TABLE
-- ==========================================

-- FACT_COMPLAINTS: Grain = one service request
CREATE TABLE warehouse.fact_complaints (
    complaint_id VARCHAR(50) PRIMARY KEY,
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    location_key BIGINT REFERENCES warehouse.dim_location(location_key),
    agency_key BIGINT REFERENCES warehouse.dim_agency(agency_key),
    
    -- Date/Time facts
    created_datetime TIMESTAMP NOT NULL,
    closed_datetime TIMESTAMP,
    
    -- Complaint details
    complaint_type VARCHAR(100),
    complaint_category VARCHAR(50),
    complaint_descriptor VARCHAR(200),
    status VARCHAR(50),
    
    -- Location details
    location_type VARCHAR(100),
    incident_address VARCHAR(200),
    street_name VARCHAR(200),
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    
    -- Metrics
    resolution_time_hours NUMERIC(10, 2),
    
    -- ETL metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_complaints_date_key ON warehouse.fact_complaints(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_complaints_location_key ON warehouse.fact_complaints(location_key);
CREATE INDEX IF NOT EXISTS idx_fact_complaints_agency_key ON warehouse.fact_complaints(agency_key);
CREATE INDEX IF NOT EXISTS idx_fact_complaints_created_datetime ON warehouse.fact_complaints(created_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_complaints_category ON warehouse.fact_complaints(complaint_category);
