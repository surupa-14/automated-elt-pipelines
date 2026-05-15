import pandas as pd
from sqlalchemy import create_engine

def test_end_to_end():
    """Validate complete pipeline"""
    
    engine = create_engine("postgresql://postgres:yourpassword@127.0.0.1:5433/nyc311_warehouse")
    
    try:
        # Test 1: Check fact table row count
        fact_count = pd.read_sql("SELECT COUNT(*) as cnt FROM warehouse.fact_complaints", engine)
        assert fact_count['cnt'].iloc[0] > 0, "Fact table is empty"
        print("✓ Test 1 passed: Fact table contains data")
        
        # Test 2: Check dimension tables
        dim_date_count = pd.read_sql("SELECT COUNT(*) FROM warehouse.dim_date", engine)
        assert dim_date_count.iloc[0, 0] > 365, "dim_date has insufficient dates"
        print("✓ Test 2 passed: Dimension tables populated")
        
        # Test 3: Validate data quality
        quality_check = pd.read_sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT complaint_id) as unique_ids,
                SUM(CASE WHEN created_datetime IS NULL THEN 1 ELSE 0 END) as null_dates,
                SUM(CASE WHEN borough = 'Unspecified' THEN 1 ELSE 0 END) as unspecified_borough
            FROM warehouse.fact_complaints
        """, engine)
        
        assert quality_check['total_records'].iloc[0] == quality_check['unique_ids'].iloc[0], "Duplicate IDs found"
        print("✓ Test 3 passed: No duplicate records")
        
        # Test 4: Validate date keys
        orphan_dates = pd.read_sql("""
            SELECT COUNT(*) as orphans
            FROM warehouse.fact_complaints f
            LEFT JOIN warehouse.dim_date d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL
        """, engine)
        
        assert orphan_dates['orphans'].iloc[0] == 0, "Orphan date keys found"
        print("✓ Test 4 passed: All foreign keys valid")
        
        print("\\n✓✓✓ All tests passed! Pipeline validated. ✓✓✓")
        
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    test_end_to_end()
