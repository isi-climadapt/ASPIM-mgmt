"""
Temporary script to explore database columns in APSIM database files.
This helps identify what columns are actually present in the database tables.
"""

import sqlite3
from pathlib import Path
from typing import Dict, List

# Configuration - adjust these paths as needed
BASE_DIR = r"C:\Users\ibian\Desktop\ClimAdapt"
FARM_NAME = "Anameka"
COORDINATE = "-31.75_117.60"
DB_FILE_NAME = "ClimAdapt_Wheat_neg31.75_117.60_past.db"

# Construct path: base_directory / FARM_NAME / COORDINATE_APSIM / DB_FILE_NAME
base_directory = Path(BASE_DIR)
COORDINATE_APSIM = f"{COORDINATE}_APSIM"
db_path = base_directory / FARM_NAME / COORDINATE_APSIM / DB_FILE_NAME

def explore_database(db_path: Path):
    """Explore database structure and columns."""
    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}")
        return
    
    print("=" * 80)
    print(f"Exploring Database: {db_path.name}")
    print(f"Full Path: {db_path}")
    print("=" * 80)
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\nFound {len(tables)} table(s): {', '.join(tables)}\n")
    
    # Explore each table
    for table_name in tables:
        print("=" * 80)
        print(f"Table: {table_name}")
        print("=" * 80)
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        row_count = cursor.fetchone()["count"]
        print(f"Rows: {row_count:,}")
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        
        print(f"\nColumns ({len(columns_info)}):")
        print("-" * 80)
        
        column_names = []
        for col in columns_info:
            col_name = col[1]
            col_type = col[2]
            not_null = "NOT NULL" if col[3] else "NULL"
            default = f" DEFAULT {col[4]}" if col[4] else ""
            primary_key = " PRIMARY KEY" if col[5] else ""
            
            column_names.append(col_name)
            
            print(f"  {col_name:30s} {col_type:15s} {not_null:10s}{default}{primary_key}")
        
        # Show sample data for Report and Daily tables
        if table_name in ['Report', 'Daily'] and row_count > 0:
            print(f"\nSample data (first 3 rows):")
            print("-" * 80)
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                
                # Print column headers
                if rows:
                    col_headers = [description[0] for description in cursor.description]
                    print(f"  {' | '.join(col_headers[:10])}")  # Show first 10 columns
                    if len(col_headers) > 10:
                        print(f"  ... and {len(col_headers) - 10} more columns")
                    
                    # Print sample rows
                    for i, row in enumerate(rows, 1):
                        values = [str(row[col])[:20] for col in col_headers[:10]]  # Truncate long values
                        print(f"  Row {i}: {' | '.join(values)}")
                        if len(col_headers) > 10:
                            print(f"         ... ({len(col_headers) - 10} more columns)")
            except Exception as e:
                print(f"  WARNING: Could not fetch sample data: {e}")
        
        print()
    
    # Summary for Report and Daily tables
    print("=" * 80)
    print("Summary: Report and Daily Tables")
    print("=" * 80)
    
    for table_name in ['Report', 'Daily']:
        if table_name in tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            column_names = [col[1] for col in columns_info]
            
            print(f"\n{table_name} table:")
            print(f"  Total columns: {len(column_names)}")
            print(f"  Column names: {', '.join(column_names)}")
            
            # Check for expected columns
            if table_name == 'Report':
                expected_cols = ['Year', 'DryYield', 'SowingDate', 'HarvestDate', 'CropSurvived']
                missing = [col for col in expected_cols if col not in column_names]
                if missing:
                    print(f"  WARNING: Missing expected columns: {missing}")
                else:
                    print(f"  OK: All expected columns present")
            
            elif table_name == 'Daily':
                expected_cols = ['Date', 'Year', 'DryYield', 'RadiationFactor', 'WaterFactor']
                missing = [col for col in expected_cols if col not in column_names]
                if missing:
                    print(f"  WARNING: Missing expected columns: {missing}")
                else:
                    print(f"  OK: All expected columns present")
        else:
            print(f"\n{table_name} table: NOT FOUND in database")
    
    conn.close()
    print("\n" + "=" * 80)
    print("Exploration complete!")
    print("=" * 80)


if __name__ == "__main__":
    explore_database(db_path)
    
    # Optionally explore multiple files
    print("\n\n" + "=" * 80)
    print("To explore other database files, modify the configuration at the top of this script.")
    print("=" * 80)
