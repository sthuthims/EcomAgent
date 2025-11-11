# database.py - DuckDB Setup (AUTO-DETECTS ALL CSV FILES)

import duckdb
import pandas as pd
import os
import glob
from pathlib import Path

# ============================================================
# GLOBAL DATABASE CONNECTION
# ============================================================

_db_instance = None

def get_db():
    """Get or create database instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = load_data()
    return _db_instance

def load_data():
    """
    Auto-loads ALL CSV files from the 'data/' folder into DuckDB
    Handles any CSV structure automatically
    """
    try:
        conn = duckdb.connect(':memory:')
        
        # Get data directory
        current_dir = Path(__file__).parent
        data_dir = current_dir / 'data'
        
        # If data folder doesn't exist in same directory, try common locations
        if not data_dir.exists():
            alternative_paths = [
                Path('data'),
                Path('./data'),
                Path('../data'),
                Path(os.path.expanduser('~/data')),
            ]
            for alt_path in alternative_paths:
                if alt_path.exists():
                    data_dir = alt_path
                    break
        
        if not data_dir.exists():
            raise FileNotFoundError(f"❌ Data folder not found. Looked in: {data_dir}")
        
        # Find all CSV files
        csv_files = glob.glob(str(data_dir / '*.csv'))
        
        if not csv_files:
            raise FileNotFoundError(f"❌ No CSV files found in {data_dir}")
        
        print(f"📂 Loading from: {data_dir}")
        print(f"🔍 Found {len(csv_files)} CSV files\n")
        
        # Load each CSV file
        loaded_tables = {}
        for filepath in sorted(csv_files):
            filename = os.path.basename(filepath)
            
            try:
                # Read CSV
                df = pd.read_csv(filepath, on_bad_lines='skip')
                
                # Create table name from filename (remove .csv and convert to lowercase)
                table_name = os.path.splitext(filename)[0].replace('-', '_').lower()
                
                # Handle special cases
                if 'order_items' in table_name:
                    table_name = 'order_items'
                elif 'orders' in table_name and 'items' not in table_name:
                    table_name = 'orders'
                elif 'customer' in table_name:
                    table_name = 'customers'
                elif 'review' in table_name:
                    table_name = 'reviews'
                elif 'seller' in table_name:
                    table_name = 'sellers'
                elif 'product' in table_name and 'category' not in table_name:
                    table_name = 'products'
                elif 'category' in table_name:
                    table_name = 'category_names'
                elif 'payment' in table_name:
                    table_name = 'payments'
                elif 'geo' in table_name:
                    table_name = 'geolocation'
                
                # Register with DuckDB
                conn.register(table_name, df)
                loaded_tables[table_name] = len(df)
                
                # Pretty print
                cols = ', '.join([f"{col}({df[col].dtype.name[:3]})" for col in df.columns[:3]])
                print(f"✅ {table_name:30s} | {len(df):8,d} rows | Cols: {cols}...")
                
            except Exception as e:
                print(f"⚠️  {filename:30s} | Error: {str(e)[:50]}")
        
        print(f"\n✅ Database loaded successfully!")
        print(f"📊 Total tables: {len(loaded_tables)}\n")
        
        return conn
        
    except Exception as e:
        print(f"\n❌ Fatal Error: {str(e)}")
        raise

def test_connection():
    """Test database connection and show all tables"""
    try:
        print("=" * 80)
        print("🔧 TESTING DATABASE CONNECTION")
        print("=" * 80)
        
        conn = load_data()
        
        # Get all tables
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='memory' ORDER BY table_name"
        ).fetchall()
        
        print("\n📋 AVAILABLE TABLES:")
        print("-" * 80)
        
        for table in tables:
            table_name = table[0]
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Get columns
            cols_result = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            col_names = [col[1] for col in cols_result]
            
            print(f"\n📦 {table_name.upper()}")
            print(f"   Rows: {row_count:,}")
            print(f"   Columns ({len(col_names)}): {', '.join(col_names[:5])}", end="")
            if len(col_names) > 5:
                print(f" + {len(col_names) - 5} more", end="")
            print()
        
        print("\n" + "=" * 80)
        print("✅ CONNECTION TEST PASSED")
        print("=" * 80)
        
        return conn
        
    except Exception as e:
        print(f"\n❌ CONNECTION TEST FAILED")
        print(f"Error: {str(e)}")
        raise

def get_table_info(table_name):
    """Get detailed info about a specific table"""
    try:
        conn = get_db()
        
        # Get schema
        schema = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        
        # Get sample data
        sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
        
        return {
            'schema': schema,
            'sample': sample
        }
    except Exception as e:
        return {'error': str(e)}

def execute_query(query_str):
    """Execute raw query (for debugging)"""
    try:
        conn = get_db()
        result = conn.execute(query_str).fetchall()
        return result
    except Exception as e:
        return {'error': str(e)}

def get_stats():
    """Get database statistics"""
    try:
        conn = get_db()
        
        stats = {}
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='memory'"
        ).fetchall()
        
        for table in tables:
            table_name = table[0]
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            stats[table_name] = count
        
        return stats
    except Exception as e:
        return {'error': str(e)}

# ============================================================
# RUN TEST ON IMPORT
# ============================================================

if __name__ == "__main__":
    # Run comprehensive test
    try:
        test_connection()
        
        # Additional tests
        print("\n" + "=" * 80)
        print("🧪 SAMPLE QUERIES")
        print("=" * 80)
        
        conn = get_db()
        
        # Test 1: Orders
        try:
            result = conn.execute("SELECT COUNT(*) as total FROM orders").fetchone()
            print(f"\n✅ Orders table: {result[0]:,} records")
        except:
            print(f"⚠️  Orders table not accessible")
        
        # Test 2: Customers
        try:
            result = conn.execute("SELECT COUNT(*) as total FROM customers").fetchone()
            print(f"✅ Customers table: {result[0]:,} records")
        except:
            print(f"⚠️  Customers table not accessible")
        
        # Test 3: Products
        try:
            result = conn.execute("SELECT COUNT(*) as total FROM products").fetchone()
            print(f"✅ Products table: {result[0]:,} records")
        except:
            print(f"⚠️  Products table not accessible")
        
        print("\n" + "=" * 80)
        print("🎉 ALL TESTS COMPLETED")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {str(e)}")
        exit(1)