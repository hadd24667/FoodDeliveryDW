import pandas as pd
from sqlalchemy import create_engine, text
import time

DB_USER = 'postgres'
DB_PASSWORD = 'minhnguyen1A'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'fastfood_db'
SCHEMA_NAME = 'public'
INPUT_FILE = r"D:\KDL2025\event_etl\data\ecommerce_clickstream_transactions.csv"
TABLE_NAME = 'fact_app_events'

CONNECTION_STRING = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

COLUMN_MAPPING = {
    'EventType': 'event_name',
    'ProductID': 'productid',
    'UserID': 'userid',
    'SessionID': 'sessionid',
    'Timestamp': 'timestamp',
    'Amount': 'amount',
    'Outcome': 'outcome'
}

def get_table_columns(engine):
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = '{TABLE_NAME}' AND table_schema = '{SCHEMA_NAME}'
    ORDER BY ordinal_position
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        columns = result.fetchall()
        
        if not columns:
            raise Exception(f"Table '{TABLE_NAME}' not found in schema '{SCHEMA_NAME}'")
        
        print(f"\nTable structure ({SCHEMA_NAME}.{TABLE_NAME}):")
        for col in columns:
            print(f"  {col[0]}: {col[1]} (nullable: {col[2]})")
        
        return {col[0]: col[1] for col in columns}

def transform_dataframe(df, table_columns):
    df_clean = df.copy()
    
    for csv_col, db_col in COLUMN_MAPPING.items():
        if csv_col in df_clean.columns:
            df_clean.rename(columns={csv_col: db_col}, inplace=True)
    
    db_cols = set(table_columns.keys())
    df_cols = set(df_clean.columns)
    
    missing_cols = db_cols - df_cols
    if missing_cols:
        print(f"\nAdding missing columns: {missing_cols}")
        for col in missing_cols:
            df_clean[col] = None
    
    extra_cols = df_cols - db_cols
    if extra_cols:
        print(f"Dropping extra columns: {extra_cols}")
        df_clean = df_clean[list(db_cols)]
    
    numeric_cols = ['userid', 'sessionid', 'amount']
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    text_cols = ['event_name', 'productid', 'outcome']
    for col in text_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].replace('nan', None)
    
    return df_clean

def load_to_postgres(df, engine):
    start = time.time()
    
    df.to_sql(
        name=TABLE_NAME,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='append',
        index=False,
        chunksize=5000,
        method='multi'
    )
    
    elapsed = time.time() - start
    return elapsed

def main():
    print("Starting ETL process...")
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"\n[EXTRACT] Loaded {len(df)} rows from CSV")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nSample data:\n{df.head()}")
        print(f"\nNull counts:\n{df.isnull().sum()}")
        
    except FileNotFoundError:
        print(f"Error: File not found at {INPUT_FILE}")
        return
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    try:
        engine = create_engine(CONNECTION_STRING)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"\n[CONNECT] Connected to database '{DB_NAME}'")
        
        table_columns = get_table_columns(engine)
        
    except Exception as e:
        print(f"\nConnection error: {e}")
        print("Check: PostgreSQL running? Database exists? Credentials correct?")
        return
    
    try:
        print("\n[TRANSFORM] Cleaning data...")
        df_clean = transform_dataframe(df, table_columns)
        
        print(f"\nTransformed {len(df_clean)} rows")
        print(f"Final columns: {df_clean.columns.tolist()}")
        print(f"\nSample:\n{df_clean.head()}")
        
    except Exception as e:
        print(f"\nTransform error: {e}")
        return
    
    try:
        print("\n[LOAD] Inserting into PostgreSQL...")
        elapsed = load_to_postgres(df_clean, engine)
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}"))
            total = result.scalar()
        
        print(f"\nInserted {len(df_clean)} rows in {elapsed:.2f}s ({len(df_clean)/elapsed:.0f} rows/sec)")
        print(f"Total rows in table: {total}")
        print("\nETL completed successfully!")
        
    except Exception as e:
        print(f"\nLoad error: {e}")
        print("\nTroubleshooting:")
        print("- Check column name mapping in COLUMN_MAPPING")
        print("- Verify table constraints (NOT NULL, UNIQUE, FK)")
        print(f"- Run: \\d {SCHEMA_NAME}.{TABLE_NAME}")
        return

if __name__ == "__main__":
    main()
