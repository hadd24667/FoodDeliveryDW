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

CONNECTION_STRING = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def create_dim_user(df):
    unique_users = df[['UserID']].drop_duplicates().reset_index(drop=True)
    unique_users['user_sk'] = 'CLK_U' + (unique_users.index + 1).astype(str)
    
    dim_user = unique_users[['user_sk', 'UserID']].rename(columns={'UserID': 'userid'})
    
    print(f"Created dim_user: {len(dim_user)} unique users")
    return dim_user

def transform_fact_events(df, dim_user):
    df_clean = df.merge(
        dim_user,
        left_on='UserID',
        right_on='userid',
        how='left'
    )
    
    df_clean = df_clean.drop(columns=['UserID', 'userid'])
    
    column_mapping = {
        'SessionID': 'sessionid',
        'Timestamp': 'timestamp',
        'EventType': 'event_name',
        'ProductID': 'productid',
        'Amount': 'amount',
        'Outcome': 'outcome'
    }
    
    df_clean = df_clean.rename(columns=column_mapping)
    
    numeric_cols = ['sessionid', 'amount']
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    text_cols = ['event_name', 'productid', 'outcome']
    for col in text_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].replace('nan', None)
    
    column_order = ['user_sk', 'sessionid', 'timestamp', 'event_name', 'productid', 'amount', 'outcome']
    df_clean = df_clean[column_order]
    
    print(f"Transformed fact_app_events: {len(df_clean)} rows")
    return df_clean

def load_dimension(df, table_name, engine):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name} CASCADE"))
        print(f"Truncated {table_name}")
    except Exception as e:
        print(f"Warning: Could not truncate {table_name}: {e}")
    
    start = time.time()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='append',
        index=False,
        chunksize=1000,
        method='multi'
    )
    elapsed = time.time() - start
    print(f"Loaded {len(df)} rows into {table_name} in {elapsed:.2f}s")

def load_fact(df, table_name, engine):
    start = time.time()
    
    df.to_sql(
        name=table_name,
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
    print("Starting ETL with User Normalization...\n")
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"[EXTRACT] Loaded {len(df)} rows from CSV")
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
        
    except Exception as e:
        print(f"\nConnection error: {e}")
        print("Check: PostgreSQL running? Database exists? Credentials correct?")
        return
    
    try:
        print("\n[TRANSFORM] Creating dimension and normalizing fact table...")
        
        dim_user = create_dim_user(df)
        print(f"\ndim_user sample:\n{dim_user.head()}")
        
        fact_events = transform_fact_events(df, dim_user)
        print(f"\nfact_app_events sample:\n{fact_events.head()}")
        
    except Exception as e:
        print(f"\nTransform error: {e}")
        import traceback
        traceback.print_exc()
        return
    
    try:
        print("\n[LOAD] Loading to PostgreSQL...")
        
        load_dimension(dim_user, 'dim_user', engine)
        elapsed = load_fact(fact_events, 'fact_app_events', engine)
        
        with engine.connect() as conn:
            dim_count = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.dim_user")).scalar()
            fact_count = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.fact_app_events")).scalar()
        
        print(f"\nInserted fact_app_events: {len(fact_events)} rows in {elapsed:.2f}s ({len(fact_events)/elapsed:.0f} rows/sec)")
        print(f"\nVerification:")
        print(f"  dim_user: {dim_count} rows")
        print(f"  fact_app_events: {fact_count} rows")
        print("\nETL completed successfully!")
        
    except Exception as e:
        print(f"\nLoad error: {e}")
        import traceback
        traceback.print_exc()
        print("\nMake sure tables exist:")
        print("""
CREATE TABLE public.dim_user (
    user_sk VARCHAR(50) PRIMARY KEY,
    userid INTEGER NOT NULL
);

CREATE TABLE public.fact_app_events (
    user_sk VARCHAR(50) REFERENCES dim_user(user_sk),
    sessionid INTEGER,
    timestamp VARCHAR(50),
    event_name VARCHAR(100),
    productid VARCHAR(100),
    amount DECIMAL(10,2),
    outcome VARCHAR(100)
);
        """)
        return

if __name__ == "__main__":
    main()
