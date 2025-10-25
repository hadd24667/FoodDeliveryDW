import pandas as pd
from sqlalchemy import text
import time
from db_connection import get_engine

# --- Config ---
INPUT_FILE = "../source_data/ecommerce_clickstream_transactions.csv"
SCHEMA_NAME = "dw"

# --- Kết nối PostgreSQL duy nhất ---
engine = get_engine()

# =====================================================
# 1. CREATE DIM_USER
# =====================================================
def create_dim_user(df):
    unique_users = df[['UserID']].drop_duplicates().reset_index(drop=True)
    unique_users['user_sk'] = 'CLK_U' + (unique_users.index + 1).astype(str)

    dim_user = unique_users[['user_sk', 'UserID']].rename(columns={'UserID': 'user_id'})
    print(f"✅ Created dim_user: {len(dim_user)} unique users")
    return dim_user


# =====================================================
# 2. TRANSFORM FACT_APP_EVENTS
# =====================================================
def transform_fact_events(df, dim_user):
    # Ép kiểu ID về string để merge an toàn
    df['UserID'] = df['UserID'].astype(str)
    dim_user['user_id'] = dim_user['user_id'].astype(str)

    df_clean = df.merge(dim_user, left_on='UserID', right_on='user_id', how='left')
    df_clean = df_clean.drop(columns=['UserID', 'user_id'])  # 🔥 chỉ còn user_id, không còn userid

    column_mapping = {
        'SessionID': 'sessionid',
        'Timestamp': 'timestamp',
        'EventType': 'event_name',
        'ProductID': 'productid',
        'Amount': 'amount',
        'Outcome': 'outcome'
    }

    df_clean = df_clean.rename(columns=column_mapping)

    # Chuẩn hoá dữ liệu
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

    print(f"✅ Transformed fact_app_events: {len(df_clean)} rows")
    return df_clean


# =====================================================
# 3. LOAD TO POSTGRESQL
# =====================================================
def load_dimension(df, table_name):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name} CASCADE"))
        print(f"🧹 Truncated {table_name}")
    except Exception as e:
        print(f"⚠️  Warning: Could not truncate {table_name}: {e}")

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
    print(f"⬆️  Loaded {len(df)} rows into {table_name} in {elapsed:.2f}s")


def load_fact(df, table_name):
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


# =====================================================
# 4. MAIN ETL
# =====================================================
def main():
    print("🚀 Starting ETL: Event Data\n")

    # Extract
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"[EXTRACT] Loaded {len(df)} rows from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"❌ File not found: {INPUT_FILE}")
        return
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    # Transform
    try:
        dim_user = create_dim_user(df)
        fact_events = transform_fact_events(df, dim_user)
    except Exception as e:
        print(f"❌ Transform error: {e}")
        return
    
    dim_user.to_csv("../staging_data/dim_user_preview.csv", index=False)
    fact_events.to_csv("../staging_data/fact_app_events_preview.csv", index=False)
    print("💾 Exported preview CSVs to staging_data/")

    # Load
    try:
        print("\n[LOAD] Writing to PostgreSQL...")
        load_dimension(dim_user, "dim_user")
        elapsed = load_fact(fact_events, "fact_app_events")

        with engine.connect() as conn:
            dim_count = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.dim_user")).scalar()
            fact_count = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.fact_app_events")).scalar()

        print(f"\n✅ Load completed: {fact_count} fact rows, {dim_count} dim rows in {elapsed:.2f}s")
    except Exception as e:
        print(f"❌ Load error: {e}")
        print("""
Make sure these tables exist:

CREATE TABLE public.dim_user (
    user_sk VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL
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

    print("\n🎯 ETL completed successfully!")


if __name__ == "__main__":
    main()
