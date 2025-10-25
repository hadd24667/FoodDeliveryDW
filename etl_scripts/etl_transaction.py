import pandas as pd
import os
from sqlalchemy import text
from db_connection import get_engine

# ==========================================================
# CONFIG
# ==========================================================
SCHEMA_NAME = "dw"
INPUT_FILE = "../source_data/order_history_kaggle_data.csv"
STAGING_DIR = "../staging_data"
engine = get_engine()

# ==========================================================
# ETL: ORDERS (TRANSACTION SOURCE)
# ==========================================================

def main():
    print("🚀 Starting ETL: Transaction Source (Orders)")

    # 1️⃣ EXTRACT
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File not found: {INPUT_FILE}")
        return

    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"[EXTRACT] Loaded {len(df)} rows from {INPUT_FILE}")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    # 2️⃣ TRANSFORM
    try:
        print("\n[TRANSFORM] Cleaning and normalizing data...")

        # --- Dimension: Restaurant ---
        dim_restaurant = df[['Restaurant ID', 'Restaurant name', 'Subzone', 'City']].copy()
        dim_restaurant.columns = ['restaurant_id', 'restaurant_name', 'subzone', 'city']

        # Xử lý trùng restaurant_id
        for c in ['restaurant_name', 'subzone', 'city']:
            dim_restaurant[c] = dim_restaurant[c].astype(str).str.strip()

        dim_restaurant = (
            dim_restaurant
                .sort_values(['restaurant_id', 'restaurant_name'], na_position='last')
                .drop_duplicates(subset=['restaurant_id'], keep='first')
                .reset_index(drop=True)
        )

        # --- Dimension: Customer Orders ---
        valid_mask = df["Customer ID"].notna()
        df.loc[valid_mask, "Customer ID"] = "ORD_" + df.loc[valid_mask, "Customer ID"].astype(str)
        dim_customer_orders = df[['Customer ID']].drop_duplicates()
        dim_customer_orders.columns = ['customer_id']

        # --- Dimension: Time ---
        df['Order Placed At'] = pd.to_datetime(df['Order Placed At'], format='%I:%M %p, %B %d %Y', errors='coerce')
        dim_time = df[['Order Placed At']].drop_duplicates().rename(columns={'Order Placed At': 'order_placed_at'})
        dim_time['date'] = dim_time['order_placed_at'].dt.date
        dim_time['time'] = dim_time['order_placed_at'].dt.time
        dim_time['day'] = dim_time['order_placed_at'].dt.day
        dim_time['month'] = dim_time['order_placed_at'].dt.month
        dim_time['year'] = dim_time['order_placed_at'].dt.year
        dim_time['weekday'] = dim_time['order_placed_at'].dt.day_name()
        dim_time = dim_time.reset_index(drop=True)

        # --- Fact: Orders ---
        fact_orders = df.rename(columns={
            'Order ID': 'order_id',
            'Restaurant ID': 'restaurant_id',
            'Customer ID': 'customer_id',
            'Order Placed At': 'order_placed_at',
            'Order Status': 'order_status',
            'Delivery': 'delivery_type',
            'Distance': 'distance',
            'Items in order': 'items_in_order',
            'Instructions': 'instructions',
            'Discount construct': 'discount_construct',
            'Bill subtotal': 'bill_subtotal',
            'Packaging charges': 'packaging_charges',
            'Restaurant discount (Promo)': 'restaurant_discount_promo',
            'Restaurant discount (Flat offs, Freebies & others)': 'restaurant_discount_flat',
            'Gold discount': 'gold_discount',
            'Brand pack discount': 'brand_pack_discount',
            'Total': 'total',
            'Cancellation / Rejection reason': 'cancellation_reason',
            'Restaurant compensation (Cancellation)': 'restaurant_compensation',
            'Restaurant penalty (Rejection)': 'restaurant_penalty',
            'KPT duration (minutes)': 'kpt_duration',
            'Rider wait time (minutes)': 'rider_wait_time',
            'Order Ready Marked': 'order_ready_marked'
        })[[
            'order_id', 'restaurant_id', 'customer_id', 'order_placed_at', 'order_status',
            'delivery_type', 'distance', 'items_in_order', 'instructions', 'discount_construct',
            'bill_subtotal', 'packaging_charges', 'restaurant_discount_promo',
            'restaurant_discount_flat', 'gold_discount', 'brand_pack_discount', 'total',
            'cancellation_reason', 'restaurant_compensation', 'restaurant_penalty',
            'kpt_duration', 'rider_wait_time', 'order_ready_marked'
        ]]

        print(f"✅ Transformed: {len(fact_orders)} fact rows, {len(dim_restaurant)} restaurants, {len(dim_customer_orders)} customers")

    except Exception as e:
        print(f"❌ Transform error: {e}")
        return

    # 3️⃣ LOAD
    # ép numeric an toàn cho các cột DECIMAL/INT trong fact_orders 
    num_cols = [
        'bill_subtotal','packaging_charges','restaurant_discount_promo','restaurant_discount_flat',
        'gold_discount','brand_pack_discount','total','restaurant_compensation',
        'restaurant_penalty','kpt_duration','rider_wait_time'
    ]
    for c in num_cols:
        fact_orders[c] = pd.to_numeric(fact_orders[c], errors='coerce')

    # Giữ distance và items_in_order là text
    fact_orders['distance'] = fact_orders['distance'].astype(str)
    fact_orders['items_in_order'] = fact_orders['items_in_order'].astype(str)

    # loại dòng không có khóa chính/ngoại cần thiết
    fact_orders.dropna(subset=['order_id','restaurant_id','customer_id','order_placed_at'], inplace=True)

    # Export staging
    os.makedirs(STAGING_DIR, exist_ok=True)
    dim_restaurant.to_csv(f"{STAGING_DIR}/dim_restaurant_staging.csv", index=False)
    dim_customer_orders.to_csv(f"{STAGING_DIR}/dim_customer_orders_staging.csv", index=False)
    dim_time.to_csv(f"{STAGING_DIR}/dim_time_staging.csv", index=False)
    fact_orders.to_csv(f"{STAGING_DIR}/fact_orders_staging.csv", index=False)
    print(f"💾 Exported 4 staging files to {STAGING_DIR}/")
    print("🟢 Review staging data before loading to DB.\n")

    # --- LOAD ---
    try:
        print("\n[LOAD] Writing to PostgreSQL...")

        # đảm bảo bảng tồn tại
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw.dim_restaurant (
                    restaurant_id VARCHAR(50) PRIMARY KEY,
                    restaurant_name VARCHAR(200),
                    subzone VARCHAR(100),
                    city VARCHAR(100)
                );
                CREATE TABLE IF NOT EXISTS dw.dim_customer_orders (
                    customer_id VARCHAR(50) PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS dw.dim_time (
                    time_key SERIAL PRIMARY KEY,
                    order_placed_at TIMESTAMP,
                    date DATE, time TIME, day INT, month INT, year INT, weekday VARCHAR(20)
                );
                CREATE TABLE IF NOT EXISTS dw.fact_orders (
                    order_id VARCHAR(50) PRIMARY KEY,
                    restaurant_id VARCHAR(50) REFERENCES dw.dim_restaurant(restaurant_id),
                    customer_id VARCHAR(50) REFERENCES dw.dim_customer_orders(customer_id),
                    order_placed_at TIMESTAMP,
                    order_status VARCHAR(50), delivery_type VARCHAR(50),
                    distance TEXT, items_in_order TEXT, instructions TEXT, discount_construct TEXT,
                    bill_subtotal DECIMAL(10,2), packaging_charges DECIMAL(10,2),
                    restaurant_discount_promo DECIMAL(10,2), restaurant_discount_flat DECIMAL(10,2),
                    gold_discount DECIMAL(10,2), brand_pack_discount DECIMAL(10,2), total DECIMAL(10,2),
                    cancellation_reason TEXT, restaurant_compensation DECIMAL(10,2),
                    restaurant_penalty DECIMAL(10,2), kpt_duration DECIMAL(10,2),
                    rider_wait_time DECIMAL(10,2), order_ready_marked VARCHAR(50)
                );
            """))

        # TRUNCATE sạch trước khi nạp
        with engine.begin() as conn:
            for table in ['fact_orders','dim_time','dim_customer_orders','dim_restaurant']:
                conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.{table} CASCADE"))
                print(f"🧹 Truncated {table}")

        # LOAD DIM
        dim_restaurant.to_sql('dim_restaurant', engine, schema=SCHEMA_NAME, if_exists='append', index=False)
        dim_customer_orders.to_sql('dim_customer_orders', engine, schema=SCHEMA_NAME, if_exists='append', index=False)
        dim_time[['order_placed_at','date','time','day','month','year','weekday']].to_sql(
            'dim_time', engine, schema=SCHEMA_NAME, if_exists='append', index=False
        )

        # LOAD FACT
        fact_orders.to_sql('fact_orders', engine, schema=SCHEMA_NAME, if_exists='append', index=False)

        print("\n✅ Loaded all tables successfully!")

    except Exception as e:
        print(f"❌ Load error: {e}")
        return

    print("\n🎯 ETL for Orders completed successfully!\n")


if __name__ == "__main__":
    main()
