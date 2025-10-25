import pandas as pd
import os
from sqlalchemy import create_engine

# Kết nối tới PostgreSQL (database DW)
engine = create_engine("postgresql+psycopg2://postgres:123456789@localhost:5432/Transaction_Source")

####### ETL PROCESS #######

# 1 Extract - Đọc dữ liệu gốc
file_path = r"D:\MyStorage\Study\y3\1. Data Warehouse\GK\source_data\order_history_kaggle_data.csv"
if not os.path.exists(file_path):
    raise FileNotFoundError(f" Không tìm thấy file CSV tại: {file_path}")

df = pd.read_csv(file_path)


# 2️ Transform - Chuẩn hóa dữ liệu

## Dimension: Restaurant
dim_restaurant = df[['Restaurant ID', 'Restaurant name', 'Subzone', 'City']].drop_duplicates()
dim_restaurant.columns = ['restaurant_id', 'restaurant_name', 'subzone', 'city']

## Dimension: Customer
df["Customer ID"] = "ORD_" + df["Customer ID"].astype(str)
dim_customer_orders = df[['Customer ID']].drop_duplicates()
dim_customer_orders.columns = ['customer_id']

## Dimension: Time
df['Order Placed At'] = pd.to_datetime(df['Order Placed At'], format='%I:%M %p, %B %d %Y')
dim_time = df[['Order Placed At']].drop_duplicates()
dim_time.rename(columns={'Order Placed At': 'order_placed_at'}, inplace=True)

dim_time['date'] = dim_time['order_placed_at'].dt.date
dim_time['time'] = dim_time['order_placed_at'].dt.time
dim_time['day'] = dim_time['order_placed_at'].dt.day
dim_time['month'] = dim_time['order_placed_at'].dt.month
dim_time['year'] = dim_time['order_placed_at'].dt.year
dim_time['weekday'] = dim_time['order_placed_at'].dt.day_name()
dim_time = dim_time.reset_index(drop=True)
dim_time['time_key'] = dim_time.index + 1

## Fact: Orders (loại bỏ toàn bộ các cột đánh giá / review / complaint)
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
})[
    [
        'order_id', 'restaurant_id', 'customer_id', 'order_placed_at', 'order_status',
        'delivery_type', 'distance', 'items_in_order', 'instructions', 'discount_construct',
        'bill_subtotal', 'packaging_charges', 'restaurant_discount_promo',
        'restaurant_discount_flat', 'gold_discount', 'brand_pack_discount', 'total',
        'cancellation_reason', 'restaurant_compensation', 'restaurant_penalty',
        'kpt_duration', 'rider_wait_time', 'order_ready_marked'
    ]
]

# 3️ Load - Ghi dữ liệu vào PostgreSQL (append)
from sqlalchemy.dialects.postgresql import insert

def upsert_dataframe(df, table_name, conn, conflict_cols):
    """Chèn dữ liệu, nếu trùng khóa thì bỏ qua."""
    for _, row in df.iterrows():
        stmt = insert(table_name).values(**row.to_dict()).on_conflict_do_nothing(index_elements=conflict_cols)
        conn.execute(stmt)

from sqlalchemy import MetaData, Table

with engine.begin() as conn:
    metadata = MetaData()
    metadata.reflect(bind=conn)
    
    # Load các bảng đã có trong DB
    dim_restaurant_tbl = Table('dim_restaurant', metadata, autoload_with=conn)
    dim_customer_tbl = Table('dim_customer_orders', metadata, autoload_with=conn)
    dim_time_tbl = Table('dim_time', metadata, autoload_with=conn)
    fact_orders_tbl = Table('fact_orders', metadata, autoload_with=conn)
    
    # Chèn dữ liệu, bỏ qua nếu đã tồn tại
    upsert_dataframe(dim_restaurant, dim_restaurant_tbl, conn, ['restaurant_id'])
    upsert_dataframe(dim_customer_orders, dim_customer_tbl, conn, ['customer_id'])
    upsert_dataframe(dim_time, dim_time_tbl, conn, ['time_key'])
    upsert_dataframe(fact_orders, fact_orders_tbl, conn, ['order_id'])

print(" ETL completed — duplicates ignored, data appended safely.")

