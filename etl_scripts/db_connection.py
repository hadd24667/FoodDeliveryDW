# etl_scripts/db_connection.py
from sqlalchemy import create_engine, text
import psycopg2

# --- Cấu hình kết nối PostgreSQL (chỉ sửa 1 lần ở đây) ---
DB_CONFIG = {
    "USER": "postgres",
    "PASSWORD": "12345678",
    "HOST": "localhost",
    "PORT": "5432",
    "NAME": "food_delivery_dw"
}

# --- Tạo SQLAlchemy engine (cho pandas.to_sql, etc.) ---
def get_engine():
    conn_str = f"postgresql://{DB_CONFIG['USER']}:{DB_CONFIG['PASSWORD']}@" \
               f"{DB_CONFIG['HOST']}:{DB_CONFIG['PORT']}/{DB_CONFIG['NAME']}"
    return create_engine(conn_str)

# --- Tạo kết nối psycopg2 (cho thao tác cursor thủ công) ---
def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["HOST"],
        database=DB_CONFIG["NAME"],
        user=DB_CONFIG["USER"],
        password=DB_CONFIG["PASSWORD"],
        port=DB_CONFIG["PORT"]
    )
