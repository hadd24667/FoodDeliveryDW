import pandas as pd
from sqlalchemy import text
from db_connection import get_engine

SCHEMA_NAME = "dw"
INPUT_FILE = "../source_data/customers.csv"

engine = get_engine()

def main():
    print("🚀 Starting ETL: Customer Dimension")

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
    df.rename(columns={
        'Age': 'age',
        'Gender': 'gender',
        'Marital Status': 'marital_status',
        'Occupation': 'occupation',
        'Educational Qualifications': 'education',
        'Family size': 'family_size',
        'Frequently used Medium': 'medium_used',
        'Frequently ordered Meal category ': 'meal_category',
        'Perference': 'preference',
        'Restaurnat Rating': 'restaurant_rating',
        'Delivery Rating': 'delivery_rating',
        'No. of orders placed': 'orders_placed',
        'Delivery Time': 'delivery_time',
        'Order Value': 'order_value',
        'Ease and convenient': 'ease_convenience',
        'Self Cooking': 'self_cooking',
        'Health Concern': 'health_concern',
        'Late Delivery': 'late_delivery',
        'Poor Hygiene': 'poor_hygiene',
        'Bad past experience': 'bad_experience',
        'More Offers and Discount': 'more_offers_discount',
        'Maximum wait time': 'max_wait_time',
        'Influence of rating': 'influence_of_rating'
    }, inplace=True)

    # tạo surrogate id cho dim_customer
    df['customer_id'] = ['CUS_' + str(i + 1) for i in range(len(df))]

    # ép numeric an toàn (không crash), loại bỏ dòng hỏng bắt buộc
    numeric_cols = [
        'age','family_size','restaurant_rating','delivery_rating',
        'orders_placed','order_value','delivery_time'
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # map thang đo đồng ý → số (nếu trống thì để 3 – Neutral)
    scale_map = {
        'Strongly agree': 5, 'Agree': 4, 'Neutral': 3,
        'Disagree': 2, 'Strongly disagree': 1
    }
    for col in ['ease_convenience','health_concern','bad_experience','more_offers_discount']:
        df[col] = df[col].map(scale_map).fillna(3)

    # boolean chuẩn
    for col in ['self_cooking','late_delivery','poor_hygiene']:
        df[col] = df[col].apply(lambda x: str(x).strip().lower() in ['yes','true','1'])

    # bắt buộc các cột numeric tối thiểu phải có (loại dòng bẩn)
    df.dropna(subset=['age','family_size','restaurant_rating','delivery_rating','orders_placed','order_value'], inplace=True)

    # ép về int thật sự cho các cột INT sau khi đã dropna
    int_cols = ['age','family_size','restaurant_rating','delivery_rating','orders_placed','order_value']
    for c in int_cols:
        df[c] = df[c].astype(int)


    # Sau khi hoàn tất chuẩn hoá df
    df.to_csv("../staging_data/dim_customer_preview.csv", index=False)
    print("💾 Exported preview: staging_data/dim_customer_preview.csv")



    # Load
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.dim_customer CASCADE"))
        df.to_sql('dim_customer', engine, schema=SCHEMA_NAME, if_exists='append', index=False)
        print(f"✅ Loaded {len(df)} rows into dim_customer")
    except Exception as e:
        print(f"❌ Load error: {e}")

    print("🎯 Customer ETL completed.\n")

if __name__ == "__main__":
    main()
