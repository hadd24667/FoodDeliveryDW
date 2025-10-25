import pandas as pd
import psycopg2
import numpy as np

# =====================
# 1. EXTRACT: Đọc dữ liệu
# =====================
df = pd.read_csv("customers.csv")

# =====================
# 2. TRANSFORM: Làm sạch & chuẩn hóa
# =====================
# Chuẩn hóa tên cột để khớp với DB
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

# Bỏ hàng có dữ liệu rỗng hoặc không hợp lệ
df.dropna(inplace=True)

# Chuẩn hóa kiểu dữ liệu
df['age'] = df['age'].astype(int)
df['family_size'] = df['family_size'].astype(int)
df['restaurant_rating'] = df['restaurant_rating'].astype(int)
df['delivery_rating'] = df['delivery_rating'].astype(int)
df['orders_placed'] = df['orders_placed'].astype(int)
df['order_value'] = df['order_value'].astype(int)

# Sinh customer_id dạng CUS_1, CUS_2, ...
df['customer_id'] = ['CUS_' + str(i + 1) for i in range(len(df))]

# Mapping các giá trị định tính sang số
scale_map = {
    'Strongly agree': 5,
    'Agree': 4,
    'Neutral': 3,
    'Disagree': 2,
    'Strongly disagree': 1
}

for col in ['ease_convenience', 'health_concern', 'bad_experience', 'more_offers_discount']:
    df[col] = df[col].map(scale_map).fillna(3)  # nếu không có giá trị thì gán 3 (Neutral)

# self_cooking, late_delivery, poor_hygiene là dạng Yes/No → chuyển thành 1/0
binary_cols = ['self_cooking', 'late_delivery', 'poor_hygiene']
for col in binary_cols:
    df[col] = df[col].apply(lambda x: 1 if str(x).strip().lower() in ['yes', 'true', '1'] else 0)

# =====================
# 3. LOAD: Ghi vào PostgreSQL
# =====================

try:
    conn = psycopg2.connect(
        dbname="food_delivery_dw",
        user="admin",
        password="yourpassword",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_customer (
                customer_id, age, gender, marital_status, occupation, education, family_size,
                medium_used, meal_category, preference, restaurant_rating, delivery_rating,
                orders_placed, delivery_time, order_value, ease_convenience, self_cooking,
                health_concern, late_delivery, poor_hygiene, bad_experience,
                more_offers_discount, max_wait_time, influence_of_rating
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    print("Dữ liệu khách hàng đã được load thành công vào dim_customer!")

except Exception as e:
    print("Lỗi khi load dữ liệu:", e)

finally:
    cursor.close()
    conn.close()
