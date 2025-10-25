import pandas as pd
import re
from textblob import TextBlob
import psycopg2

# =========================
# 1. EXTRACT - Đọc dữ liệu
# =========================
df = pd.read_csv("Reviews.csv", encoding='latin1')

# =========================
# 2. TRANSFORM - Làm sạch & Chuẩn hóa
# =========================

# Giữ lại cột cần thiết
df = df[['UserId', 'Score', 'Time', 'Summary']]

# Loại bỏ dòng trùng lặp
df.drop_duplicates(inplace=True)

# Loại bỏ dòng giá trị thiếu
df.dropna(subset=['UserId', 'Score', 'Summary'], inplace=True)

# Chuẩn hóa cột user_id
df['user_id'] = 'REV_' + df['UserId'].astype(str)

# Xóa cột UserId cũ
df = df.drop(columns=['UserId'])

# Tính điểm cảm xúc (sentiment)
def get_sentiment(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return 0

df['sentiment_score'] = df['Summary'].astype(str).apply(get_sentiment)

# Đưa các cột về đúng thứ tự cần tải
df = df[['user_id', 'Score', 'Time', 'Summary', 'sentiment_score']]

# =========================
# 3. LOAD - Tải vào PostgreSQL
# =========================

# Thông tin kết nối DB (điền đúng thông số của bạn)
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="soosdden2018",
    port=5432
)

cur = conn.cursor()

# Chèn từng dòng dữ liệu
for index, row in df.iterrows():
    cur.execute("""
        INSERT INTO fact_reviews (user_id, score, time, summary, sentiment_score)
        VALUES (%s, %s, %s, %s, %s)
    """, (row['user_id'], row['Score'], row['Time'], row['Summary'], row['sentiment_score']))

# Xác nhận và đóng kết nối
conn.commit()
cur.close()
conn.close()

print("✅ ETL Completed Successfully")