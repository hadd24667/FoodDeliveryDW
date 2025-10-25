import pandas as pd
from textblob import TextBlob
from sqlalchemy import text
from db_connection import get_engine

INPUT_FILE = "../source_data/Reviews.csv"
SCHEMA_NAME = "dw"

engine = get_engine()

def get_sentiment(text_input):
    try:
        return TextBlob(text_input).sentiment.polarity
    except:
        return 0

def main():
    print("🚀 Starting ETL: Reviews Fact")

    # Extract
    try:
        df = pd.read_csv(INPUT_FILE, encoding='latin1')
        print(f"[EXTRACT] Loaded {len(df)} rows from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"❌ File not found: {INPUT_FILE}")
        return
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    # Transform
    df = df[['UserId', 'Score', 'Time', 'Summary']].drop_duplicates()
    df.dropna(subset=['UserId', 'Score', 'Summary'], inplace=True)
    df['reviewer_id'] = 'REV_' + df['UserId'].astype(str)   # 👈 Đổi user_id → reviewer_id
    df['sentiment_score'] = df['Summary'].astype(str).apply(get_sentiment)
    # Đổi tên cột sang lowercase cho khớp PostgreSQL
    df = df.rename(columns={
        'UserId': 'user_id',
        'Score': 'score',
        'Time': 'time',
        'Summary': 'summary'
    })

    df = df[['reviewer_id', 'score', 'time', 'summary', 'sentiment_score']]


    df.to_csv("../staging_data/fact_reviews_preview.csv", index=False)
    print("💾 Exported preview: staging_data/fact_reviews_preview.csv")


    # -> build dimension + DDL tối thiểu
    dim_reviewer = df[['reviewer_id']].drop_duplicates()

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw.dim_reviewer (
                reviewer_id VARCHAR(50) PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS dw.fact_reviews (
                reviewer_id VARCHAR(50) REFERENCES dw.dim_reviewer(reviewer_id),
                score INT, time VARCHAR(50), summary TEXT, sentiment_score FLOAT
                );
            """))

        # nạp dimension trước
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.fact_reviews CASCADE"))
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA_NAME}.dim_reviewer CASCADE"))
        dim_reviewer.to_sql('dim_reviewer', engine, schema=SCHEMA_NAME, if_exists='append', index=False)

        # nạp fact
        df.to_sql('fact_reviews', engine, schema=SCHEMA_NAME, if_exists='append', index=False)
        print(f"✅ Loaded {len(df)} rows into fact_reviews")
    except Exception as e:
        print(f"❌ Load error: {e}")

    print("🎯 Reviews ETL completed.\n")

if __name__ == "__main__":
    main()
