from customer_etl import main as customer_main
from etl_event_script import main as event_main
from etl_reviews import main as reviews_main
from etl_transaction import main as transaction_main

def main():
    print(" Starting all ETL jobs...\n")
    customer_main()
    transaction_main()
    event_main()
    reviews_main()
    print("\n All ETL jobs finished successfully!")

if __name__ == "__main__":
    main()
