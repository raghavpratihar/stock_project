import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DB_NAME = os.getenv('DB_NAME', 'stocks_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

def create_database():
    """Create the database if it doesn't exist"""
    conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (DB_NAME,))
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute(f'CREATE DATABASE {DB_NAME}')
        print(f"Database {DB_NAME} created successfully")
    
    cursor.close()
    conn.close()

def setup_tables():
    """Create necessary tables in the database"""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    
    # Create stocks table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            name VARCHAR(255) NOT NULL,
            price DECIMAL,
            consolidated BOOLEAN,
            avg_eps DECIMAL,
            total_eps INTEGER,
            three_yr_avg_eps DECIMAL,
            fifteen_times_eps DECIMAL,
            current_market_price DECIMAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

def insert_data():
    """Insert data from Excel file into the database"""
    # Read the Excel file
    df = pd.read_excel('LOW.xlsx')
    
    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Insert data into the database
    df.to_sql('stocks', engine, if_exists='replace', index=False)
    print("Data inserted successfully")

def main():
    try:
        create_database()
        setup_tables()
        insert_data()
        print("Database setup and data insertion completed successfully")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 