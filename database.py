import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        self.initialize_tables()

    @contextmanager
    def get_connection(self):
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)

    @contextmanager
    def get_cursor(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cursor.close()

    def initialize_tables(self):
        with self.get_cursor() as cursor:
            # Users Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Dates Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dates (
                    id SERIAL PRIMARY KEY,
                    date_key VARCHAR(50) UNIQUE NOT NULL,
                    is_open BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Bets Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bets (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    date_id INTEGER REFERENCES dates(id),
                    number INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Break Limits Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS break_limits (
                    id SERIAL PRIMARY KEY,
                    date_id INTEGER REFERENCES dates(id),
                    limit_amount INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date_id)
                )
            """)
            
            # Pnumber Table (Power Numbers)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pnumbers (
                    id SERIAL PRIMARY KEY,
                    date_id INTEGER REFERENCES dates(id),
                    number INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date_id)
                )
            """)
            
            # Comandza Table (Commissions and Za)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS comandza (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    com_percentage INTEGER DEFAULT 0,
                    za_multiplier INTEGER DEFAULT 80,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id)
                )
            """)
            
            # Alldata Table (Combined data for reporting)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alldata (
                    id SERIAL PRIMARY KEY,
                    date_id INTEGER REFERENCES dates(id),
                    user_id INTEGER REFERENCES users(id),
                    total_bet INTEGER DEFAULT 0,
                    power_bet INTEGER DEFAULT 0,
                    commission INTEGER DEFAULT 0,
                    win_amount INTEGER DEFAULT 0,
                    net_result INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date_id, user_id)
                )
            """)

db = Database()
