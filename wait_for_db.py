import time
import psycopg2
from psycopg2 import OperationalError
from loader_django import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

def wait_for_db():
    db_name = POSTGRES_DB
    db_user = POSTGRES_USER
    db_password = POSTGRES_PASSWORD
    db_host = 'db'
    db_port = 5432

    print("⏳ Waiting for PostgreSQL to be ready...")

    while True:
        try:
            conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            conn.close()
            print("✅ PostgreSQL is available!")
            break
        except Exception as e:
            print(f"🔁 PostgreSQL not ready yet, retrying in 1s... Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    wait_for_db()
