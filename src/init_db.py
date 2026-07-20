# Create the database schema (run once against a fresh database).
import os

from dotenv import load_dotenv

load_dotenv()

from src.db import connect_db, update_db, show_tables, end_connection


def main():
    conn = connect_db()

    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        ddl = f.read()

    update_db(conn, ddl)
    print("Schema applied. Tables now in the database:")
    show_tables(conn)

    end_connection(conn)


if __name__ == "__main__":
    main()
