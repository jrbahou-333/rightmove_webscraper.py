# Functions to connect, query and update the properties database.
# Connection details come from the DATABASE_URL environment variable, so the same
# code works against a local Postgres or a cloud-hosted one by changing one value.
import os
import pandas as pd
import psycopg2


def connect_db():
    """Connect to the database specified by the DATABASE_URL environment variable."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable must be set.")
    try:
        conn = psycopg2.connect(database_url)
        print("Connection to database successful.")
        return conn
    except psycopg2.Error as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None


def show_tables(conn):
    """Show all tables in the connected database."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table[0])
    except psycopg2.Error as e:
        print(f"An error occurred while fetching tables: {e}")


def query_db(conn, query):
    """Execute a query and return (rows, column_names)."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        print("Query executed successfully.", columns)
        return (results, columns)
    except psycopg2.Error as e:
        print(f"An error occurred while executing the query: {e}")
        return None


def update_db(conn, query):
    """Execute an update/DDL query on the database."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print("Update executed successfully.")
    except psycopg2.Error as e:
        print(f"An error occurred while executing the update: {e}")
        conn.rollback()


def insert_data(conn, data, table_name):
    """Insert a pandas DataFrame into the given table."""
    try:
        cur = conn.cursor()

        cols = list(data.columns)
        col_str = ", ".join(cols)

        query = f"""
        INSERT INTO {table_name} ({col_str})
        VALUES ({", ".join(["%s"] * len(cols))})
        """

        for row in data.itertuples(index=False, name=None):
            # Convert pandas NaN/NA to None so they insert as SQL NULL, not "NaN".
            clean_row = tuple(None if pd.isna(v) else v for v in row)
            cur.execute(query, clean_row)

        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        conn.rollback()
        print("Error:", e)


def end_connection(conn, cursor=None):
    """Close the database connection."""
    try:
        if cursor:
            cursor.close()
        conn.close()
        print("Database connection closed.")
    except psycopg2.Error as e:
        print(f"An error occurred while closing the database connection: {e}")
