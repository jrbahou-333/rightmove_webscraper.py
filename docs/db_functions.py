# This Script deals with all the functions required to connect, query and update the postgresql database. 
import psycopg2

def connect_db(db_name):
    """Connect to the PostgreSQL database specified by db_name."""
    try:
        conn = psycopg2.connect(database=db_name, user="postgres", password="Amman123", host="localhost", port="5432")
        print("Connection to database successful.")
        return conn
    except psycopg2.Error as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None

# conn = connect_db("house_db")

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
    """Execute a query on the database and return the results. include headers"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        print("Query executed successfully.", columns)
        return (results, columns)
    except psycopg2.Error as e:
        print(f"An error occurred while executing the query: {e}")
        return None
    
def get_col_types(conn, table_name):
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'; ;
    """
    return query_db(conn, query)

# results = query_db(conn, "select * from house_info limit 5;")

def update_db(conn, query):
    """Execute an update query on the database."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print("Update executed successfully.")
    except psycopg2.Error as e:
        print(f"An error occurred while executing the update: {e}")
        conn.rollback()

# results = update_db(conn, "insert into house_info values (1, '01/02/2026', 'detached');")

def insert_data(conn, data, table_name):
    """Insert DataFrame into PostgreSQL safely."""
    try:
        cur = conn.cursor()

        cols = list(data.columns)
        col_str = ", ".join(cols)

        query = f"""
        INSERT INTO {table_name} ({col_str})
        VALUES ({", ".join(["%s"] * len(cols))})
        """

        for row in data.itertuples(index=False, name=None):
            cur.execute(query, row)

        conn.commit()

        print("Data inserted successfully.")

        results = query_db(conn, f"SELECT * FROM {table_name} LIMIT 5;")
        print(results)

    except Exception as e:
        conn.rollback()
        print("Error:", e)

    except psycopg2.Error as e:
        print(f"An error occurred while inserting data: {e}")
        conn.rollback()

def insert_data_csv(conn, csv_path, table_name):
    """Insert data from a CSV file into the specified table in the database."""
    try:
        cur = conn.cursor()
        with open(csv_path, "r") as f:
            # Skip the header row
            next(f)
            cur.copy_expert(f"COPY {table_name} FROM {csv_path} WITH CSV", f)
            conn.commit()
        print("Data inserted successfully.\nResults:")

        # print first 5 rows
        results = query_db(conn, f"select * from {table_name} limit 5;")
        print(results)

    except psycopg2.Error as e:
        print(f"An error occurred while inserting data: {e}")
        conn.rollback()
    f.close()

def end_connection(conn, cursor = None):
    """Close the database connection."""
    try:
        if cursor:
            cursor.close()
        conn.close()

        print("Database connection closed.")
    except psycopg2.Error as e:
        print(f"An error occurred while closing the database connection: {e}")

# conn = connect_db("house_db")

# insert_data(conn, "./docs/rightmove_crosby_all_data_cleaned.csv", "house_info_test")

# query = "COPY house_info_test FROM './docs/rightmove_crosby_all_data_cleaned.csv' DELIMITER ',' CSV HEADER;"

# update_db(conn, query)

# # check it was dropped:
# query = "select * from house_info_test;" 
# results = query_db(conn, query)
# print(results)