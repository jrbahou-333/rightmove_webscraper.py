# One-time migration: add price tracking (properties.current_price and the
# price_history table) via the DDL in schema.sql.
# Safe to re-run: every statement in schema.sql is idempotent (CREATE ... IF
# NOT EXISTS / ADD COLUMN IF NOT EXISTS). No data is copied here — the next
# monitor run will silently record a price for every existing property.
import os

from dotenv import load_dotenv

load_dotenv()

from src.db import connect_db, update_db, query_db, show_tables, end_connection


def main():
    conn = connect_db()

    # 1. Apply the schema, including the new current_price column and price_history table.
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        ddl = f.read()
    update_db(conn, ddl)

    # 2. Report.
    rows, _ = query_db(conn, "SELECT count(*) FROM properties WHERE current_price IS NULL;")
    print(f"Properties awaiting an initial price (current_price IS NULL): {rows[0][0]}")

    rows, _ = query_db(conn, "SELECT count(*) FROM price_history;")
    print(f"Rows in price_history: {rows[0][0]}")

    show_tables(conn)
    print("Migration complete. The next monitor run will record an initial price")
    print("for every existing property without sending a notification.")

    end_connection(conn)


if __name__ == "__main__":
    main()
