# One-time migration: create the shared `properties` table and copy the rows
# from the old per-location `crosby_properties` table into it.
# Safe to re-run: the schema uses CREATE TABLE IF NOT EXISTS and the copy uses
# ON CONFLICT DO NOTHING. The old table is NOT dropped — do that manually once
# you're happy with the result.
import os

from dotenv import load_dotenv

load_dotenv()

from src.db import connect_db, update_db, query_db, show_tables, end_connection


def main():
    conn = connect_db()

    # 1. Create the new shared table.
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        ddl = f.read()
    update_db(conn, ddl)

    # 2. Copy existing crosby rows across.
    update_db(conn, """
        INSERT INTO properties (property_id, search_name, address, postcode, bedrooms, property_type, url)
        SELECT property_id, 'crosby', address, postcode, bedrooms, property_type, url
        FROM crosby_properties
        ON CONFLICT (property_id) DO NOTHING;
    """)

    # 3. Report.
    counts, _ = query_db(conn, "SELECT search_name, count(*) FROM properties GROUP BY search_name;")
    print("Rows in properties by search_name:")
    for search_name, count in counts:
        print(f"  {search_name}: {count}")
    show_tables(conn)
    print("Migration complete. crosby_properties was left in place — drop it manually once satisfied:")
    print("  DROP TABLE crosby_properties;")

    end_connection(conn)


if __name__ == "__main__":
    main()
