-- Schema for the Rightmove listing monitor.
-- One shared table for all searches; search_name records which search (see
-- src/searches.py) first found the property. Columns match what
-- src/monitor.py inserts (see insert_data).
-- Stored as text: bedrooms may be NULL or values like "0" (studio), so text is safest.
CREATE TABLE IF NOT EXISTS properties (
    property_id   text PRIMARY KEY,
    search_name   text NOT NULL,
    address       text,
    postcode      text,
    bedrooms      text,
    property_type text,
    url           text
);
