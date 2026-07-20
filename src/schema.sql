-- Schema for the Rightmove listing monitor.
-- Columns match what src/monitor.py inserts (see insert_data).
-- Stored as text: bedrooms may be NULL or values like "0" (studio), so text is safest.
CREATE TABLE IF NOT EXISTS crosby_properties (
    property_id   text PRIMARY KEY,
    address       text,
    postcode      text,
    bedrooms      text,
    property_type text,
    url           text
);
