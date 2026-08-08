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

-- Denormalized cache of the latest price, for a cheap lookup each monitor run.
-- Nullable so existing rows survive the ALTER; NULL means "no price on record yet".
ALTER TABLE properties ADD COLUMN IF NOT EXISTS current_price integer;

-- Full price history: one row per observed price change (including the first).
CREATE TABLE IF NOT EXISTS price_history (
    id            serial PRIMARY KEY,
    property_id   text NOT NULL REFERENCES properties(property_id),
    price         integer NOT NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_price_history_property_recorded
    ON price_history (property_id, recorded_at DESC);

-- Log of "new listing" / "price drop" notifications actually sent. The monitor
-- runs as a separate process every 2 hours, so this is how the last run of the
-- day knows whether an earlier run already found something today.
CREATE TABLE IF NOT EXISTS notification_log (
    id          serial PRIMARY KEY,
    kind        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
