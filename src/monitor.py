# Scrape the configured Rightmove searches, compare against listings stored in
# the database, and send any new listings to Telegram.
import argparse
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

# Load .env before importing modules that read environment variables at import time.
load_dotenv()

from rightmove_webscraper.scraper import RightmoveData
from src.searches import SEARCHES
from src.notifications import send_message, bot_token, chat_id
from src.db import connect_db, query_db, insert_data, end_connection

TABLE_NAME = "properties"


def scrape_listings(search_url):
    """Scrape a search URL and return a cleaned DataFrame of current listings."""
    rmd = RightmoveData(search_url)
    print(f"Results count: {rmd.results_count}")

    # Copy so our cleanup below doesn't trigger SettingWithCopyWarning.
    data = rmd.get_results.copy()

    # Extract the property ID from the listing URL.
    data["property_id"] = data["url"].str.extract(r"/properties/(\d+)#/")

    # Drop duplicates on property_id.
    data = data.drop_duplicates(subset=["property_id"], keep="first")

    # Use full_postcode when available, otherwise fall back to postcode.
    data["postcode"] = data.apply(
        lambda row: row["full_postcode"] if pd.notnull(row["full_postcode"]) else row["postcode"],
        axis=1,
    )

    # Drop columns not stored in the DB.
    data = data.drop(columns=["search_date", "agent_url", "full_postcode"])

    # Rename columns to match the DB schema.
    data = data.rename(columns={
        "number_bedrooms": "bedrooms",
        "type": "property_type",
    })

    return data


def _record_price(conn, property_id, price):
    """Insert a price_history row and update properties.current_price to match."""
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO price_history (property_id, price) VALUES (%s, %s);",
            (property_id, price),
        )
        cur.execute(
            "UPDATE properties SET current_price = %s WHERE property_id = %s;",
            (price, property_id),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error recording price:", e)


def _log_notification(conn, kind):
    """Record that a 'new_listing' or 'price_drop' notification was sent."""
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO notification_log (kind) VALUES (%s);", (kind,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error logging notification:", e)


def _found_something_today(conn):
    """Whether a new_listing/price_drop notification has already gone out today (UTC).

    Each cron run is a separate process, so this is how the last run of the day
    knows whether an earlier run already found something worth notifying about.
    """
    now = datetime.now(timezone.utc)
    start_of_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM notification_log WHERE kind IN ('new_listing', 'price_drop') AND created_at >= %s;",
        (start_of_day,),
    )
    return cur.fetchone()[0] > 0


def main(seed=False):
    """Run the monitor over every search in SEARCHES.

    Args:
        seed (bool): one-time initial seeding. Record all current listings
            WITHOUT sending notifications, so the first real run doesn't send a
            burst of messages for listings that already exist. Also use this
            after adding a new search to src/searches.py.
    """
    conn = connect_db()

    # Read all known property IDs and their last-seen price once; the anti-join
    # is global, so a property already found by one search is never re-notified
    # by an overlapping one.
    rows, _ = query_db(conn, f"SELECT property_id, current_price FROM {TABLE_NAME};")
    known_prices = {str(row[0]): row[1] for row in rows}
    known_ids = set(known_prices.keys())

    total_new = 0
    total_drops = 0
    for search_name, search_url in SEARCHES.items():
        print(f"--- Search: {search_name} ---")
        data = scrape_listings(search_url)
        new_listings = data[~data["property_id"].isin(known_ids)].copy()
        existing_listings = data[data["property_id"].isin(known_ids)].copy()

        # --- New listings ---
        if new_listings.empty:
            print(f"{search_name}: no new listings.")
        else:
            new_listings["search_name"] = search_name
            # Track these IDs so overlapping searches later in the run skip them.
            known_ids.update(new_listings["property_id"])

            if not seed:
                for _, new_listing in new_listings.iterrows():
                    price_line = (
                        f"Price: £{new_listing['price']:,.0f}\n"
                        if pd.notnull(new_listing["price"])
                        else ""
                    )
                    message = (
                        "New property for sale\n"
                        f"Search: {search_name}\n"
                        f"{price_line}"
                        f"Address: {new_listing['address']}\n"
                        f"URL: {new_listing['url']}"
                    )
                    send_message(bot_token, chat_id, message)
                    _log_notification(conn, "new_listing")

            insert_df = new_listings.rename(columns={"price": "current_price"})
            insert_data(conn, insert_df, TABLE_NAME)

            for _, new_listing in new_listings.iterrows():
                if pd.notnull(new_listing["price"]):
                    price = int(new_listing["price"])
                    _record_price(conn, new_listing["property_id"], price)
                    known_prices[new_listing["property_id"]] = price

            total_new += len(new_listings)
            print(f"{search_name}: recorded {len(new_listings)} new listings.")

        # --- Existing listings: check for price changes ---
        drops_this_search = 0
        for _, listing in existing_listings.iterrows():
            if pd.isna(listing["price"]):
                continue
            pid = listing["property_id"]
            scraped_price = int(listing["price"])
            stored_price = known_prices.get(pid)

            if stored_price is None:
                # Legacy property with no price on record yet (e.g. right after
                # the price-tracking migration) — record silently, no notification.
                _record_price(conn, pid, scraped_price)
                known_prices[pid] = scraped_price
                continue

            if scraped_price == stored_price:
                continue

            _record_price(conn, pid, scraped_price)
            known_prices[pid] = scraped_price

            if scraped_price < stored_price:
                drop = stored_price - scraped_price
                pct = drop / stored_price * 100
                drops_this_search += 1
                if not seed:
                    message = (
                        "Price drop!\n"
                        f"Address: {listing['address']}\n"
                        f"Was: £{stored_price:,}\n"
                        f"Now: £{scraped_price:,}\n"
                        f"Drop: £{drop:,} ({pct:.1f}%)\n"
                        f"URL: {listing['url']}"
                    )
                    send_message(bot_token, chat_id, message)
                    _log_notification(conn, "price_drop")

        if drops_this_search:
            print(f"{search_name}: {drops_this_search} price drop(s) recorded.")
        total_drops += drops_this_search

    if seed:
        print(f"Seed complete: recorded {total_new} listings, no notifications sent.")
    elif total_new == 0 and total_drops == 0:
        # Only notify on the last scheduled run of the day (21:00 UTC) to reduce noise,
        # and only if nothing was found by an earlier run today either.
        current_hour = datetime.now(timezone.utc).hour
        if current_hour >= 21:
            if _found_something_today(conn):
                print("Nothing found this run, but an earlier run today already found something — skipping notification.")
            else:
                send_message(bot_token, chat_id, "No new listings or price changes found today.")
        else:
            print("No new listings or price changes — skipping notification (not last run of the day).")

    end_connection(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rightmove listing monitor.")
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Record current listings without sending notifications (initial seeding, or after adding a new search).",
    )
    args = parser.parse_args()
    main(seed=args.seed)
