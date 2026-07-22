# Scrape the configured Rightmove searches, compare against listings stored in
# the database, and send any new listings to Telegram.
import argparse

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
    data = data.drop(columns=["search_date", "agent_url", "full_postcode", "price"])

    # Rename columns to match the DB schema.
    data = data.rename(columns={
        "number_bedrooms": "bedrooms",
        "type": "property_type",
    })

    return data


def main(seed=False):
    """Run the monitor over every search in SEARCHES.

    Args:
        seed (bool): one-time initial seeding. Record all current listings
            WITHOUT sending notifications, so the first real run doesn't send a
            burst of messages for listings that already exist. Also use this
            after adding a new search to src/searches.py.
    """
    conn = connect_db()

    # Read all known property IDs once; the anti-join is global, so a property
    # already found by one search is never re-notified by an overlapping one.
    rows, _ = query_db(conn, f"select property_id from {TABLE_NAME};")
    known_ids = {str(row[0]) for row in rows}

    total_new = 0
    for search_name, search_url in SEARCHES.items():
        print(f"--- Search: {search_name} ---")
        data = scrape_listings(search_url)
        new_listings = data[~data["property_id"].isin(known_ids)].copy()

        if new_listings.empty:
            print(f"{search_name}: no new listings.")
            continue

        new_listings["search_name"] = search_name
        # Track these IDs so overlapping searches later in the run skip them.
        known_ids.update(new_listings["property_id"])

        if not seed:
            for _, new_listing in new_listings.iterrows():
                message = (
                    "New property for sale\n"
                    f"Search: {search_name}\n"
                    f"Address: {new_listing['address']}\n"
                    f"URL: {new_listing['url']}"
                )
                send_message(bot_token, chat_id, message)

        insert_data(conn, new_listings, TABLE_NAME)
        total_new += len(new_listings)
        print(f"{search_name}: recorded {len(new_listings)} new listings.")

    if seed:
        print(f"Seed complete: recorded {total_new} listings, no notifications sent.")
    elif total_new == 0:
        send_message(bot_token, chat_id, "No new listings found.")

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
