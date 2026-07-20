# Scrape a Rightmove search, compare against listings stored in the database,
# and send any new listings to Telegram.
import argparse

import pandas as pd
from dotenv import load_dotenv

# Load .env before importing modules that read environment variables at import time.
load_dotenv()

from rightmove_webscraper.scraper import RightmoveData
from src.notifications import send_message, bot_token, chat_id
from src.db import connect_db, query_db, insert_data, end_connection

# The Rightmove search to monitor.
# NOTE: do not include an `index=` parameter here — the scraper appends its own
# for pagination, and a hardcoded index pins every page to the first 25 results.
SEARCH_URL = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced&sortType=2&channel=BUY&transactionType=BUY&displayLocationIdentifier=Crosby&radius=1.0"

TABLE_NAME = "crosby_properties"


def scrape_listings():
    """Scrape the search URL and return a cleaned DataFrame of current listings."""
    rmd = RightmoveData(SEARCH_URL)
    print(f"Results count: {rmd.results_count}")

    data = rmd.get_results

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
    """Run the monitor.

    Args:
        seed (bool): one-time initial seeding. Record all current listings
            WITHOUT sending notifications, so the first real run doesn't send a
            burst of messages for listings that already exist.
    """
    conn = connect_db()

    # Read existing listings from the DB.
    prev_data, col_names = query_db(conn, f"select * from {TABLE_NAME};")
    prev_df = pd.DataFrame(prev_data, columns=col_names)
    prev_df["property_id"] = prev_df["property_id"].astype(str)

    # Scrape and keep only listings not already stored.
    new_data = scrape_listings()
    new_listings = new_data[~new_data["property_id"].isin(prev_df["property_id"])]

    if seed:
        # Silent seeding: record current listings, no messages.
        if new_listings.empty:
            print("Seed complete: nothing to record (table already up to date).")
        else:
            insert_data(conn, new_listings, TABLE_NAME)
            print(f"Seed complete: recorded {len(new_listings)} listings, no notifications sent.")
    elif new_listings.empty:
        send_message(bot_token, chat_id, "No new listings found.")
    else:
        for _, new_listing in new_listings.iterrows():
            message = (
                "New property for sale\n"
                f"Address: {new_listing['address']}\n"
                f"URL: {new_listing['url']}"
            )
            send_message(bot_token, chat_id, message)

        insert_data(conn, new_listings, TABLE_NAME)

    end_connection(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rightmove listing monitor.")
    parser.add_argument(
        "--seed",
        action="store_true",
        help="One-time initial seeding: record current listings without sending notifications.",
    )
    args = parser.parse_args()
    main(seed=args.seed)
