# Scrape a Rightmove search, compare against listings stored in the database,
# and send any new listings to Telegram.
import pandas as pd
from dotenv import load_dotenv

# Load .env before importing modules that read environment variables at import time.
load_dotenv()

from rightmove_webscraper.scraper import RightmoveData
from src.notifications import send_message, bot_token, chat_id
from src.db import connect_db, query_db, insert_data, end_connection

# The Rightmove search to monitor.
SEARCH_URL = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced&sortType=2&channel=BUY&transactionType=BUY&displayLocationIdentifier=Crosby&index=0&radius=1.0"

TABLE_NAME = "crosby_properties"


def main():
    # Step 1: Connect to DB and read in existing properties.
    conn = connect_db()

    prev_data, col_names = query_db(conn, f"select * from {TABLE_NAME};")
    prev_df = pd.DataFrame(prev_data, columns=col_names)
    # Convert property_id to string to match the scraped data.
    prev_df["property_id"] = prev_df["property_id"].astype(str)

    # Step 2: Get new data from Rightmove and clean it.
    rmd = RightmoveData(SEARCH_URL)
    print(f"Results count: {rmd.results_count}")

    new_data = rmd.get_results

    # Extract the property ID from the listing URL.
    new_data["property_id"] = new_data["url"].str.extract(r"/properties/(\d+)#/")

    # Drop duplicates on property_id.
    new_data = new_data.drop_duplicates(subset=["property_id"], keep="first")

    # Use full_postcode when available, otherwise fall back to postcode.
    new_data["postcode"] = new_data.apply(
        lambda row: row["full_postcode"] if pd.notnull(row["full_postcode"]) else row["postcode"],
        axis=1,
    )

    # Drop columns not stored in the DB.
    new_data = new_data.drop(columns=["search_date", "agent_url", "full_postcode", "price"])

    # Rename columns to match the DB schema.
    new_data = new_data.rename(columns={
        "number_bedrooms": "bedrooms",
        "type": "property_type",
    })

    # Anti-join to keep only listings not already in the DB.
    new_listings = new_data[~new_data["property_id"].isin(prev_df["property_id"])]

    # Step 3: Notify and persist.
    if new_listings.empty:
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
    main()
