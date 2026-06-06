# Setup:
import os, sys
sys.path.append(os.path.dirname(os.getcwd()))
import pandas as pd
import re
from rightmove_webscraper.scraper import RightmoveData
from docs.telegram_notifications import send_message, bot_token, chat_id 
from docs.db_functions import connect_db, query_db, show_tables, insert_data, update_db, end_connection

# Step 1: Connect to DB and read in properties data
conn=connect_db("house_db")
   
# get table names
# show_tables(conn)

# read in all data
query = "select * from crosby_properties;"
prev_data, col_names = query_db(conn, query)

prev_df = pd.DataFrame(prev_data, columns=col_names)

# convert property_id to string to match new data
prev_df["property_id"] = prev_df["property_id"].astype(str)
prev_df.head()


# Step 2: Get new data from rightmove and clean
url = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced&sortType=2&channel=BUY&transactionType=BUY&displayLocationIdentifier=Crosby&index=0&radius=1.0"
rmd = RightmoveData(url)
print(f"Results count: {rmd.results_count}")

# Get new data
new_data = rmd.get_results
new_data.head()

# Extract the number before "#/" to get property ID
new_data['property_id'] = new_data['url'].str.extract(r'/properties/(\d+)#/')

# Drop duplicates based on property_id, keeping the first occurrence
new_data = new_data.drop_duplicates(subset=['property_id'], keep="first")
new_data.head()

# process postcodes - if full_postcode not avaialble use postcode
new_data["postcode"] = new_data.apply(lambda row: row["full_postcode"] if pd.notnull(row["full_postcode"]) else row["postcode"], axis=1)

# drop cols
new_data = new_data.drop(columns=["search_date", "agent_url", "full_postcode", "price"])

# rename cols to match db
new_data = new_data.rename(columns={
    "number_bedrooms":"bedrooms",
    "type":"property_type"
    })

new_data.head()

# anti join to get only new listings
new_listings = new_data[
    ~new_data["property_id"].isin(prev_df["property_id"])
]

# build message and send to telegram
if new_listings.empty:
    message = "No new listings found."
    send_message(bot_token, chat_id, message)

else:
    for index, new_listing in new_listings.iterrows():
        address = new_listing["address"]
        url = new_listing["url"]
        message = f"New property for sale\nAddress: {address}\nURL: {url}"

        send_message(bot_token, chat_id, message)

    # add new listings to database
    insert_data(conn, new_listings, "crosby_properties")
    

# end db connection
end_connection(conn)