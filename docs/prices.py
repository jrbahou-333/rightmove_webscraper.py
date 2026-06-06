# Setup:
import os, sys
sys.path.append(os.path.dirname(os.getcwd()))
import pandas as pd
import re
from rightmove_webscraper import RightmoveData
from docs.telegram_notifications import send_message, bot_token, chat_id 
from docs.db_functions import connect_db, query_db, show_tables, insert_data, update_db, end_connection, get_col_types

# Step 1: Connect to DB and read in price
conn=connect_db("house_db")
   
# get table names
show_tables(conn)

#  change property ID to text in db
# query = """ALTER TABLE sale_prices
# drop property_id;

# ALTER TABLE sale_prices
# ADD COLUMN property_id numeric PRIMARY KEY;


# ALTER TABLE sale_prices 
# ADD CONSTRAINT sale_prices_pk FOREIGN KEY (property_id) REFERENCES crosby_properties(property_id);
# """ 

update_db(conn, query)

get_col_types(conn, "sale_prices")


# read in price data
query = "select * from sale_prices;"
price_data, col_names = query_db(conn, query)

price_data = pd.DataFrame(price_data, columns=col_names)

# convert property_id to string to match new data
price_data["property_id"] = price_data["property_id"].astype(str)
price_data.head()

# Step 2: Get new data from rightmove and clean
url = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced&sortType=2&channel=BUY&transactionType=BUY&displayLocationIdentifier=Crosby&index=0&radius=1.0"
rmd = RightmoveData(url)
print(f"Results count: {rmd.results_count}")

# Get new data
new_data = rmd.get_results
new_data.head()

# Extract the number before "#/" to get property ID
rm_prices = new_data.copy()
rm_prices['property_id'] = rm_prices['url'].str.extract(r'/properties/(\d+)#/')

# keep only certain cols
rm_prices = rm_prices[["property_id", "price", "search_date"]]

# drop duplicates based on property_id and price, keeping the first occurrence. (How do we detect when a property comes back on the market)
new_prices = rm_prices.drop_duplicates(subset=["property_id", "price"], keep="first")

rm_prices = rm_prices.rename(columns={
    "search_date":"date"
})

rm_prices.head()

# anti join to get only new prices (on property ID and price)
new_prices = pd.merge(
    rm_prices,
    price_data.drop(columns=["date"]),
    on=["property_id", "price"], 
    how="left", 
    indicator=True
    )

new_prices = new_prices[new_prices["_merge"] == "left_only"].drop(columns=["_merge"])
new_prices.head()

# build message and send to telegram
if new_prices.empty:
    message = "No new listings found."
    send_message(bot_token, chat_id, message)

else:
    # add new listings to database
    insert_data(conn, new_prices, "sale_prices")
    

