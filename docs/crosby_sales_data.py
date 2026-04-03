# Setup:
import os, sys
sys.path.append(os.path.dirname(os.getcwd()))
import pandas as pd
from rightmove_webscraper import RightmoveData

# url = "https://www.rightmove.co.uk/property-for-sale/find.html?\searchType=SALE&locationIdentifier=REGION%5E94346"

url = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced&sortType=2&channel=BUY&transactionType=BUY&displayLocationIdentifier=Crosby&index=0&radius=1.0"
rmd = RightmoveData(url)
print(f"Results count: {rmd.results_count}")


# Get functions from telegram script
from docs.telegram_notifications import send_message, bot_token, chat_id    

# read in all data
prev_data = pd.read_csv("rightmove_crosby_all_data.csv")
prev_data.head()

# Get new data
new_data = rmd.get_results
new_data.head()

# anti join to get only new listings
id_cols = ["type", "address"]
new_listings = pd.merge(
    new_data,
    prev_data[["type", "address"]],
    on=id_cols,
    how="left_anti",
)

new_listings = new_listings.drop_duplicates(subset=id_cols, keep="first")


# build message and send to telegram
if new_listings.empty:
    message = "No new listings found."
    send_message(bot_token, chat_id, message)

else:
    for index, new_listing in new_listings.iterrows():
        price = new_listing["price"]
        address = new_listing["address"]
        url = new_listing["url"]
        message = f"New property for sale\nAddress: {address}\nPrice: £{price}\nURL: {url}"

        send_message(bot_token, chat_id, message)

    # now concat new listings to previous data and save
    saved_data = pd.concat([prev_data, new_listings], ignore_index=True)

    saved_data.to_csv("rightmove_crosby_all_data.csv", index=False)

