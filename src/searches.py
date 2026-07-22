# The Rightmove searches to monitor: name -> search URL.
#
# To add a location:
#   1. Build the search on rightmove.co.uk and copy the results-page URL.
#   2. Add an entry below with a short unique name (it is stored in the DB's
#      search_name column and shown in Telegram messages).
#
# NOTE: do not include an `index=` parameter in any URL — the scraper appends
# its own for pagination, and a hardcoded index pins every page to the first
# 25 results.
SEARCHES = {
    "crosby": (
        "https://www.rightmove.co.uk/property-for-sale/find.html"
        "?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000"
        "&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced"
        "&sortType=2&channel=BUY&transactionType=BUY"
        "&displayLocationIdentifier=Crosby&radius=1.0"
    ),
}
