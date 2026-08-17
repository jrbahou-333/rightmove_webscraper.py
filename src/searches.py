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

# Price and bedroom criteria shared by every search: £200k–£350k, 3+ beds.
_CRITERIA = (
    "minPrice=200000&maxPrice=350000&minBedrooms=3"
    "&sortType=2&channel=BUY&transactionType=BUY"
)

# Detached and semi-detached only — no terraced, no flats.
_HOUSES_ONLY = "detached%2Csemi-detached"


def _station_search(station_id, radius):
    """Build a search URL centred on a Rightmove station, out to `radius` miles.

    Station IDs come from Rightmove's location typeahead:
    https://los.rightmove.co.uk/typeahead?query=mossley+hill — take the `id` of
    the match whose `type` is STATION.
    """
    return (
        "https://www.rightmove.co.uk/property-for-sale/find.html"
        f"?locationIdentifier=STATION%5E{station_id}&{_CRITERIA}"
        f"&propertyTypes={_HOUSES_ONLY}&radius={radius}"
    )


SEARCHES = {
    # Crosby, "this area only" (radius=0.0).
    "crosby": (
        "https://www.rightmove.co.uk/property-for-sale/find.html"
        "?locationIdentifier=REGION%5E7515&minPrice=200000&maxPrice=350000"
        "&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced"
        "&sortType=2&channel=BUY&transactionType=BUY"
        "&displayLocationIdentifier=Crosby&radius=0.0"
    ),
    # Formby Station, Merseyside + 1 mile radius.
    "formby_station": (
        "https://www.rightmove.co.uk/property-for-sale/find.html"
        "?locationIdentifier=STATION%5E3584&minPrice=200000&maxPrice=350000"
        "&minBedrooms=3&propertyTypes=detached%2Csemi-detached%2Cterraced"
        "&sortType=2&channel=BUY&transactionType=BUY&radius=1.0"
    ),
    # --- South Liverpool: ~15 minute walk (~ 1 mile) around each station. ---
    # These four are detached/semi-detached only (see _station_search)
    "mossley_hill_station": _station_search("6404", "1"),
    "broad_green_station": _station_search("1406", "1"),
    "west_allerton_station": _station_search("9800", "1"),
    "liverpool_south_parkway_station": _station_search("15037", "1"),
}
