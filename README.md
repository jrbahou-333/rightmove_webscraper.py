# Rightmove listing monitor

Scrapes a [Rightmove](https://www.rightmove.co.uk/) property search, compares the results
against listings already stored in a Postgres database, and sends any **new** listings to
Telegram. Designed to run on a schedule (GitHub Actions) so you get notified when a
property matching your search first appears.

## How it works

1. Scrape the configured Rightmove search URL using the `rightmove_webscraper` engine.
2. Read the existing listings from the database.
3. Anti-join to find listings that aren't in the database yet.
4. Send each new listing to Telegram and insert it into the database.

## Project layout

```
rightmove_webscraper/   # scraping engine (RightmoveData)
src/
├── monitor.py          # entrypoint: scrape → compare → notify → store
├── db.py               # Postgres helpers (connection via DATABASE_URL)
└── notifications.py    # Telegram helpers
.github/workflows/monitor.yml   # daily scheduled run
```

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in your values:
   ```
   DATABASE_URL=postgresql://user:password@host:5432/dbname
   TELEGRAM_BOT_TOKEN=...
   TELEGRAM_CHAT_ID=...
   ```
3. Create the table (run once against a fresh database):
   ```bash
   python -m src.init_db
   ```
   This applies [`src/schema.sql`](src/schema.sql) (creates `crosby_properties`).

## First-run seeding

To avoid a burst of Telegram messages the first time you run against an empty table,
seed it once — this records the current listings **without** sending notifications:

```bash
python -m src.monitor --seed
```

After seeding, normal runs only alert on genuinely new listings.

## Running

From the repo root:

```bash
python -m src.monitor
```

To monitor a different search, edit `SEARCH_URL` (and `TABLE_NAME` if needed) in
[`src/monitor.py`](src/monitor.py). **Do not** put an `index=` parameter in the search
URL — the scraper paginates by appending its own, and a hardcoded index pins every page
to the first 25 results.

## Scheduled runs

`.github/workflows/monitor.yml` runs the monitor daily. Set `DATABASE_URL`,
`TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID` as **GitHub Actions secrets** in the repo
settings. The database must be reachable from GitHub's runners (a cloud-hosted Postgres),
not a local `localhost` instance.

## Credits

The scraping engine is based on
[toby-p/rightmove_webscraper.py](https://github.com/toby-p/rightmove_webscraper.py) (MIT).
