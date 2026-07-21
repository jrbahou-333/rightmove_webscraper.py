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

## Scheduled runs (GitHub Actions)

`.github/workflows/monitor.yml` runs the monitor **every 2 hours between 07:00 and 21:00
UTC** (= 08:00–22:00 UK time during BST), and can also be triggered manually from the
**Actions** tab (`workflow_dispatch`).

> GitHub Actions cron is always **UTC** — it has no timezone support. The schedule above
> matches 08:00–22:00 UK time during BST; after the clocks go back (GMT) it runs
> 07:00–21:00 local. Shift the hours in the cron if you want to correct for that.

These are stored as **environment secrets** on a GitHub environment named **`env`**
(Settings → Environments → `env`):

- `DATABASE_URL` — the cloud Postgres connection string (must be reachable from GitHub's
  runners; a local `localhost` database will not work)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Because they are environment (not repository) secrets, the workflow job **must** declare
the environment or the values arrive empty:

```yaml
jobs:
  run-monitor:
    runs-on: ubuntu-latest
    environment: env
```

### Current deployment: testing from a branch

GitHub only fires `schedule` triggers from the **default branch (`master`)**. To let us
test against the real schedule before merging, the setup is currently split:

- `monitor.yml` lives on **`master`** (so the schedule fires), but its checkout step is
  pinned to the feature branch:
  ```yaml
  - uses: actions/checkout@v5
    with:
      ref: feature/adding_db_functionality
  ```
  So the scheduled/manual run executes **this branch's** code, not `master`'s.
- Any change you want to go live must therefore land on `feature/adding_db_functionality`
  (or re-point the `ref:`).

### Merging to `master` (once tested)

When ready to promote this to mainline:

1. Merge `feature/adding_db_functionality` into `master`.
2. In `.github/workflows/monitor.yml`, **remove the pinned `ref:`** from the checkout step
   so it runs `master` normally:
   ```yaml
   - uses: actions/checkout@v5
   ```
3. Confirm the three repository secrets are still set.
4. (Optional) delete the feature branch.

## Credits

The scraping engine is based on
[toby-p/rightmove_webscraper.py](https://github.com/toby-p/rightmove_webscraper.py) (MIT).
