# PakGrocery Checker

A web scraping and price comparison tool for Pakistani grocery stores. It collects product data from multiple stores and provides a REST API for querying prices and tracking price changes over time.

## Project Structure

| File | Description |
|------|-------------|
| `grocery_fetcher.py` | Web scraper that collects grocery product data from store websites using Playwright, processes the data, stores it in PostgreSQL, and uploads CSV backups to Backblaze B2 cloud storage. |
| `pak_grocery_backend.py` | FastAPI backend providing REST API endpoints to query products, stores, categories, and price changes. |
| `date_compare_fetcher.py` | Utility script that fetches historical CSV data from B2 storage for multiple time periods and inserts combined data into the `date_compare_table` for price trend analysis. |
| `data_transfer.py` | One-time data migration utility for transferring data between PostgreSQL databases (Render to Supabase). |

## API Endpoints

| Endpoint | Method | Parameters | Description |
|----------|--------|------------|-------------|
| `/fetchProducts` | GET | `store_name`, `category` | Fetch products by store and category |
| `/fetchStores` | GET | — | List all available stores |
| `/fetchCategory` | GET | — | List all product categories |
| `/priceChanges` | GET | `from_date`, `to_date`, `store_name`, `category` | Compare prices between two dates with percentage change |

## Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/SheikhFareed99/PakGroceyChecker.git
   cd PakGroceyChecker
   ```

2. **Install dependencies:**
   ```bash
   pip install fastapi sqlalchemy psycopg2-binary pandas playwright boto3 python-dotenv uvicorn
   playwright install chromium
   ```

3. **Configure environment variables:**

   Create a `.env` file in the project root with your Backblaze B2 credentials:
   ```
   ACCESS_KEY=your_access_key
   SECRET_KEY=your_secret_key
   ENDPOINT=your_endpoint_url
   BUCKET=your_bucket_name
   ```

4. **Run the scraper:**
   ```bash
   python grocery_fetcher.py
   ```

5. **Start the API server:**
   ```bash
   uvicorn pak_grocery_backend:app --reload
   ```

## Technologies

- **Python** — Core language
- **FastAPI** — REST API framework
- **Playwright** — Browser automation for web scraping
- **PostgreSQL** — Database (hosted on Supabase / Render)
- **SQLAlchemy** — Database ORM for the backend
- **Pandas** — Data processing and manipulation
- **Backblaze B2** — Cloud storage for CSV backups
- **boto3** — AWS-compatible S3 client for B2 uploads
