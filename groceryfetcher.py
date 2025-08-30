from io import StringIO
import traceback
from playwright.sync_api import sync_playwright
import psycopg2
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import boto3

load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
ENDPOINT = os.getenv("ENDPOINT")
BUCKET = os.getenv("BUCKET")

s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT
)


def scroll_and_scrape(page):
    previous_height = 0
    max_scrolls = 10
    for _ in range(max_scrolls):
        page.mouse.wheel(0, 10000)
        page.wait_for_load_state("networkidle")
        current_height = page.evaluate("() => document.body.scrollHeight")
        if current_height == previous_height:
            break
        previous_height = current_height


conn = psycopg2.connect(
    dbname="pakgrocery",
    user="postgres",
    password="Snsfan@11",
    host="db.ycaxonvcibqvkhisfevw.supabase.co",
    port="5432"
)

cursor = conn.cursor()

cursor.execute("DELETE FROM daily_products")
conn.commit()

cursor.execute("SELECT * FROM store;")
store_columns = [desc[0] for desc in cursor.description]
stores = cursor.fetchall()

cursor.execute("SELECT * FROM store_category_links;")
link_columns = [desc[0] for desc in cursor.description]
all_links = cursor.fetchall()

cursor.execute("SELECT category_id, category_name FROM category;")
category_map = dict(cursor.fetchall())

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    all_rows = []

    for store_row in stores:
        store_dict = dict(zip(store_columns, store_row))

        links_for_store = [dict(zip(link_columns, l)) for l in all_links if l[link_columns.index(
            "store_id")] == store_dict["store_id"]]

        for link_dict in links_for_store:
            url = link_dict['url']
            cate_name = category_map.get(link_dict['category_id'], "Unknown")

            try:
                page.goto(url, timeout=60000)
                scroll_and_scrape(page)

                product_cards = page.query_selector_all(
                    store_dict['product_card_selector'])
                print(
                    f"[{store_dict['store_name']}] {cate_name} - Products: {len(product_cards)}")

                products = []
                for card in product_cards:
                    try:
                        name = card.query_selector(
                            store_dict['name_selector']).inner_text().strip()
                        price = card.query_selector(
                            store_dict['price_selector']).inner_text().strip()
                        products.append({"name": name, "price": price})
                    except:
                        continue

                data_set = pd.DataFrame(products)
                if data_set.empty:
                    continue

              

                data_set['category'] = cate_name
                data_set['store_name'] = store_dict['store_name']

                if 'name' not in data_set.columns:
                    data_set['name'] = None

                data_set['unit'] = data_set['name'].str.extract(
                    r"(?i)(per.*|\d.*|Dozen.*|PC.*|bunch.*)", expand=False)
                data_set['name'] = data_set['name'].str.extract(
                    r"(?i)^(.*?)(?=\s*per|\d|Dozen|PC|bunch)", expand=False)
                data_set['name'] = data_set['name'].fillna(
                    "").str.replace(r"[()]", "", regex=True)
                data_set['unit'] = data_set['unit'].fillna(
                    "").str.replace(r"[()]", "", regex=True)

                mask = data_set['unit'].str.contains(
                    r'\bto\b', case=False, na=False)
                data_set.loc[mask, 'unit'] = (
                    data_set.loc[mask, 'unit']
                    .str.replace(r'(?i).*\bto\b\s*', '', regex=True)
                    .str.strip()
                )

                data_set.loc[data_set['price'].str.count(r'Rs\.?\d+') >= 2, 'price'] = (
                    data_set.loc[data_set['price'].str.count(
                        r'Rs\.?\d+') >= 2, 'price']
                    .str.extract(r'(Rs\.?\d+)')[0]
                )
                data_set['price'] = data_set['price'].str.replace(
                    r'Rs[./]?\s*', '', regex=True)
                data_set['price'] = data_set['price'].str.replace(
                    ',', '', regex=False)
                data_set['price'] = pd.to_numeric(
                    data_set['price'], errors='coerce')
                data_set['price'] = data_set['price']

                data_set['name'] = data_set['name'].str.replace(
                    r'[\/\\]', '', regex=True)

                if 'unit' not in data_set.columns:
                    data_set['unit'] = None
                
                data_set['date'] = datetime.now()

                data_set = data_set[~(data_set == "").any(axis=1)]

                all_rows.append(data_set)

            except Exception as e:
                print(f"[ERROR] {url} -> {str(e)}")
                traceback.print_exc()
                continue

    browser.close()

if all_rows:
    combined_df = pd.concat(all_rows, ignore_index=True)
else:
    combined_df = pd.DataFrame(
        columns=['name', 'price', 'category', 'store_name', 'unit', 'date'])


def insert_b2(data):
    temp_b2_data = StringIO()
    file_name = f"raw/file_{datetime.now().strftime('%Y-%m-%d')}.csv"
    df = pd.DataFrame(data)
    df.to_csv(temp_b2_data, index=False)
    s3.put_object(Bucket=BUCKET, Key=file_name, Body=temp_b2_data.getvalue())


def bulk_insert(cursor, query, rows):
    execute_values(cursor, query, rows, page_size=500)
    conn.commit()


insert_query_history = """
    INSERT INTO products_history (name, price, category, store_name, unit, date)
    VALUES %s
"""
insert_query_daily = """
    INSERT INTO daily_products (name, price, category, store_name, unit, date)
    VALUES %s
"""
# Upload CSV
insert_b2(combined_df)

# Bulk insert
bulk_insert(cursor, insert_query_history, combined_df.values.tolist())
bulk_insert(cursor, insert_query_daily, combined_df.values.tolist())


cursor.close()
conn.close()

print("all data inserted successfully")
