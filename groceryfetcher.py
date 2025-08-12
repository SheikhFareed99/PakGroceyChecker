import traceback
from playwright.sync_api import sync_playwright
import json
import time
import psycopg2
import pandas as pd
from datetime import datetime


def scroll_and_scrape(page):
    previous_height = 0
    max_scrolls = 10

    for _ in range(max_scrolls):
        page.mouse.wheel(0, 10000)
        time.sleep(2)
        current_height = page.evaluate("() => document.body.scrollHeight")
        if current_height == previous_height:
            break
        previous_height = current_height


conn = psycopg2.connect(
    dbname="pakGrocery",
    user="fareed",
    password="rYssJteYMh7UgxdeUk3Wmy0vq3wyXJmC",
    host="dpg-d29r4lqdbo4c739mbrb0-a.oregon-postgres.render.com",
    port="5432"
)
cursor = conn.cursor()

cursor.execute("DELETE FROM daily_products")
conn.commit()

cursor.execute("SELECT * FROM store;")
store_columns = [desc[0] for desc in cursor.description]
stores = cursor.fetchall()

for store_row in stores:
    store_dict = dict(zip(store_columns, store_row))

    cursor.execute(
        "SELECT * FROM store_category_links WHERE store_id = %s",
        (store_dict['store_id'],)
    )

    link_columns = [desc[0] for desc in cursor.description]
    links = cursor.fetchall()

    for link_row in links:
        link_dict = dict(zip(link_columns, link_row))
        url = link_dict['url']

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=60000)
                page.wait_for_timeout(3000)

                scroll_and_scrape(page)

                product_cards = page.query_selector_all(
                    store_dict['product_card_selector'])
                print(
                    f"[{store_dict['store_name']}] Total products found: {len(product_cards)}")

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

                browser.close()

                cursor.execute(
                    "SELECT category_name FROM category WHERE category_id = %s",
                    (link_dict['category_id'],)
                )
                cate_name = cursor.fetchone()[0]

                data_set = pd.DataFrame(products)
                data_set['Date'] = datetime.now()
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

                # data_set['price']=data_set['price'].str.extract(r"(?i)^(Rs\.\d*)")

                data_set['unit'] = data_set['unit'].str.replace(
                    r'\s+', ' ', regex=True).str.strip()

                mask = data_set['unit'].str.contains(
                    r'\bto\b', case=False, na=False)

                data_set.loc[mask, 'unit'] = data_set.loc[mask, 'unit'].str.replace(
                    r'(?i)\bto\b.*', '', regex=True
                ).str.strip()

                if 'unit' not in data_set.columns:
                    data_set['unit'] = None

                if data_set.empty:
                    continue
                filename = f"{store_dict['store_name']}_{cate_name}.json"
                data_set.to_json(filename, orient="records",
                                 force_ascii=False, indent=4)
                print(f"Saved: {filename}")

                data_set = data_set.dropna()

                rows = list(data_set[['name', 'price', 'category', 'Date', 'store_name', 'unit']].itertuples(
                    index=False, name=None))

                cursor.executemany(
                    "INSERT INTO products_history (name, price, category, date,store_name, unit) VALUES (%s, %s, %s, %s, %s, %s)",
                    rows
                )
                conn.commit()

                cursor.executemany(
                    "INSERT INTO daily_products (name, price, category, date,store_name, unit) VALUES (%s, %s, %s, %s, %s, %s)",
                    rows
                )
                conn.commit()

        except Exception as e:
            print(f"[ERROR] Failed to scrape {url} -> {str(e)}")
            traceback.print_exc()
            continue

cursor.close()
conn.close()
