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

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            page.wait_for_timeout(3000)

            scroll_and_scrape(page)

            product_cards = page.query_selector_all(store_dict['product_card_selector'])
            print(f"[{store_dict['store_name']}] Total products found: {len(product_cards)}")

            products = []
            for card in product_cards:
                try:
                    name = card.query_selector(store_dict['name_selector']).inner_text().strip()
                    price = card.query_selector(store_dict['price_selector']).inner_text().strip()
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

         
            filename = f"{store_dict['store_name']}_{cate_name}.json"
            data_set.to_json(filename, orient="records", force_ascii=False, indent=4)
            print(f"Saved: {filename}")

            
            rows = list(data_set[['name', 'price', 'category', 'Date']].itertuples(index=False, name=None))

         
            cursor.executemany(
                "INSERT INTO products_history (name, price, category, date) VALUES (%s, %s, %s, %s)",
                rows
            )
            conn.commit()

           
            cursor.execute("DELETE FROM daily_products")
            conn.commit()

       
            cursor.executemany(
                "INSERT INTO daily_products (name, price, category, date) VALUES (%s, %s, %s, %s)",
                rows
            )
            conn.commit()

cursor.close()
conn.close()
