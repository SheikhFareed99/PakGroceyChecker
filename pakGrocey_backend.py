
from sqlalchemy import create_engine, text, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI
from datetime import date  

db_url = "postgresql://fareed:rYssJteYMh7UgxdeUk3Wmy0vq3wyXJmC@dpg-d29r4lqdbo4c739mbrb0-a.oregon-postgres.render.com:5432/pakGrocery"
engine = create_engine(db_url, connect_args={"sslmode": "require"})
LocalSession = sessionmaker(bind=engine)
Base = declarative_base()

app = FastAPI()


@app.get('/fetchProducts')
def fetch_products(store_name: str, category: str):
    db = LocalSession()
    query = text("""
        SELECT * 
        FROM daily_products 
        WHERE store_name = :store_name 
        AND category = :category
    """)
    result = db.execute(
        query, {"store_name": store_name, "category": category}
    ).mappings().all()
    db.close()
    return [dict(row) for row in result]


@app.get('/fetchStores')
def fetch_stores():
    db = LocalSession()
    result = db.execute(text("SELECT store_name FROM store")).mappings().all()
    db.close()
    return [dict(row) for row in result]


@app.get('/fetchCategory')
def fetch_category():
    db = LocalSession()
    result = db.execute(
        text("SELECT category_name FROM category")).mappings().all()
    db.close()
    return [dict(row) for row in result]


@app.get("/priceChanges")
def price_changes(from_date: date, to_date: date, store_name: str, category: str):
    db = LocalSession()

    sql = text("""
        SELECT 
            COALESCE(dp1.price, dp2.price) AS price_from,
            COALESCE(dp2.price, dp1.price) AS price_to,
            COALESCE(dp1.store_name, dp2.store_name) AS store_name,
            COALESCE(dp1.category, dp2.category) AS category,
            COALESCE(dp1.name, dp2.name) AS name,
            COALESCE(dp1.unit, dp2.unit) AS unit,
         COALESCE(
    ROUND(
        CAST(
            CASE 
                WHEN dp1.price IS NULL OR dp1.price = 0 THEN 0
                ELSE ((dp2.price - dp1.price) / dp1.price) * 100
            END 
        AS numeric), 
    2),
0) AS price_change_percentage,

            CASE 
                WHEN dp1.price IS NULL OR dp2.price IS NULL THEN 'no change'
                WHEN dp1.price > dp2.price THEN 'decrease'
                WHEN dp1.price < dp2.price THEN 'increase'
                ELSE 'no change'
            END AS change_sign
        FROM (
            SELECT * FROM products_history WHERE date = :from_date
        ) dp1
        FULL OUTER JOIN (
            SELECT * FROM products_history WHERE date = :to_date
        ) dp2
            ON dp1.store_name = dp2.store_name
            AND dp1.category = dp2.category
            AND dp1.name = dp2.name
        WHERE COALESCE(dp1.store_name, dp2.store_name) = :store_name
          AND COALESCE(dp1.category, dp2.category) = :category
    """)

    result = db.execute(sql, {
        "from_date": from_date,
        "to_date": to_date,
        "store_name": store_name,
        "category": category
    }).mappings().all()

    db.close()

    return {"data": [dict(row) for row in result]}
