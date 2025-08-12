from sqlalchemy import text
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI

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
