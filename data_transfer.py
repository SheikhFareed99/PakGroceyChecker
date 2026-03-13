import psycopg2

# Supabase connection (destination)
conn = psycopg2.connect(
    dbname="pakgrocery",   # make sure the db name is correct
    user="postgres",
    password="Snsfan@11",
    host="db.ycaxonvcibqvkhisfevw.supabase.co",
    port="5432"
)
cursor = conn.cursor()

# Render connection (source)
conn2 = psycopg2.connect(
    dbname="pakGrocery",
    user="fareed",
    password="rYssJteYMh7UgxdeUk3Wmy0vq3wyXJmC",
    host="dpg-d29r4lqdbo4c739mbrb0-a.oregon-postgres.render.com",
    port="5432"
)
cursor2 = conn2.cursor()

# List of tables to migrate
tables = ["store","category","store_category_links", "products_history", "daily_products"]

for table in tables:
    print(f"Migrating table: {table}")

    # fetch all data from source
    cursor2.execute(f"SELECT * FROM {table};")
    rows = cursor2.fetchall()
    if not rows:
        print(f" No rows found in {table}, skipping...")
        continue

    # get column names dynamically
    col_names = [desc[0] for desc in cursor2.description]
    placeholders = ",".join(["%s"] * len(col_names))
    col_list = ",".join(col_names)

    # insert into destination
    insert_query = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f" Migrated {len(rows)} rows into {table}")

# Close connections
cursor.close()
conn.close()
cursor2.close()
conn2.close()

print(" Migration completed successfully!")
