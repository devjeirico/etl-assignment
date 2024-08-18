import sqlite3
import pandas as pd

conn = sqlite3.connect("S30 ETL Assignment.db")

sql_query = """
SELECT
    c.customer_id,
    c.age,
    i.item_name,
    SUM(COALESCE(o.quantity, 0)) AS total_quantity
FROM
    customers c
JOIN
    sales s ON c.customer_id = s.customer_id
JOIN
    orders o ON s.sales_id = o.sales_id
JOIN
    items i ON o.item_id = i.item_id
WHERE
    c.age BETWEEN 18 AND 35
GROUP BY
    c.customer_id, c.age, i.item_name
HAVING
    total_quantity > 0
ORDER BY
    c.customer_id, i.item_name;
"""

df_sql = pd.read_sql_query(sql_query, conn)
df_sql.columns = ['Customer', 'Age', 'Item', 'Quantity']
df_customers = pd.read_sql_query("SELECT * FROM customers WHERE age BETWEEN 18 AND 35;", conn)
df_sales = pd.read_sql_query("SELECT * FROM sales;", conn)
df_orders = pd.read_sql_query("SELECT * FROM orders;", conn)
df_items = pd.read_sql_query("SELECT * FROM items;", conn)
merged_df = df_sales.merge(df_customers, on='customer_id')\
                    .merge(df_orders, on='sales_id')\
                    .merge(df_items, on='item_id')
df_pandas = merged_df.groupby(['customer_id', 'age', 'item_name'], as_index=False)\
                     .agg({'quantity': 'sum'})\
                     .rename(columns={'quantity': 'total_quantity'})
df_pandas['total_quantity'] = df_pandas['total_quantity'].astype(int)
df_pandas = df_pandas[df_pandas['total_quantity'] > 0]
df_pandas.columns = ['Customer', 'Age', 'Item', 'Quantity']
df_sql.to_csv('output_sql.csv', sep=';', index=False)
df_pandas.to_csv('output_pandas.csv', sep=';', index=False)

# Close the database connection
conn.close()
