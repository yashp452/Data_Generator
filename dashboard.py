import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px

st.set_page_config(page_title="Big Data Quality Dashboard", layout="wide")
st.title("🛍️ 10GB Retail Data Quality Dashboard (Powered by DuckDB)")

st.markdown("This dashboard leverages **DuckDB**, an out-of-core OLAP SQL engine, to query **tens of millions of rows in milliseconds** directly against the compressed Parquet data lake on disk (preventing Out-Of-Memory errors).")

@st.cache_resource
def get_db():
    # In-memory DuckDB connection
    conn = duckdb.connect(database=':memory:', read_only=False)
    return conn

conn = get_db()

# --- 1. Volume Metrics ---
st.header("1. Data Lake Volume Metrics")

# Query counts directly from parquet files across all run_00X folders
with st.spinner("Executing COUNT(*) across millions of rows..."):
    try:
        total_orders = conn.execute("SELECT COUNT(*) FROM read_parquet('./output/retail/facts/run_*/orders.parquet')").fetchone()[0]
        total_line_items = conn.execute("SELECT COUNT(*) FROM read_parquet('./output/retail/facts/run_*/order_line_items.parquet')").fetchone()[0]
        total_customers = conn.execute("SELECT COUNT(*) FROM read_parquet('./output/retail/dims/customers.parquet')").fetchone()[0]
        total_stores = conn.execute("SELECT COUNT(*) FROM read_parquet('./output/retail/dims/stores.parquet')").fetchone()[0]
    except Exception as e:
        st.error(f"Error reading Parquet files. Did you run the generator yet? {e}")
        st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Orders", f"{total_orders:,}")
c2.metric("Total Line Items", f"{total_line_items:,}")
c3.metric("Total Customers", f"{total_customers:,}")
c4.metric("Total Stores", f"{total_stores:,}")

st.divider()

# --- 2. Realism Verification (Pricing) ---
st.header("2. Pricing Realism (Product Lookups)")
st.markdown("Verifying that our `seeds/products.csv` lookup correctly bonded Category to Price. Categories like Electronics should cost vastly more than Groceries.")

with st.spinner("Joining massive Line Items fact table to Products dimension..."):
    pricing_query = """
    SELECT 
        p.category, 
        ROUND(AVG(l.unit_price), 2) as avg_price,
        COUNT(*) as total_items_sold
    FROM read_parquet('./output/retail/facts/run_*/order_line_items.parquet') AS l
    JOIN read_parquet('./output/retail/dims/products.parquet') AS p
      ON l.product_id = p.product_id
    GROUP BY p.category
    ORDER BY avg_price DESC
    """
    pricing_df = conn.execute(pricing_query).df()

fig_pricing = px.bar(pricing_df, x='category', y='avg_price', color='total_items_sold', 
                     title="Average Item Selling Price by Category")
st.plotly_chart(fig_pricing, use_container_width=True)

st.divider()

# --- 3. Pareto Verification (80/20 Rule) ---
st.header("3. The 80/20 Rule Verification (Zipf Distribution)")
st.markdown("Verifying that a small group of 'power users' accounts for a massive proportion of all orders due to the Pareto foreign-key logic.")

with st.spinner("Aggregating Orders by Customer ID using Analytical Window Functions..."):
    pareto_query = """
    WITH CustomerTotals AS (
        SELECT customer_id, COUNT(*) as order_count
        FROM read_parquet('./output/retail/facts/run_*/orders.parquet')
        GROUP BY customer_id
    ),
    RankedCustomers AS (
        SELECT 
            customer_id, 
            order_count,
            PERCENT_RANK() OVER (ORDER BY order_count DESC) as pct_rank
        FROM CustomerTotals
    )
    SELECT 
        CASE 
            WHEN pct_rank <= 0.20 THEN 'Top 20% Customers'
            ELSE 'Bottom 80% Customers'
        END AS Segment,
        SUM(order_count) as Total_Orders
    FROM RankedCustomers
    GROUP BY 
        CASE 
            WHEN pct_rank <= 0.20 THEN 'Top 20% Customers'
            ELSE 'Bottom 80% Customers'
        END
    """
    pareto_df = conn.execute(pareto_query).df()

fig_pareto = px.pie(pareto_df, values='Total_Orders', names='Segment', 
                    title="Proportion of Orders Driven By Top 20% vs Bottom 80%",
                    hole=0.4)
st.plotly_chart(fig_pareto, use_container_width=True)

st.divider()

# --- 4. Geographic Clustering ---
st.header("4. Population-Weighted Geographic Clustering")
st.markdown("Verifying that orders inherently cluster around major heavily-populated global cities (like Tokyo and Delhi) rather than being randomly spread out.")

with st.spinner("Executing Geographic JOIN between Orders and Stores..."):
    geo_query = """
    SELECT 
        s.city, 
        s.country,
        COUNT(*) as Total_Orders
    FROM read_parquet('./output/retail/facts/run_*/orders.parquet') AS o
    JOIN read_parquet('./output/retail/dims/stores.parquet') AS s
      ON o.store_id = s.store_id
    GROUP BY s.city, s.country
    ORDER BY Total_Orders DESC
    LIMIT 20
    """
    geo_df = conn.execute(geo_query).df()

fig_geo = px.bar(geo_df, x='city', y='Total_Orders', color='country', 
                 title="Top 20 Cities by Global Order Volume")
st.plotly_chart(fig_geo, use_container_width=True)