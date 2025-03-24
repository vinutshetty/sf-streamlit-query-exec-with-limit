import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas as pd
from snowflake.snowpark.context import get_active_session
import json 

# Snowflake session configuration (replace with your Snowflake credentials)
# Get the current credentials
session = get_active_session()

data_size_limt=102400000

warehouse_sql = f"USE WAREHOUSE BI_WH"
session.sql(warehouse_sql).collect()

# Execute the SQL using a different warehouse
sql = """SELECT * from INFORMATION_SCHEMA.PACKAGES limit 100"""
session.sql(sql).collect()

# Streamlit UI
st.title("Snowflake SQL Query Executor")

# Input for SQL query
sql_query = st.text_area("Enter your SQL query:", "SELECT * FROM your_table LIMIT 10")

# Function to check query execution plan and validate bytesAssigned
def validate_query(sql_query):
    try:
        # Use EXPLAIN to get the query execution plan in JSON format
        explain_result = session.sql(f"EXPLAIN USING JSON {sql_query}").collect()
        
        # Convert the result to JSON (the first row is the output)
        explain_json = json.loads(explain_result[0][0])

        # Access bytesAssigned from GlobalStats
        bytes_assigned = explain_json.get("GlobalStats", {}).get("bytesAssigned", 0)

        # Check if bytesAssigned exceeds 10MB (10240 bytes)
        if bytes_assigned > data_size_limt:  # 100MB
            return False, bytes_assigned
        else:
            return True, bytes_assigned
    except Exception as e:
        return False, str(e)

# Run query button
if st.button("Run Query"):
    if sql_query:
        # Step 1: Validate the query plan
        is_valid, result = validate_query(sql_query)
        
        if not is_valid:
            if isinstance(result, str):
                st.error(f"Error: {result}")
            else:
                st.error(f"Query execution plan shows bytesAssigned is {result} bytes, which exceeds {data_size_limt} bytes. Query is not allowed.")
        else:
            # Step 2: If the plan is valid, run the query
            try:
                df = session.sql(sql_query).to_pandas()
                st.write("Query Results:")
                st.dataframe(df)
            except Exception as e:
                st.error(f"Error executing query: {e}")
    else:
        st.error("Please enter a valid SQL query.")
