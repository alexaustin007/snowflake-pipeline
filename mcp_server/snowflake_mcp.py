import json
import os
import snowflake.connector
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

mcp = FastMCP("Snowflake Flight Data")

def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="FLIGHT_PIPELINE_DB",
        warehouse="PIPELINE_WH",
        role="ACCOUNTADMIN"
    )

@mcp.tool()
def list_tables() -> str:
    """List all available tables and views in the flight pipeline database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name, table_type
        FROM FLIGHT_PIPELINE_DB.information_schema.tables
        WHERE table_schema IN ('RAW', 'CURATED', 'AI')
        ORDER BY table_schema, table_name
    """)
    rows = cursor.fetchall()
    conn.close()
    result = [{"schema": r[0], "table": r[1], "type": r[2]} for r in rows]
    return json.dumps(result, indent=2)

@mcp.tool()
def describe_table(table_name: str, schema_name: str) -> str:
    """Get column names and types for a specific table."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM FLIGHT_PIPELINE_DB.information_schema.columns
        WHERE table_schema = '{schema_name.upper()}'
        AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
    """)
    rows = cursor.fetchall()
    conn.close()
    result = [{"column": r[0], "type": r[1]} for r in rows]
    return json.dumps(result, indent=2)

@mcp.tool()
def query_snowflake(sql: str) -> str:
    """Run a read-only SELECT query against the flight pipeline data and return results."""
    if any(word in sql.upper() for word in ["DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER", "CREATE"]):
        return "Error: only SELECT queries are allowed."
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchmany(100)
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    result = [dict(zip(columns, row)) for row in rows]
    return json.dumps(result, indent=2, default=str)

if __name__ == "__main__":
    mcp.run()
