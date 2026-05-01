
# Adds: AI Route Insights panel (Cortex-generated classification + summary).                                                                                                                     
                                                                                                                                                                                                
import streamlit as st                                                                                                                                                                           
import pandas as pd                                                                                                                                                                              
import plotly.express as px
import pydeck as pdk
from snowflake.snowpark.context import get_active_session                                                                                                                                        

session = get_active_session()                                                                                                                                                                   
                
st.set_page_config(page_title="Flight Routes Dashboard", layout="wide")                                                                                                                          
st.title("Flight Routes Dashboard")
st.caption("Live data from FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES")                                                                                                                             

# -------- AI Route Insights (top of page) --------                                                                                                                                              
                                                                                                                                         
st.subheader("AI Route Insights")
st.caption("Enter a route key like `JFK-LHR` to see the Cortex-generated classification and summary.")
                                                                                                                                                                                                
# Persist the searched route across reruns
if "ai_route_lookup" not in st.session_state:                                                                                                                                                    
    st.session_state.ai_route_lookup = None                                                                                                                                                      

ai_col1, ai_col2 = st.columns([2, 1])                                                                                                                                                            
with ai_col1:   
    route_input = st.text_input("Route key", placeholder="JFK-LHR").strip().upper()
with ai_col2:                                                                                                                                                                                    
    st.write("")
    st.write("")                                                                                                                                                                                 
    if st.button("Get AI insights", type="primary"):
        st.session_state.ai_route_lookup = route_input                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                           
if st.session_state.ai_route_lookup:                                                                                                                                                             
    route = st.session_state.ai_route_lookup                                                                                                                                                     
                
    enriched = session.sql(f"""                                                                                                                                                                  
    SELECT
    ROUTE_KEY, AIRLINE_NAME, FLIGHT_NUMBER,                                                                                                                                                    
    ORIGIN_CITY, ORIGIN_COUNTRY, DESTINATION_CITY, DESTINATION_COUNTRY,                                                                                                                        
    DISTANCE_KM, SEATS, AIRCRAFT_TYPE,                                                                                                                                                         
    ROUTE_CLASSIFICATION, ROUTE_SUMMARY                                                                                                                                                        
    FROM FLIGHT_PIPELINE_DB.AI.V_ENRICHED_ROUTES_DASHBOARD                                                                                                                                       
    WHERE ROUTE_KEY = '{route}' AND ROUTE_SUMMARY IS NOT NULL
    LIMIT 1                                                                                                                                                                                      
    """).to_pandas()
                                                                                                                                                                                                
    if not enriched.empty:
        row = enriched.iloc[0]                                                                                                                                                                   
        st.success(f"Found enriched data for **{row['ROUTE_KEY']}**")
                                                                                                                                                                                                
        info_col1, info_col2, info_col3 = st.columns(3)                                                                                                                                          
        info_col1.metric("Classification", row["ROUTE_CLASSIFICATION"])                                                                                                                          
        info_col2.metric("Distance", f"{row['DISTANCE_KM']:,.0f} km")                                                                                                                            
        info_col3.metric("Aircraft", row["AIRCRAFT_TYPE"])                                                                                                                                       
                                                                                                                                                                                                
        st.markdown("**AI Summary**")                                                                                                                                                            
        st.info(row["ROUTE_SUMMARY"])                                                                                                                                                            
                                                                                                                                                                                                
        st.markdown("**Route details**")
        st.write(                                                                                                                                                                                
            f"- **Airline:** {row['AIRLINE_NAME']} (flight {row['FLIGHT_NUMBER']})\n"
            f"- **From:** {row['ORIGIN_CITY']}, {row['ORIGIN_COUNTRY']}\n"                                                                                                                       
            f"- **To:** {row['DESTINATION_CITY']}, {row['DESTINATION_COUNTRY']}\n"                                                                                                               
            f"- **Seats:** {row['SEATS']}"                                                                                                                                                       
        )                                                                                                                                                                                        
    else:                                                                                                                                                                                        
        curated = session.sql(f"""                                                                                                                                                               
        SELECT ROUTE_KEY FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES
        WHERE ROUTE_KEY = '{route}' LIMIT 1                                                                                                                                                      
        """).to_pandas()
                                                                                                                                                                                                
        if curated.empty:
            st.error(f"No route found with key `{route}`.")                                                                                                                                      
        else:   
            st.warning(
                f"Route `{route}` exists in curated data but hasn't been AI-enriched yet."                                                                                                       
            )
            if st.button("Run Cortex enrichment now", key="enrich_btn"):                                                                                                                         
                with st.spinner("Calling Cortex AI... (~10 seconds)"):                                                                                                                           
                    session.sql(f"""                                                                                                                                                             
                    INSERT INTO FLIGHT_PIPELINE_DB.AI.ENRICHED_ROUTES                                                                                                                            
                    SELECT                                                                                                                                                                       
                    AIRLINE_CODE, AIRLINE_NAME, FLIGHT_NUMBER, ROUTE_KEY,
                    ORIGIN_CITY, ORIGIN_COUNTRY, DESTINATION_CITY, DESTINATION_COUNTRY,                                                                                                        
                    DISTANCE_KM, SEATS, AIRCRAFT_TYPE, IS_INTERNATIONAL, DISTANCE_CATEGORY,                                                                                                    
                    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(                                                                                                                                            
                        CONCAT(                                                                                                                                                                  
                        'Route from ', ORIGIN_CITY, ', ', ORIGIN_COUNTRY,                                                                                                                      
                        ' to ', DESTINATION_CITY, ', ', DESTINATION_COUNTRY,                                                                                                                   
                        '. Distance: ', DISTANCE_KM, ' km.'                                                                                                                                    
                        ),                                                                                                                                                                       
                        ['DOMESTIC_SHORT', 'DOMESTIC_LONG', 'INTERNATIONAL_REGIONAL', 'INTERNATIONAL_LONG_HAUL']                                                                                 
                    ):label::STRING AS ROUTE_CLASSIFICATION,                                                                                                                                   
                    SNOWFLAKE.CORTEX.COMPLETE(                                                                                                                                                 
                        'claude-4-sonnet',                                                                                                                                                       
                        CONCAT(                                                                                                                                                                  
                        'In one sentence, describe this flight route: ',                                                                                                                       
                        AIRLINE_NAME, ' flight ', FLIGHT_NUMBER,
                        ' from ', ORIGIN_CITY, ' (', ORIGIN_COUNTRY, ')',                                                                                                                      
                        ' to ', DESTINATION_CITY, ' (', DESTINATION_COUNTRY, ')',                                                                                                              
                        ', distance ', DISTANCE_KM, ' km, ',                                                                                                                                   
                        SEATS, ' seats on ', AIRCRAFT_TYPE, '.'                                                                                                                                
                        )                                                                                                                                                                        
                    ) AS ROUTE_SUMMARY,
                    CURRENT_TIMESTAMP() AS ENRICHED_AT                                                                                                                                         
                    FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                                                 
                    WHERE ROUTE_KEY = '{route}'
                    LIMIT 1                                                                                                                                                                      
                    """).collect()
                st.success("Enrichment complete. Reloading...")                                                                                                                                  
                st.rerun()                                                                                                                                                                       

st.divider()                                                                                                                                                                                           
                                                                                                                                                                                                
# -------- Sidebar filters --------                                                                                                                                                              
st.sidebar.header("Filters")

airlines_df = session.sql(                                                                                                                                                                       
    "SELECT DISTINCT AIRLINE_NAME FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES "
    "WHERE AIRLINE_NAME IS NOT NULL ORDER BY AIRLINE_NAME"                                                                                                                                       
).to_pandas()                                                                                                                                                                                    
airline_options = ["(All)"] + airlines_df["AIRLINE_NAME"].tolist()                                                                                                                               
selected_airline = st.sidebar.selectbox("Airline", airline_options)                                                                                                                              
                
distance_categories = ["(All)", "SHORT", "MEDIUM", "LONG"]                                                                                                                                       
selected_distance = st.sidebar.selectbox("Distance category", distance_categories)
                                                                                                                                                                                                
international_only = st.sidebar.checkbox("International only", value=False)
                                                                                                                                                                                                
                                                                                                                                                      
where_clauses = []
if selected_airline != "(All)":                                                                                                                                                                  
    where_clauses.append(f"AIRLINE_NAME = '{selected_airline}'")
if selected_distance != "(All)":                                                                                                                                                                 
    where_clauses.append(f"DISTANCE_CATEGORY = '{selected_distance}'")
if international_only:                                                                                                                                                                           
    where_clauses.append("IS_INTERNATIONAL = TRUE")                                                                                                                                              

where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""                                                                                                                    
                                                                                                                                                                                    
kpi = session.sql(f"""
SELECT                                                                                                                                                                                           
COUNT(*)                                                       AS TOTAL_FLIGHTS,
COUNT(DISTINCT ROUTE_KEY)                                      AS UNIQUE_ROUTES,                                                                                                               
COUNT(DISTINCT AIRLINE_CODE)                                   AS AIRLINES,                                                                                                                    
ROUND(100.0 * SUM(IFF(IS_INTERNATIONAL, 1, 0)) / NULLIF(COUNT(*), 0), 1) AS PCT_INTL                                                                                                           
FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                                                                     
{where_sql}                                                                                                                                                                                      
""").to_pandas().iloc[0]                                                                                                                                                                         
                                                                                                                                                                                                
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total flights", f"{int(kpi['TOTAL_FLIGHTS']):,}")                                                                                                                                     
c2.metric("Unique routes", f"{int(kpi['UNIQUE_ROUTES']):,}")                                                                                                                                     
c3.metric("Airlines", f"{int(kpi['AIRLINES']):,}")                                                                                                                                               
c4.metric("% International", f"{kpi['PCT_INTL'] or 0:.1f}%")                                                                                                                                     
                                                                                                                                                                                                
st.divider()    
                                                                                                                                                                                                
# -------- Charts --------
left, right = st.columns(2)

with left:                                                                                                                                                                                       
    st.subheader("Flights per airline")
    airline_df = session.sql(f"""                                                                                                                                                                
    SELECT AIRLINE_NAME, COUNT(*) AS FLIGHT_COUNT
    FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                                                                 
    {where_sql}                                                                                                                                                                                  
    GROUP BY AIRLINE_NAME                                                                                                                                                                        
    ORDER BY FLIGHT_COUNT DESC                                                                                                                                                                   
    LIMIT 15                                                                                                                                                                                     
    """).to_pandas()
    if not airline_df.empty:                                                                                                                                                                     
        fig = px.bar(
            airline_df, x="FLIGHT_COUNT", y="AIRLINE_NAME", orientation="h",
            labels={"FLIGHT_COUNT": "Flights", "AIRLINE_NAME": "Airline"},                                                                                                                       
        )                                                                                                                                                                                        
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=450)                                                                                                                
        st.plotly_chart(fig, use_container_width=True)                                                                                                                                           
    else:       
        st.info("No data for the current filters.")
                                                                                                                                                                                                
with right:
    st.subheader("Routes by distance category")                                                                                                                                                  
    dist_df = session.sql(f"""
    SELECT DISTANCE_CATEGORY, COUNT(*) AS FLIGHT_COUNT
    FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                                                                 
    {where_sql}                                                                                                                                                                                  
    GROUP BY DISTANCE_CATEGORY                                                                                                                                                                   
    """).to_pandas()                                                                                                                                                                             
    if not dist_df.empty:
        fig = px.pie(dist_df, values="FLIGHT_COUNT", names="DISTANCE_CATEGORY", hole=0.4)                                                                                                        
        fig.update_layout(height=450)                                                                                                                                                            
        st.plotly_chart(fig, use_container_width=True)                                                                                                                                           
    else:                                                                                                                                                                                        
        st.info("No data for the current filters.")

st.subheader("Top 10 busiest airports")                                                                                                                                                          
airports_df = session.sql(f"""
SELECT AIRPORT_CODE, COUNT(*) AS APPEARANCES                                                                                                                                                     
FROM (                                                                                                                                                                                           
SELECT ORIGIN_AIRPORT      AS AIRPORT_CODE FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES {where_sql}
UNION ALL                                                                                                                                                                                      
SELECT DESTINATION_AIRPORT AS AIRPORT_CODE FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES {where_sql}
)                                                                                                                                                                                                
GROUP BY AIRPORT_CODE
ORDER BY APPEARANCES DESC                                                                                                                                                                        
LIMIT 10                                                                                                                                                                                         
""").to_pandas()
if not airports_df.empty:                                                                                                                                                                        
    fig = px.bar(
        airports_df, x="AIRPORT_CODE", y="APPEARANCES",
        labels={"AIRPORT_CODE": "Airport", "APPEARANCES": "Flights touching airport"},                                                                                                           
    )                                                                                                                                                                                            
    fig.update_layout(height=400)                                                                                                                                                                
    st.plotly_chart(fig, use_container_width=True)                                                                                                                                               
                                                                                                                                                                                                
st.divider()
                                                                                                                                                                                                
# -------- World map --------
st.subheader("Route map")
st.caption("Each line connects an origin airport to its destination. Limited to 200 sample routes for readability.")                                                                             
                                                                                                                                                                                                
map_df = session.sql(f"""                                                                                                                                                                        
SELECT DISTINCT                                                                                                                                                                                  
ROUTE_KEY,                                                                                                                                                                                     
ORIGIN_AIRPORT, ORIGIN_CITY, ORIGIN_LATITUDE, ORIGIN_LONGITUDE,
DESTINATION_AIRPORT, DESTINATION_CITY, DESTINATION_LATITUDE, DESTINATION_LONGITUDE,                                                                                                            
IS_INTERNATIONAL                                                                                                                                                                               
FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                                                                     
{where_sql}                                                                                                                                                                                      
LIMIT 200                                                                                                                                                                                        
""").to_pandas()
                                                                                                                                                                                                
if not map_df.empty:
    map_df["color"] = map_df["IS_INTERNATIONAL"].apply(                                                                                                                                          
        lambda x: [220, 50, 50, 160] if x else [50, 100, 220, 160]
    )                                                                                                                                                                                            

    line_layer = pdk.Layer(                                                                                                                                                                      
        "ArcLayer",
        data=map_df,                                                                                                                                                                             
        get_source_position=["ORIGIN_LONGITUDE", "ORIGIN_LATITUDE"],
        get_target_position=["DESTINATION_LONGITUDE", "DESTINATION_LATITUDE"],                                                                                                                   
        get_source_color="color",                                                                                                                                                                
        get_target_color="color",                                                                                                                                                                
        get_width=2,                                                                                                                                                                             
        pickable=True,
        auto_highlight=True,                                                                                                                                                                     
    )
                                                                                                                                                                                                
    origins = map_df[["ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", "ORIGIN_AIRPORT"]].rename(                                                                                                          
        columns={"ORIGIN_LATITUDE": "lat", "ORIGIN_LONGITUDE": "lon", "ORIGIN_AIRPORT": "airport"}
    )                                                                                                                                                                                            
    dests = map_df[["DESTINATION_LATITUDE", "DESTINATION_LONGITUDE", "DESTINATION_AIRPORT"]].rename(
        columns={"DESTINATION_LATITUDE": "lat", "DESTINATION_LONGITUDE": "lon", "DESTINATION_AIRPORT": "airport"}                                                                                
    )                                                                                                                                                                                            
    points_df = pd.concat([origins, dests]).drop_duplicates()                                                                                                                                    
                                                                                                                                                                                                
    point_layer = pdk.Layer(
        "ScatterplotLayer",                                                                                                                                                                      
        data=points_df,
        get_position=["lon", "lat"],                                                                                                                                                             
        get_radius=40000,
        get_fill_color=[40, 40, 40, 200],                                                                                                                                                        
        pickable=True,                                                                                                                                                                           
    )
                                                                                                                                                                                                
    avg_lat = (map_df["ORIGIN_LATITUDE"].mean() + map_df["DESTINATION_LATITUDE"].mean()) / 2                                                                                                     
    avg_lon = (map_df["ORIGIN_LONGITUDE"].mean() + map_df["DESTINATION_LONGITUDE"].mean()) / 2
                                                                                                                                                                                                
    view_state = pdk.ViewState(latitude=avg_lat, longitude=avg_lon, zoom=1.2, pitch=30)                                                                                                          
                                                                                                                                                                                                
    st.pydeck_chart(                                                                                                                                                                             
        pdk.Deck(
            map_style=None,                                                                                                                                                                      
            initial_view_state=view_state,
            layers=[line_layer, point_layer],                                                                                                                                                    
            tooltip={"text": "{airport}"},
        ),                                                                                                                                                                                       
        use_container_width=True,
    )                                                                                                                                                                                            
else:
    st.info("No routes match the current filters.")                                                                                                                                              
                
st.divider()

# -------- Sample data table --------
st.subheader("Sample of filtered routes")
sample_df = session.sql(f"""                                                                                                                                                                     
SELECT AIRLINE_NAME, FLIGHT_NUMBER, ROUTE_KEY, DISTANCE_KM, DISTANCE_CATEGORY, IS_INTERNATIONAL, FLIGHT_DATE
FROM FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                                                                     
{where_sql}     
LIMIT 50                                                                                                                                                                                         
""").to_pandas()
st.dataframe(sample_df, use_container_width=True)    

st.divider()
                                                                                                                                                                                    
# -------- NL→SQL Chatbot --------                                                                                                                                                    
st.subheader("Ask a question about the routes data")
st.caption("Type a question in plain English. Cortex will generate the SQL and run it.")                                                                                              
                                                                                                                                                                                    
                                                                                                           
SCHEMA_CONTEXT = """                                                                                                                                                                  
You are a Snowflake SQL expert. Generate ONLY a SQL query — no explanations,                                                                                                          
no markdown, no code fences. The query must be valid Snowflake SQL.                                                                                                                   
                                                                                                                                                                                    
Available table:                                                                                                                                                                      
FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES
                                                                                                                                                                                    
Columns:
AIRLINE_CODE STRING            -- airline code (e.g. 'AA', 'BA')                                                                                                                    
AIRLINE_NAME STRING            -- full airline name                                                                                                                                 
FLIGHT_NUMBER STRING                                                                                                                                                                
ORIGIN_AIRPORT STRING          -- 3-letter IATA code                                                                                                                                
ORIGIN_CITY STRING                                                                                                                                                                  
ORIGIN_COUNTRY STRING
ORIGIN_REGION STRING           -- e.g. 'Europe', 'Asia', 'North America'                                                                                                            
ORIGIN_LATITUDE FLOAT                                                                                                                                                               
ORIGIN_LONGITUDE FLOAT
DESTINATION_AIRPORT STRING                                                                                                                                                          
DESTINATION_CITY STRING
DESTINATION_COUNTRY STRING                                                                                                                                                          
DESTINATION_REGION STRING
DESTINATION_LATITUDE FLOAT                                                                                                                                                          
DESTINATION_LONGITUDE FLOAT
DISTANCE_KM FLOAT              -- route distance in kilometers                                                                                                                      
SEATS NUMBER                                                                                                                                                                        
AIRCRAFT_TYPE STRING
FLIGHT_DATE DATE                                                                                                                                                                    
FLIGHT_YEAR NUMBER
FLIGHT_MONTH NUMBER                                                                                                                                                                 
ROUTE_KEY STRING               -- 'ORIGIN-DESTINATION', e.g. 'JFK-LHR'
IS_INTERNATIONAL BOOLEAN                                                                                                                                                            
DISTANCE_CATEGORY STRING       -- 'SHORT', 'MEDIUM', or 'LONG'
                                                                                                                                                                                    
Rules:          
- Always fully qualify the table name as FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES                                                                                                      
- Use COUNT(*), SUM, AVG, etc. for aggregations                                                                                                                                       
- Always include LIMIT 100 unless the question implies a smaller result                                                                                                               
- Return only the SQL — no commentary, no semicolon needed                                                                                                                            
"""                                                                                                                                                                                   
                
# Persist last question + SQL across reruns                                                                                                                                           
if "nl_question" not in st.session_state:
    st.session_state.nl_question = ""                                                                                                                                                 
if "generated_sql" not in st.session_state:
    st.session_state.generated_sql = ""
                                                                                                                                                                                    
question = st.text_input(
    "Your question",                                                                                                                                                                  
    placeholder="e.g. Which airline has the most international flights?"
)                                                                                                                                                                                     

if st.button("Ask", type="primary"):                                                                                                                                                  
    if question.strip():
        st.session_state.nl_question = question.strip()
        with st.spinner("Cortex is generating SQL..."):                                                                                                                               
            # Escape single quotes so they don't break the prompt SQL string
            safe_question = question.replace("'", "''")                                                                                                                               
            prompt = f"{SCHEMA_CONTEXT}\n\nUser question: {safe_question}"
                                                                                                                                                                                    
            sql_result = session.sql(f"""                                                                                                                                             
            SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-4-sonnet', $${prompt}$$) AS GENERATED_SQL                                                                                        
            """).to_pandas()                                                                                                                                                          
                
            generated_sql = sql_result["GENERATED_SQL"].iloc[0].strip()                                                                                                               
                
            # Strip any markdown code fences the LLM might still add                                                                                                                  
            if generated_sql.startswith("```"):
                generated_sql = generated_sql.split("```")[1]                                                                                                                         
                if generated_sql.startswith("sql"):
                    generated_sql = generated_sql[3:]                                                                                                                                 
                generated_sql = generated_sql.strip()
            generated_sql = generated_sql.rstrip(";").strip()                                                                                                                         
                                                                                                                                                                                    
            st.session_state.generated_sql = generated_sql
                                                                                                                                                                                    
                                                                                                                                 
if st.session_state.generated_sql:
    st.markdown("**Generated SQL**")                                                                                                                                                  
    st.code(st.session_state.generated_sql, language="sql")
                                                                                                                                                                                    
    # Safety guardrail: block destructive statements
    forbidden = ["DROP ", "DELETE ", "TRUNCATE ", "INSERT ", "UPDATE ", "ALTER ", "CREATE "]                                                                                          
    sql_upper = st.session_state.generated_sql.upper()                                                                                                                                
    if any(word in sql_upper for word in forbidden):
        st.error("Refusing to run — generated SQL contains a write/DDL statement.")                                                                                                   
    else:                                                                                                                                                                             
        try:
            result_df = session.sql(st.session_state.generated_sql).to_pandas()                                                                                                       
            st.markdown("**Result**")
            if result_df.empty:                                                                                                                                                       
                st.info("Query returned no rows.")
            else:                                                                                                                                                                     
                st.dataframe(result_df, use_container_width=True)                                                                                                                     
        except Exception as e:
            st.error(f"Query failed: {e}")                                                                                                                                            
            st.caption("The LLM may have generated invalid SQL. Try rephrasing your question.")