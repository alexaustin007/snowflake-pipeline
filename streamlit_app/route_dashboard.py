# Flight Routes Dashboard — Phase 1, version 4
# Adds: world map showing route lines between origin and destination airports.                                                                                                                   
                                                                                                                                                                                                
import streamlit as st                                                                                                                                                                           
import pandas as pd                                                                                                                                                                              
import plotly.express as px
import pydeck as pdk
from snowflake.snowpark.context import get_active_session                                                                                                                                        
                                                                                                                                                                                                
session = get_active_session()                                                                                                                                                                   
                                                                                                                                                                                                
st.set_page_config(page_title="Flight Routes Dashboard", layout="wide")                                                                                                                          
st.title("Flight Routes Dashboard")
st.caption("Live data from FLIGHT_PIPELINE_DB.CURATED.CLEAN_ROUTES")                                                                                                                             
                                                                                                                                                                                                
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
                                                                                                                                                                                                
# -------- Build the WHERE clause once, reuse everywhere --------                                                                                                                                
where_clauses = []
if selected_airline != "(All)":                                                                                                                                                                  
    where_clauses.append(f"AIRLINE_NAME = '{selected_airline}'")                                                                                                                                 
if selected_distance != "(All)":
    where_clauses.append(f"DISTANCE_CATEGORY = '{selected_distance}'")                                                                                                                           
if international_only:                                                                                                                                                                           
    where_clauses.append("IS_INTERNATIONAL = TRUE")
                                                                                                                                                                                                
where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
                                                                                                                                                                                                
# -------- KPI tiles --------                                                                                                                                                                    
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
                                                                                                                                                                                                
# -------- World map of routes --------
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
    # Color routes: red = international, blue = domestic
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
                                                                                                                                                                                                
    # Origin and destination dots
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
                                                                                                                                                                                                
    # Center the view at average latitude/longitude of the data                                                                                                                                  
    avg_lat = (map_df["ORIGIN_LATITUDE"].mean() + map_df["DESTINATION_LATITUDE"].mean()) / 2
    avg_lon = (map_df["ORIGIN_LONGITUDE"].mean() + map_df["DESTINATION_LONGITUDE"].mean()) / 2                                                                                                   
                                                                                                                                                                                                
    view_state = pdk.ViewState(                                                                                                                                                                  
        latitude=avg_lat,                                                                                                                                                                        
        longitude=avg_lon,                                                                                                                                                                       
        zoom=1.2,
        pitch=30,                                                                                                                                                                                
    )           

    st.pydeck_chart(                                                                                                                                                                             
        pdk.Deck(
            map_style=None,                                                                                                                                                                      
            initial_view_state=view_state,
            layers=[line_layer, point_layer],
            tooltip={"text": "{airport}\n{ORIGIN_AIRPORT} → {DESTINATION_AIRPORT}"},                                                                                                             
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