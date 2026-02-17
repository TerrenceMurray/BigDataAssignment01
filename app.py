import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import os
import requests

st.set_page_config(page_title="NYC Yellow Taxi Trips", layout="wide")

CHART_COLOR = '#2a9d8f'

# Load data
DATA_DIR = './data/raw'

def ensure_data():
    parquet_path = f'{DATA_DIR}/yellow_tripdata_2024-01.parquet'
    csv_path = f'{DATA_DIR}/taxi_zone_lookup.csv'

    os.makedirs(DATA_DIR, exist_ok=True)

    if not os.path.exists(csv_path):
        with st.spinner('Downloading taxi zone lookup...'):
            r = requests.get('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv', stream=True)
            r.raise_for_status()
            with open(csv_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

    if not os.path.exists(parquet_path):
        with st.spinner('Downloading trip data (~48 MB)...'):
            r = requests.get('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet', stream=True)
            r.raise_for_status()
            with open(parquet_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

    return parquet_path, csv_path

parquet_path, csv_path = ensure_data()

# Create DuckDB connection with views over on-disk files (no full data load)
con = duckdb.connect()

con.execute(f"""
    CREATE VIEW trips AS
    SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        PULocationID,
        DOLocationID,
        trip_distance,
        fare_amount,
        total_amount,
        payment_type,
        DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
        CASE
            WHEN DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) != 0
            THEN trip_distance / (DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0)
            ELSE 0
        END AS trip_speed_mph,
        HOUR(tpep_pickup_datetime) AS pickup_hour,
        DAYNAME(tpep_pickup_datetime) AS pickup_day_of_week
    FROM read_parquet('{parquet_path}')
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND PULocationID IS NOT NULL
      AND DOLocationID IS NOT NULL
      AND fare_amount IS NOT NULL
      AND trip_distance > 0
      AND fare_amount >= 0
      AND fare_amount <= 500.0
      AND tpep_dropoff_datetime >= tpep_pickup_datetime
""")

con.execute(f"CREATE TABLE zones AS SELECT * FROM read_csv_auto('{csv_path}')")

# Title and introduction
st.title("New York City Yellow Taxi Trips")
st.markdown("This dashboard presents an analysis of NYC yellow taxi trip data for **January 2024**, covering trip patterns, fare trends, payment methods, tipping behavior, and popular routes.")

st.markdown("")

# --- Interactive Filters ---
with st.sidebar:
    st.header("Filters")

    date_bounds = con.execute("""
        SELECT MIN(CAST(tpep_pickup_datetime AS DATE)),
               MAX(CAST(tpep_pickup_datetime AS DATE))
        FROM trips
    """).fetchone()
    min_date, max_date = date_bounds

    date_range = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    hour_range = st.slider("Hour Range", 0, 23, (0, 23))

    payment_labels = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown'}
    selected_payments = st.multiselect(
        "Payment Type",
        options=list(payment_labels.keys()),
        default=list(payment_labels.keys()),
        format_func=lambda x: payment_labels[x],
    )

# Validate filters
if not selected_payments:
    st.warning("Please select at least one payment type.")
    st.stop()

if len(date_range) != 2:
    st.info("Please select both a start and end date.")
    st.stop()

start_date, end_date = date_range
payment_list = ','.join(str(p) for p in selected_payments)

# Apply filters as a streaming view (no data materialised)
con.execute(f"""
    CREATE OR REPLACE VIEW filtered AS
    SELECT * FROM trips
    WHERE CAST(tpep_pickup_datetime AS DATE) >= '{start_date}'
      AND CAST(tpep_pickup_datetime AS DATE) <= '{end_date}'
      AND pickup_hour >= {hour_range[0]}
      AND pickup_hour <= {hour_range[1]}
      AND payment_type IN ({payment_list})
""")

count = con.execute("SELECT COUNT(*) FROM filtered").fetchone()[0]
if count == 0:
    st.warning("No trips match the selected filters. Try adjusting the date range, hours, or payment types.")
    st.stop()

# Key metrics
stats = con.execute("""
    SELECT
        COUNT(*) as total_trips,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(trip_distance), 2) as avg_distance,
        ROUND(AVG(trip_duration_minutes), 1) as avg_duration
    FROM filtered
""").fetchone()

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Trips", f"{stats[0]:,}")
col2.metric("Avg Fare", f"${stats[1]:,.2f}")
col3.metric("Total Revenue", f"${stats[2]:,.2f}")
col4.metric("Avg Distance", f"{stats[3]:.2f} mi")
col5.metric("Avg Duration", f"{stats[4]:.1f} min")

# Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Pickup Zones", "Hourly Fares", "Trip Distance", "Payment Types", "Weekly Patterns"])

# --- Tab 1: Bar chart - Top 10 pickup zones ---
with tab1:
    st.header("Top 10 Busiest Pickup Zones")
    result = con.execute("""
        SELECT z.Zone as zone, COUNT(*) as trip_count
        FROM filtered f
        JOIN zones z ON f.PULocationID = z.LocationID
        GROUP BY f.PULocationID, z.Zone
        ORDER BY trip_count DESC
        LIMIT 10
    """).pl().reverse()

    fig = px.bar(
        result, x='trip_count', y='zone', orientation='h',
        labels={'trip_count': 'Trip Count', 'zone': ''},
        text='trip_count',
    )
    fig.update_traces(texttemplate='%{text:,}', textposition='outside', marker_color=CHART_COLOR)
    fig.update_layout(
        template='plotly_white', title='',
        xaxis=dict(tickformat=',', range=[0, result['trip_count'].max() * 1.15]),
        yaxis=dict(ticksuffix='  '),
        height=500, autosize=True,
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("""
    **Insights:** Midtown Center and Upper East Side South consistently rank as the busiest pickup zones,
    each handling over 140,000 trips in January 2024. Airport zones (JFK and LaGuardia) feature prominently
    in the top 10, reflecting steady demand for airport transfers. The concentration of top zones in
    Manhattan highlights the borough's dominance in yellow taxi usage.
    """)

# --- Tab 2: Line chart - Average fare by hour ---
with tab2:
    st.header("Average Fare by Hour of Day")
    result = con.execute("""
        SELECT pickup_hour as hour_of_day,
               ROUND(AVG(fare_amount), 2) as avg_fare
        FROM filtered
        GROUP BY pickup_hour
        ORDER BY pickup_hour ASC
    """).pl()

    fig = px.line(
        result, x='hour_of_day', y='avg_fare',
        labels={'avg_fare': 'Average Fare', 'hour_of_day': 'Hour of Day'},
        markers=True,
    )
    fig.update_traces(line_color=CHART_COLOR, marker_color=CHART_COLOR)
    fig.update_layout(
        template='plotly_white', title='',
        xaxis=dict(dtick=1, range=[-0.5, 23.5]),
        yaxis=dict(tickprefix='$', tickformat='.2f'),
        height=500, autosize=True,
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("""
    **Insights:** Fares peak sharply around 5-6 AM, likely driven by early morning airport runs which
    tend to be longer-distance trips. Fares dip to their lowest during the late evening hours (6-7 PM)
    when shorter intra-city commutes dominate. The late-night hours (11 PM - 1 AM) show a slight uptick
    as longer trips to outer boroughs become more common.
    """)

# --- Tab 3: Histogram - Trip distance distribution ---
with tab3:
    st.header("Distribution of Trip Distances")
    # Pre-aggregate into bins in SQL to avoid materialising all individual rows
    trip_hist = con.execute("""
        SELECT
            FLOOR(trip_distance * 2) / 2.0 AS bin_start,
            COUNT(*) AS trip_count
        FROM filtered
        WHERE trip_distance <= 30
        GROUP BY bin_start
        ORDER BY bin_start
    """).pl()

    fig = px.bar(
        trip_hist, x='bin_start', y='trip_count',
        labels={'bin_start': 'Trip Distance (miles)', 'trip_count': 'Number of Trips'},
    )
    fig.update_traces(marker_color=CHART_COLOR)
    fig.update_layout(
        template='plotly_white', title='',
        xaxis=dict(dtick=5),
        yaxis=dict(title='Number of Trips'),
        height=500, autosize=True,
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("""
    **Insights:** The distribution is heavily right-skewed, with the vast majority of trips falling
    under 5 miles, reflecting typical short urban rides within Manhattan. A secondary smaller peak
    appears around 17-19 miles, corresponding to the fixed-distance JFK Airport trips from Midtown.
    Very few trips exceed 20 miles, confirming that yellow taxis primarily serve short-to-medium urban routes.
    """)

# --- Tab 4: Bar chart - Payment type breakdown ---
with tab4:
    st.header("Trip Breakdown by Payment Type")
    result = con.execute("""
        SELECT
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                ELSE 'Other'
            END as payment_type,
            COUNT(*) as total,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM filtered
        GROUP BY payment_type
        ORDER BY total ASC
    """).pl()

    fig = px.bar(
        result, x='percentage', y='payment_type', orientation='h',
        labels={'percentage': 'Percentage (%)', 'payment_type': ''},
        text='percentage',
    )
    fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside', marker_color=CHART_COLOR)
    fig.update_layout(
        template='plotly_white', title='',
        xaxis=dict(range=[0, result['percentage'].max() * 1.15], ticksuffix='%'),
        yaxis=dict(ticksuffix='  '),
        height=400, autosize=True,
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("""
    **Insights:** Credit card payments overwhelmingly dominate at around 80% of all trips, reflecting
    the shift toward cashless transactions in modern taxis. Cash still accounts for roughly 15% of trips,
    suggesting a significant minority of riders prefer or require cash payment. Disputes and no-charge
    trips together represent less than 2% of all rides.
    """)

# --- Tab 5: Heatmap - Trips by day of week and hour ---
with tab5:
    st.header("Trip Volume by Day of Week and Hour")
    result = con.execute("""
        SELECT pickup_day_of_week, pickup_hour, COUNT(*) as trip_count
        FROM filtered
        GROUP BY pickup_day_of_week, pickup_hour
    """).pl()

    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    pivot = result.pivot(
        on='pickup_hour',
        index='pickup_day_of_week',
        values='trip_count',
    )

    # Ensure all days are present (fill missing days with zeros)
    for day in day_order:
        if day not in pivot['pickup_day_of_week'].to_list():
            row = {c: 0 for c in pivot.columns}
            row['pickup_day_of_week'] = day
            pivot = pl.concat([pivot, pl.DataFrame([row])], how='diagonal_relaxed')

    # Ensure all hours 0-23 are present (fill missing hours with zeros)
    for h in range(24):
        if str(h) not in pivot.columns:
            pivot = pivot.with_columns(pl.lit(0).alias(str(h)))

    pivot = pivot.sort(
        pl.col('pickup_day_of_week').map_elements(lambda x: day_order.index(x), return_dtype=pl.Int32)
    ).fill_null(0)

    hour_cols = sorted([c for c in pivot.columns if c != 'pickup_day_of_week'], key=int)
    z_values = pivot.select(hour_cols).to_numpy()

    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=[str(h) for h in hour_cols],
        y=day_order,
        colorscale='Teal',
        text=z_values,
        texttemplate='%{text:,.2s}',
        textfont=dict(size=9),
    ))
    fig.update_layout(
        template='plotly_white', title='',
        xaxis=dict(title='Hour of Day', dtick=1),
        yaxis=dict(title='', autorange='reversed'),
        height=450, autosize=True,
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("""
    **Insights:** Weekday evenings (5-7 PM) show the highest trip volumes, aligning with the evening
    rush hour commute. Saturday nights stand out with sustained high activity from 6 PM through midnight,
    reflecting nightlife-driven demand. Early morning hours (3-5 AM) are consistently the quietest across
    all days, with slightly more activity on weekend mornings as late-night riders head home.
    """)
