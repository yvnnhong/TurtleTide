import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import os

import json
import tempfile

if "gcp" in st.secrets:
    credentials_dict = json.loads(st.secrets["gcp"]["credentials"])
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(credentials_dict, f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
else:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS", "C:/Users/yvonn/TurtleTide/turtletide-key.json"
    )

PROJECT = "manifest-stream-452700-g7"

st.set_page_config(
    page_title="TurtleTide",
    page_icon="🐢",
    layout="wide"
)

st.title("TurtleTide")
st.caption("Sea turtle occurrence dashboard powered by OBIS, Delta Lake, dbt, and BigQuery")

@st.cache_data
def load_sightings():
    client = bigquery.Client(project=PROJECT)
    query = """
        SELECT occurrence_id, scientific_name, latitude, longitude,
               event_date, depth, life_stage, basis_of_record,
               sea_surface_temp, sea_surface_salinity, dataset_id
        FROM `manifest-stream-452700-g7.turtletide_silver.occurrences`
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return client.query(query).to_dataframe()

@st.cache_data
def load_anomalies():
    client = bigquery.Client(project=PROJECT)
    query = """
        SELECT ocean_basin, scientific_name, year, month,
               sighting_count, avg_sst, avg_sss, avg_depth
        FROM `manifest-stream-452700-g7.turtletide_gold.rpt_basin_anomalies`
        ORDER BY year, month
    """
    return client.query(query).to_dataframe()

with st.spinner("Loading turtle data..."):
    df = load_sightings()
    anomaly_df = load_anomalies()

st.success(f"Loaded {len(df):,} sightings")

# ── Sidebar filters ────────────────────────────────────────────────
st.sidebar.header("Filters")

SPECIES_MAP = {
    "Leatherback": ["Dermochelys coriacea"],
    "Green Turtle": ["Chelonia mydas"],
    "Loggerhead": ["Caretta caretta"],
    "Hawksbill": ["Eretmochelys imbricata"],
}

# Build reverse map: scientific name → common name
all_scientific = df["scientific_name"].dropna().unique().tolist()
def map_to_common(sci_name):
    for common, variants in SPECIES_MAP.items():
        for v in variants:
            if v.lower() in sci_name.lower():
                return common
    return "Other"

df["common_name"] = df["scientific_name"].apply(map_to_common)
df["life_stage_clean"] = df["life_stage"].fillna("Unknown")

selected_common = st.sidebar.multiselect(
    "Species",
    list(SPECIES_MAP.keys()) + ["Other"],
    default=list(SPECIES_MAP.keys()) + ["Other"]
)

filtered_df = df[
    df["common_name"].isin(selected_common) &
    df["life_stage_clean"].isin(selected_life_stages)
]

# normalize life stage for filtering
df["life_stage_clean"] = df["life_stage"].fillna("Unknown")
life_stage_options = sorted(df["life_stage_clean"].unique().tolist())
selected_life_stages = st.sidebar.multiselect("Life Stage", life_stage_options, default=life_stage_options)

filtered_df = df[
    df["scientific_name"].isin(selected_species) &
    df["life_stage_clean"].isin(selected_life_stages)
]

# ── Map ────────────────────────────────────────────────────────────
st.subheader("Sighting Locations")

map_df = filtered_df[["latitude", "longitude"]].dropna().copy()

layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_df,
    get_position="[longitude, latitude]",
    get_color=[0, 180, 140, 160],
    get_radius=30000,
    pickable=True,
)

view_state = pdk.ViewState(latitude=0, longitude=0, zoom=1)
st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
))

# ── Metrics row ────────────────────────────────────────────────────
st.subheader("Summary Stats")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Sightings", f"{len(filtered_df):,}")
col2.metric("Avg SST (C)", f"{filtered_df['sea_surface_temp'].mean():.1f}")
col3.metric("Avg Depth (m)", f"{filtered_df['depth'].mean():.1f}")
col4.metric("Unique Datasets", f"{filtered_df['dataset_id'].nunique()}")

# ── Charts row 1 ───────────────────────────────────────────────────
st.subheader("Environmental Distributions")
col1, col2 = st.columns(2)

with col1:
    sst_data = filtered_df["sea_surface_temp"].dropna()
    fig = px.histogram(sst_data, nbins=40, title="Sea Surface Temperature (C)",
                       labels={"value": "SST (C)"},
                       color_discrete_sequence=["#00b49c"])
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    depth_data = filtered_df["depth"].dropna()
    fig = px.histogram(depth_data, nbins=40, title="Depth Distribution (m)",
                       labels={"value": "Depth (m)"},
                       color_discrete_sequence=["#0068c9"])
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Charts row 2 ───────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    life_stage_counts = filtered_df["life_stage_clean"].value_counts().reset_index()
    life_stage_counts.columns = ["life_stage", "count"]
    fig = px.bar(life_stage_counts, x="life_stage", y="count",
                 title="Sightings by Life Stage",
                 color_discrete_sequence=["#00b49c"])
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    scatter_data = filtered_df[["sea_surface_temp", "depth", "scientific_name"]].dropna()
    fig = px.scatter(scatter_data, x="sea_surface_temp", y="depth",
                     color="scientific_name",
                     title="SST vs Depth",
                     labels={"sea_surface_temp": "SST (C)", "depth": "Depth (m)"},
                     opacity=0.5,
                     color_discrete_sequence=["#00b49c", "#0068c9", "#00d4ff", "#7b2fff"])
    st.plotly_chart(fig, use_container_width=True)

# ── Anomaly chart ──────────────────────────────────────────────────
st.subheader("Sightings Over Time by Ocean Basin")
if not anomaly_df.empty:
    anomaly_df["date"] = pd.to_datetime(
        anomaly_df[["year", "month"]].assign(day=1)
    )
    fig = px.line(anomaly_df, x="date", y="sighting_count",
                  color="ocean_basin",
                  title="Monthly Sightings by Ocean Basin",
                  labels={"sighting_count": "Sightings", "date": "Date"},
                  color_discrete_sequence=["#00b49c", "#0068c9", "#00d4ff", "#7b2fff", "#00ff88"])
    st.plotly_chart(fig, use_container_width=True)

st.caption("Data source: OBIS (CC0) | Pipeline: GCS -> Dataproc -> Delta Lake -> dbt -> BigQuery")