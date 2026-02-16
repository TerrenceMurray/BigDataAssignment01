# COMP 3610 — Assignment 1: NYC Yellow Taxi Trip Analysis

An end-to-end data pipeline that ingests, transforms, and analyzes the NYC Yellow Taxi Trip dataset (January 2024), with an interactive Streamlit dashboard.

## Requirements

| Library | Purpose |
|---------|---------|
| `requests` | Downloading dataset files |
| `polars` | Data transformation and cleaning |
| `pyarrow` | Parquet schema validation |
| `duckdb` | SQL querying |
| `plotly` | Visualizations |
| `streamlit` | Interactive dashboard |

Install all dependencies:

```bash
pip install -r requirements.txt
```

## How to Run

### 1. Run the Notebook

The notebook downloads the data, validates the schema, cleans the dataset, and runs the SQL analyses.

```bash
jupyter notebook Assignmet01.ipynb
```

Run all cells in order. This will download `yellow_tripdata_2024-01.parquet` and `taxi_zone_lookup.csv` into `data/raw/`.

### 2. Launch the Dashboard

```bash
streamlit run app.py
```

This opens an interactive dashboard with filters for date range, hour, and payment type, along with five visualizations covering pickup zones, hourly fares, trip distances, payment types, and weekly patterns.
