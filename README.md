# E-commerce Clickstream Analytics with Apache Spark

A big data pipeline built with **Apache Spark (PySpark)** that processes e-commerce clickstream events:
**Generate -> ETL (Parquet) -> Batch Analytics -> ML Recommender**.

## 1. Problem Statement
Online stores generate millions of click events (views, add-to-cart, purchases). A single-machine tool
(Excel/pandas) cannot process this volume efficiently. This project uses Spark's distributed
DataFrame engine to clean, store, analyse and model such data at scale.

## 2. Objectives
- Build a scalable ETL pipeline with a defined schema and data cleaning
- Store data in columnar, partitioned Parquet format
- Compute business insights: revenue, top products, funnel, sessions
- Build a collaborative-filtering recommender with Spark MLlib (ALS)

## 3. Architecture
```
 generate_data.py --> raw_events.csv
        |
        v
 etl.py: schema, drop null users, dedupe, add date/hour --> Parquet (partitioned by date)
        |
        +--> analytics.py: revenue, top products, hourly traffic, funnel, sessionization --> output/*.csv
        |
        +--> recommend.py: ALS model, RMSE, top-5 products per user
```

## 4. Tech Stack
Python 3, Apache Spark 3.5 (PySpark: DataFrames, Spark SQL, Window functions, MLlib), Parquet, Pandas.

## 5. Setup & Run
Requirements: Java 8/11/17, Python 3.9+
```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
python src/generate_data.py --rows 500000
python run_pipeline.py
```
Spark UI during execution: http://localhost:4040

## 6. Results (500,000 generated events)
| Metric | Value |
|---|---|
| Raw rows | 500,003 |
| Clean rows after ETL | 494,961 (about 1% dirty rows removed) |
| Sessions detected (30-min inactivity rule) | 58,645 |
| Avg events per session | 8.44 |
| Avg session duration | 695 s |
| ALS recommender RMSE | 1.21 |

**Revenue by category:** beauty 1.12M > home 1.08M > books 1.05M > electronics 1.05M > fashion 1.05M > sports 0.89M

**Conversion funnel (distinct users):** view 18,908 -> cart 17,394 -> purchase 12,629

Full outputs are in `output/` after running the pipeline (`revenue_by_category.csv`, `top_products.csv`,
`hourly_traffic.csv`, `funnel.csv`, `als_metrics.txt`).

## 7. Key Concepts Used
- **Lazy evaluation & DAG** - Spark builds a plan and executes on actions
- **Parquet + partitioning** - columnar compression, faster reads, partition pruning by `date`
- **Window functions** - `lag` + running `sum` for sessionization
- **ALS** - matrix-factorization collaborative filtering on implicit ratings (view=1, cart=3, purchase=5)

## 8. Limitations & Future Work
- Data is synthetic; the funnel is not strictly nested (users are counted per event type)
- Runs in local mode, not on a multi-node cluster
- Future: Kafka + Structured Streaming, HDFS/S3 storage, Streamlit dashboard, performance benchmarks

## 9. Project Structure
```
src/            generate_data.py, etl.py, analytics.py, recommend.py, spark_session.py, config.py
run_pipeline.py runs all stages
data/ output/   generated at runtime (git-ignored)
docs/roadmap.md
```
