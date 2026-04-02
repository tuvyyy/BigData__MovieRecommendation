# PySpark Movie Recommendation System

Production-ready end-to-end MovieLens recommendation system using ETL + ALS + Hybrid Re-ranking + FastAPI + Streamlit/React UI.

## Run Commands

Build full pipeline (ETL + training + fallback):

```bash
python run_pipeline.py
```

Run Streamlit UI:

```bash
streamlit run ui/streamlit_app.py
```

Optional API server:

```bash
python run_api.py
```

Run React web UI (landing page + recommendation studio):

```bash
cd web
npm install
npm run dev
```

## Features

- Explicit `StructType` schemas (no `inferSchema`)
- Reads MovieLens ratings/movies from `.csv` or `.dat` (`::` delimiter)
- ETL quality checks:
  - drop nulls
  - filter ratings to `[0.5, 5.0]`
  - remove duplicates
  - user bucketing + repartition
- Silver outputs:
  - `data/silver/ratings_parquet` (partitioned by `user_bucket`)
  - `data/silver/movies_parquet`
- ALS training:
  - fixed-seed split `80/20`
  - hyperparameter tuning (`rank`, `regParam`, `maxIter`)
  - RMSE evaluation
  - ranking evaluation (`Precision@K`, `Recall@K`, `NDCG@K`)
  - best model saved + timestamped versioning
  - RMSE table saved to `metrics/rmse_results.csv`
- Hybrid recommendation:
  - content-based user genre profile
  - weighted blend score: `0.7 * ALS + 0.3 * Content` (configurable)
  - re-ranking with genre boost + diversity penalty + popularity/recency boosts
  - explanation fields per item:
    - `Because you liked ...`
    - `Users similar to you liked this`
    - `Matches your favorite genre ...`
- Precomputed serving design:
  - offline build for all users
  - saved to `data/gold/recommendations_precomputed`
  - API reads precomputed parquet (with optional Redis cache)
- Spark optimization:
  - AQE enabled
  - shuffle partitions = 400
  - broadcast movie metadata join
  - cached reused DataFrames
  - explain plans saved before/after optimization
- Cold-start fallback:
  - global Top-N
  - genre Top-N
  - router Hybrid vs fallback
- Feedback loop:
  - append ratings
  - optional retrain trigger
- User behavior tracking:
  - `/event` endpoint for `click/view/rate/skip`
  - event logs in `data/logs/user_events`
- Logging:
  - `logs/app.log`
  - `logs/etl.log`
  - `logs/train.log`
- Frontend options:
  - Streamlit quick demo UI (`ui/streamlit_app.py`)
  - React product-style web UI (`web/`) with:
    - Landing home page
    - Recommendation studio page
    - In-page rating submission flow

## Project Structure

```text
.
|-- api/
|   `-- main.py
|-- configs/
|   |-- app_config.yaml
|   `-- spark_config.yaml
|-- data/
|   |-- feedback/
|   |-- gold/
|   |-- raw/
|   `-- silver/
|-- logs/
|-- metrics/
|-- models/
|   `-- versions/
|-- src/
|   |-- etl/
|   |   |-- ingest.py
|   |   |-- pipeline.py
|   |   |-- schemas.py
|   |   `-- transform.py
|   |-- model/
|   |   |-- evaluate.py
|   |   |-- fallback.py
|   |   |-- feedback.py
|   |   |-- pipeline.py
|   |   |-- precompute.py
|   |   `-- train_als.py
|   |-- hybrid/
|   |   |-- content_based.py
|   |   |-- explain.py
|   |   `-- rerank.py
|   |-- serving/
|   |   |-- cache.py
|   |   `-- events.py
|   |-- service/
|   |   `-- recommendation_service.py
|   `-- utils/
|       |-- config.py
|       |-- logger.py
|       `-- spark_session.py
|-- ui/
|   `-- streamlit_app.py
|-- web/
|   |-- src/
|   |-- package.json
|   `-- vite.config.ts
|-- run_api.py
|-- run_pipeline.py
`-- requirements.txt
```

## Data Setup

Place MovieLens files under `data/raw/` (or subfolders):

- `ratings.csv` or `ratings.dat`
- `movies.csv` or `movies.dat`

The ETL layer auto-detects supported files.

## Environment Notes

- Python 3.11/3.12 recommended
- Java 8/11/17+ required for Spark (Java 17 recommended)
- Local Spark UI during pipeline run: `http://localhost:4040`
- Node.js 20+ recommended for React web UI

## API Endpoints

- `GET /health`
- `GET /recommend/{user_id}?top_n=10&genre=Action`
- `POST /rate`
- `POST /event`

## React UI Notes

- React UI default URL: `http://localhost:5173`
- API default URL: `http://127.0.0.1:8000`
- CORS origins are configured in `configs/app_config.yaml` under `api.cors_origins`
- Optional frontend env file: `web/.env` (see `web/.env.example`)
