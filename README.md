# PySpark Movie Recommendation System

Production-ready end-to-end MovieLens recommendation system using ETL + ALS + Hybrid Re-ranking + FastAPI + Streamlit/React UI.

## Run Commands

One-command full stack (build frontend, start API, serve dist):

```powershell
cd backend
powershell -ExecutionPolicy Bypass -File .\run_one_http.ps1
```

Split mode (dev):

```powershell
# Terminal 1 - API
cd backend
..\.venv\Scripts\python.exe run_api.py

# Terminal 2 - React dev server
cd frontend\web
npm install
npm run dev -- --host 127.0.0.1 --port 4173 --strictPort
```

Pipeline (ETL + training + fallback precompute):

```bash
cd backend
python run_pipeline.py
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

## Project Structure (simplified)

```text
.
|-- backend/
|   |-- api/                    # FastAPI app (main.py)
|   |-- configs/                # YAML configs (app, spark)
|   |-- data/                   # data/raw, data/silver, data/gold, data/sql
|   |-- src/                    # ETL, model, serving, utils
|   |-- run_api.py              # start API
|   |-- run_pipeline.py         # ETL + training pipeline
|   `-- run_one_http.ps1        # build frontend + start API
|-- frontend/
|   `-- web/                    # React/Vite UI (dark-gold)
|       |-- src/
|       |-- package.json
|       `-- vite.config.ts
|-- tools/                      # optional
|-- ui/                         # legacy Streamlit demo
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
- `POST /dang-ky`
- `POST /dang-nhap`
- `GET /ho-so`
- `POST /ho-so`
- `GET /goi-y/{id_ho_so}?top_n=10&genre=Drama`
- `POST /phan-hoi`
- `GET /lich-su-goi-y/{id_ho_so}`

Default demo account (auto-seeded in SQL store):

- `ten_dang_nhap`: `demo`
- `mat_khau`: `demo123`

## React UI Notes

- React UI default URL: `http://localhost:5173`
- API default URL: `http://127.0.0.1:8000`
- CORS origins are configured in `configs/app_config.yaml` under `api.cors_origins`
- Optional frontend env file: `web/.env` (see `web/.env.example`)
