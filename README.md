# TDK Data Pipeline — Microservices Architecture

A microservices-based data pipeline for ingesting pipe-delimited DSV data into Oracle DB, and exporting it as TSV files. Scheduled execution via APScheduler with Docker Compose orchestration.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Docker Compose Network                       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────────┐    │
│  │  scheduler    │───▶│data-ingestion│───▶│   Oracle DB     │    │
│  │  (APScheduler)│    │  (Flask:5001)│    │ (XE Container)  │    │
│  │  cron: 00:00  │    └──────────────┘    └────────┬────────┘    │
│  │               │                                 │               │
│  │               │    ┌──────────────┐             │               │
│  │               │───▶│ data-export  │◀────────────┘               │
│  │               │    │  (Flask:5002)│                              │
│  └──────────────┘    └──────────────┘                              │
│                             │                                       │
│                     data/output/*.tsv                               │
└─────────────────────────────────────────────────────────────────┘
```

| Service | Port | Endpoint | Role |
|---------|------|----------|------|
| `oracle-db` | 1521 | — | Oracle Database XE 21c (PDB: `XEPDB1`) |
| `data-ingestion` | 5001 | `POST /ingest` | Parse `RAW DATA.dsv` → insert into `C_DUNS_V` |
| `data-export` | 5002 | `POST /export` | Query `C_DUNS_V` → write TSV to `data/output/` |
| `scheduler` | — | — | Triggers ingestion + export nightly at midnight |

## Quick Start

### Prerequisites
- Docker & Docker Compose

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/<your-user>/tdk-data-pipeline.git
cd tdk-data-pipeline

# 2. Setup Config
# Copy .env.example to .env (pre-configured for Docker Oracle XE)
cp .env.example .env

# 3. Place input data
cp "RAW DATA.dsv" data/input/

# 4. Build and run
docker-compose up --build
```

The pipeline runs automatically on startup (`RUN_ON_STARTUP=true` in `.env`).  
Check outputs in `data/output/`.

### Environment Variables

| Variable | Default (Container) | Description |
|----------|---------------------|-------------|
| `ORACLE_HOST` | `oracle-db` | Oracle DB hostname |
| `ORACLE_PORT` | `1521` | Oracle DB port |
| `ORACLE_SERVICE_NAME` | `XEPDB1` | **Required** for XE PDB connection |
| `ORACLE_USER` | `pipeline_user` | Oracle username (auto-created) |
| `ORACLE_PASSWORD` | `oracle123` | Oracle password |
| `RUN_ON_STARTUP` | `true` | Run pipeline immediately on scheduler start |

## Project Structure

```
├── docker-compose.yml        # Orchestrates python services + Oracle XE
├── .env.example              # Environment template
├── shared/
│   └── db_config.py          # Oracle connection helper
├── data-ingestion/
│   ├── Dockerfile
│   ├── app.py                # Flask API
│   └── ingest.py             # DSV parser + Oracle insert
├── data-export/
│   ├── Dockerfile
│   ├── app.py                # Flask API
│   └── export.py             # Oracle query + TSV writer
├── scheduler/
│   ├── Dockerfile
│   └── scheduler.py          # APScheduler cron orchestrator
└── data/
    ├── input/                # RAW DATA.dsv goes here
    └── output/               # TSV exports written here
```

## Manual Trigger

You can trigger services individually without waiting for the nightly schedule.

1. **Open a new terminal window** (keep the `docker-compose` window running).
2. Run the following commands based on your terminal:

### Windows PowerShell
```powershell
# Trigger Ingestion
Invoke-RestMethod -Method Post -Uri "http://localhost:5001/ingest"

# Trigger Export
Invoke-RestMethod -Method Post -Uri "http://localhost:5002/export"
```

### Git Bash / Linux / Mac (curl)
```bash
# Trigger Ingestion
curl -X POST http://localhost:5001/ingest

# Trigger Export
curl -X POST http://localhost:5002/export
```
