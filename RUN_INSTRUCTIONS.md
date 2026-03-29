# Run Instructions

This guide explains how to run the ETL project both manually and with Apache Airflow.

## 1) Prerequisites

- Python 3.8 or 3.9 recommended (Airflow 2.2.3 compatibility)
- PostgreSQL, MySQL, and SQL Server instances available (or whichever sources you plan to test)
- ODBC Driver 17 for SQL Server (needed by `pyodbc` connection string in `config.ini`)
- Network access to your database hosts and to AccuWeather API

## 2) Clone and Install

From the project root:

- Windows (PowerShell):
  - `python -m venv .venv`
  - `.venv\Scripts\Activate.ps1`
  - `pip install -r requirements.txt`

- Linux/macOS:
  - `python3 -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -r requirements.txt`

## 3) Configure Connections (`config.ini`)

Update sections in `config.ini`:

- `[sqlserver]` source metadata and table list
- `[mysql]` source metadata and table list
- `[postgresql]` target metadata

Important fields:
- `host`, `port`, `user`, `db`, `schema`
- `dbtype`, `library`, `driver` for SQLAlchemy URI creation
- `tables` (comma-separated source tables)

## 4) Set Secrets

Set these environment variables before running:

- `PASSWORD`: password used for all database users in generated connection URI
- `API_KEY`: AccuWeather API key

PowerShell example:
- `$env:PASSWORD="your_db_password"`
- `$env:API_KEY="your_accuweather_key"`

If unset, code tries Airflow Variables:
- `password`
- `api_key`

## 5) Prepare PostgreSQL Objects

Execute `slowly_changing_dimension.sql` in your PostgreSQL target database to create:
- `public.dim_employees`
- `public.employees_skey_serial`
- `public.transform_dim_employees()` stored procedure

## 6) Fix Config Path in Code (Required)

`extract.py` contains a machine-specific absolute path:
- `CONFIG_FILE = "/home/aahmed/code/data-engineering-tutorial-youtube/config.ini"`

Before running, change this to your local project path (or refactor to relative path).

## 7) Manual Execution

Run from project root:

1. Extract and load DB tables:
   - `python extract.py`
2. Extract API data and load into PostgreSQL:
   - `python extract_from_api.py`
3. Run transformation procedure:
   - `python transform.py`

## 8) Airflow Execution

### Initialize Airflow

- `airflow db init`
- `airflow users create --username admin --firstname admin --lastname user --role Admin --email admin@example.com --password admin`

### Register Variables (if not using env vars)

- `airflow variables set password "your_db_password"`
- `airflow variables set api_key "your_accuweather_key"`

### Ensure DAG Is Discoverable

Because `dags.py` and helper modules are in this repo root, point Airflow DAG folder to this project root.

PowerShell example:
- `$env:AIRFLOW__CORE__DAGS_FOLDER="C:\Users\User\Downloads\data-engineering-tutorial-youtube-main\data-engineering-tutorial-youtube-main"`

### Start Airflow Services

- Terminal 1: `airflow webserver --port 8080`
- Terminal 2: `airflow scheduler`

Open [http://localhost:8080](http://localhost:8080), enable `etl_tutorial_dag`, and trigger it.

## 9) Troubleshooting

- `ModuleNotFoundError` from DAG imports:
  - Verify `AIRFLOW__CORE__DAGS_FOLDER` points to this repo root.
- SQL Server connection failures:
  - Confirm ODBC Driver 17 is installed and `driver` in `config.ini` matches.
- Authentication failures:
  - Confirm `PASSWORD`/`API_KEY` env vars or Airflow Variables are set correctly.
- Transform step fails:
  - Ensure `slowly_changing_dimension.sql` was executed in target PostgreSQL DB.
