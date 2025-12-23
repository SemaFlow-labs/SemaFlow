# SemaFlow Examples

Examples for DuckDB, PostgreSQL, and BigQuery backends using shared semantic definitions.

## Directory Structure

```
examples/
├── flows/                  # Shared semantic definitions (all backends use these)
│   ├── tables/             # Table definitions (customers, orders, products)
│   └── flows/              # Flow definitions (sales)
│
├── duckdb/                 # DuckDB-specific files
│   └── demo.py             # DuckDB demo script
│
├── postgres/               # PostgreSQL-specific files
│   ├── docker-compose.yml  # Spin up PostgreSQL
│   ├── init.sql            # Seed data (auto-runs)
│   └── demo.py             # PostgreSQL demo script
│
├── bigquery/               # BigQuery-specific files
│   └── demo.py             # BigQuery demo script
│
├── semaflow.toml           # Configuration (data sources, flows path)
├── semantic_api.py         # FastAPI server example
└── README.md               # This file
```

## Shared Semantic Layer

All backends use the same semantic definitions in `flows/`. The `data_source` field references a named connection configured per-backend:

```yaml
# flows/tables/orders.yaml
name: orders
data_source: warehouse  # Configured differently per backend
table: fct_orders
# ...
```

---

## DuckDB Example

DuckDB is embedded - no server needed.

```bash
cd examples/duckdb
uv run python demo.py
```

The demo creates a database, seeds it, and runs queries.

---

## PostgreSQL Example

### 1. Start PostgreSQL

```bash
cd examples/postgres
docker compose up -d
```

Wait for it to be ready:
```bash
docker compose logs -f
# Look for: "database system is ready to accept connections"
```

### 2. Run the Demo

```bash
uv run python demo.py
```

### 3. Stop PostgreSQL

```bash
docker compose down      # Keep data
docker compose down -v   # Delete data
```

---

## BigQuery Example

Requires Google Cloud credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
cd examples/bigquery
uv run python demo.py
```

---

## Configuring Data Sources

Each backend configures the `warehouse` data source differently:

### DuckDB

```python
ds = DataSource.duckdb("sales.duckdb", name="warehouse")
handle = FlowHandle.from_dir("../flows/", [ds])
```

### PostgreSQL

```python
ds = DataSource.postgres(
    "postgresql://user:pass@localhost:5432/db",
    schema="public",
    name="warehouse"
)
handle = FlowHandle.from_dir("../flows/", [ds])
```

### BigQuery

```python
ds = DataSource.bigquery(
    project_id="my-project",
    dataset="analytics",
    name="warehouse"
)
handle = FlowHandle.from_dir("../flows/", [ds])
```

---

## Connection Options

### DuckDB

```python
ds = DataSource.duckdb("path/to/database.duckdb", name="my_db")
```

### PostgreSQL

```python
# URL format (recommended)
ds = DataSource.postgres(
    "postgresql://user:password@host:5432/database",
    schema="public",
    name="my_pg"
)

# Key-value format
ds = DataSource.postgres(
    "host=localhost port=5432 user=myuser password=mypass dbname=mydb",
    schema="public",
    name="my_pg"
)
```

URL-encode special characters in passwords: `@` → `%40`, `:` → `%3A`, `/` → `%2F`

### BigQuery

```python
ds = DataSource.bigquery(
    project_id="my-project",
    dataset="analytics",
    name="bq"
)
```

Uses Application Default Credentials or `GOOGLE_APPLICATION_CREDENTIALS`.

---

## Security Best Practices

1. **Never commit credentials to git** - use environment variables or secrets managers
2. **Use read-only database users** for analytics queries
3. **Restrict network access** - don't expose databases to the internet
4. **Use SSL in production** - add `?sslmode=require` to PostgreSQL connection strings

---

## Connection Pool

All backends use connection pooling:

```python
# DuckDB
ds = DataSource.duckdb("db.duckdb", name="duck", max_concurrency=8)

# PostgreSQL
ds = DataSource.postgres(conn, schema="public", name="pg", max_concurrency=10)
```

---

## Troubleshooting

### PostgreSQL Connection Refused

1. Check Docker is running: `docker ps`
2. Check PostgreSQL is ready: `docker compose logs postgres`
3. Verify port 5432 is available: `lsof -i :5432`

### Unknown Backend Type

Rebuild with the required backend:
```bash
uv run maturin develop --features "python,postgres,bigquery"
```

### Schema Not Found

Verify the schema parameter matches where your tables are:
```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name = 'orders';
```
