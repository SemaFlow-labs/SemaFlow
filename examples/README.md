# SemaFlow Examples

This directory contains examples for both DuckDB and PostgreSQL backends.

## Directory Structure

```
examples/
├── duckdb/                 # DuckDB examples
│   ├── flows/              # Semantic layer definitions
│   │   ├── tables/         # Table definitions
│   │   └── flows/          # Flow definitions (joins)
│   └── demo.py             # Run this to test DuckDB
│
├── postgres/               # PostgreSQL examples
│   ├── flows/              # Semantic layer definitions
│   │   ├── tables/         # Table definitions
│   │   └── flows/          # Flow definitions (joins)
│   ├── docker-compose.yml  # Spin up PostgreSQL
│   ├── init.sql            # Seed data (auto-runs)
│   └── demo.py             # Run this to test PostgreSQL
│
└── README.md               # This file
```

---

## DuckDB Example

DuckDB is embedded - no server needed.

```bash
cd examples/duckdb
uv run python demo.py
```

The demo creates `sales.duckdb`, seeds it, and runs queries.

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

## Passing Credentials

### DuckDB

DuckDB just needs a file path - no credentials:

```python
ds = DataSource.duckdb("path/to/database.duckdb", name="my_db")
```

### PostgreSQL

PostgreSQL credentials can be passed in several ways:

#### Option 1: URL Format (Recommended)

```python
ds = DataSource.postgres(
    "postgresql://user:password@host:5432/database",
    schema="public",
    name="my_pg"
)
```

URL-encode special characters in passwords:
- `@` → `%40`
- `:` → `%3A`
- `/` → `%2F`

#### Option 2: Key-Value Format

```python
ds = DataSource.postgres(
    "host=localhost port=5432 user=myuser password=mypass dbname=mydb",
    schema="public",
    name="my_pg"
)
```

#### Option 3: Environment Variables

```python
import os

conn = f"postgresql://{os.environ['PG_USER']}:{os.environ['PG_PASS']}@{os.environ['PG_HOST']}/{os.environ['PG_DB']}"
ds = DataSource.postgres(conn, schema="public", name="my_pg")
```

### Security Best Practices

1. **Never commit credentials to git** - use environment variables or secrets managers
2. **Use read-only database users** for analytics queries
3. **Restrict network access** - don't expose PostgreSQL to the internet
4. **Use SSL in production** - add `?sslmode=require` to connection string

---

## Schema Parameter

PostgreSQL tables live in schemas (default: `public`). The schema parameter is **required**:

```python
# Query tables in the "public" schema
ds = DataSource.postgres(conn_string, schema="public", name="pg")

# Query tables in a custom schema
ds = DataSource.postgres(conn_string, schema="analytics", name="pg")
```

The schema is used when querying `information_schema` for table metadata and when qualifying table names in generated SQL.

---

## Connection Pool

Both backends use connection pooling:

```python
# DuckDB - set max concurrent connections
ds = DataSource.duckdb("db.duckdb", name="duck", max_concurrency=8)

# PostgreSQL - pool size via deadpool (default: CPU count)
ds = DataSource.postgres(conn, schema="public", name="pg", max_concurrency=10)
```

---

## Troubleshooting

### PostgreSQL Connection Refused

```
get postgres connection: Connection refused
```

1. Check Docker is running: `docker ps`
2. Check PostgreSQL is ready: `docker compose logs postgres`
3. Verify port 5432 is available: `lsof -i :5432`

### Unknown Backend Type

```
unknown backend_type: postgres. Supported: duckdb, postgres
```

Rebuild with PostgreSQL support:
```bash
uv run maturin develop --features "python,postgres"
```

### Schema Not Found

```
relation "orders" does not exist
```

Verify the schema parameter matches where your tables are:
```sql
-- Check which schema has your tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name = 'orders';
```
