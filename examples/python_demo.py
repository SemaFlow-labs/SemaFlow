"""
End-to-end Python demo using the Rust core via PyO3 bindings.

Steps:
- seed a DuckDB file with sample data
- load semantic tables/models from semaflowrs/examples/models
- list models/dimensions/measures
- build SQL for a request
- execute and print results
"""

from pathlib import Path

import duckdb

from semaflow import build_sql, load_models_from_dir, run_query


def seed_duckdb(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            country VARCHAR
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DOUBLE,
            created_at TIMESTAMP
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'US'),
            (2, 'Bob', 'UK'),
            (3, 'Carla', 'US');
        INSERT INTO orders VALUES
            (1, 1, 100.0, '2023-01-01'),
            (2, 1, 50.0, '2023-01-02'),
            (3, 2, 25.0, '2023-01-03');
        """
    )
    conn.close()


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    model_root = project_root / "semaflowrs" / "examples" / "models"
    db_path = project_root / "examples" / "demo_python.duckdb"

    seed_duckdb(db_path)

    tables, models = load_models_from_dir(model_root)
    data_sources = {"duckdb_local": str(db_path)}
    table_map = {t["name"]: t for t in tables}

    print("Models:")
    for model in models:
        print(f"- {model['name']}")
    print()

    for model in models:
        base = model["base_table"]["semantic_table"]
        dims = list(table_map[base].get("dimensions", {}).keys())
        measures = list(table_map[base].get("measures", {}).keys())
        print(f"Model {model['name']}:")
        print(f"  base_table: {base}")
        print(f"  dimensions: {dims}")
        print(f"  measures: {measures}")
        for join_name, join in model.get("joins", {}).items():
            jt = table_map[join["semantic_table"]]
            jdims = list(jt.get("dimensions", {}).keys())
            jmeasures = list(jt.get("measures", {}).keys())
            print(f"  join {join_name}: dims={jdims}, measures={jmeasures}")
    print()

    request = {
        "model": "sales",
        "dimensions": ["country"],
        "measures": ["order_total", "distinct_customers"],
        "filters": [],
        "order": [{"column": "order_total", "direction": "desc"}],
        "limit": 10,
    }

    sql = build_sql(tables, models, data_sources, request)
    print("SQL:")
    print(sql)
    print()

    rows = run_query(tables, models, data_sources, request)
    print("Results:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
