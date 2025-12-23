# BigQuery Example

Demonstrates SemaFlow with Google BigQuery as the backend.

## Prerequisites

1. **GCP Project** with BigQuery enabled
2. **Authentication** via one of:
   - Application Default Credentials: `gcloud auth application-default login`
   - Service account key file: Set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`
3. **Tables** matching the flow definitions in `flows/`

## Setup

Set required environment variables:

```bash
export GCP_PROJECT_ID=your-project-id
export BQ_DATASET=your_dataset_name
```

## Running

From the repository root:

```bash
uv run python examples/bigquery/demo.py
```

Or with explicit environment variables:

```bash
GCP_PROJECT_ID=my-project BQ_DATASET=my_dataset uv run python examples/bigquery/demo.py
```

## Flow Definition

The example uses a `sales` flow defined in `flows/` that expects:

- `fct_orders` - Orders fact table with `order_id`, `customer_id`, `product_id`, `total_amount`, etc.
- `dim_customers` - Customer dimension with `customer_id`, `country`, `email`, etc.
- `dim_products` - Product dimension with `product_id`, `name`, `category`, `price`

Modify the YAML files in `flows/` to match your schema.

## What It Demonstrates

1. **Table Qualification** - BigQuery tables are properly qualified as `` `project`.`dataset`.`table` ``
2. **Multi-grain Queries** - Measures from multiple tables with proper re-aggregation
3. **Filter Placement** - LEFT join filters applied at outer query level
4. **Derived Measures** - Computed measures like `avg_order_amount = order_total / order_count`
