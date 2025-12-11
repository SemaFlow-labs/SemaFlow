# Join Semantics

This document explains how joins and filters interact in SemaFlow, particularly regarding NULL values and row preservation.

## Join Types

SemaFlow supports four join types, defined in the flow configuration:

| Type | SQL | Behavior |
|------|-----|----------|
| `left` | `LEFT JOIN` | Keep all base rows, NULL for non-matching joined rows |
| `inner` | `INNER JOIN` | Only rows that match in both tables |
| `right` | `RIGHT JOIN` | Keep all joined rows, NULL for non-matching base rows |
| `full` | `FULL JOIN` | Keep all rows from both tables |

### Configuration

```yaml
# flows/sales.yaml
joins:
  c:
    semantic_table: customers
    to_table: o
    join_type: left    # ← This controls join behavior
    join_keys:
      - left: customer_id
        right: id
```

---

## Filter Behavior with Joins

### Key Principle

**Filters on joined tables go INTO that table's aggregation (CTE), not the final WHERE clause.**

This has important implications for LEFT JOINs:

### Example: LEFT JOIN with Filter

```python
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "c.customer_count"],
    "filters": [{"field": "c.country", "op": "==", "value": "US"}]
})
```

**Generated SQL:**

```sql
SELECT "c_agg"."country", "o_agg"."order_total", "c_agg"."customer_count"
FROM (
    SELECT "o"."customer_id", SUM("o"."amount") AS "order_total"
    FROM "orders" "o"
    GROUP BY "o"."customer_id"
) "o_agg"
LEFT JOIN (
    SELECT "c"."id", "c"."country", COUNT("c"."id") AS "customer_count"
    FROM "customers" "c"
    WHERE ("c"."country" = 'US')   -- Filter is INSIDE the CTE
    GROUP BY "c"."id", "c"."country"
) "c_agg" ON ("o_agg"."customer_id" = "c_agg"."id")
```

**Result:**

| c.country | o.order_total | c.customer_count |
|-----------|---------------|------------------|
| US | 150.0 | 1 |
| NULL | 25.0 | NULL |

The second row appears because:
1. The orders CTE aggregates ALL orders
2. The customers CTE only includes US customers
3. LEFT JOIN preserves orders without matching customers → NULL

---

## Controlling NULL Behavior

### Option 1: Change Join Type to INNER

If you want to exclude non-matching rows entirely, use `join_type: inner`:

```yaml
joins:
  c:
    semantic_table: customers
    to_table: o
    join_type: inner   # ← Changed from 'left'
    join_keys:
      - left: customer_id
        right: id
```

**Result with INNER JOIN:**

| c.country | o.order_total | c.customer_count |
|-----------|---------------|------------------|
| US | 150.0 | 1 |

Only orders with matching US customers are returned.

### Option 2: Accept NULL Values

LEFT JOIN + filter is valid when you want:
- "Show all orders, but only include customer data for US customers"
- Base rows (orders) are preserved
- Joined data (customer info) is NULL for non-matches

This is the intended SQL semantic for LEFT JOIN.

---

## When to Use Each Approach

| Use Case | Join Type | Filter Behavior |
|----------|-----------|-----------------|
| "Show all orders, customer info where available" | `left` | NULLs for non-matching |
| "Show only orders with US customers" | `inner` | No NULLs, excludes non-matching |
| "Show all customers, their orders where available" | `right` | NULLs for customers without orders |

---

## Multi-Table Measures

When measures come from multiple tables, each table gets its own CTE:

```python
# Request measures from both orders and customers tables
{
    "measures": ["o.order_total", "c.customer_count"],
    "dimensions": ["c.country"]
}
```

```sql
-- Each table is pre-aggregated independently
FROM (orders aggregated) o_agg
LEFT JOIN (customers aggregated) c_agg ON ...
```

### Filter Placement in Multi-Grain

- Filters on table X go into table X's CTE
- Filters on base table go into base CTE
- Filters on dimension-only tables go into final WHERE

```
Filter on c.country = 'US'
    ↓
Goes into customers CTE (c_agg)
    ↓
customers CTE only contains US customers
    ↓
LEFT JOIN preserves orders without US customer match → NULL
```

---

## Cardinality and Pre-Aggregation

### Why Pre-Aggregation?

Without pre-aggregation, joining can cause "fanout" - inflating aggregates:

```sql
-- BAD: If customers had duplicates, SUM would be wrong
SELECT SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE c.country = 'US'
```

SemaFlow detects fanout risk and uses CTEs:

```sql
-- GOOD: Aggregate first, then join
SELECT o_agg.order_total
FROM (SELECT customer_id, SUM(amount) as order_total FROM orders GROUP BY customer_id) o_agg
JOIN customers c ON o_agg.customer_id = c.id
WHERE c.country = 'US'
```

### Cardinality Hints

For multi-table measures, SemaFlow needs to know join cardinality:

```yaml
joins:
  c:
    semantic_table: customers
    to_table: o
    join_type: left
    cardinality: many_to_one  # orders → customers is many-to-one
    join_keys:
      - left: customer_id
        right: id
```

If cardinality cannot be inferred from primary keys and no hint is provided, an error is returned.

---

## Summary

| Scenario | Result |
|----------|--------|
| LEFT JOIN + no filter | All base rows, NULLs for non-matches |
| LEFT JOIN + filter on joined table | All base rows, NULLs where filter excludes |
| INNER JOIN + filter on joined table | Only matching rows, no NULLs |
| Filter on base table | Applied to base table CTE |
| Filter on dimension-only table | Applied in final WHERE |

**Key takeaway**: The join type you define in your flow determines whether non-matching rows are preserved (LEFT) or excluded (INNER). Filters on joined tables affect what gets aggregated, not which base rows are returned.
