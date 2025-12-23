-- PostgreSQL initialization script for SemaFlow demo
-- This runs automatically when the container first starts

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50)
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    amount DOUBLE PRECISION,  -- Use DOUBLE for compatibility with tokio-postgres
    created_at TIMESTAMP
);

-- Seed with sample data
INSERT INTO customers VALUES
    (1, 'Alice', 'US'),
    (2, 'Bob', 'UK'),
    (3, 'Carla', 'US'),
    (4, 'David', 'DE');

INSERT INTO orders VALUES
    (1, 1, 100.00, '2024-01-01'),
    (2, 1, 50.00, '2024-01-02'),
    (3, 2, 25.00, '2024-01-03'),
    (4, 3, 200.00, '2024-01-04'),
    (5, 3, 75.00, '2024-01-05');

-- Create indexes for better query performance
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_created_at ON orders(created_at);
