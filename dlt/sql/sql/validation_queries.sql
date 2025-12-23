-- List all bronze tables
SHOW TABLES IN raw.transactions;

-- Validate orders
SELECT COUNT(*) FROM raw.transactions.orders_bronze;

-- Sample data
SELECT * FROM raw.transactions.orders_bronze LIMIT 10;

-- Check schema
DESCRIBE TABLE raw.transactions.orders_bronze;
