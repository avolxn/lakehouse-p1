CREATE TABLE iceberg.gold.shipping_priority
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, sum(net_revenue) AS revenue, orderdate, shippriority
FROM iceberg.silver.order_lines
GROUP BY orderkey, orderdate, shippriority
ORDER BY revenue DESC, orderdate, orderkey
LIMIT 10
