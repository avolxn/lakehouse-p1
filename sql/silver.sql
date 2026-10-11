CREATE TABLE iceberg.silver.order_lines
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT l.orderkey, l.linenumber, c.custkey,
       upper(trim(c.mktsegment)) AS mktsegment,
       o.orderdate, o.shippriority, l.shipdate,
       CAST(l.extendedprice AS DECIMAL(18,2)) AS extendedprice,
       CAST(l.discount AS DECIMAL(5,2)) AS discount,
       CAST(l.extendedprice AS DECIMAL(18,2)) *
         (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2))) AS net_revenue,
       date_diff('day', o.orderdate, l.shipdate) AS days_to_ship
FROM iceberg.bronze.customer c
JOIN iceberg.bronze.orders o ON c.custkey=o.custkey
JOIN iceberg.bronze.lineitem l ON o.orderkey=l.orderkey
WHERE upper(trim(c.mktsegment))='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
  AND c.custkey IS NOT NULL AND o.orderkey IS NOT NULL
  AND l.extendedprice >= 0 AND l.discount BETWEEN 0 AND 1
