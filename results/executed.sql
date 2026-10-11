CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

CREATE SCHEMA IF NOT EXISTS iceberg.silver;

CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE SCHEMA IF NOT EXISTS iceberg.bench;

CREATE SCHEMA IF NOT EXISTS iceberg.demo;

CREATE TABLE iceberg.bronze.customer WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT custkey,mktsegment FROM tpch.sf1.customer;

CREATE TABLE iceberg.bronze.orders WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT orderkey,custkey,orderdate,shippriority FROM tpch.sf1.orders;

CREATE TABLE iceberg.bronze.lineitem WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT orderkey,linenumber,extendedprice,discount,shipdate FROM tpch.sf1.lineitem;

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
  AND l.extendedprice >= 0 AND l.discount BETWEEN 0 AND 1;

CREATE TABLE iceberg.gold.shipping_priority
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, sum(net_revenue) AS revenue, orderdate, shippriority
FROM iceberg.silver.order_lines
GROUP BY orderkey, orderdate, shippriority
ORDER BY revenue DESC, orderdate, orderkey
LIMIT 10;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_customer;

CREATE TABLE iceberg.bench.parquet_zstd_customer WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_orders;

CREATE TABLE iceberg.bench.parquet_zstd_orders WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_lineitem;

CREATE TABLE iceberg.bench.parquet_zstd_lineitem WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_customer;

CREATE TABLE iceberg.bench.parquet_snappy_customer WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_orders;

CREATE TABLE iceberg.bench.parquet_snappy_orders WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_lineitem;

CREATE TABLE iceberg.bench.parquet_snappy_lineitem WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_customer;

CREATE TABLE iceberg.bench.parquet_gzip_customer WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_orders;

CREATE TABLE iceberg.bench.parquet_gzip_orders WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_lineitem;

CREATE TABLE iceberg.bench.parquet_gzip_lineitem WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_customer;

CREATE TABLE iceberg.bench.orc_zstd_customer WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_orders;

CREATE TABLE iceberg.bench.orc_zstd_orders WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_lineitem;

CREATE TABLE iceberg.bench.orc_zstd_lineitem WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_customer;

CREATE TABLE iceberg.bench.parquet_none_customer WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_orders;

CREATE TABLE iceberg.bench.parquet_none_orders WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_lineitem;

CREATE TABLE iceberg.bench.parquet_none_lineitem WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_customer;

CREATE TABLE iceberg.bench.parquet_zstd_customer WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_orders;

CREATE TABLE iceberg.bench.parquet_zstd_orders WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_lineitem;

CREATE TABLE iceberg.bench.parquet_zstd_lineitem WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_customer;

CREATE TABLE iceberg.bench.parquet_gzip_customer WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_orders;

CREATE TABLE iceberg.bench.parquet_gzip_orders WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_lineitem;

CREATE TABLE iceberg.bench.parquet_gzip_lineitem WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_customer;

CREATE TABLE iceberg.bench.parquet_none_customer WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_orders;

CREATE TABLE iceberg.bench.parquet_none_orders WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_lineitem;

CREATE TABLE iceberg.bench.parquet_none_lineitem WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_customer;

CREATE TABLE iceberg.bench.orc_zstd_customer WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_orders;

CREATE TABLE iceberg.bench.orc_zstd_orders WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_lineitem;

CREATE TABLE iceberg.bench.orc_zstd_lineitem WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_customer;

CREATE TABLE iceberg.bench.parquet_snappy_customer WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_orders;

CREATE TABLE iceberg.bench.parquet_snappy_orders WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_lineitem;

CREATE TABLE iceberg.bench.parquet_snappy_lineitem WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_customer;

CREATE TABLE iceberg.bench.parquet_zstd_customer WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_orders;

CREATE TABLE iceberg.bench.parquet_zstd_orders WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_zstd_lineitem;

CREATE TABLE iceberg.bench.parquet_zstd_lineitem WITH (format='PARQUET', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_customer;

CREATE TABLE iceberg.bench.parquet_snappy_customer WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_orders;

CREATE TABLE iceberg.bench.parquet_snappy_orders WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_snappy_lineitem;

CREATE TABLE iceberg.bench.parquet_snappy_lineitem WITH (format='PARQUET', compression_codec='SNAPPY') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_customer;

CREATE TABLE iceberg.bench.parquet_gzip_customer WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_orders;

CREATE TABLE iceberg.bench.parquet_gzip_orders WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_gzip_lineitem;

CREATE TABLE iceberg.bench.parquet_gzip_lineitem WITH (format='PARQUET', compression_codec='GZIP') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_customer;

CREATE TABLE iceberg.bench.parquet_none_customer WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_orders;

CREATE TABLE iceberg.bench.parquet_none_orders WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.parquet_none_lineitem;

CREATE TABLE iceberg.bench.parquet_none_lineitem WITH (format='PARQUET', compression_codec='NONE') AS SELECT * FROM iceberg.bronze.lineitem;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_customer;

CREATE TABLE iceberg.bench.orc_zstd_customer WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.customer;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_orders;

CREATE TABLE iceberg.bench.orc_zstd_orders WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.orders;

DROP TABLE IF EXISTS iceberg.bench.orc_zstd_lineitem;

CREATE TABLE iceberg.bench.orc_zstd_lineitem WITH (format='ORC', compression_codec='ZSTD') AS SELECT * FROM iceberg.bronze.lineitem;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_snappy_customer c
JOIN iceberg.bench.parquet_snappy_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_snappy_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_gzip_customer c
JOIN iceberg.bench.parquet_gzip_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_gzip_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_zstd_customer c
JOIN iceberg.bench.parquet_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.orc_zstd_customer c
JOIN iceberg.bench.orc_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.orc_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_none_customer c
JOIN iceberg.bench.parquet_none_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_none_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.orc_zstd_customer c
JOIN iceberg.bench.orc_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.orc_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_zstd_customer c
JOIN iceberg.bench.parquet_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_gzip_customer c
JOIN iceberg.bench.parquet_gzip_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_gzip_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_none_customer c
JOIN iceberg.bench.parquet_none_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_none_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_snappy_customer c
JOIN iceberg.bench.parquet_snappy_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_snappy_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_gzip_customer c
JOIN iceberg.bench.parquet_gzip_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_gzip_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_snappy_customer c
JOIN iceberg.bench.parquet_snappy_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_snappy_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_none_customer c
JOIN iceberg.bench.parquet_none_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_none_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_zstd_customer c
JOIN iceberg.bench.parquet_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.orc_zstd_customer c
JOIN iceberg.bench.orc_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.orc_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_snappy_customer c
JOIN iceberg.bench.parquet_snappy_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_snappy_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_zstd_customer c
JOIN iceberg.bench.parquet_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_none_customer c
JOIN iceberg.bench.parquet_none_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_none_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_gzip_customer c
JOIN iceberg.bench.parquet_gzip_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_gzip_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.orc_zstd_customer c
JOIN iceberg.bench.orc_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.orc_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.orc_zstd_customer c
JOIN iceberg.bench.orc_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.orc_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_none_customer c
JOIN iceberg.bench.parquet_none_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_none_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_snappy_customer c
JOIN iceberg.bench.parquet_snappy_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_snappy_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_gzip_customer c
JOIN iceberg.bench.parquet_gzip_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_gzip_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM iceberg.bench.parquet_zstd_customer c
JOIN iceberg.bench.parquet_zstd_orders o ON c.custkey=o.custkey
JOIN iceberg.bench.parquet_zstd_lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10;

CREATE TABLE iceberg.demo.snapshot_demo AS SELECT * FROM iceberg.silver.order_lines;

ALTER TABLE iceberg.demo.snapshot_demo ADD COLUMN source_system VARCHAR;

ALTER TABLE iceberg.demo.snapshot_demo RENAME COLUMN source_system TO data_origin;

DELETE FROM iceberg.demo.snapshot_demo WHERE orderkey=(SELECT min(orderkey) FROM iceberg.demo.snapshot_demo);

ALTER TABLE iceberg.demo.snapshot_demo EXECUTE rollback_to_snapshot(5875152496857109988);

