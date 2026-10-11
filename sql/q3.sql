SELECT l.orderkey,
       sum(CAST(l.extendedprice AS DECIMAL(18,2)) *
           (DECIMAL '1.00' - CAST(l.discount AS DECIMAL(5,2)))) AS revenue,
       o.orderdate, o.shippriority
FROM {schema}.customer c
JOIN {schema}.orders o ON c.custkey=o.custkey
JOIN {schema}.lineitem l ON o.orderkey=l.orderkey
WHERE c.mktsegment='BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate > DATE '1995-03-15'
GROUP BY l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC, o.orderdate, l.orderkey
LIMIT 10
