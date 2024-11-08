-- Compute the average quantity, price, and discount for line items grouped by
-- their return and processing status.
SELECT l_returnflag,
    l_linestatus,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc
FROM lineitem
GROUP BY l_returnflag,
    l_linestatus
ORDER BY l_returnflag,
    l_linestatus
LIMIT 10;

-- Compute the average discount for line items associated with orders, grouped
-- by the order priority.
SELECT o_orderpriority,
    AVG(l_discount) AS avg_discount
FROM orders
    JOIN lineitem ON o_orderkey = l_orderkey
GROUP BY o_orderpriority
ORDER BY o_orderpriority
LIMIT 10;

-- Compute the average quantity of line items for each order date.
SELECT o_orderdate,
    AVG(l_quantity) AS avg_quantity
FROM orders
    JOIN lineitem ON o_orderkey = l_orderkey
GROUP BY o_orderdate
ORDER BY o_orderdate
LIMIT 10;

-- Compute the average total price of all orders placed by each customer.
SELECT c_custkey,
    AVG(o_totalprice) AS avg_price
FROM customer
    JOIN orders ON c_custkey = o_custkey
GROUP BY c_custkey
ORDER BY avg_price DESC
LIMIT 10;