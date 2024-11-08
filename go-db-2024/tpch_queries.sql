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
    l_linestatus;
-- Compute the average discount for line items associated with orders, grouped
-- by the order priority.
SELECT o_orderpriority,
    AVG(l_discount) AS avg_discount
FROM orders
    JOIN lineitem ON o_orderkey = l_orderkey
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
-- Compute the average total order price for each region.
SELECT r_name AS region_name,
    AVG(o_totalprice) AS avg_order_price
FROM region
    JOIN nation ON r_regionkey = n_regionkey
    JOIN customer ON n_nationkey = c_nationkey
    JOIN orders ON c_custkey = o_custkey
GROUP BY r_name
ORDER BY avg_order_price DESC;
-- Compute the average total price of all orders placed by each customer. Get
-- the top 10 greatest average prices.
SELECT c_custkey,
    AVG(o_totalprice) AS avg_price
FROM customer
    JOIN orders ON c_custkey = o_custkey
GROUP BY c_custkey
ORDER BY avg_price DESC
LIMIT 10;
-- Compute the average quantity of items per order for each supplier. Get the
-- top 10 greatest average quantities.
SELECT s_name AS supplier_name,
    AVG(l_quantity) AS avg_quantity
FROM supplier
    JOIN lineitem ON s_suppkey = l_suppkey
GROUP BY s_name
ORDER BY avg_quantity DESC
LIMIT 10;
-- Compute the average quantity of items shipped before their committed ship
-- date for every ship mode.
SELECT l_shipmode,
    AVG(l_quantity) AS avg_quantity_shipped
FROM lineitem
WHERE l_shipdate <= l_commitdate
GROUP BY l_shipmode
ORDER BY avg_quantity_shipped DESC;