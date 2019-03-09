CREATE SCHEMA IF NOT EXISTS tpch100g_parquet;
CREATE TABLE tpch100g_parquet.nation STORED AS parquet AS SELECT * FROM tpch100g_text.nation;
CREATE TABLE tpch100g_parquet.region STORED AS parquet AS SELECT * FROM tpch100g_text.region;
CREATE TABLE tpch100g_parquet.supplier STORED AS parquet AS SELECT * FROM tpch100g_text.supplier;
CREATE TABLE tpch100g_parquet.customer STORED AS parquet AS SELECT * FROM tpch100g_text.customer;
CREATE TABLE tpch100g_parquet.part STORED AS parquet AS SELECT * FROM tpch100g_text.part;
CREATE TABLE tpch100g_parquet.partsupp STORED AS parquet AS SELECT * FROM tpch100g_text.partsupp;
CREATE TABLE tpch100g_parquet.orders STORED AS parquet AS SELECT * FROM tpch100g_text.orders;
CREATE TABLE tpch100g_parquet.lineitem STORED AS parquet AS SELECT * FROM tpch100g_text.lineitem;

COMPUTE STATS tpch100g_parquet.nation;
COMPUTE STATS tpch100g_parquet.region;
COMPUTE STATS tpch100g_parquet.supplier;
COMPUTE STATS tpch100g_parquet.customer;
COMPUTE STATS tpch100g_parquet.part;
COMPUTE STATS tpch100g_parquet.partsupp;
COMPUTE STATS tpch100g_parquet.orders;
COMPUTE STATS tpch100g_parquet.lineitem;


SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders 
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority;

SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders 
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey)
GROUP BY o_orderpriority;

SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders 
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04' AND EXISTS (SELECT * FROM tpch100g_sample.lineitem_sample_100p_ss  WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority;

SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders 
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04' AND EXISTS (SELECT * FROM tpch100g_sample.lineitem_sample_1p_ss  WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority;

SELECT o_orderpriority, COUNT(*), avg(cnt), COUNT(*) * 100 / pow(avg(cnt), 98) as order_count
FROM orders o LEFT JOIN
  (SELECT COUNT(ssid) as cnt, l_orderkey FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04') AND (tmp.cnt > 0)
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

SELECT o_orderpriority, avg(cnt) FROM (
SELECT o_orderkey, o_orderpriority, (case when cnt is null then 0 else cnt end) as cnt FROM orders o LEFT JOIN
  (SELECT l_orderkey, COUNT(ssid) as cnt FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp
GROUP BY o_orderpriority;

SELECT o_orderpriority, cnt * (1 - pow(1-prob, 100)) / (1-(1-prob)) as order_count FROM (
SELECT o_orderpriority, sum(case when cnt is null then 0 else 1 end) as cnt, avg(case when cnt is null then 0 else cnt end) as prob FROM orders o LEFT JOIN
  (SELECT l_orderkey, COUNT(ssid) as cnt FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority) t;

SELECT o_orderpriority, cnt * (1 - pow(1-prob, 10000)) / (1-(1-prob)) as order_count FROM (
SELECT o_orderpriority, sum(case when cnt is null then 0 else 1 end) as cnt, avg(case when cnt is null then 0 else 1 end) as prob FROM orders o LEFT JOIN
  (SELECT l_orderkey, COUNT(*) as cnt FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate and ssid=1 GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority) t;

SELECT o_orderpriority, sum(case when cnt is null then 0 else 1 end) as cnt, avg(case when cnt is null then 0 else cnt end) as prob FROM orders o LEFT JOIN
  (SELECT l_orderkey, COUNT(*) as cnt FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')
GROUP BY o_orderpriority

SELECT * FROM tpch100g_sample.lineitem_sample_1p_ss  WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate

SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders 
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04' AND EXISTS (SELECT * FROM tpch100g_sample.lineitem_sample_1p_6 as lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate))
OR 1-pow(1-(1 - pow(1-(1/(3700000+2)), 6000000)), 99) > rand(unix_timestamp())
GROUP BY o_orderpriority;

SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders o JOIN
  (SELECT COUNT(*) as cnt, l_orderkey FROM tpch100g_sample.lineitem_sample_100p_ss WHERE l_commitdate < l_receiptdate AND ssid <= 50 GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04') AND (tmp.cnt > 0)
GROUP BY o_orderpriority;

SELECT o_orderpriority, sum(cnt) 
FROM (
SELECT o_orderpriority, ssid, count(*) cnt
FROM orders o JOIN
  (SELECT ssid, l_orderkey FROM tpch100g_sample.lineitem_sample_100p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey, ssid) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')
GROUP BY o_orderpriority, ssid) t
GROUP BY o_orderpriority;


SELECT o_orderpriority, ssid, sum(order_count) over (partition by o_orderpriority,ssid order by o_orderpriority, ssid)
FROM (
SELECT o_orderpriority, ssid, COUNT(*) AS order_count
FROM orders o JOIN
  (SELECT COUNT(*) as cnt, ssid, l_orderkey FROM tpch100g_sample.lineitem_sample_100p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey, ssid) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04') AND (tmp.cnt > 0)
GROUP BY o_orderpriority, ssid
) t
ORDER BY o_orderpriority, ssid;


SELECT o_orderpriority, ssid, sum(order_count) over (partition by o_orderpriority order by o_orderpriority, ssid)
FROM (
SELECT o_orderpriority, ssid, COUNT(*) AS order_count
FROM orders o JOIN
  (SELECT COUNT(*) as cnt, ssid, l_orderkey FROM tpch100g_sample.lineitem_sample_10p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey, ssid) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04') AND (tmp.cnt > 0)
GROUP BY o_orderpriority, ssid
) t
ORDER BY o_orderpriority, ssid;

SELECT COUNT(*) as cnt FROM tpch100g_sample.lineitem_sample_1p_6 WHERE l_commitdate < l_receiptdate

WITH sample_total as (SELECT COUNT(*) cnt FROM tpch100g_sample.lineitem_sample_1p_6)
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders o LEFT JOIN
  (SELECT COUNT(*) as cnt, l_orderkey FROM tpch100g_sample.lineitem_sample_1p_6 WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey, sample_total
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04') AND (tmp.cnt > 0 OR 1-pow(1-(1 - pow(1-(1/(3700000+2)), 6000000)), 99) > rand(unix_timestamp()))
GROUP BY o_orderpriority;

SELECT avg(cnt) FROM
(SELECT count(*) as cnt FROM orders JOIN tpch100g_sample.lineitem_sample_1p_6 ON l_orderkey = o_orderkey  GROUP BY o_orderkey) tmp;


WITH ss as (SELECT l_orderkey FROM orders JOIN tpch100g_sample.lineitem_sample_1p_6 ON l_orderkey = o_orderkey 
            WHERE l_commitdate < l_receiptdate AND O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04'
             GROUP BY l_orderkey),
sample_total as (SELECT COUNT(*) cnt FROM tpch100g_sample.lineitem_sample_1p_6)
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders, sample_total
WHERE
(EXISTS (SELECT * FROM ss WHERE ss.l_orderkey = o_orderkey))
OR ((1 - pow((1/(sample_total.cnt+2)), 99)) > rand(unix_timestamp()))
GROUP BY o_orderpriority;


WITH sample_total as (SELECT COUNT(*) cnt FROM tpch100g_sample.lineitem_sample_1p_6)
SELECT cnt, 1-pow(1-(1/(sample_total.cnt+2)), 6000000)
FROM sample_total;

, avg(1 - pow((1/(sample_total.cnt+2)), 99))
  , sample_total
 OR ((1 - pow((1/(sample_total.cnt+2)), 99)) > rand(unix_timestamp()))

SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders 
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04' AND
 EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority;


SELECT AVG(total_price) AS avg_total_price, AVG(total_price) - 1.96 * (STDDEV(total_price) / sqrt(100)), AVG(total_price) + 1.96 * (STDDEV(total_price) / sqrt(100))
FROM (
      SELECT ssid, SUM(total_price) as total_price FROM (
      SELECT l_suppkey, ssid, SUM(l_extendedprice) * 100 AS total_price
      FROM tpch100g_sample.lineitem_sample_1p_ss GROUP BY l_suppkey, ssid) tmp
      GROUP BY ssid
      ) tmp2
;

SELECT AVG(avg_quantity) as avg_quantity, AVG(avg_quantity) - 1.96 * (STDDEV(avg_quantity) / sqrt(100)), AVG(avg_quantity) + 1.96 * (STDDEV(avg_quantity) / sqrt(100))
FROM (
SELECT t.ssid, AVG(l_quantity) as avg_quantity
FROM tpch100g_sample.lineitem_sample_1p_ss AS t, (SELECT ssid, 1.2 * avg(l_extendedprice)   
     AS compare_price FROM tpch100g_sample.lineitem_sample_1p_ss GROUP BY ssid) AS tmp
WHERE t.l_extendedprice > tmp.compare_price and t.ssid = tmp.ssid
GROUP BY ssid) tmp2;

SELECT o_orderpriority, avg(case when is_nan(order_count) then 0 else order_count end) as order_count
FROM (SELECT o_orderpriority, cnt * (1 - pow(1-prob, 10000)) / (1-(1-prob)) as order_count 
      FROM (SELECT o_orderpriority, ssid, sum(case when cnt is null then 0 else 1 end) as cnt, avg(case when cnt is null then 0 else 1 end) as prob 
            FROM orders o LEFT JOIN (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate 
                                     GROUP BY ssid, l_orderkey) tmp
                          ON tmp.l_orderkey = o_orderkey
            WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')
            GROUP BY ssid, o_orderpriority) t
      GRO
     ) tmp2
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

with ss as (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate and ssid=1
                                     GROUP BY ssid, l_orderkey)
SELECT o_orderpriority, ssid, cnt * (1 - pow(1-prob, 10000)) / (1-(1-prob)) as order_count 
FROM (
SELECT o_orderpriority, ssid, sum(case when cnt is null then 0 else 1 end) as cnt, sum((case when cnt is null then 0 else cnt end) / fail) as prob
FROM (
SELECT o_orderpriority, ssid, cnt, SUM(case when ssid is null then 1 else 0 end) OVER () as fail
FROM (SELECT o_orderpriority, ssid, cnt 
      FROM orders o LEFT JOIN ss s1 ON s1.l_orderkey = o_orderkey
      WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp
) tmp2
GROUP BY o_orderpriority, ssid
) tmp3
ORDER BY o_orderpriority, order_count;


SELECT o_orderpriority, ssid, cnt * (1 - pow(1-prob, 10000)) / (1-(1-prob)) as order_count 
FROM (
SELECT o_orderpriority, ssid, sum(case when cnt is null then 0 else 1 end) as cnt, avg(success)/(avg(success)+avg(fail)) as prob 
FROM (
SELECT o_orderpriority, ssid, cnt, SUM(case when cnt is null then 0 else 1 end) OVER (PARTITION BY o_orderpriority, ssid) as success, SUM(case when cnt is null then 1 else 0 end) OVER () as fail
FROM (SELECT o_orderpriority, ssid, cnt 
      FROM orders o LEFT JOIN (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate and ssid<=2
                                     GROUP BY ssid, l_orderkey) s1 ON s1.l_orderkey = o_orderkey
      WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp
) tmp2
GROUP BY o_orderpriority, ssid
) tmp3
WHERE ssid is not null
ORDER BY o_orderpriority, order_count;

SELECT o_orderpriority, ssid, cnt, success, fail FROM (
SELECT o_orderpriority, ssid, cnt, SUM(case when cnt is null then 0 else 1 end) OVER (PARTITION BY o_orderpriority, ssid) as success, SUM(case when cnt is null then 1 else 0 end) OVER () as fail
FROM (SELECT o_orderpriority, ssid, cnt 
      FROM orders o LEFT JOIN (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate and ssid<=2
                                     GROUP BY ssid, l_orderkey) s1 ON s1.l_orderkey = o_orderkey
      WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp
) tmp2
WHERE ssid is not null;

SELECT count(*), sum(cnt), SUM(case when cnt is null then 1 else 0 end), SUM(case when cnt is null then 0 else 1 end)
FROM (SELECT o_orderpriority, ssid, cnt 
      FROM orders o LEFT JOIN (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate and ssid=1
                                     GROUP BY ssid, l_orderkey) s1 ON s1.l_orderkey = o_orderkey
      WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp




with ss as (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate 
                                     GROUP BY ssid, l_orderkey)
SELECT o_orderpriority, ssid, sum(case when cnt is null then 0 else cnt end) as cnt, sum((case when cnt is null then 0 else cnt end) / fail * 100) as prob
FROM (
SELECT o_orderpriority, ssid, cnt, SUM(case when cnt is null then 1 else 0 end) OVER () as fail
FROM (SELECT o_orderpriority, ssid, cnt 
      FROM orders o LEFT JOIN ss s1 ON s1.l_orderkey = o_orderkey
      WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp
) tmp2
GROUP BY o_orderpriority, ssid
ORDER BY ssid, o_orderpriority



with ss as (SELECT ssid, l_orderkey, COUNT(*) as cnt 
                                     FROM tpch100g_sample.lineitem_sample_1p_ss 
                                     WHERE l_commitdate < l_receiptdate 
                                     GROUP BY ssid, l_orderkey)
SELECT o_orderpriority, ssid, cnt, SUM(case when ssid is null then 1 else 0 end) OVER () as fail
FROM (SELECT o_orderpriority, ssid, cnt 
      FROM orders o LEFT JOIN ss s1 ON s1.l_orderkey = o_orderkey
      WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')) tmp
LIMIT 10;
 

SELECT t.o_orderpriority, t.ssid, t.l_orderkey, t.o_orderkey, s2.cnt
            FROM (SELECT * FROM ss as s1 JOIN orders o ON s1.l_orderkey = o_orderkey) as t LEFT JOIN ss as s2 ON s2.l_orderkey = t.o_orderkey
            WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-07-04')
            AND (o_orderkey is null or t.l_orderkey is null)
            LIMIT 1000;


SELECT o_orderpriority, 
       cnt * (1 - pow(1-prob, 100)) / (1-(1-prob)) AS 
       order_count 
FROM (SELECT o_orderpriority, ssid,
        SUM(case when cnt is null then 0 else 1 end) AS 
        cnt, AVG(case when cnt is null then 0 else 1 
        end) as prob 
      FROM (SELECT *, 1+cast(rand(unix_timestamp()) * 100 as int) as ssid1  FROM orders) o LEFT JOIN 
        (SELECT l_orderkey, ssid, COUNT(*) as cnt 
         FROM tpch100g_sample.lineitem_sample_1p_ss 
         WHERE l_commitdate < l_receiptdate 
         GROUP BY l_orderkey, ssid) tmp 
      ON tmp.l_orderkey = o_orderkey AND o.ssid1 = tmp.ssid
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority, ssid) t
ORDER BY o_orderpriority;

       AVG(cnt * 100 * (1 - pow(1-prob, 100)) / (1-(1-prob))) - 1.96 * STDDEV(cnt * 100 * (1 - pow(1-prob, 100)) / (1-(1-prob))) / SQRT(100)  AS order_count_lb,
       AVG(cnt * 100 * (1 - pow(1-prob, 100)) / (1-(1-prob))) - 1.96 * STDDEV(cnt * 100 * (1 - pow(1-prob, 100)) / (1-(1-prob))) / SQRT(100)  AS order_count_ub

SELECT o_orderpriority, 
       AVG(cnt * 100 * (1 - pow(1-prob, 100)) / (1-(1-prob))) AS order_count
FROM (SELECT o_orderpriority, ssid,
        SUM(case when cnt is null then 0 else 1 end) AS 
        cnt, AVG(case when cnt is null then 0 else 1 
        end) as prob 
      FROM (SELECT *, 1+cast(rand(unix_timestamp()) * 100 as int) as ssid1  FROM orders) o LEFT JOIN 
        (SELECT l_orderkey, ssid, COUNT(*) as cnt 
         FROM tpch100g_sample.lineitem_sample_1p_ss 
         WHERE l_commitdate < l_receiptdate 
         GROUP BY l_orderkey, ssid) tmp 
      ON tmp.l_orderkey = o_orderkey 
      WHERE o.ssid1 = tmp.ssid
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority, ssid) t
GROUP BY o_orderpriority;

SELECT o_orderpriority, ssid,
        SUM(case when cnt is null then 0 else 1 end) AS 
        cnt, AVG(case when cnt is null then 0 else 1 
        end) as prob 
      FROM 
        (SELECT * FROM
        (SELECT *, 1+cast(rand(unix_timestamp()) * 100 as int) as ssid1  FROM orders
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')) as o JOIN 
        (SELECT l_orderkey, ssid, COUNT(*) as cnt 
         FROM tpch100g_sample.lineitem_sample_1p_ss 
         WHERE l_commitdate < l_receiptdate 
         GROUP BY l_orderkey, ssid) tmp 
      ON tmp.l_orderkey = o_orderkey
      WHERE o.ssid1 = tmp.ssid
      ) tmp2
GROUP BY o_orderpriority, ssid;


SELECT o_orderpriority, 
       (cnt * 100 * (1 - pow(1-prob, 100)) / (1-(1-prob))) AS order_count
FROM (
SELECT o_orderpriority, ssid1 as ssid,
        SUM(case when cnt is null then 0 else 1 end) AS 
        cnt, AVG(case when cnt is null then 0 else 1 
        end) as prob 
      FROM (SELECT *, 1+cast(rand(unix_timestamp()) * 100 as int) as ssid1  FROM orders) o LEFT JOIN 
        (SELECT l_orderkey, ssid, COUNT(*) as cnt 
         FROM tpch100g_sample.lineitem_sample_1p_ss 
         WHERE l_commitdate < l_receiptdate 
         GROUP BY l_orderkey, ssid) tmp 
      ON tmp.l_orderkey = o_orderkey AND o.ssid1 = tmp.ssid
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority, ssid) tmp2
ORDER BY o_orderpriority, order_count
GROUP BY o_orderpriority;

SELECT o_orderkey, ssid, cnt FROM (SELECT *, 1+cast(rand(unix_timestamp()) * 100 as int) as ssid1  FROM orders) o LEFT JOIN 
        (SELECT l_orderkey, ssid, COUNT(*) as cnt 
         FROM tpch100g_sample.lineitem_sample_1p_ss 
         WHERE l_commitdate < l_receiptdate 
         GROUP BY l_orderkey, ssid) tmp 
      ON tmp.l_orderkey = o_orderkey AND o.ssid1 = tmp.ssid
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
LIMIT 500;

WITH SQ AS (SELECT o_orderpriority, ssid, SUM(cnt) as cnt FROM orders o LEFT JOIN
  (SELECT l_orderkey, ssid, COUNT(*) as cnt FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey, ssid) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority, ssid
)
SELECT o_orderpriority, 
       AVG(cnt * (1 - pow(1-prob, 10000)) / (1-(1-prob))) AS order_count
FROM (
SELECT t1.o_orderpriority, t1.ssid, sum(case when t1.ssid = t2.ssid and t1.o_orderpriority = t2.o_orderpriority then 1 else 0 end) as cnt, sum(case when t1.ssid = t2.ssid and t1.o_orderpriority = t2.o_orderpriority then 1 else 0 end) / (sum(case when t1.ssid = t2.ssid then 1 else 0 end) + sum(case when t1.ssid <> t2.ssid then t2.cnt else 0 end)) as prob
FROM SQ t1, SQ t2
GROUP BY t1.o_orderpriority, t1.ssid
) tmp
WHERE ssid is not null
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

WITH SQ AS (SELECT o_orderpriority, ssid, SUM(cnt) as cnt FROM orders o LEFT JOIN
  (SELECT l_orderkey, ssid, COUNT(*) as cnt FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey, ssid) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority, ssid
)
SELECT t1.o_orderpriority, t1.ssid, sum(case when t1.ssid = t2.ssid and t1.o_orderpriority = t2.o_orderpriority then t2.cnt else 0 end) as cnt, sum(case when t1.ssid = t2.ssid and t1.o_orderpriority = t2.o_orderpriority then t2.cnt else 0 end) / (sum(case when t1.ssid = t2.ssid then t2.cnt else 0 end) + sum(case when t1.ssid <> t2.ssid then t2.cnt else 0 end)) as prob
FROM SQ t1, SQ t2
WHERE t1.ssid is not null
GROUP BY t1.o_orderpriority, t1.ssid
ORDER BY t1.o_orderpriority, t1.ssid;

SELECT o_orderpriority, cnt * (1 - pow(1-prob, 100)) / (1-(1-prob)) as order_count FROM (
SELECT o_orderpriority, sum(case when l_orderkey is null then 0 else 1 end) as cnt, avg(case when l_orderkey is null then 0 else 1 end) as prob FROM orders o LEFT JOIN
  (SELECT l_orderkey FROM tpch100g_sample.lineitem_sample_1p_ss WHERE l_commitdate < l_receiptdate GROUP BY l_orderkey) tmp ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority) t;