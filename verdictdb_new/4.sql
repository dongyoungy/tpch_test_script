select
 o_orderpriority,
 count(*) as order_count
from
 orders
where
 o_orderdate >= '1993-07-01'
 and o_orderdate < '1993-10-01'
 and exists (
  select
   *
  from
   lineitem
  where
   l_orderkey = o_orderkey
   and l_commitdate < l_receiptdate
 )
group by
 o_orderpriority
order by
 o_orderpriority;

-- +-----------------+-------------+
-- | o_orderpriority | order_count |
-- +-----------------+-------------+
-- | 1-URGENT        | 1051801     |
-- | 2-HIGH          | 1051366     |
-- | 3-MEDIUM        | 1051587     |
-- | 4-NOT SPECIFIED | 1050950     |
-- | 5-LOW           | 1051725     |
-- +-----------------+-------------+

SELECT o_orderpriority, 
       cnt,
       numkey,
       total,
       sample_total,
       prob,
       total * prob,
       avgcnt,
       avgcnt2,
       cnt * (1 - pow((1-prob), 100)) / (1-((1-prob))) AS order_count,
       cnt * (1 - pow((1-prob)/avgcnt, 100)) / (1-((1-prob)/avgcnt)) AS order_count2,
       cnt + cnt * (1 - pow((1-prob), 99)) / (1-((1-prob))) / avgcnt AS 
       order_count3,
      (1 - pow((1-prob), 100)) / (1-((1-prob))) as prob1
FROM (SELECT o_orderpriority,  count(*) as total, count(distinct o_orderkey) numkey, avg(totalcnt) as sample_total,
        SUM(case when cnt is null then 0 else 1 end) AS 
        cnt, AVG(case when cnt is null then 0 else 1 
        end) as prob,
        sum(case when l_orderkey is not null then cnt else 0 end) / sum(case when l_orderkey is not null then 1 else 0 end) as avgcnt,
        avg(cnt) as avgcnt2
      FROM orders o LEFT JOIN 
        (SELECT l_orderkey, COUNT(*) as cnt, count(*) over () as totalcnt
         FROM tpch100g_parquet.lineitem_scramble
         WHERE l_commitdate < l_receiptdate and verdictdbblock <= 1
         GROUP BY l_orderkey) tmp 
      ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority) t
ORDER BY o_orderpriority;

SELECT o_orderpriority, 
      total,
       cnt * (1 - pow(1-prob, 100)) / (1-(1-prob)) AS 
       order_count 
FROM (SELECT o_orderpriority, count(*) as total,
        SUM(case when cnt is null then 0 else 1 end) AS 
        cnt, AVG(case when cnt is null then 0 else 1 
        end) as prob 
      FROM orders o LEFT JOIN 
        (SELECT l_orderkey, COUNT(*) as cnt 
         FROM tpch100g_sample.lineitem_sample_1p_ss 
         WHERE l_commitdate < l_receiptdate 
         GROUP BY l_orderkey) tmp 
      ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
GROUP BY o_orderpriority) t
ORDER BY o_orderpriority;



SELECT o_orderpriority, 
       sum(case when l_orderkey is not null then 1 else  1 - (pow((1-prob)/1.01, 99)) end) as order_count
FROM (SELECT o_orderpriority, l_orderkey,
        AVG(case when cnt is null then 0 else 1 end) over () as prob
      FROM orders o LEFT JOIN 
        (SELECT l_orderkey, COUNT(*) as cnt, count(*) over () as totalcnt
         FROM tpch100g_parquet.lineitem_scramble
         WHERE l_commitdate < l_receiptdate and verdictdbblock = 1
         GROUP BY l_orderkey) tmp 
      ON tmp.l_orderkey = o_orderkey
WHERE (O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01')
) t
GROUP BY o_orderpriority
ORDER BY o_orderpriority;