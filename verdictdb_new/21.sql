select
 s_name,
 count(*) as numwait
from
 supplier,
 lineitem l1,
 orders,
 nation
where
 s_suppkey = l1.l_suppkey
 and o_orderkey = l1.l_orderkey
 and o_orderstatus = 'F'
 and l1.l_receiptdate > l1.l_commitdate
 and exists (
  select
   *
  from
   lineitem l2
  where
   l2.l_orderkey = l1.l_orderkey
   and l2.l_suppkey <> l1.l_suppkey
 )
 and not exists (
  select
   *
  from
   lineitem l3
  where
   l3.l_orderkey = l1.l_orderkey
   and l3.l_suppkey <> l1.l_suppkey
   and l3.l_receiptdate > l3.l_commitdate
 )
 and s_nationkey = n_nationkey
 and n_name = 'SAUDI ARABIA'
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;

select
 s_name,
 count(*) as numwait
from
 supplier,
 lineitem l1,
 orders,
 nation
where
 s_suppkey = l1.l_suppkey
 and o_orderkey = l1.l_orderkey
 and o_orderstatus = 'F'
 and l1.l_receiptdate > l1.l_commitdate
 and exists (
  select
   *
  from
   lineitem l2
  where
   l2.l_orderkey = l1.l_orderkey
   and l2.l_suppkey <> l1.l_suppkey
   and l2.l_receiptdate <= l2.l_commitdate
 )
 and s_nationkey = n_nationkey
 and n_name = 'SAUDI ARABIA'
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;


select s_name,
      cnt * (1 - pow(1-prob, 100)) / (1-(1-prob)) AS numwait 
from (
select s_name,
       sum(case when cnt1 is not null and cnt2 is null then 1 else 0 end) AS cnt, 
       avg(case when cnt1 is not null and cnt2 is null then 1 else 0 end) AS prob 
from
 supplier,
 tpch100g_sample.lineitem_sample_1p_ss l1
 LEFT JOIN (SELECT l_orderkey, l_suppkey, count(*) as cnt1   
            FROM tpch100g_sample.lineitem_sample_1p_ss group by l_orderkey, l_suppkey) l2 
       ON l1.l_orderkey = l2.l_orderkey and l1.l_suppkey <> l2.l_suppkey
 LEFT JOIN (SELECT l_orderkey, l_suppkey, count(*) as cnt2
            FROM tpch100g_sample.lineitem_sample_1p_ss
       WHERE l_receiptdate > l_commitdate group by l_orderkey, l_suppkey) l3 
       ON l1.l_orderkey = l3.l_orderkey and l1.l_suppkey <> l3.l_suppkey,
 orders,
 nation
where
 s_suppkey = l1.l_suppkey
 and o_orderkey = l1.l_orderkey
 and o_orderstatus = 'F'
 and l1.l_receiptdate > l1.l_commitdate
 and s_nationkey = n_nationkey
 and n_name = 'SAUDI ARABIA'
group by
 s_name
) t 
order by
 numwait desc,
 s_name
limit 100;
