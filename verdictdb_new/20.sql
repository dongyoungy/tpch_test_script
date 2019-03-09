select
 s_name,
 s_address
from
 supplier,
 nation
where
 s_suppkey in (
  select
   ps_suppkey
  from
   partsupp
  where
   ps_partkey in (
    select
     p_partkey
    from
     part
    where
     p_name like 'forest%'
   )
   and ps_availqty > (
    select
     0.5 * sum(l_quantity)
    from
     lineitem
    where
     l_partkey = ps_partkey
     and l_suppkey = ps_suppkey
     and l_shipdate >= '1994-01-01'
     and l_shipdate < '1995-01-01'
   )
 )
 and s_nationkey = n_nationkey
 and n_name = 'CANADA'
order by
 s_name;
-- Fetched 17971 row(s) in 54.52s


select
 s_name,
 s_address
from
 supplier,
 nation
where
 s_suppkey in (
  select
   ps_suppkey
  from
   partsupp as ps left join (select l_partkey, l_suppkey,
     0.5 * sum(l_quantity) as cmp
    from
     tpch100g_sample.lineitem_sample_1p_ss
    where
     l_shipdate >= '1994-01-01'
     and l_shipdate < '1995-01-01'
  group by l_partkey, l_suppkey
       ) as tmp
    on ps.ps_partkey = tmp.l_partkey and ps.ps_suppkey = tmp.l_suppkey
  where
   ps_partkey in (
    select
     p_partkey
    from
     part
    where
     p_name like 'forest%'
   )
   and (ps.ps_availqty > tmp.cmp or (tmp.cmp is null and rand(unix_timestamp()) < 0.5) )
 )
 and s_nationkey = n_nationkey
 and n_name = 'CANADA'
order by
 s_name;