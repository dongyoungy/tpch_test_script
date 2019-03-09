with q1 as
 (select
  l_suppkey as supplier_no,
  sum(l_extendedprice * (1 - l_discount)) as total_revenue
 from
  lineitem
 where
  l_shipdate >= '1995-01-01'
  and l_shipdate < '1995-04-01'
 group by
  l_suppkey)
select
 s_suppkey,
 s_name,
 s_address,
 s_phone,
 total_revenue
from
 supplier,
 q1
where
 s_suppkey = supplier_no
 and total_revenue = (
  select
   max(total_revenue)
  from
   q1
 )
order by
 s_suppkey;
