with cond as (select ps_partkey,
    sum(ps_supplycost * ps_availqty) * 1.5 as val
   from
    partsupp,
    supplier,
    nation
   where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
	group by ps_partkey
)
select t.ps_partkey, t.val from (
select
  ps_partkey,
  sum(ps_supplycost * ps_availqty) as val
from
  partsupp,
  supplier,
  nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
  ps_partkey
) t, cond c
where t.ps_partkey = c.ps_partkey and t.val < c.val
order by t.val desc
limit 100;
