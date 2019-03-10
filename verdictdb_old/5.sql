select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	VERDICT_DATABASE.lineitem_scramble,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= cast('1994-12-01' as timestamp)
	and o_orderdate < cast('1995-12-01' as timestamp)
group by
	n_name
order by
	revenue desc;