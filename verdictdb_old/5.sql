select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	tpch100g_parquet.customer,
	tpch100g_parquet.orders,
	tpch100g_parquet.lineitem_scramble,
	tpch100g_parquet.supplier,
	tpch100g_parquet.nation,
	tpch100g_parquet.region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= date '1994-12-01'
	and o_orderdate < date '1995-12-01'
group by
	n_name
order by
	revenue desc;