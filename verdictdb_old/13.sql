select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
			tpch100g_parquet.customer left outer join tpch100g_parquet.orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;

select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) as c_count from tpch100g_parquet.customer left outer join tpch100g_parquet.orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey) as c_orders group by c_count order by custdist desc, c_count desc;