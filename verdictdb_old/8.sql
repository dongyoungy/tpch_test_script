select
	o_year,
	sum(case
		when nation = 'BRAZIL' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			tpch100g_parquet.part,
			tpch100g_parquet.supplier,
			tpch100g_parquet.lineitem_scramble,
			tpch100g_parquet.orders,
			tpch100g_parquet.customer,
			tpch100g_parquet.nation n1,
			tpch100g_parquet.nation n2,
			tpch100g_parquet.region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by
	o_year
order by
	o_year;

select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from tpch100g_parquet.part, tpch100g_parquet.supplier, tpch100g_parquet.lineitem_scramble, tpch100g_parquet.orders, tpch100g_parquet.customer, tpch100g_parquet.nation n1, tpch100g_parquet.nation n2, tpch100g_parquet.region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year;