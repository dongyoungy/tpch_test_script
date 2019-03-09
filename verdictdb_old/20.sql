select
	s_name,
	s_address
from
	tpch100g_parquet.supplier,
	tpch100g_parquet.nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			tpch100g_parquet.partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					tpch100g_parquet.part
				where
					p_name like 'forest%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					tpch100g_parquet.lineitem_scramble
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

select s_name, s_address from tpch100g_parquet.supplier, tpch100g_parquet.nation where s_suppkey in ( select ps_suppkey from tpch100g_parquet.partsupp where ps_partkey in ( select p_partkey from tpch100g_parquet.part where p_name like 'forest%') and ps_availqty > ( select 0.5 * sum(l_quantity) from tpch100g_parquet.lineitem_scramble where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01')) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name;
