select
	sum(l_extendedprice * l_discount) as revenue
from
	VERDICT_DATABASE.LINEITEM
where
	l_shipdate >= cast('1994-01-01' as timestamp)
	and l_shipdate < cast('1995-01-01' as timestamp)
	and l_discount between  0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;