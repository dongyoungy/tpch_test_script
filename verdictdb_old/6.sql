select
	sum(l_extendedprice * l_discount) as revenue
from
	VERDICT_DATABASE.lineitem_scramble
where
	l_shipdate >= '1994-01-01'
	and l_shipdate < '1995-01-01'
	and l_discount between  0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;