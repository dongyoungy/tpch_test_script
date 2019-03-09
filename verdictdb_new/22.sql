select
 cntrycode,
 count(*) as numcust,
 sum(c_acctbal) as totacctbal
from
 (
  select
   substring(c_phone,1,2) as cntrycode,
   c_acctbal
  from
   customer
  where
   substring(c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
   and c_acctbal > (
    select
     avg(c_acctbal)
    from
     customer
    where
     c_acctbal > 0.00
     and substring(c_phone,1,2) in
      ('13', '31', '23', '29', '30', '18', '17')
   )
   and not exists (
    select
     *
    from
     orders
    where
     o_custkey = c_custkey
   )
 ) as custsale
group by
 cntrycode
order by
 cntrycode;

-- +-----------+---------+--------------+
-- | cntrycode | numcust | totacctbal   |
-- +-----------+---------+--------------+
-- | 13        | 90748   | 679530260.80 |
-- | 17        | 91314   | 685030261.91 |
-- | 18        | 91293   | 684593250.42 |
-- | 23        | 90359   | 677680422.77 |
-- | 29        | 91124   | 683683023.65 |
-- | 30        | 91358   | 685118386.83 |
-- | 31        | 90553   | 678671645.35 |
-- +-----------+---------+--------------+


select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone,1,2) as cntrycode, c_acctbal from tpch100g_parquet.customer where substring(c_phone,1,2) in ('13', '31', '23', '29', '30', '18', '17') and c_acctbal > ( select avg(c_acctbal) from tpch100g_parquet.customer where c_acctbal > 0.00 and substring(c_phone,1,2) in ('13', '31', '23', '29', '30', '18', '17')) and not exists ( select * from tpch100g_parquet.orders_scramble where o_custkey = c_custkey)) as custsale group by cntrycode order by cntrycode;

select
 cntrycode,
 count(*) as numcust,
 sum(c_acctbal) as totacctbal
from
 (
  select
   substring(c_phone,1,2) as cntrycode,
   c_acctbal
  from
   customer
  where
   substring(c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
    and
  exists (
    select
     *
    from
     orders
    where
     o_custkey = c_custkey
   )
 ) as custsale
group by
 cntrycode
order by
 cntrycode;

-- +-----------+---------+---------------+
-- | cntrycode | numcust | totacctbal    |
-- +-----------+---------+---------------+
-- | 13        | 400618  | 1800854292.38 |
-- | 17        | 399395  | 1795317869.66 |
-- | 18        | 399219  | 1795337037.53 |
-- | 23        | 400808  | 1802776016.69 |
-- | 29        | 399930  | 1799412733.92 |
-- | 30        | 400270  | 1801673439.57 |
-- | 31        | 400006  | 1801662386.44 |
-- +-----------+---------+---------------+



select 
 cntrycode,
 totalcust,
 numcust,
 numcust2,
 numcust2 * (1 - pow(1-prob2, 3.16)) / (1-(1-prob2)) as t2,
 9*totalcust - 10*numcust2,
 totacctbal,
 avgacctbal,
 prob
from (
select
 cntrycode,
 count(*) as totalcust,
 sum(case when o_custkey is null then 1 else 0 end) as numcust,
 sum(case when o_custkey is not null then 1 else 0 end) as numcust2,
 sum(case when o_custkey is null then c_acctbal else 0 end) as totacctbal,
 sum(case when o_custkey is null then c_acctbal else 0 end) / sum(case when o_custkey is null then 1 else 0 end) as avgacctbal,
 sum(case when o_custkey is not null then c_acctbal else 0 end) / sum(case when o_custkey is not null then 1 else 0 end) as avgacctbal2,
 avg(case when o_custkey is null then 1 else 0 end) as prob,
 avg(case when o_custkey is not null then 1 else 0 end) as prob2
from
 (
  select
   substring(c_phone,1,2) as cntrycode,
   c_acctbal, 
  o_custkey, cnt
  from
  customer
  left join (select o_custkey, count(*) as cnt from tpch100g_parquet.orders_scramble where verdictdbblock <= 50 group by o_custkey) o1 on c_custkey = o1.o_custkey
  where 
   substring(c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
   and c_acctbal > (
    select
     avg(c_acctbal)
    from
     customer
    where
     c_acctbal > 0.00
     and substring(c_phone,1,2) in
      ('13', '31', '23', '29', '30', '18', '17')
   )
) tmp
group by
 cntrycode
) tmp2
order by
 cntrycode;



select cntrycode,
 totalcust,
 avgcnt,
 avgcnt2,
 numcust,
 numcust2,
 totalcust - numcust2 * (1 - pow(prob/avgcnt, 10)) / (1-(prob/avgcnt)) as finalcust,
 (totalcust - numcust2 * (1 - pow(prob/avgcnt, 10)) / (1-(prob/avgcnt))) * avgacctbal as finalval,
 numcust2 * (1 - pow(prob/sqrt(avgcnt), 10)) / (1-(prob/sqrt(avgcnt))) as val1,
 numcust2 * (1 - pow(prob/avgcnt, 10)) / (1-(prob/avgcnt)) as val1_2,
 numcust2 * (1 - pow(prob/avgcnt, 10)) / (1-(prob/avgcnt)) * avgacctbal as val2,
 numcust2 * (1 - pow(prob, 10)) / (1-(prob)) as val3,
 numcust2 * (1 - pow(prob, 10)) / (1-(prob)) * avgacctbal as val4
from (
select
 cntrycode,
 count(*) as totalcust,
 sum(case when o_custkey is null then 1 else 0 end) as numcust,
 sum(case when o_custkey is not null then 1 else 0 end) as numcust2,
 sum(case when o_custkey is null then c_acctbal else 0 end) as totacctbal,
 sum(case when o_custkey is null then c_acctbal else 0 end) / sum(case when o_custkey is null then 1 else 0 end) as avgacctbal,
 avg(case when o_custkey is null then 1 else 0 end) as prob,
 avg(case when o_custkey is not null then 1 else 0 end) as prob2,
 avg(case when o_custkey is not null then cnt else 0 end) as prob3,
 sum(case when o_custkey is not null then cnt else 0 end) /sum(case when o_custkey is not null then 1 else 0 end) as avgcnt,
 avg(cnt) as avgcnt2
from
 (
  select
   substring(c_phone,1,2) as cntrycode,
   c_acctbal, 
   o_custkey,
	 cnt
  from
  customer
  left join (select o_custkey, count(*) as cnt from tpch100g_parquet.orders_scramble where verdictdbblock <= 1 group by o_custkey) o1 on c_custkey = o1.o_custkey
  where 
   substring(c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
   and c_acctbal > (
    select
     avg(c_acctbal)
    from
     customer
    where
     c_acctbal > 0.00
     and substring(c_phone,1,2) in
      ('13', '31', '23', '29', '30', '18', '17')
   )
   ) tmp
group by
 cntrycode
) tmp2
order by
 cntrycode;


select cntrycode,
 numcust,
 numcust * avg3 as finalval
from (
select
 cntrycode,
 count(*) as totalcust,
 sum(case when c_custkey is not null then 1 else 0 end) totalcust2,
 sum(case when o_custkey is not null and c_custkey is not null then 1 when c_custkey is not null and o_custkey is null then (1 - pow((1-prob), 9)) / (1-(1-prob)) else 0 end) as numcust,
 sum(case when o_custkey is not null then 1 else 1 - pow((1-prob)/1.17, 99) end) as numcust2,
 sum(case when o_custkey is null then c_acctbal else 0 end) as totacctbal,
 sum(case when o_custkey is not null and c_custkey is not null then c_acctbal else 0 end) / 
 sum(case when o_custkey is not null and c_custkey is not null then 1 else 0 end) as avgacctbal,
 sum(case when c_custkey is not null then c_acctbal else 0 end) / 
 sum(case when c_custkey is not null then 1 else 0 end) as avgacctbal2,
 avg(case when c_custkey is not null then c_acctbal else 0 end) avg3 
from
 (
  select
   substring(c1.c_phone,1,2) as cntrycode,
   c1.c_acctbal, 
	 c2.c_custkey,
   o_custkey,
	 avg(case when o_custkey is not null then 1 else 0 end) over () prob,
	 avg(case when c2.c_custkey is not null then 1 else 0 end) over () prob2,
	 avg(case when c2.c_custkey is not null and o_custkey is not null then 1 else 0 end) over () prob3,
	 avg(cnt) over () as avgcnt
  from
	customer c1 left join
  (select * from customer c where
   substring(c.c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
   and c_acctbal > (
    select
     avg(c_acctbal)
    from
     customer cc
    where
     cc.c_acctbal > 0.00
     and substring(cc.c_phone,1,2) in
      ('13', '31', '23', '29', '30', '18', '17')
   )) c2 on c1.c_custkey = c2.c_custkey 
  left join (select o_custkey, count(*) as cnt from tpch100g_parquet.orders_scramble where verdictdbblock <= 10 group by o_custkey) o1 on c1.c_custkey = o1.o_custkey
  ) tmp
group by
 cntrycode
) tmp2
order by
 cntrycode;


select
 cntrycode,
 totalcust,
 avgacctbal,
 cnt1,
 cnt1 * (1 - pow((1-prob1), 100)) / (1-((1-prob1))) AS cnt
from (
select cntrycode, count(*) as totalcust, avg(c_acctbal) avgacctbal, 
sum(case when o_custkey is not null then 1 else 0 end) as cnt1,
avg(case when o_custkey is not null then 1 else 0 end) as prob1
from
  (select
   substring(c_phone,1,2) as cntrycode,
   c_custkey,
   c_acctbal
  from
   customer
  where
   substring(c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
   and c_acctbal > (
    select
     avg(c_acctbal)
    from
     customer
    where
     c_acctbal > 0.00
     and substring(c_phone,1,2) in
      ('13', '31', '23', '29', '30', '18', '17')
   )) t1 left join (select distinct o_custkey from tpch100g_parquet.orders_scramble where verdictdbblock <= 1) t2 on t2.o_custkey = t1.c_custkey
group by
 cntrycode
) tmp
order by
 cntrycode;


select
 cntrycode,
 totalcust,
 avgacctbal,
 cnt1,
 cnt2,
 cnt1 * (1 - pow((1-prob1), 100)) / (1-((1-prob1))) * 9999832 / 15000000 AS val,
 totalcust - (cnt1 * (1 - pow((1-prob1), 100)) / (1-((1-prob1))) * 9999832 / 15000000) AS numcust,
 (totalcust - (cnt1 * (1 - pow((1-prob1), 100)) / (1-((1-prob1))))  * 9999832 / 15000000) * avgacctbal as val
from (
select substring(c_phone,1,2) as cntrycode, count(*) as totalcust, avg(c_acctbal) avgacctbal, 
sum(case when o_custkey is not null then 1 else 0 end) as cnt1,
sum(case when o_custkey is null then 1 else 0 end) as cnt2,
avg(case when o_custkey is not null then 1 else 0 end) as prob1,
avg(cnt) as avgcnt
from
   customer c
   left join (select o_custkey, count(*) as cnt from tpch100g_sample.orders_sample_1p_ss group by o_custkey) t2 on t2.o_custkey = c.c_custkey
where
  substring(c_phone,1,2) in
  ('13', '31', '23', '29', '30', '18', '17')
  and c_acctbal > (
  select
    avg(c_acctbal)
  from
    customer
  where
    c_acctbal > 0.00
    and substring(c_phone,1,2) in
    ('13', '31', '23', '29', '30', '18', '17')
  )
group by
 cntrycode
) tmp
order by
 cntrycode;

-- +---------------------------+
-- | count(distinct o_custkey) |
-- +---------------------------+
-- | 9999832                   |
-- +---------------------------+

-- +---------------------------+
-- | count(distinct c_custkey) |
-- +---------------------------+
-- | 15000000                  |
-- +---------------------------+

+----------------------------+
| count(distinct l_orderkey) |
+----------------------------+
| 150000000                  |
+----------------------------+