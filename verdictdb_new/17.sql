select
 sum(l_extendedprice) / 7.0 as avg_yearly
from
 lineitem,
 part
where
 p_partkey = l_partkey
 and p_brand = 'Brand#23'
 and p_container = 'MED BOX'
 and l_quantity < (
  select
   0.2 * avg(l_quantity)
  from
   lineitem
  where
   l_partkey = p_partkey
 );

-- +-------------+
-- | avg_yearly  |
-- +-------------+
-- | 32087018.99 |
-- +-------------+
-- Fetched 1 row(s) in 56.17s
33985072.89
32087018.99
30550513
3279116660.011317
3616866406.693367
3279888931.441

CREATE TABLE tpch100g_parquet.lineitem_sample_st_5_l_partkey stored as parquet 
as select * from (
	select *, row_number() over (partition by l_partkey order by l_partkey) as rownum
	from tpch100g_parquet.lineitem 
) tmp
where tmp.rownum <= 5;

select
 sum(l_extendedprice) / 7.0 as avg_yearly
from
 tpch100g_parquet.lineitem,
 tpch100g_parquet.part
where
 p_partkey = l_partkey
 and p_brand = 'Brand#23'
 and p_container = 'MED BOX'
 and l_quantity < (
  select
   0.2 * avg(l_quantity)
  from
   tpch100g_parquet.lineitem_scramble
  where
   l_partkey = p_partkey
 );
 select sum(l_extendedprice) / 7.0 as avg_yearly from tpch100g_parquet.lineitem, tpch100g_parquet.part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < ( select 0.2 * avg(l_quantity) from tpch100g_parquet.lineitem_scramble where l_partkey = p_partkey);


--  (select * from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10) t1,
--  sum(t1.l_extendedprice * (1/prob)) / 7.0 as avg_yearly, avg(prob)
-- why 10?
select
 sum(t1.l_extendedprice * (1 - pow((1-prob), 100)) / (1-((1-(prob))))) / 7.0 as avg_yearly,
 sum(t1.l_extendedprice * (1/prob)) / 7.0 as avg_yearly2
from
 lineitem t1,
 (select p_partkey, t.cmp, avg(case when t.cmp is not null then 1 else 0 end) over () as prob, avg(cnt) over () as avgcnt
 from
 part p left join
 (select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp from tpch100g_parquet.lineitem_scramble where verdictdbblock = 1 GROUP BY l_partkey) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2
where
 t1.l_partkey = t2.p_partkey
 and (t1.l_quantity < t2.cmp);


 and p_brand = 'Brand#23'
 and p_container = 'MED BOX'

select
 avg(case when t.k1 is not null then 1 else 0 end) prob
 from
 part p left join
 (select l_partkey as k1, 0.2 * avg(l_quantity) as cmp from tpch100g_sample.lineitem_sample_1p_ss  GROUP BY l_partkey) t on p_partkey = t.k1
where
p_brand = 'Brand#23'
 and p_container = 'MED BOX'

select count(*) from
(
 select l_partkey, count(*) as groupsize from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10 group by l_partkey
) tmp
where groupsize > 10;

select *
from
 tpch100g_sample.lineitem_sample_1p_ss t1 left join 
 part p on t1.l_partkey = p.p_partkey
 LEFT JOIN (select l_partkey as k1, 0.2 * avg(l_quantity) as cmp from tpch100g_sample.lineitem_sample_1p_ss GROUP BY l_partkey) t on p_partkey = t.k1
 where cmp is null;

select
 (sum(l_extendedprice) / 7.0) as avg_yearly
from
 lineitem t1,
 part p
where
 p_partkey = t1.l_partkey
 and p_brand = 'Brand#23'
 and p_container = 'MED BOX'
 and l_quantity < 5.098;

SELECT l_partkey, 0.2 * avg(l_quantity) as cmp FROM tpch100g_sample.lineitem_sample_1p_ss GROUP BY l_partkey;

SELECT sum(l_extendedprice) FROM part, tpch100g_sample.lineitem_sample_1p_ss where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < 5.1;
SELECT sum(l_extendedprice) FROM part, lineitem where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < 5.1;

228685243.73
22953816620.08

2874105.32
270610050.93

select
        sum(extendedprice * (20000000/numkey)) / 7.0  as avg_yearly
from    
        (
        select
                l_quantity as quantity,
                l_extendedprice as extendedprice,
                count(*) over () numkey,
                t_avg_quantity
        from
                (select
                        l_partkey as t_partkey,
                        0.2 * avg(l_quantity) as t_avg_quantity
                from
                        tpch100g_sample.lineitem_sample_1p_ss
                group by l_partkey) as q17_lineitem_tmp_cached 
                Inner Join
                (select
                        l_quantity,
                        l_partkey,
                        l_extendedprice
                from
                        part,
                        lineitem
                where
                        p_partkey = l_partkey
                        and p_brand = 'Brand#23'
                        and p_container = 'MED BOX'
                ) as l1 on l1.l_partkey = t_partkey
        ) a
where quantity < t_avg_quantity;


select sum(extendedprice) / 7.0 as avg_yearly from    ( select l_quantity as quantity, l_extendedprice as extendedprice, t_avg_quantity from (select l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity from tpch100g_parquet.lineitem_scramble group by l_partkey) as q17_lineitem_tmp_cached Inner Join (select l_quantity, l_partkey, l_extendedprice from tpch100g_parquet.part, tpch100g_parquet.lineitem where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX') as l1 on l1.l_partkey = t_partkey) a where quantity < t_avg_quantity;


select
        sum(extendedprice) / 7.0 as avg_yearly
from    
        (
        select
                l_quantity as quantity,
                l_extendedprice as extendedprice,
                t_avg_quantity
        from
                (select
                        l_partkey as t_partkey,
                        0.2 * avg(l_quantity) as t_avg_quantity
                from
                        tpch100g_sample.lineitem_sample_1p_ss
                group by l_partkey) as q17_lineitem_tmp_cached 
                Inner Join
                (select
                        l_quantity,
                        l_partkey,
                        l_extendedprice
                from
                        part,
                        lineitem
                where
                        p_partkey = l_partkey
                        and p_brand = 'Brand#23'
                        and p_container = 'MED BOX'
                ) as l1 on l1.l_partkey = t_partkey
        ) a
where quantity < t_avg_quantity;

select sum(l_extendedprice * (case when pass = 1 then 1 else prob2 * (1 - pow(1-prob2, 99)) end)) / 7.0 * 100 as avg_yearly from (
select t1.l_extendedprice, (case when t1.l_quantity < t2.cmp and p_partkey is not null then 1 else 0 end) pass,
avg(case when t1.l_quantity < t2.cmp and p_partkey is not null then 1 else 0 end) over () prob2
from
 (select * from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 1) t1 left join
 (select p_partkey, t.cmp, avg(case when t.cmp is not null then 1 else 0 end) over () as prob, avg(cnt) over () as avgcnt
 from
 part p left join
 (select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 1 GROUP BY l_partkey) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2 on t1.l_partkey = t2.p_partkey
) tmp;

select sum(l_extendedprice / samplesize * groupsize ) / 7.0 as avg_yearly
from
 (select * from tpch100g_sample.lineitem_st_50p_1000_l_partkey) t1 join
 (select p_partkey, t.cmp
  from tpch100g_parquet.part p join
 (select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp, avg(groupsize) as groupsize, avg(samplesize) as samplesize
 from tpch100g_sample.lineitem_st_50p_1000_l_partkey GROUP BY l_partkey) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2 on t1.l_partkey = t2.p_partkey
 where t1.l_quantity < t2.cmp;

---- F(x) = 1/(1 + exp(-0.07056 x^3 – 1.5976 x))


select sum(l_extendedprice * (case when prob is not null then prob when p_partkey is null then 0 when prob is null and p_partkey is not null then avg_prob else 0 end)) / 7.0 as avg_yearly,
sum(l_extendedprice * (case when prob is not null then prob else 0 end) / numkey * 20000000 * numpass/(numpass+numfail)) / 7.0 as avg_yearly2,
sum(l_extendedprice * (case when prob is null and p_partkey is not null then numpass/(numpass+numfail) else 0 end)) / 7.0 as avg_yearly3
from (
select t1.l_extendedprice, t2.p_partkey, 1/(1 + exp(-0.07056 * pow((5 * t1.l_quantity - t2.cmp) / t2.std, 3) - (1.5976 * (5 * t1.l_quantity - t2.cmp) / t2.std))) as prob,
avg(1/(1 + exp(-0.07056 * pow((5 * t1.l_quantity - t2.cmp) / t2.std, 3) - (1.5976 * (5 * t1.l_quantity - t2.cmp) / t2.std)))) over () as avg_prob,
sum(case when 5*t1.l_quantity < t2.cmp then 1 else 0 end) over () numpass,
sum(case when 5*t1.l_quantity >= t2.cmp then 1 else 0 end) over () numfail,
sum(case when t2.cmp is null then 1 else 0 end) over () numnull,
sum(case when t2.cmp is not null then 1 else 0 end) over () numkey
from
 (select * from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10) t1 left join
 (select p_partkey, t.cmp, t.cnt, t.std
 from part p left join
 (select l_partkey, count(*) as cnt, avg(l_quantity) as cmp, stddev(l_quantity) std
  from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10
  GROUP BY l_partkey having cnt >= 10
  ) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2 on t1.l_partkey = t2.p_partkey
) tmp;

select sum(l_extendedprice * (case when pass = 1 then 1 when pass = -1 then 0 else numpass/(numpass+numfail) end)) / 7.0 as avg_yearly from (
select t1.l_extendedprice, (case when t1.l_quantity < t2.cmp then 1 when t1.l_quantity >= t2.cmp then -1 else 0 end) pass,
sum(case when t1.l_quantity < t2.cmp then 1 else 0 end) over () numpass,
sum(case when t1.l_quantity >= t2.cmp then 1 else 0 end) over () numfail,
sum(case when t2.cmp is not null then 1 else 0 end) over () numkey,
sum(case when t2.cmp is null then 1 else 0 end) over () numnull
from
 (select * from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10) t1 join
 (select p_partkey, t.cmp, t.cnt, t.std
 from part p left join
 (select l_partkey, count(*) as cnt, avg(l_quantity) as cmp, stddev(l_quantity) std
  from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10
  GROUP BY l_partkey
  HAVING cnt >= 10) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2 on t1.l_partkey = t2.p_partkey
) tmp;

select min(cnt), max(cnt) from (
select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp
  from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 20
  GROUP BY l_partkey) tmp;

select sum(case when t1.l_quantity < t2.cmp then 1 else 0 end) pass,
sum(case when t1.l_quantity >= t2.cmp then 1 else 0 end) fail,
sum(case when t2.cmp is null then 1 else 0 end) nullcase
from
 (select * from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10) t1 join
 (select p_partkey, t.cmp
 from part p left join
 (select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp
  from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10
  GROUP BY l_partkey
  HAVING cnt >= 10) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2 on t1.l_partkey = t2.p_partkey

select count(*) from (
select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10 GROUP BY l_partkey
) tmp;

-- 35895
-- 19300743

select t1.l_extendedprice, (case when t1.l_quantity < t2.cmp and p_partkey is not null then 1 else 0 end) pass,
sum(case when t1.l_quantity >= t2.cmp and p_partkey is not null then 1 else 0 end) over () notpass,
sum(case when p_partkey is null then 1 else 0 end) over () nullcase,
sum(case when t1.l_quantity < t2.cmp then 1 else 0 end) over () numpass
from
 (select * from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10) t1 join
 (select p_partkey, t.cmp
 from part p left join
 (select l_partkey, count(*) as cnt, 0.2 * avg(l_quantity) as cmp
  from tpch100g_parquet.lineitem_scramble where verdictdbblock <= 10
  GROUP BY l_partkey
  HAVING cnt >= 10) t on p_partkey = t.l_partkey
 where p_brand = 'Brand#23' and p_container = 'MED BOX'
 ) t2 on t1.l_partkey = t2.p_partkey


SELECT (sum((vt1620.`avg_yearly` * vt1620.`__vpsize`)) / sum(vt1620.`__vpsize`)) AS `avg_yearly`, (((stddev(vt1620.`avg_yearly`) * sqrt(avg(vt1620.`__vpsize`))) / sqrt(sum(vt1620.`__vpsize`))) * 1.96) AS `avg_yearly_err` FROM (SELECT (((sum((`l_extendedprice` / 0.01)) / count(*)) * sum(count(*)) OVER ()) / 7.0) AS `avg_yearly`, vt1610.`ssid` AS `ssid`, count(*) AS `__vpsize`, avg(1.0) AS `verdict_vprob` FROM tpch100g_sample.lineitem_sample_1p_ss AS vt1610 INNER JOIN (SELECT vt1611.`l_partkey` AS `partkey`, (0.2 * (sum((`l_quantity` / 0.01)) / sum((CASE WHEN (`l_quantity` IS NULL) THEN 0 ELSE (1.0 / 0.01) END)))) AS `small_quantity`, vt1611.`ssid` AS `ssid`, count(*) AS `__vpsize`, avg(0.02) AS `verdict_vprob` FROM tpch100g_sample.lineitem_sample_1p_ss AS vt1611 INNER JOIN tpch100g_parquet.part AS vt1612 ON vt1611.`l_partkey` = vt1612.`p_partkey` GROUP BY vt1611.`l_partkey`, vt1611.`ssid`) AS t ON vt1610.`l_partkey` = t.`partkey` INNER JOIN tpch100g_parquet.part AS vt1617 ON vt1610.`l_partkey` = vt1617.`p_partkey` WHERE ((vt1617.`p_brand` = 'Brand#23') AND (vt1617.`p_container` = 'MED BOX')) AND (vt1610.`l_quantity` < t.`small_quantity`) GROUP BY vt1610.`ssid`) AS vt1620