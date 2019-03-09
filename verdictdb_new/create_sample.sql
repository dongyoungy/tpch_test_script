create table tpch100g_sample.lineitem_st_50p_1000_l_partkey stored as parquet as 
(select *, (case when groupsize = 1 then 1 when groupsize < 2000 then floor(groupsize/2) else 1000 end) as samplesize
from (
select *, count(*) over (partition by l_partkey) as groupsize, row_number() over (partition by l_partkey order dy l_partkey)  as rownum
from tpch100g_parquet.lineitem
order by rand()) tmp
where  rownum <= (case when groupsize = 1 then 1 when groupsize < 2000 then groupsize/2 else 1000 end)
)

create table tpch500g_sample.lineitem_st_50p_1000_l_orderkey stored as parquet as 
(select *, (case when groupsize = 1 then 1 when groupsize < 2000 then floor(groupsize/2) else 1000 end) as samplesize
from (
select *, count(*) over (partition by l_orderkey) as groupsize, row_number() over (partition by l_orderkey order by l_orderkey)  as rownum
from tpch500g_parquet.lineitem
order by rand()) tmp
where  rownum <= (case when groupsize = 1 then 1 when groupsize < 2000 then groupsize/2 else 1000 end)
)
