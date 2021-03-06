select
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice,
 sum(l_quantity)
from
 customer,
 orders,
 lineitem
where
 o_orderkey in (
  select
   l_orderkey
  from
   lineitem
  group by
   l_orderkey having
    sum(l_quantity) > 300
 )
 and c_custkey = o_custkey
 and o_orderkey = l_orderkey
group by
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice
order by
 o_totalprice desc,
 o_orderdate
limit 100;

-- +--------------------+-----------+------------+---------------------+--------------+-----------------+
-- | c_name             | c_custkey | o_orderkey | o_orderdate         | o_totalprice | sum(l_quantity) |
-- +--------------------+-----------+------------+---------------------+--------------+-----------------+
-- | Customer#011472112 | 11472112  | 458304292  | 1998-02-05 00:00:00 | 591036.15    | 322.00          |
-- | Customer#012090925 | 12090925  | 501322081  | 1995-02-04 00:00:00 | 586945.44    | 319.00          |
-- | Customer#001392379 | 1392379   | 332381222  | 1998-01-24 00:00:00 | 575600.00    | 311.00          |
-- | Customer#013458721 | 13458721  | 333307747  | 1997-12-19 00:00:00 | 572334.88    | 319.00          |
-- | Customer#008643083 | 8643083   | 84927619   | 1997-06-29 00:00:00 | 571417.48    | 316.00          |
-- | Customer#010543705 | 10543705  | 163142919  | 1996-06-10 00:00:00 | 569798.10    | 313.00          |
-- | Customer#000026377 | 26377     | 586594688  | 1992-04-14 00:00:00 | 569044.84    | 311.00          |
-- | Customer#005914657 | 5914657   | 55799200   | 1996-02-11 00:00:00 | 568754.48    | 327.00          |
-- | Customer#011461310 | 11461310  | 20662370   | 1992-07-22 00:00:00 | 566353.20    | 315.00          |
-- | Customer#007682894 | 7682894   | 532736640  | 1993-07-22 00:00:00 | 563532.16    | 316.00          |
-- | Customer#012070685 | 12070685  | 34201984   | 1997-06-18 00:00:00 | 563492.49    | 322.00          |
-- | Customer#012878113 | 12878113  | 42290181   | 1997-11-26 00:00:00 | 563479.57    | 318.00          |
-- | Customer#000506836 | 506836    | 116567399  | 1992-03-21 00:00:00 | 563342.71    | 319.00          |
-- | Customer#012848143 | 12848143  | 501771233  | 1994-12-06 00:00:00 | 562642.03    | 317.00          |
-- | Customer#003493022 | 3493022   | 182898470  | 1996-01-09 00:00:00 | 561296.06    | 318.00          |
-- | Customer#013083751 | 13083751  | 28077922   | 1996-10-27 00:00:00 | 560893.88    | 319.00          |
-- | Customer#006580315 | 6580315   | 343593507  | 1994-05-20 00:00:00 | 560058.60    | 321.00          |
-- | Customer#009427249 | 9427249   | 48881602   | 1993-09-19 00:00:00 | 559502.82    | 327.00          |
-- | Customer#009995401 | 9995401   | 530897159  | 1994-05-02 00:00:00 | 558771.77    | 305.00          |
-- | Customer#006094567 | 6094567   | 156159680  | 1994-11-15 00:00:00 | 558037.12    | 308.00          |
-- | Customer#006594658 | 6594658   | 555833473  | 1997-08-01 00:00:00 | 557961.99    | 324.00          |
-- | Customer#005925541 | 5925541   | 446215044  | 1992-05-13 00:00:00 | 556993.03    | 318.00          |
-- | Customer#007010650 | 7010650   | 587222276  | 1993-12-17 00:00:00 | 555946.71    | 329.00          |
-- | Customer#008211575 | 8211575   | 218305350  | 1992-12-19 00:00:00 | 555691.15    | 316.00          |
-- | Customer#005523215 | 5523215   | 499594150  | 1997-12-30 00:00:00 | 552295.59    | 322.00          |
-- | Customer#003592226 | 3592226   | 94936672   | 1992-02-28 00:00:00 | 551850.29    | 307.00          |
-- | Customer#006048598 | 6048598   | 45897379   | 1994-05-30 00:00:00 | 550195.48    | 318.00          |
-- | Customer#013889867 | 13889867  | 322196069  | 1992-12-24 00:00:00 | 549996.06    | 313.00          |
-- | Customer#007847146 | 7847146   | 75799361   | 1994-06-22 00:00:00 | 549769.03    | 307.00          |
-- | Customer#002538608 | 2538608   | 406952547  | 1992-04-14 00:00:00 | 549380.73    | 304.00          |
-- | Customer#011113726 | 11113726  | 346088583  | 1996-02-21 00:00:00 | 548790.02    | 303.00          |
-- | Customer#003262783 | 3262783   | 327814241  | 1997-04-16 00:00:00 | 548355.89    | 319.00          |
-- | Customer#002099413 | 2099413   | 507968289  | 1993-01-23 00:00:00 | 547809.74    | 316.00          |
-- | Customer#009950764 | 9950764   | 442334435  | 1996-10-07 00:00:00 | 547526.61    | 308.00          |
-- | Customer#012123581 | 12123581  | 496277122  | 1995-03-13 00:00:00 | 547335.02    | 312.00          |
-- | Customer#005800261 | 5800261   | 493616454  | 1997-01-11 00:00:00 | 547151.89    | 314.00          |
-- | Customer#002395564 | 2395564   | 584277191  | 1993-10-14 00:00:00 | 546290.18    | 311.00          |
-- | Customer#001382375 | 1382375   | 43491654   | 1992-05-20 00:00:00 | 546035.62    | 308.00          |
-- | Customer#001359841 | 1359841   | 373555077  | 1995-05-08 00:00:00 | 545367.48    | 304.00          |
-- | Customer#000569440 | 569440    | 119020930  | 1995-02-19 00:00:00 | 545201.16    | 309.00          |
-- | Customer#012179552 | 12179552  | 157723879  | 1992-01-06 00:00:00 | 545008.93    | 320.00          |
-- | Customer#000328568 | 328568    | 584293472  | 1997-02-04 00:00:00 | 544997.41    | 317.00          |
-- | Customer#005098117 | 5098117   | 82043264   | 1996-04-15 00:00:00 | 544838.33    | 330.00          |
-- | Customer#012375202 | 12375202  | 272622276  | 1995-08-13 00:00:00 | 543957.13    | 301.00          |
-- | Customer#006394964 | 6394964   | 410876964  | 1994-11-15 00:00:00 | 543654.37    | 324.00          |
-- | Customer#012130760 | 12130760  | 152975303  | 1992-11-23 00:00:00 | 543496.47    | 311.00          |
-- | Customer#005021956 | 5021956   | 163897538  | 1992-05-10 00:00:00 | 543411.84    | 315.00          |
-- | Customer#014745380 | 14745380  | 323533249  | 1993-03-31 00:00:00 | 543411.82    | 322.00          |
-- | Customer#005387083 | 5387083   | 394137797  | 1993-09-14 00:00:00 | 542890.62    | 308.00          |
-- | Customer#001070318 | 1070318   | 91475014   | 1996-11-15 00:00:00 | 542874.97    | 314.00          |
-- | Customer#011639476 | 11639476  | 512860422  | 1995-11-20 00:00:00 | 542855.00    | 311.00          |
-- | Customer#009557132 | 9557132   | 80293413   | 1992-06-12 00:00:00 | 542680.80    | 305.00          |
-- | Customer#012639745 | 12639745  | 215706726  | 1992-10-29 00:00:00 | 541693.09    | 305.00          |
-- | Customer#010933264 | 10933264  | 505312545  | 1995-07-19 00:00:00 | 541378.70    | 309.00          |
-- | Customer#006633514 | 6633514   | 561405959  | 1994-09-10 00:00:00 | 541176.68    | 308.00          |
-- | Customer#008668609 | 8668609   | 19730208   | 1994-01-24 00:00:00 | 540531.95    | 305.00          |
-- | Customer#000793051 | 793051    | 103963585  | 1996-10-27 00:00:00 | 540445.27    | 313.00          |
-- | Customer#000194932 | 194932    | 233298977  | 1997-11-16 00:00:00 | 540319.71    | 304.00          |
-- | Customer#007585013 | 7585013   | 41325120   | 1997-05-02 00:00:00 | 540248.88    | 309.00          |
-- | Customer#005034994 | 5034994   | 571000834  | 1997-08-02 00:00:00 | 540244.15    | 303.00          |
-- | Customer#013646452 | 13646452  | 109414535  | 1997-01-09 00:00:00 | 539803.25    | 302.00          |
-- | Customer#010763563 | 10763563  | 575451169  | 1994-01-18 00:00:00 | 539403.56    | 329.00          |
-- | Customer#003878665 | 3878665   | 465165376  | 1995-08-03 00:00:00 | 539331.83    | 312.00          |
-- | Customer#009475150 | 9475150   | 486502979  | 1995-01-21 00:00:00 | 539173.83    | 308.00          |
-- | Customer#007359265 | 7359265   | 402919200  | 1997-03-14 00:00:00 | 539028.84    | 312.00          |
-- | Customer#009274702 | 9274702   | 545970215  | 1997-12-17 00:00:00 | 538976.86    | 303.00          |
-- | Customer#008402254 | 8402254   | 539392967  | 1993-02-23 00:00:00 | 538886.31    | 333.00          |
-- | Customer#008654869 | 8654869   | 100176839  | 1993-04-12 00:00:00 | 538846.61    | 317.00          |
-- | Customer#007498450 | 7498450   | 140933856  | 1993-04-16 00:00:00 | 538585.17    | 309.00          |
-- | Customer#001291604 | 1291604   | 220248583  | 1995-08-18 00:00:00 | 538171.18    | 304.00          |
-- | Customer#005159660 | 5159660   | 160674945  | 1996-05-16 00:00:00 | 537771.10    | 310.00          |
-- | Customer#014286853 | 14286853  | 423877508  | 1995-03-02 00:00:00 | 536949.18    | 319.00          |
-- | Customer#008779822 | 8779822   | 211965092  | 1996-07-18 00:00:00 | 536768.43    | 314.00          |
-- | Customer#004955200 | 4955200   | 510010278  | 1996-01-08 00:00:00 | 536693.57    | 302.00          |
-- | Customer#008208034 | 8208034   | 446989058  | 1993-07-12 00:00:00 | 536601.61    | 315.00          |
-- | Customer#009666952 | 9666952   | 475794275  | 1994-09-23 00:00:00 | 536427.97    | 322.00          |
-- | Customer#011930665 | 11930665  | 107950688  | 1995-08-05 00:00:00 | 536328.78    | 309.00          |
-- | Customer#014831182 | 14831182  | 92086567   | 1996-06-12 00:00:00 | 536294.92    | 322.00          |
-- | Customer#009515017 | 9515017   | 462317029  | 1997-01-28 00:00:00 | 535876.92    | 308.00          |
-- | Customer#008795228 | 8795228   | 275406304  | 1994-12-19 00:00:00 | 535829.88    | 304.00          |
-- | Customer#013677323 | 13677323  | 387783079  | 1992-04-22 00:00:00 | 535504.63    | 316.00          |
-- | Customer#004965692 | 4965692   | 250733185  | 1994-06-12 00:00:00 | 535482.20    | 306.00          |
-- | Customer#004663856 | 4663856   | 159914310  | 1996-09-12 00:00:00 | 535322.28    | 315.00          |
-- | Customer#014560954 | 14560954  | 420830819  | 1994-03-05 00:00:00 | 534940.24    | 301.00          |
-- | Customer#003555572 | 3555572   | 190167361  | 1994-08-29 00:00:00 | 534477.19    | 304.00          |
-- | Customer#000299162 | 299162    | 303098309  | 1996-01-28 00:00:00 | 534421.58    | 315.00          |
-- | Customer#005747806 | 5747806   | 540886884  | 1994-04-14 00:00:00 | 534288.51    | 314.00          |
-- | Customer#013094119 | 13094119  | 111128577  | 1995-05-09 00:00:00 | 534239.89    | 316.00          |
-- | Customer#008259454 | 8259454   | 546232737  | 1997-12-07 00:00:00 | 534055.44    | 303.00          |
-- | Customer#000090529 | 90529     | 112311041  | 1998-01-22 00:00:00 | 534035.86    | 307.00          |
-- | Customer#004938865 | 4938865   | 595346338  | 1996-08-18 00:00:00 | 533883.75    | 310.00          |
-- | Customer#012726752 | 12726752  | 92511430   | 1995-03-06 00:00:00 | 533441.05    | 318.00          |
-- | Customer#009638569 | 9638569   | 16783360   | 1998-02-16 00:00:00 | 533372.62    | 308.00          |
-- | Customer#000437614 | 437614    | 569537412  | 1992-01-07 00:00:00 | 533305.85    | 305.00          |
-- | Customer#011219830 | 11219830  | 156065860  | 1996-07-03 00:00:00 | 533119.17    | 325.00          |
-- | Customer#005060768 | 5060768   | 255829888  | 1997-03-18 00:00:00 | 533054.97    | 308.00          |
-- | Customer#014112568 | 14112568  | 280056096  | 1997-01-30 00:00:00 | 532881.87    | 313.00          |
-- | Customer#012373496 | 12373496  | 61135843   | 1993-01-30 00:00:00 | 532734.68    | 303.00          |
-- | Customer#007910675 | 7910675   | 158520577  | 1992-02-29 00:00:00 | 532689.01    | 304.00          |
-- | Customer#013106491 | 13106491  | 88925410   | 1995-01-10 00:00:00 | 532631.76    | 310.00          |
-- +--------------------+-----------+------------+---------------------+--------------+-----------------+


select
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice,
 sum(val * (1 - pow((1-prob), 100)) / (1-((1-prob)))) as val
from
 customer,
 (select o_custkey, o_orderkey, o_orderdate, o_totalprice, l_orderkey, val,
         avg(case when l_orderkey is not null then 1 else 0 end) over () as prob
 from orders left join
             (select l_orderkey, sum(l_quantity) * 100 as val 
              from tpch100g_parquet.lineitem_scramble 
              where verdictdbblock = 1 
              group by l_orderkey 
              having sum(l_quantity) * 100 > 300) tmp
             on o_orderkey = l_orderkey
 ) tmp2
where
  tmp2.l_orderkey is not null
  and c_custkey = tmp2.o_custkey
group by
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice
order by
 o_totalprice desc,
 o_orderdate
limit 100;
