-- test data
drop table sourceData; create table sourceData (i UInt32, x Float64, y Float64) engine = Memory;
insert into sourceData select number, rand32()%100, rand64()%100 from numbers(4);
insert into sourceData select number+4+i*500 as i,x+rand64()%3000/100,y+rand()%3000/100 from sourceData as t1,numbers(50) as t2;

-- interface to sourceData
create or replace view YH as select i, (x,y) as Y from sourceData;

-- Centroids and Clusters
drop table WCR;
create table WCR ( step UInt32, j Int32, C Tuple(Float64,Float64) ) engine = MergeTree order by step;
insert into WCR select 0, rowNumberInAllBlocks()+1, Y from YH limit 40,1; -- first centroid
insert into WCR select * from centroidsInit;                                     -- next centroid

-- random centroids initialization as k-means++ algo
create or replace view centroidsInit as
with (select (step,j) from WCR order by step desc limit 1) as prev
select prev.1 as step, prev.2+1 as j, y
from (
         select y,
         sum(d) over () as total,
         sum(d) over (rows between unbounded preceding and current row ) as cum
         from (
                 select argMin(Y, L2Distance(Y,C) as dx2) as y, min(dx2) as d
                 from YH
                 cross join (select * from WCR order by step desc limit 1 by j) as WCR
                 where Y not in (select C from WCR)
                 group by Y
              )
     )
where total * (select rand32()/4294967295) < cum
order by cum
limit 1;

create or replace view nearestCentroid as
WITH ( SELECT  groupArray(j), groupArray(C), any(step) FROM WCR  WHERE step = ( SELECT max(step) FROM WCR) ) AS jC
SELECT  untuple(Y), jC.3+1 as step,
        arraySort((j, C) -> L2Distance(C, Y), jC.1, jC.2)[1] AS j
FROM YH;

-- recalculate centroids
INSERT INTO WCR SELECT
    step,  j,
    tuple(COLUMNS('tupleElement') APPLY avg) AS C
FROM nearestCentroid
GROUP BY j, step;

-- критерий остановки - дистанция между двумя последними позициями центроидов
create or replace view deltaFinish as
with 10 as one_delta
select toUInt32(sum(d)*one_delta) as d from
    ( with groupArray(2)(C) as l
      select j, L2Distance(l[1], l[2]) as d
      from (select * from WCR order by step desc limit 2 by step)
      group by j
    );

-- results for drawing
with tuple(COLUMNS('tupleElement')) as a
select a.1 as x,
       if(j=1,a.2,null) as p1,
       if(j=2,a.2,null) as p2,
       if(j=3,a.2,null) as p3,
       if(j=4,a.2,null) as p4,
       if(j=5,a.2,null) as p5
from nearestCentroid;
