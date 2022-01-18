-- test data
drop table sourceData; create table sourceData (i UInt32, x Int32, y Int32) engine = Memory;
insert into sourceData select number, rand32()%100, rand64()%100 from numbers(4);
insert into sourceData select number+4+i*50 as i,x+rand64()%30,y+rand()%30 from sourceData as t1,numbers(50) as t2;

-- interface to sourceData
create or replace view YH as select i, (toInt32(x),toInt32(y)) as Y from sourceData;

-- Centroids and Clusters
drop table WCR;
create table WCR ( ts DateTime, j Int32, C Tuple(Int32,Int32) ) engine = MergeTree order by ts;
insert into WCR select now(), rowNumberInAllBlocks()+1, Y from YH limit 40,1; -- first centroid
insert into WCR select * from centroidsInit;                                     -- next centroid

-- random centroids initialization as k-means++ algo
create or replace view centroidsInit as
with (select (ts,j) from WCR order by ts desc limit 1) as prev
select prev.1 as ts, prev.2+1 as j, y
from (
         select y,
         sum(d) over () as total,
         sum(d) over (rows between unbounded preceding and current row ) as cum
         from (
                 select argMin(Y, L2Distance(Y,C) as dx2) as y, min(dx2) as d
                 from YH
                 cross join (select * from WCR order by ts desc limit 1 by j) as WCR
                 where Y not in (select C from WCR)
                 group by Y
              )
     )
where total * (select rand32()/4294967295) < cum
order by cum
limit 1;

create or replace view nearestCentroid as
WITH ( SELECT  groupArray(j), groupArray(C) FROM WCR  WHERE ts = ( SELECT max(ts) FROM WCR) ) AS jC
SELECT  untuple(Y), i,
        arraySort((j, C) -> L2Distance(C, Y), jC.1, jC.2)[1] AS j
FROM YH;

-- recalculate centroids
INSERT INTO WCR SELECT
    now(),  j,
    tuple(COLUMNS('tupleElement') APPLY avg) AS C
FROM nearestCentroid
GROUP BY j;

-- критерий остановки
create or replace view deltaFinish as
select sum(d) as d from
    ( with groupArray(2)(C) as l
      select j, L2Distance(l[1], l[2]) as d
      from (select * from WCR order by ts desc )
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
