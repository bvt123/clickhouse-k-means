
-- useful functions
create or replace function ArrL2Distance as (A,B) -> arraySum(a,b->(a-b)*(a-b),A,B);
create or replace function ArrAvg        as (A)   -> arrayMap(i->i/count(),sumMap(arrayEnumerate(A),A).2);

-- test data
drop table sourceData; create table sourceData (i UInt32, x Int32, y Int32) engine = Memory;
insert into sourceData select number, rand32()%100, rand64()%100 from numbers(4);
insert into sourceData select number+4+i*50 as i,x+rand64()%30,y+rand()%30 from sourceData as t1,numbers(50) as t2;

-- interface to sourceData
create or replace view YH as select i, [toInt32(x),toInt32(y)] as Y from sourceData;

-- Centroids and Clusters
drop table WCR; create table WCR ( ts DateTime, j Int32, C Array(Int32), P Array(Int32) ) engine = MergeTree order by ts;
insert into WCR select now(), rowNumberInAllBlocks()+1, Y  , [] from YH limit 40,1;

-- random centroids initialization as k-means++ algo
insert into WCR
select now(), (select j from WCR order by ts desc limit 1)+1 as j, y, []
from ( select y,
         sum(d) over () as total,
         sum(d) over (rows between unbounded preceding and current row ) as cum
         from (  select argMin(Y, ArrL2Distance(Y,C) as dx2) as y, min(dx2) as d
                 from YH, (select * from WCR order by ts desc limit 1 by j) as WCR
                 where Y not in (select C from WCR)
                 group by Y)) as t1,
     ( select rand32()/4294967295 as r ) as t2
where total*r < cum
order by cum
limit 1;

-- k-means. recalculate centroids and produce groups of PK of original data. should run several times to get better approximations
insert into WCR;
select now(), j, ArrAvg(Y) as C, groupArray(i)
from ( with  arrayMap(j->(j.1, ArrL2Distance(j.2,Y)),WCR) as D
       select Y, i, arrayReduce('argMin',D.1,D.2) as j  from YH
       global cross join (select groupArray((j,C)) as WCR from (select j,C from WCR order by ts desc limit 1 by j) ) as W
     )
group by j order by j;

create or replace function getWCR as (x) ->  (select arrayJoin(P) from (select P from WCR where j=x order by ts  desc limit 1));
-- results for drawing
select * from
(select Y[1] as x, Y[2] p1, null p2, null p3, null p4, null p5 from YH where i in getWCR(1)
union all
select Y[1] as x, null p1, Y[2] p2, null p3, null p4, null p5 from YH where i in getWCR(2)
union all
select Y[1] as x, null p1, null p2, Y[2] p3, null p4, null p5 from YH where i in getWCR(3)
union all
select Y[1] as x, null p1, null p2, null p3, Y[2] p4, null p5 from YH where i in getWCR(4)
union all
select Y[1] as x, null p1, null p2, null p3, null p4, Y[2] p5 from YH where i in getWCR(5))
order by x
;



