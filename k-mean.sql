/*
 данных много, они лежат на разных серверах (YH может быть Distributed)
 в результате мы должны получить массив индексов выявленных кластеров
 */

-- some functions
drop function ArrL2Distance; create function ArrL2Distance as (A,B) -> arraySum(a,b->(a-b)*(a-b),A,B);
drop function ArrAvg;        create function ArrAvg        as (A)   -> arrayMap(i->i/count(),sumMap(arrayEnumerate(A),A).2);

-- data
drop table YH; create table YH (i UInt32, Y Array(Int32)) engine = Memory;
insert into YH select number, [toInt32(number), toInt32(pow(number-30,2) + (rand()%3-1) * rand() % 20)] from numbers(200);
alter table YH delete where intDiv(i,10) in [3,4,12,13,17,18];

-- Centroids
drop table WCR; create table WCR ( ts DateTime, j Int32, C Array(Int32), P Array(Array(Int32)) ) engine = Memory;
insert into WCR select now(), rowNumberInAllBlocks()+1, Y  , [] from YH limit 40,1;
--insert into WCR values (toDateTime(0),1,(0,0),[]),(0,2,(50,0),[]);

-- random centroids for k-means++
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
limit 1
;

insert into WCR;
select now(), j, ArrAvg(Y) as C, groupArray(i)
from ( with  arrayMap(j->(j.1,ArrL2Distance(j.2,Y)),WCR) as D
       select Y, i, arrayReduce('argMin',D.1,D.2) as j  from YH
       global cross join (select groupArray((j,C)) as WCR from (select j,C from WCR order by ts desc limit 1 by j) ) as W
     )
group by j order by j;
