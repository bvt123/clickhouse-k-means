-- test data
drop table sourceData; create table sourceData (i UInt32, x Int32, y Int32) engine = Memory;
insert into sourceData select number, rand32()%100, rand64()%100 from numbers(4);
insert into sourceData select number+4+i*50 as i,x+rand64()%30,y+rand()%30 from sourceData as t1,numbers(50) as t2;

-- interface to sourceData
create or replace view YH as select i, (toInt32(x),toInt32(y)) as Y from sourceData;

-- Centroids and Clusters
drop table WCR;
--create table WCR ( ts DateTime, j Int32, C Array(Int32), P Array(Int32) ) engine = MergeTree order by ts;
create table WCR ( ts DateTime, j Int32, C Tuple(Int32,Int32), P Array(Int32) ) engine = MergeTree order by ts;
insert into WCR select now(), rowNumberInAllBlocks()+1, Y  , [] from YH limit 40,1;

-- random centroids initialization as k-means++ algo
insert into WCR
select now(), (select j from WCR order by ts desc limit 1)+1 as j, y, []
from ( select y,
         sum(d) over () as total,
         sum(d) over (rows between unbounded preceding and current row ) as cum
         from (  select argMin(Y, L2Distance(Y,C) as dx2) as y, min(dx2) as d
                 from YH, (select * from WCR order by ts desc limit 1 by j) as WCR
                 where Y not in (select C from WCR)
                 group by Y)) as t1,
     ( select rand32()/4294967295 as r ) as t2
where total*r < cum
order by cum
limit 1;


WITH (SELECT max(ts) FROM WCR) as max_ts

FROM WCR WHERE ts = max_ts

-- k-means. recalculate centroids and produce groups of PK of original data. should run several times to get better approximations
INSERT INTO WCR SELECT
    now(),
    j,
    tuple(COLUMNS('tupleElement') APPLY avg) AS C,
    groupArray(i)
FROM
(
    WITH ( SELECT  groupArray(j), groupArray(C) FROM WCR  WHERE ts = ( SELECT max(ts) FROM WCR) ) AS jC
    SELECT
        untuple(Y),
        i,
        arraySort((j, C) -> L2Distance(C, Y), jC.1, jC.2)[1] AS j
    FROM YH
)
GROUP BY j
;
/*
 для тех кто офигевает от синтаксиса КХ, поясняю:
 - untuple(Y) - развертывает тапл в простой список, как будто это отдельные столбцы.  Имена придумываются и содержат слово tupleElement
 - COLUMNS('tupleElement') - берет столбцы по regex
 - APPLY avg - применяет к ним аггрегатрую функцию
 - tuple() полученный список отдельных столбцов снова сворачивается в тупл

 arrayZip(groupArray(j), groupArray(C)) - против бага - https://github.com/ClickHouse/ClickHouse/issues/33156
 global cross join - чтобы работало на кластере Distributed
 */

create or replace function getWCR as (x) ->  (select arrayJoin(P) from (select P from WCR where j=x order by ts  desc limit 1));
-- results for drawing
select * from
(select Y.1 as x, Y.2 p1, null p2, null p3, null p4, null p5 from YH where i in getWCR(1)
union all
select Y.1 as x, null p1, Y.2 p2, null p3, null p4, null p5 from YH where i in getWCR(2)
union all
select Y.1 as x, null p1, null p2, Y.2 p3, null p4, null p5 from YH where i in getWCR(3)
union all
select Y.1 as x, null p1, null p2, null p3, Y.2 p4, null p5 from YH where i in getWCR(4)
union all
select Y.1 as x, null p1, null p2, null p3, null p4, Y.2 p5 from YH where i in getWCR(5))
order by x
;
