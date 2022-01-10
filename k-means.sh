#!/bin/bash

ch() {
  clickhouse-client -c ~/.clickhouse-client/of.xml -q "$1"
}

# init centroids
ch  "truncate table WCR"
ch  "insert into WCR select now(), 1, Y, [] from YH limit $((1 + $RANDOM % 100)),1"
# add random-weighted centroids 
for j in {2..4}; do
ch  "
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
limit 1
"
done

# press ^C when finish
while true 
do
 
ch  "
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
"

ch "select C from WCR order by ts desc limit 1 by j"
echo .
done
