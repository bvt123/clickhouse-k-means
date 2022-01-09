#!/bin/bash

ch() {
  clickhouse-client -c ~/.clickhouse-client/of.xml -q "$1"
}

# init centroids
ch  "truncate table WCR"
ch  "insert into WCR select now(), 1, Y, [] from YH limit $((1 + $RANDOM % 100)),1"
# add random-weighted centroids 
for j in {2..3}; do
ch  "
insert into WCR
select now(), $j as j, y, []
from ( select y,
         sum(d) over () as total,
         sum(d) over (rows between unbounded preceding and current row ) as cum
         from (  select argMin(Y,L2Distance(Y,C) as dx2) as y, min(dx2) as d
                 from YH, (select * from WCR order by ts desc limit 1 by j) as WCR
                 where Y not in (select C from WCR)
                 group by Y)) as t1,
     ( select rand32()/4294967295 as r ) as t2
where total*r < cum
order by cum
limit 1
"
done

while true 
do
 
ch  "
insert into WCR
select now(), j,(avg(Y.1),avg(Y.2)) as C, groupArray(Y)
from (select i, argMin(j,L2Distance(Y,C)) as j, Y
    from YH, (select * from WCR order by ts desc limit 1 by j) as WCR
    group by i,Y )
group by j order by j
"

ch "select C from WCR order by ts desc limit 1 by j"
echo .
done
