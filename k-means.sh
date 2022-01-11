#!/bin/bash

ch() {
  clickhouse-client -c ~/.clickhouse-client/of.xml -q "$1" "${@:2}"
}

# init centroids
ch  "truncate table WCR"
ch  "insert into WCR select now(), 1, Y from YH limit $((1 + $RANDOM % 100)),1"
# add random-weighted centroids 
for j in {2..4}; do
ch  "insert into WCR select * from centroidsInit"
done
sleep 1

rezult='1'

while [ $rezult -ne 0 ] ; do

ch  "
INSERT INTO WCR SELECT
    now(),  j,
    tuple(COLUMNS('tupleElement') APPLY avg) AS C
FROM nearestCentroid
GROUP BY j
"

ch "select C from WCR order by ts desc limit 1 by j"
echo .

rezult=`ch "select round(d) from deltaFinish"`

done

# results for drawing in google sheets
ch "
with tuple(COLUMNS('tupleElement')) as a
select a.1 as x,
       if(j=1,a.2,null) as p1,
       if(j=2,a.2,null) as p2,
       if(j=3,a.2,null) as p3,
       if(j=4,a.2,null) as p4,
       if(j=5,a.2,null) as p5
from nearestCentroid
" -f TSVWithNames  --format_tsv_null_representation ' ' | pbcopy
