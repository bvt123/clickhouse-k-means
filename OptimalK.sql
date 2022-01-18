-- https://medium.com/analytics-vidhya/how-to-determine-the-optimal-k-for-k-means-708505d204eb

-- Elbow Method
WITH ( SELECT  groupArray(j), groupArray(C) FROM (select j,C from WCR  order by ts desc limit 1 by j) ) AS jC  -- for small datasets when 1 epoch takes less 1 sec
,  arraySort(x->x.2,arrayMap(j,C->(j,L2Distance(C, Y)), jC.1, jC.2))[1] as pd
select sum(pd.2)
FROM YH
;

--Silhouette Method
WITH ( SELECT  groupArray(j), groupArray(C) FROM (select j,C from WCR  order by ts desc limit 1 by j) ) AS jC  -- for small datasets when 1 epoch takes less 1 sec
    , arraySort((j, C) -> L2Distance(C, a.Y), jC.1, jC.2)[1] AS ja
    , arraySort((j, C) -> L2Distance(C, b.Y), jC.1, jC.2)[1] AS jb
    , L2Distance(a.Y, b.Y) as distance
select
    avgIf(distance, ja = jb) as ai,
    avgIf(distance, ja != jb) as bi,
    (bi-ai)/if(bi>ai,bi,ai) as si
FROM YH a, YH b
where a.i != b.i
;
