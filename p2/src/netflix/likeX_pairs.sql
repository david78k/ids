-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_likeX_pairs;
set tname1=netflix_titles;
set tname2=netflix_ratings;
set numrows = 100000; -- number of rows of the left large table

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  --title1 STRING,
  --title2 STRING,
  mid1 INT,
  mid2 INT,
  rating1 INT,
  rating2 INT--,
  --count INT
  );

-- insert the data with ratings > 3
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELect t4.title, t5.title, count(t1.rating > 3 AND t2.rating > 3), t3.freq from
--SELect t4.title, t5.title, count(t1.rating > 3 AND t2.rating > 3), t3.freq from
--SELECT t3.mid1, t3.mid2, count(*) freq
--FROM (
	SELect
        --t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim, count(*) freq
        --t1.mid mid1, t2.mid mid2, count(t1.rating > 3 AND t2.rating > 3)/count(*) sim, count(*) freq
        --t1.mid mid1, t2.mid mid2, count(t1.rating > 3 AND t2.rating > 3) sim, count(t1.rating >= 0) freq
        t1.mid mid1, t2.mid mid2, t1.rating r1, t2.rating r2
	FROM 
        --(select * from ${hiveconf:tname2} limit 1000000) t1 -- too long
        --(select * from ${hiveconf:tname2} limit 600000) t1 -- 20m = 1273.538 seconds
        --(select * from ${hiveconf:tname2} limit 500000) t1 -- 20m = 1273.538 seconds
        --(select * from ${hiveconf:tname2} limit 400000) t1 -- 12m
        --(select * from ${hiveconf:tname2} limit 300000) t1 -- 12m
        --(select * from ${hiveconf:tname2} limit 100000) t1 -- 10m
        (select * from ${hiveconf:tname2} limit ${hiveconf:numrows}) t1
        --(select * from ${hiveconf:tname2} limit 1000) t1
        JOIN ${hiveconf:tname2} t2
        ON t1.cid = t2.cid
        WHERE t1.mid > t2.mid
	AND t1.rating > 3 
	--AND t2.rating > 3
--) t3
--	JOIN
--	SELECT t3.mid1, t3.mid2, count(t1.rating > 3 AND t2.rating > 3)
--        GROUP BY t3.mid1, t3.mid2
	--HAVING freq >= 100
        --ORDER BY sim DESC

	--LIMIT 100
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname}
LIMIT 10000;

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

