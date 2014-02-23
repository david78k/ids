-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_similar_pairs;
set tname1=netflix_titles;
set tname2=netflix_likeX_pairs;
--set tname2=netflix_ratings;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  --title1 STRING,
  --title2 STRING,
  mid1 INT,
  mid2 INT,
 -- similarity FLOAT,
--  r2 FLOAT--,
  count INT
  );

-- insert the data
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELect t4.title, t5.title, t3.sim, t3.freq from
--SELect t4.title, t5.title, count(t1.rating > 3 AND t2.rating > 3), t3.freq from
--SELect t4.title, t5.title, count(t1.rating > 3 AND t2.rating > 3), t3.freq from
SELECT t3.mid1, t3.mid2, likeXY/total sim
FROM (
	SELect
        --t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim, count(*) freq
        --t1.mid mid1, t2.mid mid2, count(t1.rating > 3 AND t2.rating > 3) sim, count(t1.rating >= 0) freq
        --t1.mid mid1, t2.mid mid2, t1.rating r1, t2.rating r2
        t1.mid1 mid1, t1.mid2 mid2, count (*) total
	FROM 
        ${hiveconf:tname2} t1
        --(select * from ${hiveconf:tname2} limit 1000) t1
        --JOIN
	-- ${hiveconf:tname2} t2
        --ON t1.cid = t2.cid
        WHERE --t1.mid > t2.mid
	--AND t1.rating > 3 
	t2.rating > 3
	) t3
	JOIN
	(SELECT t4.mid1, t4.mid2 FROM ${hiveconf:tname2} t4
	) t5
        --GROUP BY t3.mid1, t3.mid2
	--HAVING freq >= 100
        --ORDER BY sim DESC

	--LIMIT 100
--) t4

--JOIN ${hiveconf:tname1} t4
--ON t3.mid1 = t4.mid

--JOIN ${hiveconf:tname1} t5
--ON t3.mid2 = t5.mid
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

