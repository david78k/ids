-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_similar_mid_pairs;
set tname1=netflix_titles;
--set tname2=netflix_likeX_pairs;
--set tname2=netflix_ratings10000;
--set tname2=netflix_ratings100000;
set tname2=netflix_ratings;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  --title1 STRING,
  --title2 STRING,
  mid1 INT,
  mid2 INT,
  similarity FLOAT,
--  likeXY FLOAT,
  total INT
  );

-- insert the data
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELECT t3.title, t4.title, t5.sim, t5.freq 
--FROM(
--SELECT t5.mid1, t5.mid2, avg(abs(t5.r1 - t5.r2)) sim, count(*) freq
--SELECT t5.mid1, t5.mid2, avg(diff) sim, count(*) freq
--FROM (
--SELECT t1.title, t2.title, avg(abs(t1.rating - t2.rating)) sim, count(*) total
--FROM --(
        --SELECT t1.mid1 mid1, t1.mid2 mid2, count (*) total
        --SELECT t1.mid mid1, t2.mid mid2, t1.rating r1, t2.rating r2
        SELECT t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim, count(*) freq
	FROM 
        ${hiveconf:tname2} t1
	JOIN ${hiveconf:tname2} t2
	ON t1.cid = t2.cid
	WHERE t1.mid > t2.mid
        GROUP BY t1.mid, t2.mid
	HAVING freq >= 100
	--HAVING count(*) >= 100
	ORDER BY sim 
	LIMIT 100
--) t5

--JOIN ${hiveconf:tname1} t3
--ON t5.mid1 = t3.mid

--JOIN ${hiveconf:tname1} t4
--ON t5.mid2 = t4.mid

;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

--select count(*) from ${hiveconf:tname};

--select * from ${hiveconf:tname} limit 10;

