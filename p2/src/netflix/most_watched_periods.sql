-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_most_watched_periods;
set tname1=netflix_titles;
set tname2=netflix_ratings;
--set tname2=netflix_ratings100;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  period INT,
  rating FLOAT,
  count INT
  );

-- insert the data
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELECT year as [period], avg(rating), count(1) [freqs]
--SELECT y1*10 y, sum(ratings)/sum(freq) r, sum(freq)
--FROM (	
	--SELECT floor(year/10) y1, sum(rating) ratings, count(1) freq
	SELECT 10*floor(year/10) y1, avg(rating) ratings, count(1) freq
	FROM 
	${hiveconf:tname1} t1
	JOIN ${hiveconf:tname2} t2
	ON t1.mid = t2.mid
	--GROUP BY y1
	GROUP BY floor(year/10)
	ORDER BY freq DESC
--) t3
--GROUP BY y1
--ORDER BY r DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

