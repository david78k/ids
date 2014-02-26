-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_most_watched;
set tname1=netflix_titles;
set tname2=netflix_ratings;
--set tname2=netflix_ratings100;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  mid INT,
  title STRING,
  year STRING,
  rating FLOAT,
  count INT
  );

-- insert the most watched movies
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT t1.mid, title, year, avg(rating), count(1) freq
FROM ${hiveconf:tname1} t1
JOIN ${hiveconf:tname2} t2
ON t1.mid = t2.mid
--LATERAL VIEW explode(split(concat_ws(',', toe, cc), ',')) t AS recipient
--WHERE frome != trim(recipient)
--	AND
--	((frome LIKE '%netflix.com%') OR (recipient LIKE '%netflix.com%'))
GROUP BY t1.mid, title, year
ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;
