-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_similar_pairs;
set tname1=netflix_titles;
set tname2=netflix_ratings;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  year INT,
  rating FLOAT,
  count INT
  );

-- insert the data
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELECT year as [period], avg(rating), count(1) [freqs]
SELECT *
FROM 
${hiveconf:tname1} t1
--JOIN ${hiveconf:tname2} t2
--ON t1.mid = t2.mid
--GROUP BY (year/10)*10
--ORDER BY rating DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

