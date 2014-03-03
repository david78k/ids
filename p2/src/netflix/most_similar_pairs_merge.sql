-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_similar_pairs;
--set tname=netflix_similar_pairs_30_40m; -- failed
--set tname=netflix_similar_pairs_5M_tail;
set tname1=netflix_ratings_0_10m;
set tname2=netflix_ratings_10_20m;
set tname3=netflix_ratings_20_30m;
set tname4=netflix_ratings_30_40m; -- failed
set tname5=netflix_ratings_40_50m;
set tname7=netflix_ratings_60_70m;
set tname8=netflix_ratings_70_80m;
set tname9=netflix_ratings_80_90m;
set tname10=netflix_ratings_90_95m;
set tname11=netflix_ratings5M_tail;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  title1 STRING,
  title2 STRING,
  similarity FLOAT,
  total INT
  );

-- insert the data
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELect *
FROM (
	SELECT * FROM ${hiveconf:tname1} t1
	UNION ALL 
	SELECT * FROM ${hiveconf:tname2} t2
	UNION ALL 
	SELECT * FROM ${hiveconf:tname3} t3
--	UNION ALL 
--	SELECT * FROM ${hiveconf:tname4} t4
	UNION ALL 
	SELECT * FROM ${hiveconf:tname5} t5
	UNION ALL 
	SELECT * FROM ${hiveconf:tname6} t6
	UNION ALL 
	SELECT * FROM ${hiveconf:tname7} t7
--	UNION ALL 
--	SELECT * FROM ${hiveconf:tname8} t8
--	UNION ALL 
--	SELECT * FROM ${hiveconf:tname9} t9
--	UNION ALL 
--	SELECT * FROM ${hiveconf:tname10} t10
	UNION ALL 
	SELECT * FROM ${hiveconf:tname11} t11
) u
ORDER BY u.similarity DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

--select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

