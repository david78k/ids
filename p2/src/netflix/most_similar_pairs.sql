-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

--set hive.auto.convert.join=false

--set tname=netflix_similar_pairs;
--set tname=netflix_likeX_pairs;
set tname=netflix_similar_pairs_35_40m; 
--set tname=netflix_similar_pairs_5M_tail;
set tname1=netflix_ratings_35_40m; 
--set tname1=netflix_ratings5M_tail;
set tname2=netflix_ratings;
set tname3=netflix_titles;

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
SELect t3.title, t4.title, t5.sim, t5.freq 
FROM
(
        SELECT t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim, count(*) freq
	FROM 
        ${hiveconf:tname1} t1
	JOIN ${hiveconf:tname2} t2
	ON t1.cid = t2.cid
	WHERE t1.mid > t2.mid
        GROUP BY t1.mid, t2.mid
	HAVING freq >= 100
	ORDER BY sim 
	LIMIT 100
) t5

JOIN ${hiveconf:tname3} t3
ON t5.mid1 = t3.mid

JOIN ${hiveconf:tname3} t4
ON t5.mid2 = t4.mid

;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

--select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

