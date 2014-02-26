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
  title1 STRING,
  title2 STRING,
  --mid1 INT,
  --mid2 INT,
  similarity FLOAT,
  likeXY FLOAT,
  total INT
  );

-- insert the data such that both of a movie pair (x, y)'s ratings > 3.
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELect t4.title, t5.title, t3.sim, t3.freq from
--SELect t4.title, t5.title, count(t1.rating > 3 AND t2.rating > 3), t3.freq from
SELECT t6.title, t7.title, likeXY/total sim, likeXY, total
FROM (
	SELECT t2.mid1, t2.mid2, total, likeXY 
	FROM
	(SELect
        	t1.mid1 mid1, t1.mid2 mid2, count (*) total
		FROM 
        	${hiveconf:tname2} t1
        	GROUP BY t1.mid1, t1.mid2
		HAVING total >= 100
	) t2
	JOIN
	--(SELECT t3.mid1, t3.mid2, count(t3.rating2 > 3) likeXY
	(SELECT t3.mid1, t3.mid2, count(*) likeXY
		FROM ${hiveconf:tname2} t3
		WHERE t3.rating2 > 3
        	GROUP BY t3.mid1, t3.mid2
		--HAVING likeXY >= 100
	) t4
	ON t2.mid1 = t4.mid1 AND t2.mid2 = t4.mid2
        --GROUP BY t3.mid1, t3.mid2
	--HAVING total >= 100
) t5

JOIN ${hiveconf:tname1} t6
ON t5.mid1 = t6.mid

JOIN ${hiveconf:tname1} t7
ON t5.mid2 = t7.mid

ORDER BY sim DESC

LIMIT 100
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

--select count(*) from ${hiveconf:tname};

--select * from ${hiveconf:tname} limit 10;

