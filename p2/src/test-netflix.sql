-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_similar_pairs;
set tname1=netflix_titles;
--set tname2=netflix_ratings10000;
--set tname2=netflix_ratings1000;
--set tname2=netflix_ratings100;
set tname2=netflix_ratings;

-- order by frequency
--INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELect t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim from
SELect t4.title, t5.title, t3.sim from
(SELect  
	t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim 
	from
	--${hiveconf:tname1} t1
	${hiveconf:tname2} t1
	JOIN (select * from ${hiveconf:tname2} limit 100) t2
	ON t1.cid = t2.cid
	WHERE t1.mid != t2.mid
	AND t1.mid < t2.mid
	
	GROUP BY t1.mid, t2.mid
	ORDER BY sim
	--LIMIT 100
) t3

JOIN ${hiveconf:tname1} t4
ON t3.mid1 = t4.mid 

JOIN ${hiveconf:tname1} t5
ON t3.mid2 = t5.mid 

--	) m2

LIMIT 10
;

