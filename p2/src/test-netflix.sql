-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_similar_pairs;
set tname1=netflix_titles;
set tname2=netflix_ratings1000;
--set tname2=netflix_ratings100;
--set tname2=netflix_ratings;

-- order by frequency
--INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELect t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim from
SELect t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim from
	--${hiveconf:tname1} t1
	${hiveconf:tname2} t1
	JOIN ${hiveconf:tname2} t2
	ON t1.cid = t2.cid
	WHERE t1.mid != t2.mid
	AND t1.mid < t2.mid
	--(SELect title, t3.mid mid3, t4.cid cid4, rating from
--	${hiveconf:tname1} t3

	--JOIN ${hiveconf:tname2} t3
	--ON t3.mid = t1.mid

--	) m2
--ON m1.cid2 = m2.cid4
--AND m1.mid2 <> m2.mid3

GROUP BY t1.mid, t2.mid
ORDER BY sim DESC
LIMIT 10
;

