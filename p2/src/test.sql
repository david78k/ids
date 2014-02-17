--set tablename=enron;
set tablename=enron100;
set newtable=enron_most_received;

INSERT 
select nametr, count(nametr) freq
from (
	select trim(name) nametr 
	from (
		SELECT
		  explode(SPLIT(${hiveconf:tablename}.toe, ',')) name
--  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(${hiveconf:tablename}.toe, ',', numbers.n), ',', -1)) name
--	t1.toe, t2.toe
		FROM
		${hiveconf:tablename} 
	--${hiveconf:tablename} t1 JOIN ${hiveconf:tablename} t2
--  numbers 
    --INNER 
--    JOIN ${hiveconf:tablename}
--  ON CHAR_LENGTH(${hiveconf:tablename}.toe)
--     -CHAR_LENGTH(REPLACE(${hiveconf:tablename}.toe, ',', ''))>=numbers.n-1
--ORDER BY
--  eid, n
	) ${hiveconf:newtable}
) table2 
group by nametr
order by freq desc
;
