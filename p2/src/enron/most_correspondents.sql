-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

--set tname_origin=enron;
set tname=enron_most_corresp;
set tname_origin=enron100;
--set tname_origin=enron;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  toe STRING,
  count INT
  );

-- insert the people with the number of correspondents
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT t3.frome3, count(DISTINCT t3.toe2) as freq
FROM (
	SELECT t2.frome frome3, t1.toe1 as toe2
	FROM (SELECT TRIM(t.name) toe1 
		FROM (SELECT EXPLODE(SPLIT(${hiveconf:tname_origin}.toe, ',')) name
			FROM ${hiveconf:tname_origin} 
		      ) t 
	      ) t1
	JOIN 
		(SELECT t0.frome, t0.toe toe0 from ${hiveconf:tname_origin} t0) t2
	--ON (t1.name like t2.toe0)
	WHERE ( find_in_set(t1.toe1, t2.toe0) > 0) 
) t3
WHERE (t3.frome3 LIKE '%@enron.com')
GROUP BY t3.frome3
ORDER BY freq DESC
;

-- hive -e 'select frome, count(*) as countf from enron100 group by frome order by countf desc'

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

--select frome, count(*) as countf from enron group by frome sort by countf;

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

