-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_outside;
--set tname_origin=enron100;
set tname_origin=enron;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  outsider STRING,
  count INT
  );

-- insert the people outside of enron with the number of correspondents
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELECT frome, size(collect_set(recip)) freq
SELECT frome, collect_set(recip)
FROM (
	SELECT frome, trim(recipient) recip
	FROM ${hiveconf:tname_origin}
	LATERAL VIEW explode(split(toe, ',')) t AS recipient
--	WHERE NOT (frome LIKE '%@enron.com')
	--WHERE NOT (frome LIKE '%@enron.com' and trim(recipient) LIKE '%@enron.com')
) t1
--WHERE NOT (recip LIKE '%@enron.com')
GROUP BY frome
--ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

