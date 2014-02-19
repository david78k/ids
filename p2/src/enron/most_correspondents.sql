-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_most_corresp;
set tname_origin=enron;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  sender STRING,
  count INT
  );

-- insert the people with the number of correspondents
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT frome, size(collect_set(recip)) freq
FROM (
	SELECT frome, trim(recipient) recip
	FROM ${hiveconf:tname_origin}
	LATERAL VIEW explode(split(concat_ws(',', toe, cc), ',')) t AS recipient
	WHERE (frome LIKE '%enron.com%') OR (recipient LIKE '%enron.com%')
) t1
WHERE (frome LIKE '%enron.com%')
GROUP BY frome
ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

