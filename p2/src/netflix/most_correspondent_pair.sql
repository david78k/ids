-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=netflix_corresp_pair;
--set tname_origin=netflix100;
set tname_origin=netflix;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  sender STRING,
  recipient STRING,
  count INT
  );

-- insert the people-pair with the number of correspondents
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT frome, trim(recipient), count(1) freq
FROM ${hiveconf:tname_origin}
LATERAL VIEW explode(split(concat_ws(',', toe, cc), ',')) t AS recipient
WHERE frome != trim(recipient)
	AND
	((frome LIKE '%netflix.com%') OR (recipient LIKE '%netflix.com%'))
GROUP BY frome, trim(recipient)
ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

