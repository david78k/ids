-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

--set tname_origin=enron100;
set tname=enron_ken;
set tname_origin=enron;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  sender STRING,
  recipient STRING,
  cc STRING,
  subject STRING,
  context STRING
  --count INT
  );

-- insert the records of ken lay
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELECT frome, trim(recipient), count(1) freq
SELECT frome, toe, cc, subject, context
FROM ${hiveconf:tname_origin}
WHERE frome LIKE '%ken%lay%'
--LATERAL VIEW explode(split(concat_ws(',', toe, cc), ',')) t AS recipient
--WHERE frome != trim(recipient)
--	AND
--	((frome LIKE '%enron.com%') OR (recipient LIKE '%enron.com%'))
--GROUP BY frome, trim(recipient)
--ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 5;

