-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

--set tname_origin=enron100;
set tname=enron_ken_recipients;
set tname_origin=enron_ken;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  recipient STRING,
  count INT
  );

-- insert the records of ken lay
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT trim(recipient), count(1) freq
FROM ${hiveconf:tname_origin} t
LATERAL VIEW explode(split(concat_ws(',', t.recipient, cc), ',')) t AS recipient
--WHERE frome != trim(recipient)
--	AND
--	((frome LIKE '%enron.com%') OR (recipient LIKE '%enron.com%'))
GROUP BY trim(recipient)
ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

