-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_ken_subjects;
set tname_origin=enron_ken;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  word STRING,
  count INT
  );

-- insert the records of ken lay
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT trim(word), count(1) freq
FROM ${hiveconf:tname_origin}
LATERAL VIEW explode(split(subject, ' +')) t AS word
GROUP BY trim(word)
ORDER BY freq DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

