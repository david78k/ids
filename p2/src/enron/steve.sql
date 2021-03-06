-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_steve;
set tname_origin=enron;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  sender STRING,
  recipient STRING,
  cc STRING,
  subject STRING,
  context STRING
  );

-- insert the records of steve kean
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT frome, toe, cc, subject, context
FROM ${hiveconf:tname_origin}
WHERE frome LIKE '%steve%kean%'
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 5;

