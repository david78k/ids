-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_outside_received;
--set tname_origin=enron100;
set tname_origin=enron_most_received;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  recipient STRING,
  count INT
  );

-- insert the people outside of enron with the number of correspondents
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT recipient, count
FROM ${hiveconf:tname_origin}
WHERE (NOT (recipient like '%enron.com%')) 
--WHERE ((NOT (frome like '%@enron.com%')) AND (recipient like '%@enron.com%'))
ORDER BY count DESC
;

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

