-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname_origin=enron;
set tname=enron_most_received;

-- create table for the most received people
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  frome STRING,
  count INT
  );

-- insert the most sent people
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT frome, count(*) as countf
--FROM enron100
FROM ${hiveconf:tname_origin}
GROUP BY frome
ORDER BY countf DESC;

-- hive -e 'select frome, count(*) as countf from enron100 group by frome order by countf desc'

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
SELECT * 
FROM ${hiveconf:tname};

--select frome, count(*) as countf from enron group by frome sort by countf;

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 5;

--select * from enron where frome = 'steven.kean@enron.com'

-- rename column name
--ALTER TABLE enron CHANGE datetime timestamp String;
