-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_most_received;
--set tname_origin=enron100;
set tname_origin=enron;

-- create table for the most received people
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  recipient STRING,
  count INT
  );

-- insert the most received people
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT recipient, count(recipient) as freq
FROM (
	SELECT TRIM(recip) recipient
	FROM ${hiveconf:tname_origin}
	LATERAL VIEW EXPLODE(SPLIT(concat(toe, cc), ',')) t AS recip
	--LATERAL VIEW EXPLODE(SPLIT(concat(toe, regexp_replace(cc, '\n', '')), ',')) t AS recip
) table2
GROUP BY recipient
ORDER BY freq DESC;

-- hive -e 'select frome, count(*) as countf from enron100 group by frome order by countf desc'

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * 
FROM ${hiveconf:tname};

--select frome, count(*) as countf from enron group by frome sort by countf;

select count(*) from ${hiveconf:tname};

select * from ${hiveconf:tname} limit 10;

--select * from enron where frome = 'steven.kean@enron.com'

-- rename column name
--ALTER TABLE enron CHANGE datetime timestamp String;

