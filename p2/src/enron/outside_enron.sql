-- create a table from the given table 
-- and insert query results into a new table and a local file
-- and display the results and the count

set tname=enron_outside;
set tname1=enron_outside_sent;
set tname2=enron_outside_received;

-- create table 
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  outsider STRING,
  count INT
  );

-- insert the people outside of enron with the number of correspondents
-- order by frequency
INSERT OVERWRITE TABLE ${hiveconf:tname}
--SELECT frome, collect_set(recip)
SELECT frome, size(collect_set(recip)) freq
FROM (
	SELECT frome, trim(recipient) recip
	FROM ${hiveconf:tname_origin}
	WHERE (((NOT (frome like '%enron.com%')) AND (recipient like '%enron.com%'))
                OR
                ((frome like '%enron.com%') AND (NOT (recipient like '%enron.com%'))))
) t1
WHERE t1.sender = t2.recipient
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

