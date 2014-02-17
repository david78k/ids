set tname=enron_most_sent;

-- create table for the most sent people
drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  frome STRING,
  count INT
  );

-- insert the most sent people
INSERT OVERWRITE TABLE ${hiveconf:tname}
SELECT frome, count(*) as countf
--FROM enron100
FROM enron
GROUP BY frome
ORDER BY countf DESC;

-- hive -e 'select frome, count(*) as countf from enron100 group by frome order by countf desc'

-- insert into a local file
INSERT OVERWRITE LOCAL DIRECTORY 'results/${hiveconf:tname}'
SELECT * 
FROM ${hiveconf:tname};

--select frome, count(*) as countf from enron group by frome sort by countf;

select * from ${hiveconf:tname} limit 5;

select count(*) from ${hiveconf:tname};

--select * from enron where frome = 'steven.kean@enron.com'
