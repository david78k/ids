drop table netflix100;

CREATE EXTERNAL TABLE netflix100 (
  eid STRING,
  timestamp STRING,
  frome STRING,
  toe STRING,
  cc STRING,
  subject STRING,
  context STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
--LOCATION '/root/ids/p2/data/netflix.100'; -- directory
--LOCATION '/user/root/data/netflix.100.refined.tab';
--LOCATION '/root/ids/p2/data/netflix.100k';

--select count(*) from netflix;

load data local inpath 'data/netflix.100.refined.tab' overwrite into table netflix100;
select * from netflix100 limit 5;
select count(*) from netflix100;
