drop table enron100;

CREATE EXTERNAL TABLE enron100 (
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
--LOCATION '/root/ids/p2/data/enron.100'; -- directory
--LOCATION '/user/root/data/enron.100.refined.tab';
--LOCATION '/root/ids/p2/data/enron.100k';

--select count(*) from enron;

load data local inpath 'data/enron.100.refined.tab' overwrite into table enron100;
select * from enron100 limit 5;
select count(*) from enron100;
