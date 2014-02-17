drop table enron;

CREATE EXTERNAL TABLE enron (
  eid STRING,
  datetime STRING,
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

