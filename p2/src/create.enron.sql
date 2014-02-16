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
STORED AS TEXTFILE
LOCATION '/root/hadoop/data/enron.100k';
--LOCATION '/root/hadoop/data/enron.100';

select count(*) from enron;

