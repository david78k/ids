set tname = netflix_titles;

drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  mid INT,
  year INT,
  title STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
--LOCATION '/root/ids/p2/data/netflix.100'; -- directory
--LOCATION '/user/root/data/netflix.100.refined.tab';
--LOCATION '/root/ids/p2/data/netflix.100k';

--select count(*) from netflix;

--load data local inpath 'data/netflix.refined.csv' overwrite into table ${hiveconf:tname};
load data local inpath 'data/movie_titles.csv' overwrite into table ${hiveconf:tname};
select count(*) from ${hiveconf:tname};
select * from ${hiveconf:tname} limit 5;

