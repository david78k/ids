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

load data local inpath 'data/movie_titles.csv' overwrite into table ${hiveconf:tname};
select count(*) from ${hiveconf:tname};
select * from ${hiveconf:tname} limit 5;

