set path = data/movie_ratings.10000.csv;
set tname = netflix_ratings10000;

drop table ${hiveconf:tname};

CREATE EXTERNAL TABLE ${hiveconf:tname} (
  mid INT,
  cid INT,
  rating INT,
  datetime STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

load data local inpath '${hiveconf:path}' overwrite into table ${hiveconf:tname};
select count(*) from ${hiveconf:tname};
select * from ${hiveconf:tname} limit 5;

