--set path = data/movie_ratings.0M_10M.csv;
set path = data/movie_ratings.35_40m.csv;
--set tname = netflix_ratings_0M_10M;
set tname = netflix_ratings_35_40m;

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
--select count(*) from ${hiveconf:tname};
select * from ${hiveconf:tname} limit 10;

