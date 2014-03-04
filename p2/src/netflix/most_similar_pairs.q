--https://s3.amazonaws.com/spring-2014-ds/movie_dataset/movie_titles/movie_titles.csv
CREATE EXTERNAL TABLE netflix_titles (
  mid INT,
  year INT,
  title STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://spring-2014-ds/movie_dataset/movie_titles/'
;

CREATE EXTERNAL TABLE netflix_ratings_30_40m (
  mid INT,
  cid INT,
  rating INT,
  datetime STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${INPUT}'
;
--LOCATION 's3://david78k-ids/input/netflix_ratings/'

CREATE EXTERNAL TABLE netflix_ratings (
  mid INT,
  cid INT,
  rating INT,
  datetime STRING
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://spring-2014-ds/movie_dataset/movie_ratings/'
;

--load data local inpath '${hiveconf:path}' overwrite into table ${hiveconf:tname};
--select count(*) from ${hiveconf:tname};
--select * from ${hiveconf:tname} limit 10;

--set tname=netflix_similar_pairs;
--set tname=netflix_similar_pairs_60_70m; -- failed
--set tname1=netflix_ratings_60_70m; -- failed
--set tname2=netflix_ratings;
--set tname3=netflix_titles;

-- create table
CREATE EXTERNAL TABLE netflix_similar_pairs (
  title1 STRING,
  title2 STRING,
  similarity FLOAT,
  total INT
  )
STORED AS SEQUENCEFILE
LOCATION '${OUTPUT}/netflix_similar_pairs/'
;

-- insert the data
-- order by frequency
INSERT OVERWRITE TABLE netflix_similar_pairs
SELect t3.title, t4.title, t5.sim, t5.freq
FROM
(
        SELECT t1.mid mid1, t2.mid mid2, avg(abs(t1.rating - t2.rating)) sim, count(*) freq
        FROM
        netflix_ratings_30_40m t1
        JOIN netflix_ratings t2
        ON t1.cid = t2.cid
        WHERE t1.mid > t2.mid
        GROUP BY t1.mid, t2.mid
        HAVING freq >= 100
        ORDER BY sim
        LIMIT 100
) t5

JOIN ${hiveconf:tname3} t3
ON t5.mid1 = t3.mid

JOIN ${hiveconf:tname3} t4
ON t5.mid2 = t4.mid
;

--select count(*) from ${hiveconf:tname};

--select * from ${hiveconf:tname} limit 10;

