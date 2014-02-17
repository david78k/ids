-- for email received people
drop table numbers;

CREATE TABLE numbers (
  n INT);

load data local inpath 'data/numbers' overwrite into table numbers;

select * from numbers;

--INSERT INTO TABLE numbers VALUES
--(1),
--(2),
--(3)
--;
