--set tablename=enron;
set tablename=enron100;
set tname_origin=enron100;
set newtable=enron_most_received;

--select frome, collect_list(toe) from ${hiveconf:tname_origin}
--select frome, count(toe) from ${hiveconf:tname_origin}
--select frome, size(collect_set(recipients))
select frome, collect_set(recipients)
from (
select frome, split(toe, ',') recipients from ${hiveconf:tname_origin}
where frome like '%@enron.com'
) t
group by frome
;

/*
select t2.frome, t1.name 
from
(
SELECT TRIM(name) name
FROM (
	SELECT EXPLODE(SPLIT(${hiveconf:tname_origin}.toe, ',')) name
	FROM ${hiveconf:tname_origin}
) t
) t1 

	JOIN
	(SELECT t0.frome, t0.toe toe0 from ${hiveconf:tname_origin} t0) t2
	where t2.toe0 RLIKE t1.name
	--ON (t1.name like t2.toe0)
--	WHERE ( find_in_set(t1.toe1, t2.toe0) > 0)
;

/*
SELECT t2.frome frome3, t1.toe1 as toe2
FROM (SELECT TRIM(t.name) toe1
	FROM (SELECT EXPLODE(SPLIT(${hiveconf:tname_origin}.toe, ',')) name
		FROM ${hiveconf:tname_origin}
		) t
	) t1
	JOIN
	(SELECT t0.frome, t0.toe toe0 from ${hiveconf:tname_origin} t0) t2
	--ON (t1.name like t2.toe0)
	WHERE ( find_in_set(t1.toe1, t2.toe0) > 0)
;
*/
