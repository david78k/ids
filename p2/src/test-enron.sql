--set tablename=enron;
set tablename=enron100;
set tname_origin=enron100;
--set tname_origin=enron;
set newtable=enron_most_received;

--select frome, collect_list(toe) from ${hiveconf:tname_origin}
--select frome, count(toe) from ${hiveconf:tname_origin}
--select frome, size(collect_set(recipients))
--select frome, collect_set(recipients)
select frome, collect_set(recip)
--select frome, count(recip) freq
--select frome, size(collect_set(recip))
from(
	--select frome, recip
	--from (
	select frome, TRIM(recipient) recip
	--select frome, count(1) freq
	from ${hiveconf:tname_origin}
	LATERAL VIEW explode(split(toe, ',')) t as recipient 
	where (((NOT (frome like '%@enron.com%')) AND (trim(recipient) like '%@enron.com%'))
		OR
		((frome like '%@enron.com%') AND (NOT (trim(recipient) like '%@enron.com%'))))
--	) t1
	--group by frome, recip
) t2
group by frome
--order by freq desc
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
