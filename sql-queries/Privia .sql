DROP TABLE IF EXISTS sdey_privia_utilization_data_202001031;
Create table mktg_dev.sdey_privia_utilization_data_202001031 AS 
SELECT
	privia.medname,
	dmh.medid,
	dmh.gcn_seqno,
	privia.count_total,
	dmh.medid is not null as found_in_fdb,
	active_price.mid is not null as found_in_price,
	am.medid is not null as found_on_site
FROM
	mktg_dev.sdey_privia_data_temp_data privia
	LEFT OUTER JOIN dwh.dim_medid_hierarchy dmh ON lower(privia.medname) = lower(dmh.med_medid_desc)
	LEFT OUTER JOIN ( SELECT DISTINCT
			(medid) AS mid
		FROM
			transactional.med_price
		WHERE
			ended_on IS NULL) active_price ON active_price.mid = dmh.medid
	LEFT OUTER JOIN transactional.available_med am ON am.medid = dmh.medid
;
GRANT SELECT ON mktg_dev.sdey_privia_utilization_data_202001031 TO "public";
GRANT SELECT ON mktg_dev.sdey_privia_data_temp_data TO "public";


select gcn,pac_low_unit from dwh.dim_gcn_seqno_hierarchy where pac_low_unit is not null;

-- select * from mktg_dev.sdey_privia_utilization_data_202001031 

select gcn,count_total
 from mktg_dev.sdey_privia_utilization_data_202001031 inner join dwh.dim_medid_hierarchy
on sdey_privia_utilization_data_202001031.medid = dim_medid_hierarchy.medid;


select gcn,min(branded) from transactional.med_price group by 1 ;


with ddd as (
SELECT
	mp.medid,
	max(mp.branded) AS branded
FROM
	mktg_dev.sdey_privia_utilization_data_202001031 privia
	INNER JOIN transactional.med_price mp ON mp.medid = privia.medid
WHERE
	found_in_fdb
	AND found_in_price
	AND NOT found_on_site
GROUP BY
	1) select branded,count(*) from ddd group by 1;




select  	count(distinct(medname)),
	count(*)
 from mktg_dev.sdey_privia_data_temp_data


SELECT
	pharmacy_network_id,
	count(DISTINCT(med_price.medid)) count_of_common_drugs,
	sum("count") as sum_of_matched_fills
 FROM
	mktg_dev.sdey_privia_utilization_data
INNER JOIN
transactional.med_price
ON
	sdey_privia_utilization_data.medid = med_price.medid
WHERE
	ended_on is null
group by 1


select
	med_desc,medid,gcn_seqno
 from
 	mktg_dev.sdey_privia_temp_key_table
 left outer JOIN
 	dwh.dim_medid_hierarchy
 ON
 	lower(sdey_privia_temp_key_table.med_desc) = replace(lower(dim_medid_hierarchy.med_medid_desc),',',':')






SELECT
	*
FROM
	dwh.dim_medid_hierarchy
WHERE
	lower(med_medid_desc) like lower('%Fluconazole%') and med_dosage_form_abbr='tab'


select * from mktg_dev.sdey_privia_utilization_data left JOIN transactional.med on sdey_privia_utilization_data.medid = med.medid;




