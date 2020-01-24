select count(*) from dwh.pharmacy_network_ledger where termination_date is null ;



with data as (SELECT
	action_timestamp::date,
	sum(iif_(action_type='approval',1,0)) as approval,
	sum(iif_(action_type='reversal',1,0)) as reversal,
	sum(iif_(action_type='denied',1,0)) as denied,
FROM
	fifo.magic_fact_order_claim
WHERE
	action_sequence = 1
	AND action_timestamp::date + 30 > CURRENT_DATE
GROUP BY
	1
ORDER BY
	1 DESC;


CREATE FUNCTION iif_(BOOLEAN, float, float) RETURNS float 
stable 
as $$ 
	SELECT CASE $1 WHEN TRUE THEN $2 ELSE $3 END
$$ language sql;

$body$
DECLARE
	rtVal DOUBLE PRECISION;
BEGIN
	rtVal := (SELECT CASE $1 WHEN TRUE THEN $2 ELSE $3 END);
	RETURN rtVal;
END;
$body$
LANGUAGE 'plpgsql' IMMUTABLE CALLED ON NULL INPUT SECURITY INVOKER;




-- select * from pg_tables where tablename like '%top_300_drugs%'
-- drop table mktg_dev.top_300_drugs_20191121

GRANT SELECT ON mktg_dev.sdey_privia_utilization_data TO "public";

SELECT
	drg_ndc.ndc,
	drg_ndc.branded_medid,
	drg_ndc.manufacturer,
	drg_ndc.label_name,
	drg_ndc.package_size,
	drg_ndc.obsolete_date,
	awp.unit_price AS awp_unit_price,
	awp.package_price AS awp_package_price,
	wac.unit_price AS wac_unit_price,
	wac.package_price AS wac_package_price
FROM
	dwh.dim_ndc_hierarchy drg_ndc
	INNER JOIN transactional.available_med am ON am.medid = drg_ndc.medid
	LEFT JOIN medispan.mf2prc awp ON drg_ndc.ndc::bigint = awp.ndc_upc_hri::bigint
		AND awp.price_code = 'A'
	LEFT JOIN medispan.mf2prc wac ON drg_ndc.ndc::bigint = wac.ndc_upc_hri::bigint
		AND wac.price_code = 'W'
WHERE
	drg_ndc.obsolete_date IS NULL AND awp_unit_price is not null
ORDER BY
	1 ASC; 



select * from dwh.dim_medid_hierarchy 
join transactional.available_med
on available_med.medid = dim_medid_hierarchy.medid
where med_name_slug ='spiriva-respimat'



select count(DISTINCT medid) from transactional.available_med;

select pharmacy_network_id, branded, count( DISTINCT medid ) from transactional.med_price where ended_on is null and pharmacy_network_id <= 4  group by 1,2


select * from medispan.mf2ndc;

SELECT
	*
FROM
	transactional.coupon_template
WHERE
	deleted = 0
	AND ( expires_on is not null or expires_on > CURRENT_DATE)
ORDER BY
	created_on DESC
LIMIT 100;


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

SELECT
	DISTINCT
	med.medid,
	available_med.type_description,
	dim_medid_hierarchy.med_name_slug
-- 	med.description,
-- 	uu.form_name as goodrx_form_name,
-- 	med.billing_unit,
-- 	uu.default_quantity as goodrx_default_quantity,
-- 	uu.dosage as goodrx_dosage,
-- -- 	uu.drug_id as goodrx_drug_id,
-- -- 	uu.slug as goodrx_slug,
-- -- 	mdim.mapping_source,
-- 	med.dosage_form,
-- 	med.strength,
-- 	med.strength_uom,
-- 	med.gcn,
-- 	med.gcn_seqno,
-- 	dim_gcn_seqno_hierarchy.gcn_symphony_2017_rank,
-- 	case when rp2.medid is null then False else True end as with_modified_retail_package
-- 	med_package.package_description,
-- 	med_package.package_size,
FROM
	mktg_dev.units_of_use_raw_data uu
	INNER JOIN api_scraper.medid_drug_id_mapping mdim ON uu.drug_id = mdim.drug_id
	INNER JOIN transactional.med ON med.medid = mdim.medid
	INNER JOIN transactional.available_med ON med.medid = available_med.medid
	INNER JOIN dwh.dim_gcn_seqno_hierarchy ON med.gcn = dim_gcn_seqno_hierarchy.gcn AND med.gcn_seqno = dim_gcn_seqno_hierarchy.gcn_seqno
	LEFT JOIN (select distinct(medid) from transactional.retail_package_v2 )  rp2 on rp2.medid = mdim.medid
	LEFT JOIN dwh.dim_medid_hierarchy ON dim_medid_hierarchy.medid = med.medid
-- 	LEFT JOIN (select distinct medid, package_description, package_size from transactional.med_package) as med_package on med_package.medid = mdim.medid
-- ORDER BY
-- 	dim_gcn_seqno_hierarchy.gcn_symphony_2017_rank,med.medid	

select * from transactional.retail_package_v2;



select count(*) from drugs_etl.med_master

SELECT
	med.billing_unit,
	med.type_description,
	count(DISTINCT med.medid)
-- 	case when rp2.medid is null then False else True end as with_modified_retail_package
FROM
	transactional.available_med AS med 
	inner JOIN ( select distinct(medid) from transactional.retail_package_v2 )  rp2 on rp2.medid = med.medid
group by 1,2
order by 1,2


select * from transactional.med WHERE medid = 473030

select * from drugs_etl.med_master where medid = 473030 limit 10;

select distinct medid, package_description, package_size from transactional.med_package where medid = 473030;


SELECT
	*
FROM
	transactional.available_med
WHERE
	available_med.medid NOT in(
		SELECT
			medid FROM mktg_dev.units_of_use_raw_data
			JOIN git_data_import.medid_drug_id_mapping ON medid_drug_id_mapping.drug_id = units_of_use_raw_data.drug_id)
		AND billing_unit = 'grams'

;

SELECT
	count(distinct(available_med.medid))
FROM
	transactional.med available_med
	JOIN git_data_import.medid_drug_id_mapping ON available_med.medid = medid_drug_id_mapping.medid
	JOIN mktg_dev.units_of_use_raw_data on medid_drug_id_mapping.drug_id = units_of_use_raw_data.drug_id


SELECT
	billing_unit, type_description,
	count(DISTINCT (available_med.medid))
FROM
	transactional.available_med
	INNER JOIN mktg_dev.units_of_use_raw_data ON lower(available_med.name) = lower(units_of_use_raw_data.slug)
GROUP BY
	1,2 
ORDER BY
	1,2
;


SELECT
	billing_unit, 
	available_med.type_description,
	count(distinct(available_med.medid))
FROM
	transactional.available_med
group by 1,2 
order by 1,2 




SELECT
	dgsh.medid,
	dgsh.generic_name_short,
	MAX(
		CASE WHEN mp.pharmacy_network_id = 1 THEN
			mp.unit_price
		ELSE
			- 10000000000
		END) AS edlp_unit_price,
	MAX(
		CASE WHEN mp.pharmacy_network_id = 2 THEN
			mp.unit_price
		ELSE
			- 10000000000
		END) AS bsd_unit_price,
	MAX(
		CASE WHEN mp.pharmacy_network_id = 3 THEN
			mp.unit_price
		ELSE
			- 10000000000
		END) AS hd_unit_price,
	MAX(
		CASE WHEN mp.pharmacy_network_id = 1 THEN
			mp.dispensing_fee_margin + 1.75
		ELSE
			- 10000000000
		END) AS edlp_fixed_price,
	MAX(
		CASE WHEN mp.pharmacy_network_id = 2 THEN
			mp.dispensing_fee_margin + 1.75
		ELSE
			- 10000000000
		END) AS bsd_fixed_price,
	MAX(
		CASE WHEN mp.pharmacy_network_id = 3 THEN
			mp.dispensing_fee_margin + 1.75
		ELSE
			- 10000000000
		END) AS hd_fixed_price,
	MAX(
		CASE WHEN mac.mac_list = 'BLINK01' THEN
			mac.unit_price
		ELSE
			- 10000000000
		END) AS bh01_mac_price,
	MAX(
		CASE WHEN mac.mac_list = 'BLINK02' THEN
			mac.unit_price
		ELSE
			- 10000000000
		END) AS bh02_mac_price,
	MAX(
		CASE WHEN mac.mac_list = 'BLINK03' THEN
			mac.unit_price
		ELSE
			- 10000000000
		END) AS bh03_mac_price,
	MAX(
		CASE WHEN mac.mac_list = 'BLINKWMT01' THEN
			mac.unit_price
		ELSE
			- 10000000000
		END) AS wmt_mac_price,
	MAX(
		CASE WHEN mac.mac_list = 'BLINKSYRx01' THEN
			mac.unit_price
		ELSE
			- 10000000000
		END) AS hd_mac_price,
	1.0 AS edlp_dispensing_fee,
	1.0 AS bsd_dispensing_fee,
	1.5 AS hd_dispensing_fee
FROM
	dwh.dim_gcn_seqno_hierarchy dgsh
	JOIN transactional.med_price mp ON dgsh.medid = mp.medid
	JOIN transactional.network_pricing_mac mac ON mac.gcn_seqno = dgsh.gcn_seqno
WHERE
	mp.ended_on IS NULL
	AND mac.end_date IS NULL
	AND dgsh.gcn_seqno = 41653
	AND mp.medid = 250555
GROUP BY
	1,
	2
	;
	
SELECT
	pharmacy_network_id,
	unit_price,
	dispensing_fee_margin
FROM
	transactional.med_price
WHERE
	medid = 157529
	AND ended_on IS NULL;

SELECT
	*
FROM
	transactional.network_pricing_mac
WHERE
	end_date IS NULL
	AND gcn_seqno = 6655;

GRANT SELECT ON mktg_dev.sdey_privia_utilization_data TO "public";

select * from mktg_dev.sdey_privia_utilization_data limit 100;



select * from 



-- select * from fifo.bsd_retirement_input order by fill_date desc;


select * from mktg_dev.sdey_privia_temp_key_table limit 10;
select count(*) from mktg_dev.sdey_privia_temp_key_table;

insert into mktg_dev.sdey_privia_temp_key_table (med_desc) VALUES ('fluticasone propionate 50 mcg/actuation nasal spray:suspension');

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
	lower(med_medid_desc)
	LIKE 'ipratropium'


select * from mktg_dev.sdey_privia_utilization_data left JOIN transactional.med on sdey_privia_utilization_data.medid = med.medid;


select count(distinct(medid)) from transactional.retail_package_v2;




SELECT
	uu.default_quantity as goodrx_default_quantity,
	uu.dosage as goodrx_dosage,
	uu.drug_id as goodrx_drug_id,
	uu.form_name as goodrx_form_name,
	uu.slug as goodrx_slug,
	mdim.mapping_source,
	med.description,
	med.dosage_form,
	med.strength,
	med.billing_unit,
	med.gcn,
	med.gcn_seqno,
	med.medid,
	case when rp2.medid is null then False else True end as with_modified_retail_package
FROM
	mktg_dev.units_of_use_raw_data uu
	INNER JOIN api_scraper.medid_drug_id_mapping mdim ON uu.drug_id = mdim.drug_id
	INNER JOIN transactional.med ON med.medid = mdim.medid
	LEFT OUTER JOIN ( select distinct(medid) from transactional.retail_package_v2 )  rp2 on rp2.medid = mdim.medid;
	

select billing_unit,count(*) from transactional.med group by 1 

select * from transactional.med_price where medid = 576527 and ended_on is null and pharmacy_network_id = 1 

select * from transactional.network_pricing_mac where network_pricing_mac.gcn_seqno = 16879 and end_date::date='2020-01-09';

select * from transactional.med where lower(med.description) like '%abilify%'
	
select * from transactional.med_name where med_name."name" like '%auvi%';

select * from transactional.medication_quantity where medication_quantity.medid = 473182;

select * from transactional.retail_package_v2;

select * from dwh.dim_gcn_seqno_hierarchy where generic_name_short like '%epinephrine%'

SELECT count(*) from drugs_etl.med_master;

select * from dwh.dim_gcn_seqno_hierarchy where generic_name_short like '%sildenafil%'

select * from dwh.dim_gcn_seqno_hierarchy where gcn = 43017 or gcn_seqno = 43017


select * from transactional.retail_package_v2 ;
where medid = 576148;

select * from transactional.med_package where medid = 152312; 

select * from transactional.med_price where medid = 288897 and ended_on is null;
select * from transactional.network_pricing_mac where gcn_seqno=21796 and end_date is null;

select * from api_scraper_external.goodrx_price_raw limit 100;

SELECT
	med_medid_desc,
	medid,
	gcn_seqno,
	med_name_maint,
	med_name_dea,
	med_ref_multi_source_code_desc,
	med_ref_gen_drug_name_code_desc
FROM
	dwh.dim_medid_hierarchy
WHERE
	replace(med_medid_desc,',',':') = 'metformin ER 500 mg tablet:extended release 24 hr';


SELECT
	network_pricing_mac.mac_list,
	count(DISTINCT gcn_seqno)
FROM
	transactional.network_pricing_mac
WHERE
	end_date IS NULL
GROUP BY
	1

SELECT
	dim_gcn_seqno_hierarchy.generic_name_short,
	dim_gcn_seqno_hierarchy.strength,
	sdey_generic_price_portfolio_datamart.default_quantity,
	sdey_generic_price_portfolio_datamart.top_30ds_quantity,
	sdey_generic_price_portfolio_datamart.top_90ds_quantity
from 
	dwh.dim_gcn_seqno_hierarchy	
left outer join
	mktg_dev.sdey_generic_price_portfolio_datamart
ON
	dim_gcn_seqno_hierarchy.gcn = 68030
	AND sdey_generic_price_portfolio_datamart.gcn = 68030


SELECT generic_name_short,strength from dwh.dim_gcn_seqno_hierarchy where gcn = 68030;


with gcns as (
	select 
		*
	from 
		mktg_dev.sdey_privia_utilization_data privia
	left outer JOIN
		(select DISTINCT gcn from transactional.med_price where pharmacy_network_id = 3) pr 
	ON
		pr.gcn = hd.gcn
	where 
		pr.gcn is NULL
), drug_details as (
	SELECT
		dgsh.gcn,
		dgsh.gcn_seqno,	
		dgsh.medid,
		dgsh.generic_name_short,
		dgsh.strength,
		dgsh.dosage_form_code,
-- 		dmh.med_name,
		MAX(case when mp.pharmacy_network_id = 1 then mp.unit_price else -10000000000 end ) AS edlp_unit_price,
		MAX(case when mp.pharmacy_network_id = 2 then mp.unit_price else -10000000000 end ) AS bsd_unit_price,
		MAX(case when mp.pharmacy_network_id = 3 then mp.unit_price else -10000000000 end ) AS hd_unit_price,
		MAX(case when mp.pharmacy_network_id = 1 then mp.dispensing_fee_margin+1.75 else -10000000000 end ) AS edlp_fixed_price,
		MAX(case when mp.pharmacy_network_id = 2 then mp.dispensing_fee_margin+1.75 else -10000000000 end ) AS bsd_fixed_price,
		MAX(case when mp.pharmacy_network_id = 3 then mp.dispensing_fee_margin+1.75 else -10000000000 end ) AS hd_fixed_price,
		MAX(case when mac.mac_list='BLINK01' then mac.unit_price else -10000000000 end ) AS bh01_mac_price,
		MAX(case when mac.mac_list='BLINK02' then mac.unit_price else -10000000000 end ) AS bh02_mac_price,
		MAX(case when mac.mac_list='BLINK03' then mac.unit_price else -10000000000 end ) AS bh03_mac_price,
		MAX(case when mac.mac_list='BLINKWMT01' then mac.unit_price else -10000000000 end ) AS wmt_mac_price,	
		MAX(case when mac.mac_list='BLINKSYRx01' then mac.unit_price else -10000000000 end ) AS hd_mac_price,
		1.0 AS edlp_dispensing_fee,
		1.0 AS bsd_dispensing_fee,
		1.5 AS hd_dispensing_fee	
	FROM
		transactional.med_price mp
	OUTER JOIN
		dwh.dim_gcn_seqno_hierarchy dgsh 
	ON
		dgsh.gcn = mp.gcn
		AND dgsh.medid = mp.medid
	OUTER JOIN
		transactional.network_pricing_mac mac
	ON
		mac.gcn_seqno = dgsh.gcn_seqno
-- 	LEFT JOIN 
-- 		dwh.dim_medid_hierarchy dmh
-- 	ON 
-- 		 mp.medid = dmh.medid
	WHERE
		mp.ended_on is null
		AND mac.end_date is NULL
	GROUP BY
		1,2,3,4,5,6
) select * from gcns left outer join drug_details on drug_details.gcn = gcns.gcn order by gcns.gcn




	SELECT
		gcn,
		MAX(case when mp.pharmacy_network_id = 1 then mp.unit_price else -10000000000 end ) AS edlp_unit_price,
		MAX(case when mp.pharmacy_network_id = 2 then mp.unit_price else -10000000000 end ) AS bsd_unit_price,
		MAX(case when mp.pharmacy_network_id = 3 then mp.unit_price else -10000000000 end ) AS hd_unit_price,
		MAX(case when mp.pharmacy_network_id = 1 then mp.dispensing_fee_margin+1.75 else -10000000000 end ) AS edlp_fixed_price,
		MAX(case when mp.pharmacy_network_id = 2 then mp.dispensing_fee_margin+1.75 else -10000000000 end ) AS bsd_fixed_price,
		MAX(case when mp.pharmacy_network_id = 3 then mp.dispensing_fee_margin+1.75 else -10000000000 end ) AS hd_fixed_price
	FROM
		transactional.med_price	mp
	WHERE
		gcn = 40720 OR gcn = 68030	
		and ended_on is null
	Group by 1
	order by 1 
		
	select 
		gcn,
		gcn_seqno
	from dwh.dim_gcn_seqno_hierarchy
	where	gcn = 40720 OR gcn = 68030	
	
	SELECT
		gcn_seqno,
-- 		gcn,
		unit_price
-- 		MAX(case when mac.mac_list='BLINK01' then mac.unit_price else -10000000000 end ) AS bh01_mac_price,
-- 		MAX(case when mac.mac_list='BLINK02' then mac.unit_price else -10000000000 end ) AS bh02_mac_price,
-- 		MAX(case when mac.mac_list='BLINK03' then mac.unit_price else -10000000000 end ) AS bh03_mac_price,
-- 		MAX(case when mac.mac_list='BLINKWMT01' then mac.unit_price else -10000000000 end ) AS wmt_mac_price,	
-- 		MAX(case when mac.mac_list='BLINKSYRx01' then mac.unit_price else -10000000000 end ) AS hd_mac_price
	FROM
		transactional.network_pricing_mac
	where 
		end_date is null
		AND gcn_seqno = 9260 -- 9260 : 40720 
		AND mac_list = 'BLINKSYRx01'




with tgnr_updated as (
	select
		*,
		case
			when pharmacy_network_name='blink' then 1
			when pharmacy_network_name='supersaver' then 2
			when pharmacy_network_name='delivery' then 3
			else -1
		end as pharmacy_network_id
	from
		mktg_dev.sdey_target_gcns_with_neg_revenue
),
target_gcn_pricing_non_balance_billing_entries as (
	SELECT
		mp.id,
		mp.started_on,
		mp.ended_on,
		mp.unit_price,
		mp.dispensing_fee_margin,
		mp.pharmacy_network_id,
		tgnr_updated.*
	FROM
		tgnr_updated
	INNER JOIN
		transactional.med_price as mp
	ON
		tgnr_updated.gcn = mp.gcn
		AND tgnr_updated.med_id = mp.medid
		AND tgnr_updated.pharmacy_network_id = mp.pharmacy_network_id
	WHERE
		NOT (
		 (mp.ended_on is NULL AND mp.started_on::TIMESTAMP::DATE + INTERVAL '30 day' < CURRENT_DATE)
		 OR  (mp.ended_on is NOT NULL AND mp.started_on::timestamp::date + INTERVAL '1 day' > mp.ended_on::TIMESTAMP::DATE)
		 OR  (mp.ended_on is NOT NULL AND mp.ended_on::timestamp::date + INTERVAL '30 day' < CURRENT_DATE))
)
select * from target_gcn_pricing_non_balance_billing_entries

-- active_prices as (
-- 	select
-- 		*
-- 	FROM
-- 		target_gcn_pricing_non_balance_billing_entries
-- 	WHERE
-- 		ended_on is NULL
-- )


-- with historical_30day_prices as (
-- 	SELECT
-- 		mp.*
-- 	FROM
-- 		mktg_dev.sdey_target_gcns_with_neg_revenue as tgnr
-- 	INNER JOIN
-- 		transactional.med_price as mp
-- 	ON
-- 		tgnr.gcn = mp.gcn
-- 		AND tgnr.med_id = mp.medid
-- 	WHERE
-- 		(mp.ended_on is NULL AND mp.started_on::timestamp::date  < CURRENT_DATE -  INTERVAL '30 day')
-- 		OR ()
-- 		(mp.ended_on::timestamp::date >= CURRENT_DATE -  INTERVAL '30 day'
-- 		AND mp.started_on::timestamp::date + INTERVAL '1 day' < mp.ended_on::TIMESTAMP::DATE ))
-- 		AND (pharmacy_network_id = 1 OR pharmacy_network_id = 2 OR pharmacy_network_id = 3 )
-- 	ORDER by
-- 		mp.gcn,mp.medid,mp.pharmacy_network_id	)


-- SELECT
-- 	mp.*
-- FROM
-- 	mktg_dev.sdey_target_gcns_with_neg_revenue as tgnr
-- INNER JOIN
-- 	transactional.med_price as mp
-- ON
-- 	tgnr.gcn = mp.gcn
-- 	AND tgnr.med_id = mp.medid
-- WHERE
-- 	(mp.ended_on is NULL
-- 	OR
-- 	(mp.ended_on::timestamp::date + INTERVAL '30 day' >= CURRENT_DATE
-- 	AND mp.started_on::timestamp::date + INTERVAL '1 day' < mp.ended_on::TIMESTAMP::DATE ))
-- 	AND (pharmacy_network_id = 1 OR pharmacy_network_id = 2 OR pharmacy_network_id = 3 )
-- ORDER by
-- 	mp.gcn,mp.medid,mp.pharmacy_network_id




-- SELECT
-- 	cpcp.*,
-- 	cpcs.status
-- FROM
-- 	mktg_dev.sdey_consumer_pricing_changes_planned AS cpcp
-- 	INNER JOIN mktg_dev.sdey_consumer_pricing_changelist_status AS cpcs ON cpcp.change_label = cpcs.change_label
-- where
-- 	status != 'planned'


with tgnr_updated as (
	select
		*,
		case
			when pharmacy_network_name='blink' then 1
			when pharmacy_network_name='supersaver' then 2
			when pharmacy_network_name='delivery' then 3
			else -1
		end as pharmacy_network_id
	from
		mktg_dev.sdey_target_gcns_with_neg_revenue
),
gcns_with_pricing_changes as (
	SELECT
		mp.gcn,
		mp.medid,
		mp.pharmacy_network_id,
		count(DISTINCT(unit_price)) count_distinct_price,
		max(mp.started_on)::TIMESTAMP::DATE AS last_start_date
	FROM
		tgnr_updated
	INNER JOIN
		transactional.med_price as mp
	ON
		tgnr_updated.gcn = mp.gcn
		AND tgnr_updated.med_id = mp.medid
		AND tgnr_updated.pharmacy_network_id = mp.pharmacy_network_id
	WHERE
		NOT (
		 (mp.ended_on is NULL AND mp.started_on::TIMESTAMP::DATE + INTERVAL '30 day' < CURRENT_DATE)
		 OR  (mp.ended_on is NOT NULL AND mp.started_on::timestamp::date + INTERVAL '1 day' > mp.ended_on::TIMESTAMP::DATE)
		 OR  (mp.ended_on is NOT NULL AND mp.ended_on::timestamp::date + INTERVAL '30 day' < CURRENT_DATE))
	GROUP BY
		1,
		2,
		3
	HAVING
		count_distinct_price > 1
),
planned_changes as (
	SELECT
		cpcp.*,
		cpcs.status
	FROM
		mktg_dev.sdey_consumer_pricing_changes_planned AS cpcp
		INNER JOIN mktg_dev.sdey_consumer_pricing_changelist_status AS cpcs ON cpcp.change_label = cpcs.change_label
	where
		status = 'planned'
),
target_gcns_changedata as (
	SELECT
		tu.*,
		CASE
			WHEN gwpc.gcn is NOT NULL THEN FALSE
			WHEN pc.gcn IS NOT NULL THEN FALSE
			ELSE TRUE
		END AS in_consideration,
		CASE
			WHEN gwpc.gcn is NOT NULL THEN CONCAT('Last Started Date : ',last_start_date::TEXT)
			WHEN pc.gcn IS NOT NULL THEN CONCAT('Planned Change : ',change_label::TEXT)
			ELSE TRUE
		END AS in_consideration_reason
	FROM
		tgnr_updated AS tu
	LEFT OUTER JOIN
		gcns_with_pricing_changes as gwpc
	ON
		tu.gcn = gwpc.gcn
		AND tu.med_id = gwpc.med_id
		AND tu.pharmacy_network_id = gwpc.pharmacy_network_id
	LEFT OUTER JOIN
		planned_changes as pc
	ON
		tu.gcn = pc.gcn
		AND tu.med_id = pc.med_id
		AND tu.pharmacy_network_id = pc.pharmacy_network_id
)
select * from target_gcns_changedata



-- active_prices as (
-- 	select
-- 		*
-- 	FROM
-- 		target_gcn_pricing_non_balance_billing_entries
-- 	WHERE
-- 		ended_on is NULL
-- )


-- with historical_30day_prices as (
-- 	SELECT
-- 		mp.*
-- 	FROM
-- 		mktg_dev.sdey_target_gcns_with_neg_revenue as tgnr
-- 	INNER JOIN
-- 		transactional.med_price as mp
-- 	ON
-- 		tgnr.gcn = mp.gcn
-- 		AND tgnr.med_id = mp.medid
-- 	WHERE
-- 		(mp.ended_on is NULL AND mp.started_on::timestamp::date  < CURRENT_DATE -  INTERVAL '30 day')
-- 		OR ()
-- 		(mp.ended_on::timestamp::date >= CURRENT_DATE -  INTERVAL '30 day'
-- 		AND mp.started_on::timestamp::date + INTERVAL '1 day' < mp.ended_on::TIMESTAMP::DATE ))
-- 		AND (pharmacy_network_id = 1 OR pharmacy_network_id = 2 OR pharmacy_network_id = 3 )
-- 	ORDER by
-- 		mp.gcn,mp.medid,mp.pharmacy_network_id	)


-- SELECT
-- 	mp.*
-- FROM
-- 	mktg_dev.sdey_target_gcns_with_neg_revenue as tgnr
-- INNER JOIN
-- 	transactional.med_price as mp
-- ON
-- 	tgnr.gcn = mp.gcn
-- 	AND tgnr.med_id = mp.medid
-- WHERE
-- 	(mp.ended_on is NULL
-- 	OR
-- 	(mp.ended_on::timestamp::date + INTERVAL '30 day' >= CURRENT_DATE
-- 	AND mp.started_on::timestamp::date + INTERVAL '1 day' < mp.ended_on::TIMESTAMP::DATE ))
-- 	AND (pharmacy_network_id = 1 OR pharmacy_network_id = 2 OR pharmacy_network_id = 3 )
-- ORDER by
-- 	mp.gcn,mp.medid,mp.pharmacy_network_id

SELECT
m.gcn
,am.name as med_name
,am.med_name_id
,count(a.last_pbm_adjudication_timestamp_approved) as fills
from dwh.fact_order_item a
left join dwh.dim_user b on a.account_id=b.account_id
left join dwh.dim_ndc_hierarchy drg_ndc on a.last_claim_ndc_approved=drg_ndc.ndc
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno
left join fdb.RNDC14_NDC_MSTR mstr on a.last_claim_ndc_approved=mstr.ndc
left join medispan.mf2ndc ms on a.last_claim_ndc_approved=ms.ndc_upc_hri
left join transactional.available_med am on a.med_id=am.medid
left join transactional.med m on am.medid=m.medid
where a.last_pbm_adjudication_timestamp_approved is not null and ms.multi_source_code = 'Y' and am.type_description = 'brand' and am.name is not null and m.gcn is not null
group by 1,2,3


-- select * from dwh.dim_gcn_seqno_hierarchy;

-- SELECT
-- 	apr.*,
-- 	dgsh.generic_name_short,
-- 	dgsh.generic_name_long,
-- 	dgsh.gcn_symphony_2017_rank,
-- 	dgsh.dosage_form_desc,
-- 	dgsh.dosage_form_code_desc,
-- 	route_desc
-- FROM
-- 	mktg_dev.sdey_automated_pricing_recommendations  as apr 
-- LEFT OUTER JOIN
-- 	dwh.dim_gcn_seqno_hierarchy as dgsh
-- ON
-- 	apr.gcn = dgsh.gcn 
-- 	AND apr.med_id = dgsh.medid
-- ;


-- with multiple_prices as (
-- 	SELECT
-- 		gcn,
-- 		pharmacy_network_id,
-- 		count(DISTINCT trunc(unit_price,3)) as unit_prices,
-- 		count(DISTINCT trunc(dispensing_fee_margin,3)) as dfms
-- 	FROM
-- 		transactional.med_price
-- 	WHERE
-- 		ended_on is NULL
-- 		AND branded = 0 
-- 	GROUP BY
-- 		gcn,
-- 		pharmacy_network_id
-- 	HAVING
-- 		unit_prices > 2 or dfms >= 2 
-- )
-- select 
-- -- 	med_price.id,
-- 	dgsh.generic_name_short,
-- -- 	dmh.is_branded_price,
-- -- 	dmh.med_name,
-- 	mps.gcn,
-- 	med_price.gcn,
-- 	med_price.medid,
-- 	mps.pharmacy_network_id,
-- 	unit_price,
-- 	dispensing_fee_margin,
-- 	med_price.branded,
-- 	started_on,
-- 	ended_on
-- FROM
-- 	multiple_prices as mps
-- INNER JOIN
-- 	transactional.med_price as med_price
-- ON
-- 	mps.gcn = med_price.gcn
-- 	and mps.pharmacy_network_id = med_price.pharmacy_network_id
-- INNER JOIN
-- 	dwh.dim_gcn_seqno_hierarchy AS dgsh
-- ON
-- 	dgsh.medid = med_price.medid AND
-- 	dgsh.gcn = med_price.gcn
-- -- INNER JOIN
-- -- 	dwh.dim_medid_hierarchy AS dmh
-- -- ON
-- -- 	dmh.medid = med_price.medid 
-- WHERE
-- 	ended_on is NULL
-- 	AND branded = 0 
-- ORDER BY
-- 	mps.gcn;







-- SELECT
-- 	dgsh.gcn,
-- 	dgsh.gcn_seqno,
-- 	dgsh.gcn_symphony_2017_rank,
-- 	dgsh.gcn_symphony_2017_fills,
-- 	dgsh.strength,
-- 	dgsh.dosage_form_desc,
-- 	dgsh.generic_name_long,
-- 	CASE
-- 		when datamart.default_quantity = cmp1.quantity then 'Default'
-- 		when datamart.top_30ds_quantity = cmp1.quantity then '30 Day Qty'
-- 		when datamart.top_90ds_quantity = cmp1.quantity then '90 Day Qty'
-- 		else 'none'
-- 	END	AS quantity_type,
-- 	cmp1.quantity as quantity,
-- 	cmp1.price_min as price_min_new,
-- 	cmp1.walmart_min as walmart_new,
-- 	cmp1.cvs_min as cvs_new,
-- 	cmp1.walgreens_min as walgreens_new,
-- 	cmp1.rite_aid_min as riteaid_new,
-- 	cmp1.kroger_min as kroger_new,
-- 	cmp2.price_min as price_min_old,
-- 	cmp2.walmart_min as walmart_old,
-- 	cmp2.cvs_min as cvs_old,
-- 	cmp2.walgreens_min as walgreens_old,
-- 	cmp2.rite_aid_min as riteaid_old,
-- 	cmp2.kroger_min as kroger_old,
-- 	mp.dispensing_fee_margin,
-- 	mp.unit_price
-- FROM
-- 	(SELECT
-- 		gcn,
-- 		gcn_seqno,
-- 		gcn_symphony_2017_rank,
-- 		gcn_symphony_2017_fills,
-- 		strength,
-- 		dosage_form_desc,
-- 		generic_name_long,
-- 		medid
-- 	FROM
-- 		dwh.dim_gcn_seqno_hierarchy
-- 	WHERE
-- 		gcn_symphony_2017_rank < 1000) AS dgsh
-- INNER JOIN
-- 	transactional.med_price AS mp
-- ON
-- 	mp.gcn = dgsh.gcn
-- 	AND mp.medid = dgsh.medid
-- 	AND mp.pharmacy_network_id = 1
-- 	AND ended_on is NULL
-- INNER JOIN 
-- 	fifo.generic_price_portfolio_datamart AS datamart
-- ON
-- 	dgsh.gcn = datamart.gcn
-- 	AND dgsh.gcn_seqno = datamart.gcn_seqno
-- INNER JOIN 
-- 	(SELECT
-- 		gcn,
-- 		quantity,
-- 		MIN(price) price_min,
-- 		MIN(CASE WHEN pharmacy = 'walmart' THEN price ELSE 1000000000000 END) AS walmart_min,
-- 		MIN(CASE WHEN pharmacy = 'cvs' THEN price ELSE 1000000000000 END) AS cvs_min,
-- 		MIN(CASE WHEN pharmacy = 'walgreens' THEN price ELSE 1000000000000 END) AS walgreens_min,
-- 		MIN(CASE WHEN pharmacy = 'rite_aid' THEN price ELSE 1000000000000 END) AS rite_aid_min,
-- 		MIN(CASE WHEN pharmacy = 'kroger' THEN price ELSE 1000000000000 END) AS kroger_min
-- 	FROM
-- 		api_scraper_external.competitor_pricing	
-- 	WHERE
-- 		site != 'all'
-- 		AND geo != 'all'
-- 		AND pharmacy != 'all'
-- 		AND pharmacy != 'all_major'
-- 		AND pharmacy != 'all_preferred'
-- 		AND pharmacy != 'other_pharmacies'
-- 		AND site = 'goodrx'
-- 		AND date = '2019-12-05'
-- 	GROUP BY
-- 		1,2) AS cmp1
-- ON
-- 	dgsh.gcn = cmp1.gcn
-- 	AND (cmp1.quantity = datamart.default_quantity OR cmp1.quantity = datamart.top_30ds_quantity or cmp1.quantity = datamart.top_90ds_quantity )
-- INNER JOIN
-- 	(SELECT
-- 		gcn,
-- 		quantity,
-- 		MIN(price) price_min,
-- 		MIN(CASE WHEN pharmacy = 'walmart' THEN price ELSE 1000000000000 END) AS walmart_min,
-- 		MIN(CASE WHEN pharmacy = 'cvs' THEN price ELSE 1000000000000 END) AS cvs_min,
-- 		MIN(CASE WHEN pharmacy = 'walgreens' THEN price ELSE 1000000000000 END) AS walgreens_min,
-- 		MIN(CASE WHEN pharmacy = 'rite_aid' THEN price ELSE 1000000000000 END) AS rite_aid_min,
-- 		MIN(CASE WHEN pharmacy = 'kroger' THEN price ELSE 1000000000000 END) AS kroger_min
-- 	FROM
-- 		api_scraper_external.competitor_pricing	
-- 	WHERE
-- 		site != 'all'
-- 		AND geo != 'all'
-- 		AND pharmacy != 'all'
-- 		AND pharmacy != 'all_major'
-- 		AND pharmacy != 'all_preferred'
-- 		AND pharmacy != 'other_pharmacies'
-- 		AND site = 'goodrx'
-- 		AND date = '2019-10-31'
-- 	GROUP BY
-- 		1,2) AS cmp2		
-- ON
-- 	cmp1.gcn = cmp2.gcn	
-- 	AND cmp1.quantity = cmp2.quantity
-- ;




-- SELECT
-- 	date,
-- 	count(DISTINCT geo),
-- 	count(*)
-- FROM
-- 	api_scraper_external.competitor_pricing
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-09-01'
-- group BY
-- 	1
-- ;
-- select pharmacy,count(*) as cnt from api_scraper_external.competitor_pricing where pharmacy not like '%all%' and date >= '2019-12-01' group by 1 order by cnt desc;
-- SELECT DISTINCT
-- 	gcn,
-- 	branded,
-- 	deleted,
-- 	started_on::timestamp::date,
-- 	CASE WHEN pharmacy_network_id IS NULL THEN
-- 		'edlp'
-- 	WHEN pharmacy_network_id = 1 THEN
-- 		'edlp'
-- 	WHEN pharmacy_network_id = 2 THEN
-- 		'bsd'
-- 	WHEN pharmacy_network_id = 3 THEN
-- 		'hd'
-- 	WHEN pharmacy_network_id >= 4 THEN
-- 		'quicksave'
-- 	ELSE
-- 		'unknown'
-- 	END AS pharmacy_network_name
-- FROM
-- 	transactional.med_price
-- WHERE
-- 	started_on::timestamp::date >= '2019-01-01';
-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price
-- WHERE
-- -- 	started_on::timestamp::date = '2019-12-28';

-- SELECT
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', last_pbm_adjudication_timestamp_approved)::timestamp::date AS "fill date",
-- 	sum(1) AS fills
-- FROM
-- 	dwh.fact_order_item
-- WHERE fill_sequence IS NOT NULL
-- 	AND is_fraud = FALSE
-- 	AND CONVERT_TIMEZONE ('UTC', 'America/New_York', last_pbm_adjudication_timestamp_approved)::timestamp::date > '2019-12-20'
-- GROUP BY
-- 	1
-- order by 
-- 	1 DESC
-- 	;


-- SELECT
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS "fill date",
-- 	foi.pharmacy_network_name AS "phamramcy network name",
-- 	foi.med_id AS "med id",
-- 	foi.generic_medid AS "generic medid",
-- 	p_medid.med_name AS "purchased med name",
-- 	f_medid.med_name AS "filled med name",
-- 	p_gcn.generic_name_short AS "purchased generic name short",
-- 	f_gcn.generic_name_short AS "filled generic name short",
-- 	p_gcn.strength AS "purchased strength",
-- 	f_gcn.strength AS "filled strength",
-- 	p_gcn.gcn AS "purchased gcn",
-- 	p_gcn.gcn_seqno AS "purchased gcn seqno",
-- 	f_gcn.gcn AS "filled gcn",
-- 	f_gcn.gcn_seqno AS "filled gcn seqno",
-- 	CASE WHEN last_claim_days_supply_approved < 84 THEN
-- 		30
-- 	ELSE
-- 		90
-- 	END AS "days supply normalized",
-- 	CASE WHEN (coalesce(foi.last_claim_med_price_approved, 0) + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0) - coalesce(foi.last_pricing_total_cost_approved, 0) - coalesce(foi.last_claim_wmt_true_up_amount_approved, 0) < 0.0) THEN
-- 		TRUE
-- 	ELSE
-- 		FALSE
-- 	END AS "is_negative_revenue_fill",
-- 	sum(1) AS fills,
-- 	count(DISTINCT foi.dw_user_id) AS "uniq users",
-- 	count(DISTINCT order_id) AS "uniq orders",
-- 	sum(coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float) AS "gross revenue",
-- 	sum(coalesce(foi.last_claim_med_price_approved, 0) + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0) - coalesce(foi.last_pricing_total_cost_approved, 0) - coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)) AS "gross margin",
-- 	sum(last_claim_days_supply_approved) AS "total days supply"
-- FROM
-- 	dwh.fact_order_item foi
-- 	LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
-- 		AND du.is_internal = FALSE
-- 		AND du.is_phantom = FALSE
-- 	LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
-- 	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
-- WHERE (foi.fill_sequence IS NOT NULL)
-- AND foi.is_fraud = FALSE
-- AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-12-01'
-- -- AND foi.last_pbm_adjudication_timestamp_approved + INTERVAL '120 day' >= CURRENT_DATE
-- GROUP BY
-- 	1,
-- 	2,
-- 	3,
-- 	4,
-- 	5,
-- 	6,
-- 	7,
-- 	8,
-- 	9,
-- 	10,
-- 	11,
-- 	12,
-- 	13,
-- 	14,
-- 	15,
-- 	16
-- order by "gross margin"
-- 	;
-- SELECT
-- -- 	date_trunc('day', CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)) AS fill_date,
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS fill_date,
-- 	foi.med_id AS purchased_medid,
-- 	foi.last_claim_medid_approved AS filled_medid,
-- 	p_medid.med_name AS purchased_med_name,
-- 	f_medid.med_name AS filled_med_name,
-- 	foi.gcn AS purchased_gcn,
-- 	foi.last_claim_gcn_approved AS filled_gcn,
-- 	p_gcn.generic_name_short AS purchased_generic_name_short,
-- 	f_gcn.generic_name_short AS filled_generic_name_short,
-- 	foi.quantity AS purchased_quantity,
-- 	foi.last_claim_quantity_approved AS filled_quantity,
-- 	foi.price AS purchased_price,
-- 	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float AS filled_price_paid_aka_realized_gross_revenue,
-- 	foi.pharmacy_network_id AS purchased_pharmacy_network_id,
-- 	CASE WHEN pharmacy_network_id IS NULL
-- 		OR pharmacy_network_id = 1 THEN
-- 		'EDLP'
-- 	WHEN pharmacy_network_id = 2 THEN
-- 		'BSD'
-- 	WHEN pharmacy_network_id = 3 THEN
-- 		'HD'
-- 	WHEN pharmacy_network_id = 4
-- 		OR pharmacy_network_id = 5 THEN
-- 		'quicksave'
-- 	ELSE
-- 		'NONE'
-- 	END AS purchased_pharmacy_network,
-- 	foi.last_claim_days_supply_approved AS filled_days_supply,
-- 	coalesce(last_pricing_total_cost_approved, 0)::float + coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS filled_ingredient_plus_dispensing_costs_aka_realized_cogs,
-- 	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float - coalesce(last_pricing_total_cost_approved, 0)::float - coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS realized_gross_profit,
-- 	coalesce(foi.order_medication_discount_amount, 0)::float + coalesce(foi.allocated_order_discount_amount, 0)::float + coalesce(foi.allocated_wallet_payment_amount, 0)::float AS filled_discounts
-- FROM
-- 	dwh.fact_order_item foi
-- 	LEFT JOIN dwh.dim_user du ON foi.account_id = du.account_id
-- 	-- i've also see a foi.dw_user_id = du.dw_user_id, should that be added?
-- 		AND du.is_internal = FALSE -- removes internal users
-- 		AND du.is_phantom = FALSE -- remove phantom users
-- 	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
-- 	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno -- add’l info for med purchased on the order, (e.g. generic med name)
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno -- add’l info for med actually filled, (e.g. generic med name)
-- WHERE
-- 	foi.is_fraud = FALSE
-- 	AND foi.last_pbm_adjudication_timestamp_approved IS NOT NULL
-- 	AND foi.quantity != foi.last_claim_quantity_approved
-- 	AND foi.gcn != foi.last_claim_gcn_approved
-- 	AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-01-01'
-- ;
-- SELECT
-- 	ms.gcn,
-- 	ms.medid,
-- 	ms.gcn_seqno,
-- 	ms.gcn_symphony_2017_rank,
-- 	ms.gcn_symphony_2017_fills,
-- 	ms.default_quantity,
-- 	ms.strength,
-- 	ms.dosage_form_desc,
-- 	ms.generic_name_long,
-- 	price.edlp_unit_price,
-- 	price.bsd_unit_price,
-- 	price.hd_unit_price,
-- 	price.quicksave_unit_price,
-- 	price.edlp_dispensing_fee_margin,
-- 	price.bsd_dispensing_fee_margin,
-- 	price.hd_dispensing_fee_margin,
-- 	price.quicksave_dispensing_fee_margin
-- FROM
-- 	(SELECT
-- 			d1.gcn,
-- 			d1.medid,
-- 			d1.gcn_seqno,
-- 			d1.gcn_symphony_2017_rank,
-- 			d1.gcn_symphony_2017_fills,
-- 			d2.default_quantity,
-- 			d1.strength,
-- 			d1.dosage_form_desc,
-- 			d1.generic_name_long
-- 		FROM
-- 			dwh.dim_gcn_seqno_hierarchy as d1
-- 		INNER JOIN
-- 			dwh.dim_medid_hierarchy as d2
-- 		ON
-- 			d1.medid = d2.medid
-- 			AND d1.gcn_seqno = d2.gcn_seqno) AS ms
-- INNER JOIN
-- 	(SELECT
-- 		gcn,
-- 		medid,
-- 		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.unit_price ELSE 0 END  ) AS edlp_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.unit_price ELSE 0 END  ) AS bsd_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.unit_price ELSE 0 END  ) AS hd_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.unit_price ELSE 0 END  ) AS quicksave_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS edlp_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS bsd_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS hd_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS quicksave_dispensing_fee_margin
-- 	FROM
-- 		transactional.med_price
-- 	WHERE
-- 		ended_on IS NULL
-- 	GROUP BY
-- 		gcn,medid) price
-- ON
-- 	ms.gcn = price.gcn
-- 	AND ms.medid = price.medid
-- ORDER BY
-- 	gcn_symphony_2017_rank ASC, ms.medid ASC
--
-- select mp.gcn,dgsh.gcn_seqno,started_on::timestamp::date,
--     case
--     	when pharmacy_network_id is null then 'edlp'
--         when pharmacy_network_id = 1 then 'edlp'
--         when pharmacy_network_id = 2 then 'bsd'
--         when pharmacy_network_id = 3 then 'hd'
--         when pharmacy_network_id = 4 then 'quicksave'
--         else 'unknown'
--     end as pharmacy_network_name
-- from transactional.med_price as mp
-- inner join dwh.dim_gcn_seqno_hierarchy as dgsh
-- on
-- 	mp.gcn = dgsh.gcn AND mp.medid = dgsh.medid
-- where
-- ended_on is null and
-- mp.gcn in ('10810',
-- '10857',
-- '16513',
-- '18010',
-- '18996',
-- '18996',
-- '25940',
-- '25940',
-- '26320',
-- '26322',
-- '26323',
-- '26324',
-- '26326',
-- '26327',
-- '47631',
-- '47632',
-- '26328',
-- '7221',
-- '31164',
-- '98921')
--
--
-- -- Peso Median
-- SELECT  AVG(1.0E * x)
-- FROM    (
--             SELECT  x,
--                     2 * ROW_NUMBER() OVER (ORDER BY x) - COUNT(*) OVER () AS y
--             FROM    @Foo
--         ) AS d
-- WHERE   y BETWEEN 0 AND 2
--
--
-- -- Peso Weighted Median
-- SELECT  SUM(1.0E * y) / SUM(1.0E * t)
-- FROM    (
--             SELECT  SUM(x) OVER (PARTITION BY x) AS y,
--                     2 * ROW_NUMBER() OVER (ORDER BY x) - COUNT(*) OVER () AS z,
--                     COUNT(*) OVER (PARTITION BY x) AS t
--             FROM    @Foo
--         ) AS d
-- WHERE   z BETWEEN 0 AND 2
-- -- select data,rank() over ( order by data), count(*) over (), 2 * rank() over ( order by data) - count(*) over () as y from mktg_dev.tmp

-- SELECT
-- 	data_name,
-- 	rank() OVER (ORDER BY data_name),
-- 	data_count,
-- 	100 * sum(data_count) OVER (ORDER BY data_name ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / sum(data_count) OVER ()
-- FROM
-- 	mktg_dev.tmp_1;

-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price AS mp
-- 	JOIN dwh.dim_gcn_seqno_hierarchy AS dgsh ON mp.gcn = dgsh.gcn
-- 		AND mp.medid = dgsh.medid
-- WHERE
-- 	started_on >= '2019-12-28'
-- 	AND ended_on IS NULL
-- 	AND branded = 0
-- 	AND pharmacy_network_id = 1;

-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price
-- WHERE
-- 	started_on >= '2019-12-01'
-- -- 	AND gcn = 57902
-- -- 	AND medid = 170033
-- 	AND pharmacy_network_id = 1
-- 	AND started_on<>ended_on
-- 	AND branded = 0
-- ORDER BY
-- 	started_on DESC;

-- SELECT
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS "fill date",
-- 	sum(1) AS fills
-- FROM
-- 	dwh.fact_order_item foi
-- 	LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
-- 		AND du.is_internal = FALSE
-- 		AND du.is_phantom = FALSE
-- 	LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
-- 	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
-- WHERE (foi.fill_sequence IS NOT NULL)
-- AND foi.is_fraud = FALSE
-- AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-12-15'
-- -- AND foi.last_pbm_adjudication_timestamp_approved + INTERVAL '120 day' >= CURRENT_DATE
-- GROUP BY
-- 	1
-- order by 1 DESC
-- ;



-- SELECT
-- 	npc.*
-- FROM
-- 	drugs_etl.network_pricing_mac AS npc
-- 	JOIN dwh.dim_gcn_seqno_hierarchy AS dgsh ON npc.gcn_seqno = dgsh.gcn_seqno
-- WHERE
-- 	dgsh.gcn = 2326
-- 	AND end_date IS NULL
;

-- select * from dwh.dim_gcn_seqno_hierarchy;

-- SELECT
-- 	apr.*,
-- 	dgsh.generic_name_short,
-- 	dgsh.generic_name_long,
-- 	dgsh.gcn_symphony_2017_rank,
-- 	dgsh.dosage_form_desc,
-- 	dgsh.dosage_form_code_desc,
-- 	route_desc
-- FROM
-- 	mktg_dev.sdey_automated_pricing_recommendations  as apr 
-- LEFT OUTER JOIN
-- 	dwh.dim_gcn_seqno_hierarchy as dgsh
-- ON
-- 	apr.gcn = dgsh.gcn 
-- 	AND apr.med_id = dgsh.medid
-- ;


-- with multiple_prices as (
-- 	SELECT
-- 		gcn,
-- 		pharmacy_network_id,
-- 		count(DISTINCT trunc(unit_price,3)) as unit_prices,
-- 		count(DISTINCT trunc(dispensing_fee_margin,3)) as dfms
-- 	FROM
-- 		transactional.med_price
-- 	WHERE
-- 		ended_on is NULL
-- 		AND branded = 0 
-- 	GROUP BY
-- 		gcn,
-- 		pharmacy_network_id
-- 	HAVING
-- 		unit_prices > 2 or dfms >= 2 
-- )
-- select 
-- -- 	med_price.id,
-- 	dgsh.generic_name_short,
-- -- 	dmh.is_branded_price,
-- -- 	dmh.med_name,
-- 	mps.gcn,
-- 	med_price.gcn,
-- 	med_price.medid,
-- 	mps.pharmacy_network_id,
-- 	unit_price,
-- 	dispensing_fee_margin,
-- 	med_price.branded,
-- 	started_on,
-- 	ended_on
-- FROM
-- 	multiple_prices as mps
-- INNER JOIN
-- 	transactional.med_price as med_price
-- ON
-- 	mps.gcn = med_price.gcn
-- 	and mps.pharmacy_network_id = med_price.pharmacy_network_id
-- INNER JOIN
-- 	dwh.dim_gcn_seqno_hierarchy AS dgsh
-- ON
-- 	dgsh.medid = med_price.medid AND
-- 	dgsh.gcn = med_price.gcn
-- -- INNER JOIN
-- -- 	dwh.dim_medid_hierarchy AS dmh
-- -- ON
-- -- 	dmh.medid = med_price.medid 
-- WHERE
-- 	ended_on is NULL
-- 	AND branded = 0 
-- ORDER BY
-- 	mps.gcn;







-- SELECT
-- 	dgsh.gcn,
-- 	dgsh.gcn_seqno,
-- 	dgsh.gcn_symphony_2017_rank,
-- 	dgsh.gcn_symphony_2017_fills,
-- 	dgsh.strength,
-- 	dgsh.dosage_form_desc,
-- 	dgsh.generic_name_long,
-- 	CASE
-- 		when datamart.default_quantity = cmp1.quantity then 'Default'
-- 		when datamart.top_30ds_quantity = cmp1.quantity then '30 Day Qty'
-- 		when datamart.top_90ds_quantity = cmp1.quantity then '90 Day Qty'
-- 		else 'none'
-- 	END	AS quantity_type,
-- 	cmp1.quantity as quantity,
-- 	cmp1.price_min as price_min_new,
-- 	cmp1.walmart_min as walmart_new,
-- 	cmp1.cvs_min as cvs_new,
-- 	cmp1.walgreens_min as walgreens_new,
-- 	cmp1.rite_aid_min as riteaid_new,
-- 	cmp1.kroger_min as kroger_new,
-- 	cmp2.price_min as price_min_old,
-- 	cmp2.walmart_min as walmart_old,
-- 	cmp2.cvs_min as cvs_old,
-- 	cmp2.walgreens_min as walgreens_old,
-- 	cmp2.rite_aid_min as riteaid_old,
-- 	cmp2.kroger_min as kroger_old,
-- 	mp.dispensing_fee_margin,
-- 	mp.unit_price
-- FROM
-- 	(SELECT
-- 		gcn,
-- 		gcn_seqno,
-- 		gcn_symphony_2017_rank,
-- 		gcn_symphony_2017_fills,
-- 		strength,
-- 		dosage_form_desc,
-- 		generic_name_long,
-- 		medid
-- 	FROM
-- 		dwh.dim_gcn_seqno_hierarchy
-- 	WHERE
-- 		gcn_symphony_2017_rank < 1000) AS dgsh
-- INNER JOIN
-- 	transactional.med_price AS mp
-- ON
-- 	mp.gcn = dgsh.gcn
-- 	AND mp.medid = dgsh.medid
-- 	AND mp.pharmacy_network_id = 1
-- 	AND ended_on is NULL
-- INNER JOIN 
-- 	fifo.generic_price_portfolio_datamart AS datamart
-- ON
-- 	dgsh.gcn = datamart.gcn
-- 	AND dgsh.gcn_seqno = datamart.gcn_seqno
-- INNER JOIN 
-- 	(SELECT
-- 		gcn,
-- 		quantity,
-- 		MIN(price) price_min,
-- 		MIN(CASE WHEN pharmacy = 'walmart' THEN price ELSE 1000000000000 END) AS walmart_min,
-- 		MIN(CASE WHEN pharmacy = 'cvs' THEN price ELSE 1000000000000 END) AS cvs_min,
-- 		MIN(CASE WHEN pharmacy = 'walgreens' THEN price ELSE 1000000000000 END) AS walgreens_min,
-- 		MIN(CASE WHEN pharmacy = 'rite_aid' THEN price ELSE 1000000000000 END) AS rite_aid_min,
-- 		MIN(CASE WHEN pharmacy = 'kroger' THEN price ELSE 1000000000000 END) AS kroger_min
-- 	FROM
-- 		api_scraper_external.competitor_pricing	
-- 	WHERE
-- 		site != 'all'
-- 		AND geo != 'all'
-- 		AND pharmacy != 'all'
-- 		AND pharmacy != 'all_major'
-- 		AND pharmacy != 'all_preferred'
-- 		AND pharmacy != 'other_pharmacies'
-- 		AND site = 'goodrx'
-- 		AND date = '2019-12-05'
-- 	GROUP BY
-- 		1,2) AS cmp1
-- ON
-- 	dgsh.gcn = cmp1.gcn
-- 	AND (cmp1.quantity = datamart.default_quantity OR cmp1.quantity = datamart.top_30ds_quantity or cmp1.quantity = datamart.top_90ds_quantity )
-- INNER JOIN
-- 	(SELECT
-- 		gcn,
-- 		quantity,
-- 		MIN(price) price_min,
-- 		MIN(CASE WHEN pharmacy = 'walmart' THEN price ELSE 1000000000000 END) AS walmart_min,
-- 		MIN(CASE WHEN pharmacy = 'cvs' THEN price ELSE 1000000000000 END) AS cvs_min,
-- 		MIN(CASE WHEN pharmacy = 'walgreens' THEN price ELSE 1000000000000 END) AS walgreens_min,
-- 		MIN(CASE WHEN pharmacy = 'rite_aid' THEN price ELSE 1000000000000 END) AS rite_aid_min,
-- 		MIN(CASE WHEN pharmacy = 'kroger' THEN price ELSE 1000000000000 END) AS kroger_min
-- 	FROM
-- 		api_scraper_external.competitor_pricing	
-- 	WHERE
-- 		site != 'all'
-- 		AND geo != 'all'
-- 		AND pharmacy != 'all'
-- 		AND pharmacy != 'all_major'
-- 		AND pharmacy != 'all_preferred'
-- 		AND pharmacy != 'other_pharmacies'
-- 		AND site = 'goodrx'
-- 		AND date = '2019-10-31'
-- 	GROUP BY
-- 		1,2) AS cmp2		
-- ON
-- 	cmp1.gcn = cmp2.gcn	
-- 	AND cmp1.quantity = cmp2.quantity
-- ;




-- SELECT
-- 	date,
-- 	count(DISTINCT geo),
-- 	count(*)
-- FROM
-- 	api_scraper_external.competitor_pricing
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-09-01'
-- group BY
-- 	1
-- ;
-- select pharmacy,count(*) as cnt from api_scraper_external.competitor_pricing where pharmacy not like '%all%' and date >= '2019-12-01' group by 1 order by cnt desc;
-- SELECT DISTINCT
-- 	gcn,
-- 	branded,
-- 	deleted,
-- 	started_on::timestamp::date,
-- 	CASE WHEN pharmacy_network_id IS NULL THEN
-- 		'edlp'
-- 	WHEN pharmacy_network_id = 1 THEN
-- 		'edlp'
-- 	WHEN pharmacy_network_id = 2 THEN
-- 		'bsd'
-- 	WHEN pharmacy_network_id = 3 THEN
-- 		'hd'
-- 	WHEN pharmacy_network_id >= 4 THEN
-- 		'quicksave'
-- 	ELSE
-- 		'unknown'
-- 	END AS pharmacy_network_name
-- FROM
-- 	transactional.med_price
-- WHERE
-- 	started_on::timestamp::date >= '2019-01-01';
-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price
-- WHERE
-- 	started_on::timestamp::date = '2019-12-28';

-- SELECT
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', last_pbm_adjudication_timestamp_approved)::timestamp::date AS "fill date",
-- 	sum(1) AS fills
-- FROM
-- 	dwh.fact_order_item
-- WHERE fill_sequence IS NOT NULL
-- 	AND is_fraud = FALSE
-- 	AND CONVERT_TIMEZONE ('UTC', 'America/New_York', last_pbm_adjudication_timestamp_approved)::timestamp::date > '2019-12-20'
-- GROUP BY
-- 	1
-- order by 
-- 	1 DESC
-- 	;


-- SELECT
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS "fill date",
-- 	foi.pharmacy_network_name AS "phamramcy network name",
-- 	foi.med_id AS "med id",
-- 	foi.generic_medid AS "generic medid",
-- 	p_medid.med_name AS "purchased med name",
-- 	f_medid.med_name AS "filled med name",
-- 	p_gcn.generic_name_short AS "purchased generic name short",
-- 	f_gcn.generic_name_short AS "filled generic name short",
-- 	p_gcn.strength AS "purchased strength",
-- 	f_gcn.strength AS "filled strength",
-- 	p_gcn.gcn AS "purchased gcn",
-- 	p_gcn.gcn_seqno AS "purchased gcn seqno",
-- 	f_gcn.gcn AS "filled gcn",
-- 	f_gcn.gcn_seqno AS "filled gcn seqno",
-- 	CASE WHEN last_claim_days_supply_approved < 84 THEN
-- 		30
-- 	ELSE
-- 		90
-- 	END AS "days supply normalized",
-- 	CASE WHEN (coalesce(foi.last_claim_med_price_approved, 0) + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0) - coalesce(foi.last_pricing_total_cost_approved, 0) - coalesce(foi.last_claim_wmt_true_up_amount_approved, 0) < 0.0) THEN
-- 		TRUE
-- 	ELSE
-- 		FALSE
-- 	END AS "is_negative_revenue_fill",
-- 	sum(1) AS fills,
-- 	count(DISTINCT foi.dw_user_id) AS "uniq users",
-- 	count(DISTINCT order_id) AS "uniq orders",
-- 	sum(coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float) AS "gross revenue",
-- 	sum(coalesce(foi.last_claim_med_price_approved, 0) + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0) - coalesce(foi.last_pricing_total_cost_approved, 0) - coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)) AS "gross margin",
-- 	sum(last_claim_days_supply_approved) AS "total days supply"
-- FROM
-- 	dwh.fact_order_item foi
-- 	LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
-- 		AND du.is_internal = FALSE
-- 		AND du.is_phantom = FALSE
-- 	LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
-- 	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
-- WHERE (foi.fill_sequence IS NOT NULL)
-- AND foi.is_fraud = FALSE
-- AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-12-01'
-- -- AND foi.last_pbm_adjudication_timestamp_approved + INTERVAL '120 day' >= CURRENT_DATE
-- GROUP BY
-- 	1,
-- 	2,
-- 	3,
-- 	4,
-- 	5,
-- 	6,
-- 	7,
-- 	8,
-- 	9,
-- 	10,
-- 	11,
-- 	12,
-- 	13,
-- 	14,
-- 	15,
-- 	16
-- order by "gross margin"
-- 	;
-- SELECT
-- -- 	date_trunc('day', CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)) AS fill_date,
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS fill_date,
-- 	foi.med_id AS purchased_medid,
-- 	foi.last_claim_medid_approved AS filled_medid,
-- 	p_medid.med_name AS purchased_med_name,
-- 	f_medid.med_name AS filled_med_name,
-- 	foi.gcn AS purchased_gcn,
-- 	foi.last_claim_gcn_approved AS filled_gcn,
-- 	p_gcn.generic_name_short AS purchased_generic_name_short,
-- 	f_gcn.generic_name_short AS filled_generic_name_short,
-- 	foi.quantity AS purchased_quantity,
-- 	foi.last_claim_quantity_approved AS filled_quantity,
-- 	foi.price AS purchased_price,
-- 	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float AS filled_price_paid_aka_realized_gross_revenue,
-- 	foi.pharmacy_network_id AS purchased_pharmacy_network_id,
-- 	CASE WHEN pharmacy_network_id IS NULL
-- 		OR pharmacy_network_id = 1 THEN
-- 		'EDLP'
-- 	WHEN pharmacy_network_id = 2 THEN
-- 		'BSD'
-- 	WHEN pharmacy_network_id = 3 THEN
-- 		'HD'
-- 	WHEN pharmacy_network_id = 4
-- 		OR pharmacy_network_id = 5 THEN
-- 		'quicksave'
-- 	ELSE
-- 		'NONE'
-- 	END AS purchased_pharmacy_network,
-- 	foi.last_claim_days_supply_approved AS filled_days_supply,
-- 	coalesce(last_pricing_total_cost_approved, 0)::float + coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS filled_ingredient_plus_dispensing_costs_aka_realized_cogs,
-- 	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float - coalesce(last_pricing_total_cost_approved, 0)::float - coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS realized_gross_profit,
-- 	coalesce(foi.order_medication_discount_amount, 0)::float + coalesce(foi.allocated_order_discount_amount, 0)::float + coalesce(foi.allocated_wallet_payment_amount, 0)::float AS filled_discounts
-- FROM
-- 	dwh.fact_order_item foi
-- 	LEFT JOIN dwh.dim_user du ON foi.account_id = du.account_id
-- 	-- i've also see a foi.dw_user_id = du.dw_user_id, should that be added?
-- 		AND du.is_internal = FALSE -- removes internal users
-- 		AND du.is_phantom = FALSE -- remove phantom users
-- 	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
-- 	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno -- add’l info for med purchased on the order, (e.g. generic med name)
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno -- add’l info for med actually filled, (e.g. generic med name)
-- WHERE
-- 	foi.is_fraud = FALSE
-- 	AND foi.last_pbm_adjudication_timestamp_approved IS NOT NULL
-- 	AND foi.quantity != foi.last_claim_quantity_approved
-- 	AND foi.gcn != foi.last_claim_gcn_approved
-- 	AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-01-01'
-- ;
-- SELECT
-- 	ms.gcn,
-- 	ms.medid,
-- 	ms.gcn_seqno,
-- 	ms.gcn_symphony_2017_rank,
-- 	ms.gcn_symphony_2017_fills,
-- 	ms.default_quantity,
-- 	ms.strength,
-- 	ms.dosage_form_desc,
-- 	ms.generic_name_long,
-- 	price.edlp_unit_price,
-- 	price.bsd_unit_price,
-- 	price.hd_unit_price,
-- 	price.quicksave_unit_price,
-- 	price.edlp_dispensing_fee_margin,
-- 	price.bsd_dispensing_fee_margin,
-- 	price.hd_dispensing_fee_margin,
-- 	price.quicksave_dispensing_fee_margin
-- FROM
-- 	(SELECT
-- 			d1.gcn,
-- 			d1.medid,
-- 			d1.gcn_seqno,
-- 			d1.gcn_symphony_2017_rank,
-- 			d1.gcn_symphony_2017_fills,
-- 			d2.default_quantity,
-- 			d1.strength,
-- 			d1.dosage_form_desc,
-- 			d1.generic_name_long
-- 		FROM
-- 			dwh.dim_gcn_seqno_hierarchy as d1
-- 		INNER JOIN
-- 			dwh.dim_medid_hierarchy as d2
-- 		ON
-- 			d1.medid = d2.medid
-- 			AND d1.gcn_seqno = d2.gcn_seqno) AS ms
-- INNER JOIN
-- 	(SELECT
-- 		gcn,
-- 		medid,
-- 		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.unit_price ELSE 0 END  ) AS edlp_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.unit_price ELSE 0 END  ) AS bsd_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.unit_price ELSE 0 END  ) AS hd_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.unit_price ELSE 0 END  ) AS quicksave_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS edlp_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS bsd_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS hd_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS quicksave_dispensing_fee_margin
-- 	FROM
-- 		transactional.med_price
-- 	WHERE
-- 		ended_on IS NULL
-- 	GROUP BY
-- 		gcn,medid) price
-- ON
-- 	ms.gcn = price.gcn
-- 	AND ms.medid = price.medid
-- ORDER BY
-- 	gcn_symphony_2017_rank ASC, ms.medid ASC
--
-- select mp.gcn,dgsh.gcn_seqno,started_on::timestamp::date,
--     case
--     	when pharmacy_network_id is null then 'edlp'
--         when pharmacy_network_id = 1 then 'edlp'
--         when pharmacy_network_id = 2 then 'bsd'
--         when pharmacy_network_id = 3 then 'hd'
--         when pharmacy_network_id = 4 then 'quicksave'
--         else 'unknown'
--     end as pharmacy_network_name
-- from transactional.med_price as mp
-- inner join dwh.dim_gcn_seqno_hierarchy as dgsh
-- on
-- 	mp.gcn = dgsh.gcn AND mp.medid = dgsh.medid
-- where
-- ended_on is null and
-- mp.gcn in ('10810',
-- '10857',
-- '16513',
-- '18010',
-- '18996',
-- '18996',
-- '25940',
-- '25940',
-- '26320',
-- '26322',
-- '26323',
-- '26324',
-- '26326',
-- '26327',
-- '47631',
-- '47632',
-- '26328',
-- '7221',
-- '31164',
-- '98921')
--
--
-- -- Peso Median
-- SELECT  AVG(1.0E * x)
-- FROM    (
--             SELECT  x,
--                     2 * ROW_NUMBER() OVER (ORDER BY x) - COUNT(*) OVER () AS y
--             FROM    @Foo
--         ) AS d
-- WHERE   y BETWEEN 0 AND 2
--
--
-- -- Peso Weighted Median
-- SELECT  SUM(1.0E * y) / SUM(1.0E * t)
-- FROM    (
--             SELECT  SUM(x) OVER (PARTITION BY x) AS y,
--                     2 * ROW_NUMBER() OVER (ORDER BY x) - COUNT(*) OVER () AS z,
--                     COUNT(*) OVER (PARTITION BY x) AS t
--             FROM    @Foo
--         ) AS d
-- WHERE   z BETWEEN 0 AND 2
-- -- select data,rank() over ( order by data), count(*) over (), 2 * rank() over ( order by data) - count(*) over () as y from mktg_dev.tmp

SELECT
	data_name,
	rank() OVER (ORDER BY data_name),
	data_count,
	100 * sum(data_count) OVER (ORDER BY data_name ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / sum(data_count) OVER ()
FROM
	mktg_dev.tmp_1;

-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price AS mp
-- 	JOIN dwh.dim_gcn_seqno_hierarchy AS dgsh ON mp.gcn = dgsh.gcn
-- 		AND mp.medid = dgsh.medid
-- WHERE
-- 	started_on >= '2019-12-28'
-- 	AND ended_on IS NULL
-- 	AND branded = 0
-- 	AND pharmacy_network_id = 1;

-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price
-- WHERE
-- 	started_on >= '2019-12-01'
-- -- 	AND gcn = 57902
-- -- 	AND medid = 170033
-- 	AND pharmacy_network_id = 1
-- 	AND started_on<>ended_on
-- 	AND branded = 0
-- ORDER BY
-- 	started_on DESC;

-- SELECT
-- 	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS "fill date",
-- 	sum(1) AS fills
-- FROM
-- 	dwh.fact_order_item foi
-- 	LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
-- 		AND du.is_internal = FALSE
-- 		AND du.is_phantom = FALSE
-- 	LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
-- 	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
-- 	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
-- WHERE (foi.fill_sequence IS NOT NULL)
-- AND foi.is_fraud = FALSE
-- AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-12-15'
-- -- AND foi.last_pbm_adjudication_timestamp_approved + INTERVAL '120 day' >= CURRENT_DATE
-- GROUP BY
-- 	1
-- order by 1 DESC
-- ;



-- SELECT
-- 	npc.*
-- FROM
-- 	drugs_etl.network_pricing_mac AS npc
-- 	JOIN dwh.dim_gcn_seqno_hierarchy AS dgsh ON npc.gcn_seqno = dgsh.gcn_seqno
-- WHERE
-- 	dgsh.gcn = 2326
-- 	AND end_date IS NULL
;
