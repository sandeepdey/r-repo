SELECT
	DISTINCT
-- 	med.medid,
-- 	available_med.type_description,
-- 	dim_medid_hierarchy.med_name_slug,
-- 	med.description,
	uu.form_name as goodrx_form_name,
-- 	med.billing_unit,
	uu.default_quantity as goodrx_default_quantity,
	uu.dosage as goodrx_dosage,
-- -- 	uu.drug_id as goodrx_drug_id,
-- -- 	uu.slug as goodrx_slug,
-- -- 	mdim.mapping_source,
-- 	med.dosage_form,
-- 	med.strength,
-- 	med.strength_uom,
	med.gcn,
	med.gcn_seqno,
	dim_gcn_seqno_hierarchy.gcn_symphony_2017_rank,
	dim_gcn_seqno_hierarchy.dosage_form_desc,
	dim_gcn_seqno_hierarchy.strength_long_desc,
	dim_gcn_seqno_hierarchy.generic_name_short
-- 	case when rp2.medid is null then False else True end as with_modified_retail_package
-- 	med_package.package_description,
-- 	med_package.package_size
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
;

-- SELECT
-- 	*
-- FROM
-- 	mktg_dev.sdey_goodrx_all_generic_drugids_info gagi
-- 	Inner JOIN ( 
-- 	SELECT 
-- 		gcn,drug_id
-- 	FROM
-- 		api_scraper.medid_drug_id_mapping mdim
-- 		INNER JOIN transactional.med ON med.medid = mdim.medid) map
-- ON
-- 	gagi.drug_id = map.drug_id
-- WHERE
-- 	map.gcn is null
-- ;


with gcn_info as (SELECT
	uu.form_name as goodrx_form_name,
	med.billing_unit,
	uu.default_quantity as goodrx_default_quantity,
	uu.dosage as goodrx_dosage,
	med.gcn,
	med.gcn_seqno,
	dim_gcn_seqno_hierarchy.gcn_symphony_2017_rank,
	dim_gcn_seqno_hierarchy.dosage_form_desc,
	dim_gcn_seqno_hierarchy.strength_long_desc,
	dim_gcn_seqno_hierarchy.generic_name_short
FROM
	mktg_dev.units_of_use_raw_data uu
	INNER JOIN api_scraper.medid_drug_id_mapping mdim ON uu.drug_id = mdim.drug_id
	INNER JOIN transactional.med ON med.medid = mdim.medid
	INNER JOIN transactional.available_med ON med.medid = available_med.medid
	INNER JOIN dwh.dim_gcn_seqno_hierarchy ON med.gcn = dim_gcn_seqno_hierarchy.gcn AND med.gcn_seqno = dim_gcn_seqno_hierarchy.gcn_seqno
	LEFT JOIN (select distinct(medid) from transactional.retail_package_v2 )  rp2 on rp2.medid = mdim.medid
	LEFT JOIN dwh.dim_medid_hierarchy ON dim_medid_hierarchy.medid = med.medid
GROUP BY 1,2,3,4,5,6,7,8,9,10
) 
SELECT
	gcn_info.*,
	unit_price,
	fixed_price
FROM
	gcn_info
	INNER JOIN (
		SELECT
			gcn,
			AVG(unit_price) AS unit_price,
			AVG(dispensing_fee_margin) AS dispensing_fee_margin
		FROM
			transactional.med_price
		WHERE
			pharmacy_network_id = 1 & ended_on IS NULL
		GROUP BY 1) AS price ON price.gcn = gcn_info.gcn;
	
