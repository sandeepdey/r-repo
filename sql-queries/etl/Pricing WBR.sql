-- Magic Card Vs BSD vs EDLP --

-- SELECT
-- 	unit_price,
-- 	dispensing_fee_margin
-- from 
-- 	transactional.med_price
-- WHERE


-- 	gcn = 16374
-- 	AND 287768		


-- JOIN

-- 	(select CURRENT_DATE - generate_series(2, 91,1) as target_date) AS dates


-- select gcn, medid , pharmacy_network_id,count(distinct(trunc(unit_price,3))) from transactional.med_price
-- where started_on::TIMESTAMP::DATE >= CURRENT_DATE  -  INTERVAL '90 day'
-- AND started_on::TIMESTAMP::DATE != ended_on::TIMESTAMP::DATE 
-- AND pharmacy_network_id <= 3 
-- group by 1 ,2,3 order by 4 desc

-- SELECT
-- 	*
-- FROM
-- 	transactional.med_price
-- WHERE
-- 	gcn = 16374 AND 287768
-- 	AND started_on::TIMESTAMP::DATE != ended_on::TIMESTAMP::DATE
-- 	AND pharmacy_network_id <= 3



-- with latest_price AS (
-- 	SELECT
-- 		gcn,
-- 		medid,
-- 		unit_price,
-- 		dispensing_fee_margin
-- 	FROM
-- 		transactional.med_price
-- 	WHERE
-- 		ended_on is NULL
-- )
-- select
-- 	lp.gcn,
-- 	lp.medid,
-- 	max(started_on)
-- FROM
-- 	latest_price lp
-- left outer JOIN
-- 	transactional.med_price mp
-- ON
-- 	lp.gcn = mp.gcn
-- 	and lp.medid = mp.medid
-- 	AND mp.started_on::date != mp.ended_on::date
-- 	and lp.unit_price != mp.ended_on
-- 	AND lp.dispensing_fee_margin != mp.dispensing_fee_margin
-- group BY
-- 	1,2



-- Competitive Data --

drop table if EXISTS mktg_dev.sdey_pricing_wbr_competition;
create table mktg_dev.sdey_pricing_wbr_competition as
with drug_details as (
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
	LEFT JOIN
		dwh.dim_gcn_seqno_hierarchy dgsh 
	ON
		dgsh.gcn = mp.gcn
		AND dgsh.medid = mp.medid
	LEFT JOIN
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
), 

gcn_revenue AS (
	SELECT
		foi.med_id,
		f_gcn.gcn,
		f_gcn.gcn_seqno,
		sum(1) as r90_fills,
		sum(quantity) as r90_quantities,
		sum(coalesce(last_claim_med_price_approved, 0) ::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0) ::float) AS r90_revenue,	
		sum(coalesce(foi.last_pricing_total_cost_approved, 0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)::float) AS r90_cogs
	FROM
		dwh.fact_order_item foi
		LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
			AND du.is_internal = FALSE
			AND du.is_phantom = FALSE
		LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
		LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
		LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
	WHERE (
		foi.fill_sequence IS NOT NULL)
	AND foi.is_fraud = FALSE
	AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '90 day' >= CURRENT_DATE
	GROUP BY
		1,2,3
), 

with_drug_list AS (
	SELECT
		dd.gcn,
		dd.gcn_seqno,	
		dd.medid,
		generic_name_short,
		strength,
		dosage_form_code,
		edlp_unit_price,
		bsd_unit_price,
		hd_unit_price,
		edlp_fixed_price,
		bsd_fixed_price,
		hd_fixed_price,
		bh01_mac_price,
		bh02_mac_price,
		bh03_mac_price,
		wmt_mac_price,	
		hd_mac_price,
		edlp_dispensing_fee,
		bsd_dispensing_fee,
		hd_dispensing_fee,
		glwr.r90_fills,
		r90_quantities,
		r90_revenue,	
		r90_cogs,
		gcn_symphony_2017_rank,
		gcn_symphony_2017_fills,
		wmt_4_dollar_list,
		wmt_9_dollar_list,
		wmt_list,
		fill_rank,
		gdqm.quantity as default_quantity,
		wtd_10th_percentile_benchmark,
		wtd_10th_percentile_phamrmacy,
		universal_price_benchmark,
		major_retailer_price_benchmark,
		walmart_price_benchmark
	FROM
		drug_details dd
	LEFT OUTER JOIN
		gcn_revenue gr
	ON
		dd.gcn = gr.gcn 
		AND dd.gcn_seqno = gr.gcn_seqno
		AND dd.medid = gr.med_id	
	LEFT OUTER JOIN
		mktg_dev.sdey_gcn_list_wmt_ranked  glwr
	ON
		dd.gcn = glwr.gcn 
		AND dd.gcn_seqno = glwr.gcn_seqno
	LEFT OUTER JOIN
		mktg_dev.sdey_gcn_default_quantity_mapping AS gdqm
	ON
		dd.gcn = gdqm.gcn
	LEFT OUTER JOIN
		mktg_dev.sdey_weighted_competitive_index AS wci
	ON
		dd.gcn = wci.gcn
		AND default_quantity = wci.quantity
		AND scrape_date = '2020-01-16'	
) SELECT * from with_drug_list;


GRANT SELECT ON mktg_dev.sdey_pricing_wbr_competition TO "public";
select count(*) from mktg_dev.sdey_pricing_wbr_competition;





