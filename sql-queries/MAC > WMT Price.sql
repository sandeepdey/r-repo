drop. table if
create table mktg_dev.sdey_tmp_table_2 as 
with fills_90_days as (
	SELECT
		foi.med_id,
		f_gcn.gcn,
		f_gcn.gcn_seqno,
		foi.pharmacy_network_name,
		foi.last_claim_pharmacy_name_approved as pharmacy_name,
		sum(1) AS fills,
		sum(coalesce(last_claim_med_price_approved, 0) ::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0) ::float) AS revenue,
		sum(coalesce(foi.last_claim_med_price_approved, 0) + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0) - coalesce(foi.last_pricing_total_cost_approved, 0) - coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)) AS margin
	FROM
		dwh.fact_order_item foi
		LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
			AND du.is_internal = FALSE
			AND du.is_phantom = FALSE
		LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
		LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno
		LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
		LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
		LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
	WHERE (
		foi.fill_sequence IS NOT NULL)
	AND foi.is_fraud = FALSE
	AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '90 day' >= CURRENT_DATE
	GROUP BY
		1,
		2,
		3,
		4,
		5
), fills_by_name_network as (
	SELECT
		med_id,
		gcn,
		gcn_seqno,
		sum(fills) as r90_fills,
		sum(revenue) as r90_revenue,
		sum(margin) as r90_margin,
		sum(case when pharmacy_network_name='blink' then fills else 0 end) as r90_edlp_fills,
		sum(case when pharmacy_network_name='supersaver' then fills else 0 end) as r90_bsd_fills,
		sum(case when pharmacy_network_name='delivery' then fills else 0 end) as r90_hd_fills,
		sum(case when pharmacy_network_name='blink' then revenue else 0 end) as r90_edlp_revenue,
		sum(case when pharmacy_network_name='supersaver' then revenue else 0 end) as r90_bsd_revenue,
		sum(case when pharmacy_network_name='delivery' then revenue else 0 end) as r90_hd_revenue,
		sum(case when pharmacy_network_name='blink' then margin else 0 end) as r90_edlp_margin,
		sum(case when pharmacy_network_name='supersaver' then margin else 0 end) as r90_bsd_margin,
		sum(case when pharmacy_network_name='delivery' then margin else 0 end) as r90_hd_margin,
		sum(case when pharmacy_network_name='blink' and pharmacy_name='Walmart' then fills else 0 end) as r90_wmt_fills,
		sum(case when pharmacy_network_name='blink' and pharmacy_name='Walmart' then revenue else 0 end) as r90_wmt_revenue,
		sum(case when pharmacy_network_name='blink' and pharmacy_name='Walmart' then margin else 0 end) as r90_wmt_margin
	from 
		fills_90_days
	group BY
		1,2,3
), drug_details as (
	SELECT
		dgsh.gcn,
		dgsh.gcn_seqno,	
		dgsh.medid,
		dgsh.dosage_form_desc,
		dgsh.dosage_form_code_desc,
		dgsh.generic_name_short,
		dgsh.strength,
		dgsh.gcn_symphony_2017_rank,
		dgsh.gcn_symphony_2017_fills,
		gppt.default_quantity,
-- 		wci.store_price as competitive_benchmark,
-- 		wci.pharmacy as competitive_benchmark_pharmacy,
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
		dwh.dim_gcn_seqno_hierarchy dgsh
	JOIN
		transactional.med_price mp 
	ON
		dgsh.gcn = mp.gcn
		AND dgsh.medid = mp.medid
	JOIN
		transactional.network_pricing_mac mac
	ON
		mac.gcn_seqno = dgsh.gcn_seqno
	JOIN
		fifo.generic_price_portfolio_tracker gppt
	ON
		gppt.gcn = dgsh.gcn 
	WHERE
		mp.ended_on is null
		AND mac.end_date is NULL
		AND gppt.default_quantity is not NULL
	GROUP BY
		1,2,3,4,5,6,7,8,9,10
-- 	LEFT OUTER JOIN
-- 		mktg_dev.sdey_weighted_competitive_index wci 
-- 	ON
-- 		dgsh.gcn = wci.gcn
-- 		and gppt.default_quantity = wci.quantity
) 
SELECT
		drug_details.gcn,
		drug_details.gcn_seqno,	
		drug_details.medid,
		dosage_form_desc,
		generic_name_short,
		strength,
		gcn_symphony_2017_rank,
		gcn_symphony_2017_fills,
		default_quantity,
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
		r90_fills,
		r90_revenue,
		r90_margin,
		r90_edlp_fills,
		r90_bsd_fills,
		r90_hd_fills,
		r90_edlp_revenue,
		r90_bsd_revenue,
		r90_hd_revenue,
		r90_edlp_margin,
		r90_bsd_margin,
		r90_hd_margin,
		r90_wmt_fills,
		r90_wmt_revenue,
		r90_wmt_margin
FROM
	drug_details
LEFT OUTER JOIN
	fills_by_name_network
ON
	drug_details.gcn = fills_by_name_network.gcn 
	AND drug_details.gcn_seqno = fills_by_name_network.gcn_seqno 
	AND drug_details.medid = fills_by_name_network.med_id 
;


SELECT
	*
FROM
	transactional.network_pricing_mac
WHERE
	(end_date is NULL OR end_date > '2019-09-01')
	AND gcn_seqno = 34468
ORDER BY mac_list, start_date
	;


SELECT
	*
FROM
	transactional.med_price
WHERE
	(ended_on is NULL OR ended_on > '2019-09-01')
	AND gcn_seqno = 34468
ORDER BY pharmacy_network_id, started_on
	;

	
-- 	SELECT
-- 		cp.gcn,
-- 		cp.quantity,
-- 		cp.pharmacy,

-- 		MEDIAN(cp.price) AS store_price,
-- 		AVG(COALESCE(pct.store_count,1)) AS store_count
-- 	FROM
-- 		api_scraper_external.competitor_pricing cp
-- 	LEFT OUTER JOIN
-- 		mktg_dev.sdey_pharmacy_count_table pct
-- 	ON
-- 		cp.pharmacy = pct.pharmacy_short_name
-- 	WHERE
-- 		geo != 'all'
-- 		AND pharmacy NOT LIKE '%all%'
-- 		AND pharmacy != 'other_pharmacies'
-- 		AND site = 'goodrx'
-- 		AND date = '2020-01-03'
-- 		AND (gcn=1431 OR gcn=10857) 
-- 		AND quantity=30
-- 	GROUP BY
-- 		1,2,3
