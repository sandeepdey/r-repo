drop table if EXISTS mktg_dev.sdey_weighted_competitive_index;
create table mktg_dev.sdey_weighted_competitive_index as
with comp_data_with_pharmacy_count AS (
	SELECT
		cp.gcn,
		cp.quantity,
		cp.date as scrape_date,
		cp.pharmacy,
		AVG(cp.default_quantity) AS default_quantity,
		MEDIAN(cp.price) AS store_price,
		AVG(COALESCE(pct.store_count,1)) AS store_count
	FROM
		api_scraper_external.competitor_pricing cp
	LEFT OUTER JOIN
		mktg_dev.sdey_pharmacy_count_table pct
	ON
		cp.pharmacy = pct.pharmacy_short_name
	WHERE
		geo != 'all'
		AND pharmacy NOT LIKE '%all%'
		AND pharmacy != 'other_pharmacies'
		AND site = 'goodrx'
		AND date > '2019-07-01'
	GROUP BY
		1,2,3,4
), other_benchmarks AS (
	SELECT
		gcn,
		quantity,
		scrape_date,
		min(store_price) as universal_price_benchmark,
		min(case when pharmacy in ('cvs','walgreens','walmart','rite_aid','kroger','publix') then store_price else 100000000 end)  major_retailer_price_benchmark,
		min(case when pharmacy = 'walmart' then store_price else 100000000 end)  walmart_price_benchmark
	FROM	
		comp_data_with_pharmacy_count
	GROUP BY
		1,2,3
), comp_data_2 AS (
	SELECT
		gcn,
		quantity,
		scrape_date,
		pharmacy,
		store_count,
		default_quantity,
		store_price,
		dense_rank() OVER (PARTITION BY gcn,quantity,scrape_date ORDER BY store_price ASC ,store_count DESC) as store_rank,
		sum(store_count) OVER (PARTITION BY gcn,quantity,scrape_date ORDER BY store_price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)*100/sum(store_count) OVER (PARTITION BY gcn,quantity,scrape_date) AS store_price_weighted_percentile
	FROM
		comp_data_with_pharmacy_count
), comp_data_3 AS (
	SELECT
		a.gcn,
		a.quantity,
		a.scrape_date,
		a.default_quantity,
		a.store_price as wtd_10th_percentile_benchmark,
		a.pharmacy as wtd_10th_percentile_phamrmacy,
		universal_price_benchmark,
		major_retailer_price_benchmark,
		walmart_price_benchmark
	FROM
		comp_data_2 AS a
	JOIN
		(SELECT gcn, quantity,scrape_date,min(store_rank) AS min_rank FROM	comp_data_2 WHERE store_price_weighted_percentile >= 10 GROUP BY 1,2,3) AS b
	ON
		a.gcn = b.gcn
		AND a.quantity = b.quantity
		AND a.scrape_date = b.scrape_date
		AND a.store_rank = b.min_rank
	JOIN
		other_benchmarks
	ON
		a.gcn = other_benchmarks.gcn
		AND a.quantity = other_benchmarks.quantity
		AND a.scrape_date = other_benchmarks.scrape_date
)
	SELECT * from comp_data_3
	order by gcn,scrape_date
;

GRANT SELECT ON mktg_dev.sdey_weighted_competitive_index TO "public";
GRANT SELECT ON mktg_dev.sdey_pharmacy_count_table TO "public";


select count(*) from mktg_dev.sdey_weighted_competitive_index;

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
		dgsh.therapeutic_class_desc_generic as therapeutic_class,
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
		1,2,3,4,5,6,7
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
		walmart_price_benchmark,
		therapeutic_class
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





