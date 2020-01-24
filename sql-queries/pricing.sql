
-- create table mktg_dev.sdey_filled_orders_2019_gcn as
-- SELECT
-- 	DATE_PART(year, CONVERT_TIMEZONE ('UTC', 'America/New_York', order_claim.ordered_timestamp))::integer AS ordered_timestamp_year,
-- 	gcn1.gcn_seqno,
-- 	gcn1.gcn,
-- 	COUNT(DISTINCT CASE WHEN order_claim.fill_sequence IS NOT NULL THEN
-- 			order_claim.order_id
-- 		ELSE
-- 			NULL
-- 		END) AS filled_orders
-- FROM
-- 	dwh.fact_order_item AS order_claim
-- 	LEFT JOIN dwh.dim_gcn_seqno_hierarchy AS gcn1 ON order_claim.last_claim_gcn_seqno_approved = gcn1.gcn_seqno
-- 	LEFT JOIN dwh.dim_user AS dim_user ON order_claim.dw_user_id = dim_user.dw_user_id
-- 		AND order_claim.account_id = dim_user.account_id
-- WHERE ((((order_claim.ordered_timestamp) >= ((CONVERT_TIMEZONE ('America/New_York', 'UTC', TIMESTAMP '2019-01-01')))
-- 			AND(order_claim.ordered_timestamp) < ((CONVERT_TIMEZONE ('America/New_York', 'UTC', DATEADD (year, 1, TIMESTAMP '2019-01-01')))))))
-- AND(NOT(gcn1.gcn_seqno IS NULL))
-- AND(NOT(gcn1.gcn IS NULL))
-- AND(dim_user.is_internal = FALSE
-- 	AND dim_user.is_phantom = FALSE
-- 	AND order_claim.is_fraud = FALSE)
-- GROUP BY
-- 	1,
-- 	2,
-- 	3
-- ORDER BY
-- 	4 DESC;

-- SELECT
-- 	gcn,
-- 	gcn_seqno,
-- 	hicl_seqno,
-- 	hicl_seqno_form,
-- 	hicl_desc,
-- 	brand_med_name,
-- 	gtc_desc,
-- 	stc_desc,
-- 	ctc_desc,
-- 	strength,
-- 	form,
-- 	maint_flg,
-- 	dea_flg,
-- 	opioid_flg,
-- 	available_med_flg,
-- 	tem_available_med_flg,
-- 	default_gcn_flag,
-- 	edlp_priced_flg,
-- 	bsd_priced_flg,
-- 	hd_priced_flg,
-- 	grx_tracked_flg,
-- 	blink_gcn_rank,
-- 	blink_hicl_gcn_rank,
-- 	blink_hicl_rank,
-- 	blink_r30_gcn_rank,
-- 	blink_r30_hicl_rank,
-- 	symphony_2017_scripts,
-- 	symphony_2017_pct_of_hicl_scripts,
-- 	blink_pct_of_hicl_scripts,
-- 	bh01_mac_price,
-- 	bh02_mac_price,
-- 	bh03_mac_price,
-- 	wmt_mac_price,
-- 	hd_syr_mac_price,
-- 	bh01_dispensing_fee,
-- 	bh01_dispensing_fee AS bh02_dispensing_fee,
-- 	bh01_dispensing_fee AS bh03_dispensing_fee,
-- 	wmt_dispensing_fee,
-- 	hd_syr_dispensing_fee,
-- 	gpi_gcn_mac_list_conflict_flg,
-- 	fixed_stripe_charge_cost,
-- 	variable_stripe_charge_cost,
-- 	pac_unit,
-- 	pac_low_unit,
-- 	pac_high_unit,
-- 	pac_retail_unit,
-- 	r90_wmt_unc_unit_cost,
-- 	r90_pblx_unc_unit_cost,
-- 	r90_beh_unc_unit_cost,
-- 	awp_unit_cost,
-- 	ger_wmt_mac,
-- 	wmt_script_pct,
-- 	edlp_script_pct,
-- 	bsd_script_pct,
-- 	hd_script_pct,
-- 	wmt_nfp_script_pct,
-- 	edlp_nfp_script_pct,
-- 	bsd_nfp_script_pct,
-- 	hd_nfp_script_pct,
-- 	edlp_fixed_price,
-- 	edlp_unit_price,
-- 	bsd_fixed_price,
-- 	bsd_unit_price,
-- 	hd_fixed_price,
-- 	hd_unit_price,
-- 	ltd_filling_patients,
-- 	ltd_scripts,
-- 	ltd_qty,
-- 	ltd_nfp,
-- 	ltd_nfp_scripts,
-- 	ltd_nfp_qty,
-- 	r30_scripts,
-- 	r30_qty,
-- 	r30_nfp_scripts,
-- 	r30_nfp_qty,
-- 	r30_wmt_scripts,
-- 	r30_wmt_qty,
-- 	r30_wmt_nfp_scripts,
-- 	r30_wmt_nfp_qty,
-- 	r30_edlp_scripts,
-- 	r30_edlp_qty,
-- 	r30_edlp_nfp_scripts,
-- 	r30_edlp_nfp_qty,
-- 	r30_bsd_scripts,
-- 	r30_bsd_qty,
-- 	r30_bsd_nfp_scripts,
-- 	r30_bsd_nfp_qty,
-- 	r30_hd_scripts,
-- 	r30_hd_qty,
-- 	r30_hd_nfp_scripts,
-- 	r30_hd_nfp_qty,
-- 	r30_bh01_edlp_scripts,
-- 	r30_bh01_edlp_qty,
-- 	r30_bh01_edlp_nfp_scripts,
-- 	r30_bh01_edlp_nfp_qty,
-- 	r30_bh02_edlp_scripts,
-- 	r30_bh02_edlp_qty,
-- 	r30_bh02_edlp_nfp_scripts,
-- 	r30_bh02_edlp_nfp_qty,
-- 	r30_bh03_edlp_scripts,
-- 	r30_bh03_edlp_qty,
-- 	r30_bh03_edlp_nfp_scripts,
-- 	r30_bh03_edlp_nfp_qty,
-- 	r30_bh01_bsd_scripts,
-- 	r30_bh01_bsd_qty,
-- 	r30_bh01_bsd_nfp_scripts,
-- 	r30_bh01_bsd_nfp_qty,
-- 	r30_bh02_bsd_scripts,
-- 	r30_bh02_bsd_qty,
-- 	r30_bh02_bsd_nfp_scripts,
-- 	r30_bh02_bsd_nfp_qty,
-- 	r30_bh03_bsd_scripts,
-- 	r30_bh03_bsd_qty,
-- 	r30_bh03_bsd_nfp_scripts,
-- 	r30_bh03_bsd_nfp_qty,
-- 	default_quantity,
-- 	blink_edlp_price,
-- 	blink_bsd_price,
-- 	blink_hd_price,
-- 	edlp_vs_bsd_gap,
-- 	edlp_vs_hd_gap,
-- 	bsd_vs_hd_gap,
-- 	min_grx,
-- 	min_retail_grx,
-- 	min_bh_retail_index_grx,
-- 	min_grx_northeast,
-- 	min_grx_south,
-- 	min_grx_midwest,
-- 	min_grx_west,
-- 	min_hwh_grx,
-- 	min_cvs_grx,
-- 	min_wag_grx,
-- 	min_wmt_grx,
-- 	min_rad_grx,
-- 	min_kr_grx,
-- 	min_sfwy_grx,
-- 	min_pblx_grx,
-- 	min_bksh_grx,
-- 	min_geagle_grx,
-- 	min_heb_grx,
-- 	lowest_tracked_price_grx_pharmacy,
-- 	lowest_tracked_grx_price,
-- 	lowest_tracked_grx_pharmacy_type,
-- 	wmt_2018_07_27_flg,
-- 	wmt_2018_07_27_qty1,
-- 	wmt_2018_07_27_price1,
-- 	wmt_2018_07_27_qty2,
-- 	wmt_2018_07_27_price2,
-- 	wmt_2018_11_28_flg,
-- 	wmt_2018_11_28_qty1,
-- 	wmt_2018_11_28_price1,
-- 	wmt_2018_11_28_qty2,
-- 	wmt_2018_11_28_price2,
-- 	wmt_retail_list_comp_price,
-- 	pblx_2018_10_12_flg,
-- 	pblx_2018_10_12_qty1,
-- 	pblx_2018_10_12_price1,
-- 	pblx_2018_10_12_qty2,
-- 	pblx_2018_10_12_price2,
-- 	wmt_est_unc_price,
-- 	pblx_est_unc_price,
-- 	beh_est_unc_price,
-- 	edlp_grx_price_leader,
-- 	edlp_min_retail_grx_price_leader,
-- 	edlp_min_bh_retail_index_grx_price_leader,
-- 	edlp_min_wmt_grx_price_leader,
-- 	edlp_vs_min_grx_gap,
-- 	edlp_vs_min_bh_retail_index_grx_gap,
-- 	edlp_vs_min_wmt_grx_gap,
-- 	bsd_vs_min_grx_gap,
-- 	bsd_vs_min_bh_retail_index_grx_gap,
-- 	bsd_vs_min_wmt_grx_gap,
-- 	hd_vs_min_grx_gap,
-- 	hd_vs_min_bh_retail_index_grx_gap,
-- 	hd_vs_min_wmt_grx_gap,
-- 	hd_vs_hwh_grx_gap,
-- 	hicl_normalized_12_mos_30ds_scripts_per_nfp,
-- 	hicl_form_normalized_12_mos_30ds_scripts_per_nfp,
-- 	gcn_normalized_12_mos_30ds_scripts_per_nfp,
-- 	projected_gcn_12_mos_30ds_normalized_scripts_from_nfp_scripts,
-- 	r30_gcn_viewed_product_sessions,
-- 	r30_gcn_purchased_product_sessions,
-- 	r30_gcn_filled_product_sessions,
-- 	r30_gcn_purchased_cvr,
-- 	r30_gcn_filled_cvr,
-- 	r30_gcn_purchase_to_fill_rate,
-- 	r30_wmt_gross_revenue,
-- 	r30_wmt_cogs,
-- 	r30_wmt_ger_true_up,
-- 	r30_wmt_gp,
-- 	r30_wmt_gp_net_tu,
-- 	r30_wmt_awp_filled,
-- 	r30_wmt_mac_paid,
-- 	r30_edlp_gross_revenue,
-- 	r30_edlp_cogs,
-- 	r30_edlp_gp,
-- 	r30_bsd_gross_revenue,
-- 	r30_bsd_cogs,
-- 	r30_bsd_gp,
-- 	r30_hd_gross_revenue,
-- 	r30_hd_cogs,
-- 	r30_hd_gp,
-- 	r30_gross_revenue,
-- 	r30_cogs,
-- 	r30_gp,
-- 	r30_wmt_ger_true_up AS r30_ger_true_up,
-- 	r30_gp_net_ger_tu,
-- 	r30_edlp_bh01_gross_revenue,
-- 	r30_edlp_bh02_gross_revenue,
-- 	r30_edlp_bh03_gross_revenue,
-- 	r30_bsd_bh01_gross_revenue,
-- 	r30_bsd_bh02_gross_revenue,
-- 	r30_bsd_bh03_gross_revenue,
-- 	r30_edlp_bh01_cogs,
-- 	r30_edlp_bh02_cogs,
-- 	r30_edlp_bh03_cogs,
-- 	r30_bsd_bh01_cogs,
-- 	r30_bsd_bh02_cogs,
-- 	r30_edlp_bh01_gp,
-- 	r30_edlp_bh02_gp,
-- 	r30_edlp_bh03_gp,
-- 	r30_bsd_bh01_gp,
-- 	r30_bsd_bh02_gp,
-- 	r30_bsd_bh03_gp,
-- 	top_30ds_quantity,
-- 	blink_edlp_price_30ds,
-- 	blink_bsd_price_30ds,
-- 	blink_hd_price_30ds,
-- 	top_90ds_quantity,
-- 	blink_edlp_price_90ds,
-- 	blink_bsd_price_90ds,
-- 	blink_hd_price_90ds,
-- 	last_30ds_qty_scrape_date,
-- 	last_30ds_qty,
-- 	min_grx_30ds,
-- 	min_retail_grx_30ds,
-- 	min_bh_retail_index_grx_30ds,
-- 	min_hwh_grx_30ds,
-- 	min_wmt_grx_30ds,
-- 	min_kr_grx_30ds,
-- 	min_sfwy_grx_30ds,
-- 	min_pblx_grx_30ds,
-- 	min_bksh_grx_30ds,
-- 	min_geagle_grx_30ds,
-- 	min_heb_grx_30ds,
-- 	last_90ds_qty_scrape_date,
-- 	last_90ds_qty,
-- 	min_grx_90ds,
-- 	min_retail_grx_90ds,
-- 	min_bh_retail_index_grx_90ds,
-- 	min_hwh_grx_90ds,
-- 	min_wmt_grx_90ds,
-- 	min_kr_grx_90ds,
-- 	min_sfwy_grx_90ds,
-- 	min_pblx_grx_90ds,
-- 	min_bksh_grx_90ds,
-- 	min_geagle_grx_90ds,
-- 	min_heb_grx_90ds,
-- 	ltd_30_day_scripts,
-- 	ltd_90_day_scripts,
-- 	ltd_30_day_scripts_pct,
-- 	ltd_90_day_scripts_pct,
-- 	r30_30_day_scripts,
-- 	r30_90_day_scripts,
-- 	r30_30_day_script_pct,
-- 	r30_90_day_script_pct,
-- 	r30_30_day_nfp_scripts,
-- 	r30_90_day_nfp_scripts,
-- 	r30_30_day_nfp_script_pct,
-- 	r30_90_day_nfp_script_pct,
-- 	r30_hd_30_day_scripts,
-- 	r30_hd_90_day_scripts,
-- 	r30_hd_30_day_script_pct,
-- 	r30_hd_90_day_script_pct,
-- 	r30_hd_30_day_nfp_scripts,
-- 	r30_hd_90_day_nfp_scripts,
-- 	r30_hd_30_day_nfp_script_pct,
-- 	r30_hd_90_day_nfp_script_pct
-- FROM
-- 	fifo.generic_price_portfolio_datamart
-- ORDER BY
-- 	blink_gcn_rank ASC


	

select * from	transactional.med_price


-- SELECT
-- 	date, count(DISTINCT(gcn))
-- FROM
-- 	api_scraper_external.competitor_pricing
-- group by
-- 	date;
-- SELECT
-- 	date,geo, count(DISTINCT(gcn))
-- FROM
-- 	api_scraper_external.competitor_pricing
-- group by
-- 	date,geo;
-- SELECT
-- -- 	gcn, quantity, count(distinct(geo)), count(*)
-- 	competitor_pricing.gcn as gcn,
-- 	quantity,
-- 	count(*) as cnt,
-- 	PERCENTILE_CONT(0) within group ( order by price) as cpr_pct_00_price,
-- 	PERCENTILE_CONT(0.25) within group ( order by price) as cpr_pct_25_price,	PERCENTILE_CONT(0.5) within group ( order by price) as cpr_pct_50_price,	PERCENTILE_CONT(0.75) within group ( order by price) as cpr_pct_75_price,
-- 	PERCENTILE_CONT(1) within group ( order by price) as cpr_pct_100_price
-- FROM
-- 	api_scraper_external.competitor_pricing
-- INNER JOIN
-- 	mktg_dev.top_300_drugs_20191121_1
-- 	on competitor_pricing.gcn = top_300_drugs_20191121_1.gcn
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-10-01'
-- -- 	AND gcn = 57903
-- -- 	AND pharmacy = 'walgreens'
-- group BY
-- 	competitor_pricing.gcn,
-- 	quantity
-- HAVING
-- 	cnt>50;
-- select geo,count(*) from api_scraper_external.competitor_pricing
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-10-01'
-- group BY
-- 	geo;
-- drop table mktg_dev.sdey_competitve_pricing_top_300_20191121;
-- create table mktg_dev.sdey_competitve_pricing_top_300_20191121 as
-- SELECT
-- 	all_geo.gcn,
-- 	all_geo.quantity AS quantity,
-- 	all_cnt,
-- 	cpr_pct_00_price,
-- 	cpr_pct_25_price,
-- 	cpr_pct_50_price,
-- 	cpr_pct_75_price,
-- 	cpr_pct_100_price,
-- 	geo_cnt,
-- 	cpr_geo_pct_00_price,
-- 	cpr_geo_pct_25_price,
-- 	cpr_geo_pct_50_price,
-- 	cpr_geo_pct_75_price,
-- 	cpr_geo_pct_100_price,
-- 	lowest_price_pharmacy,
-- 	brookshire_price
-- FROM (
-- 	SELECT
-- 		competitor_pricing.gcn AS gcn,
-- 		quantity,
-- 		count(*) AS all_cnt,
-- 		PERCENTILE_CONT(0)
-- 		WITHIN GROUP (ORDER BY price) AS cpr_pct_00_price,
-- 		PERCENTILE_CONT(0.25)
-- 		WITHIN GROUP (ORDER BY price) AS cpr_pct_25_price,
-- 		PERCENTILE_CONT(0.5)
-- 		WITHIN GROUP (ORDER BY price) AS cpr_pct_50_price,
-- 		PERCENTILE_CONT(0.75)
-- 		WITHIN GROUP (ORDER BY price) AS cpr_pct_75_price,
-- 		PERCENTILE_CONT(1)
-- 		WITHIN GROUP (ORDER BY price) AS cpr_pct_100_price
-- 	FROM
-- 		api_scraper_external.competitor_pricing
-- 		INNER JOIN mktg_dev.top_300_drugs_20191121_1 ON competitor_pricing.gcn = top_300_drugs_20191121_1.gcn
-- 	WHERE
-- 		site != 'all'
-- 		AND geo != 'all'
-- 		AND pharmacy != 'all'
-- 		AND pharmacy != 'all_major'
-- 		AND pharmacy != 'all_preferred'
-- 		AND pharmacy != 'other_pharmacies'
-- 		AND site = 'goodrx'
-- 		AND date >= '2019-10-01' GROUP BY
-- 			competitor_pricing.gcn, quantity
-- 		HAVING
-- 			all_cnt > 50) all_geo
-- LEFT OUTER JOIN (
-- 	SELECT
-- 	competitor_pricing.gcn AS gcn,
-- 	quantity,
-- 	count(*) AS geo_cnt,
-- 	PERCENTILE_CONT(0)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_geo_pct_00_price,
-- 	PERCENTILE_CONT(0.25)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_geo_pct_25_price,
-- 	PERCENTILE_CONT(0.5)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_geo_pct_50_price,
-- 	PERCENTILE_CONT(0.75)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_geo_pct_75_price,
-- 	PERCENTILE_CONT(1)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_geo_pct_100_price,
-- 	MIN(CASE WHEN pharmacy = 'brookshires' THEN price ELSE 10000 END) AS brookshire_price,
-- 	MIN(CASE WHEN price_rank = 1 THEN pharmacy ELSE 'zzzzzzzzzzzz' END) AS lowest_price_pharmacy
-- 	FROM (
-- 		SELECT
-- 			*,
-- 			rank() OVER (PARTITION BY gcn, quantity ORDER BY price ASC) as price_rank
-- 			FROM
-- 				api_scraper_external.competitor_pricing
-- 		WHERE
-- 			site != 'all'
-- 			AND geo != 'all'
-- 			AND geo LIKE '%tx%'
-- 			AND pharmacy != 'all'
-- 			AND pharmacy != 'all_major'
-- 			AND pharmacy != 'all_preferred'
-- 			AND pharmacy != 'other_pharmacies'
-- 			AND site = 'goodrx'
-- 			AND date >= '2019-10-01' ) competitor_pricing
-- 	INNER JOIN
-- 		mktg_dev.top_300_drugs_20191121_1 ON competitor_pricing.gcn = top_300_drugs_20191121_1.gcn
-- 	GROUP BY
-- 		competitor_pricing.gcn,
-- 		quantity
-- 	HAVING
-- 		geo_cnt > 11 ) tx_geo
-- ON
-- 	all_geo.gcn = tx_geo.gcn
-- 	AND all_geo.quantity = tx_geo.quantity;
-- SELECT
-- 	pharmacy,count(*)
-- FROM
-- 	api_scraper_external.competitor_pricing
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND geo LIKE '%tx%'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- -- 	AND date >= '2019-10-01'
-- GROUP BY
-- 	pharmacy
-- SELECT
-- 	competitor_pricing.gcn,
-- 	quantity,
-- -- 	pharmacy,
-- 	geo,
-- -- 	date,
-- 	count(*) as cnt,
-- -- 	count(DISTINCT(price)),
-- 	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS cpr_geo_pct_50_price,
-- 	cast ( STDDEV (price) as dec(18,2)),
-- 	min(price) min_price,
-- 	max(price) max_price,
-- 	cast( max_price - min_price as dec(10,2)) as price_diff,
-- 	AVG(gcn_symphony_2017_fills) AS symphony_2017_fills,
-- 	AVG(gcn_symphony_2017_rank) AS symphony_2017_rank
-- FROM
-- 	api_scraper_external.competitor_pricing
-- inner JOIN
-- 	dwh.dim_gcn_seqno_hierarchy
-- ON
-- 	competitor_pricing.gcn = dim_gcn_seqno_hierarchy.gcn
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- -- 	AND geo LIKE '%tx%'
-- -- 	AND pharmacy = 'walmart'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-08-01'
-- 	AND gcn_symphony_2017_rank <= 300
-- group by
-- 	1,
-- 	2,
-- 	3
-- HAVING
-- 	symphony_2017_fills is not NULL
-- 	AND cnt > 5
-- order BY
-- 	symphony_2017_rank ASC ,
-- 	quantity ASC
-- SELECT
-- 	ms.gcn,
-- 	ms.gcn_seqno,
-- 	ms.gcn_symphony_2017_rank,
-- 	ms.dosage_form_desc,
-- 	ms.strength,
-- 	started_on,
-- 	ended_on,
-- 	branded,
-- 	med_price.unit_price as unit_price_sell,
-- 	generic_name_long,
-- -- 	network_pricing_mac.unit_price as unit_price_mac,
-- 	unit_margin,
-- 	dispensing_fee_margin,
-- 	dispensing_fee_margin::float + 1.75 AS fixed_price,
--     case
--     	when pharmacy_network_id is null then 'edlp'
--         when pharmacy_network_id = 1 then 'edlp'
--         when pharmacy_network_id = 2 then 'bsd'
--         when pharmacy_network_id = 3 then 'hd'
--         when pharmacy_network_id = 4 then 'quicksave'
--         else 'unknown'
--     end as "pharmacy network name"
-- FROM
-- 	(SELECT
-- 			gcn,
-- 			gcn_seqno,
-- 			gcn_symphony_2017_rank,
-- 			strength,
-- 			dosage_form_desc,
-- 			generic_name_long
-- 		FROM
-- 			dwh.dim_gcn_seqno_hierarchy
-- 		WHERE
-- 			(generic_name_long like '%metformin%')) AS ms
-- 	INNER JOIN transactional.med_price ON ms.gcn = med_price.gcn
-- -- 	INNER JOIN drugs_etl.network_pricing_mac ON network_pricing_mac.gcn_seqno = ms.gcn_seqno
-- WHERE
-- -- 	pharmacy_network_id = 1
-- 	ended_on IS NULL
-- -- 	AND end_date is NULL
-- -- 	AND mac_list = 'BLINK02'
-- 	AND gcn='98921'
-- -- 	;

-- select date,geo,count(*) from api_scraper_external.competitor_pricing
-- where date>'2019-11-01' and geo not like '%all%' group by 1,2 order by 1 asc,2;


-- SELECT
-- 	gcn,
-- 	quantity,
-- 	date,
-- 	count(*) AS all_cnt,
-- 	PERCENTILE_CONT(0)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_00_price,
-- 	PERCENTILE_CONT(0.25)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_25_price,
-- 	PERCENTILE_CONT(0.5)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_50_price,
-- 	PERCENTILE_CONT(0.75)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_75_price,
-- 	PERCENTILE_CONT(1)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_100_price,
-- 	MIN(
-- 		CASE WHEN price_rank = 1 THEN
-- 			pharmacy
-- 		ELSE
-- 			'zzzzzzzzzzzz'
-- 		END) AS lowest_price_pharmacy
-- FROM
-- 	api_scraper_external.competitor_pricing
-- 	INNER JOIN (
-- 		SELECT
-- 			*
-- 		FROM (
-- 			SELECT
-- 				gcn AS gcn_1,
-- 				quantity AS quantity_1,
-- 				date AS date_1,
-- 				pharmacy AS pharmacy_1,
-- 				rank() OVER (PARTITION BY gcn,
-- 					quantity,
-- 					date ORDER BY price ASC) AS price_rank
-- 			FROM
-- 				api_scraper_external.competitor_pricing
-- 			WHERE
-- 				site != 'all'
-- 				AND geo != 'all'
-- 				-- 			AND geo LIKE '%tx%'
-- 				AND pharmacy != 'all'
-- 				AND pharmacy != 'all_major'
-- 				AND pharmacy != 'all_preferred'
-- 				AND pharmacy != 'other_pharmacies'
-- 				AND site = 'goodrx'
-- 				AND date >= '2019-10-01'
-- 				AND gcn in(26324, 26320, 26328, 26327, 26326, 47631, 47632, 26323, 26321, 26322)) mins
-- 		WHERE
-- 			price_rank = 1) AS cp ON gcn = gcn_1
-- 	AND quantity = quantity_1
-- 	AND date = date_1
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-10-01'
-- 	AND gcn in(26324, 26320, 26328, 26327, 26326, 47631, 47632, 26323, 26321, 26322)
-- GROUP BY
-- 	competitor_pricing.gcn, quantity;



SELECT
	ms.gcn,
	ms.gcn_seqno,
	ms.gcn_symphony_2017_rank,
	ms.dosage_form_desc,
	ms.strength,
	branded,
	generic_name_long,
	sum(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.unit_price ELSE 0 END  ) AS edlp_unit_price,
	sum(CASE WHEN pharmacy_network_id = 2 THEN med_price.unit_price ELSE 0 END  ) AS bsd_unit_price,
	sum(CASE WHEN pharmacy_network_id = 3 THEN med_price.unit_price ELSE 0 END  ) AS hd_unit_price,
	sum(CASE WHEN pharmacy_network_id = 4 THEN med_price.unit_price ELSE 0 END  ) AS quicksave_unit_price,
-- 	network_pricing_mac.unit_price as unit_price_mac,
-- 	unit_margin,
	sum(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS edlp_dispensing_fee_margin,
	sum(CASE WHEN pharmacy_network_id = 2 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS bsd_dispensing_fee_margin,
	sum(CASE WHEN pharmacy_network_id = 3 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS hd_dispensing_fee_margin,
	sum(CASE WHEN pharmacy_network_id = 4 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS quicksave_dispensing_fee_margin
FROM
	(SELECT
			gcn,
			medid,
			gcn_seqno,
			gcn_symphony_2017_rank,
			strength,
			dosage_form_desc,
			generic_name_long
		FROM
			dwh.dim_gcn_seqno_hierarchy
		WHERE
			generic_name_long like '%levothyroxine sodium%' AND gcn_symphony_2017_rank<300) AS ms
	INNER JOIN transactional.med_price ON ms.gcn = med_price.gcn AND ms.medid = med_price.medid
-- 	LEFT JOIN drugs_etl.network_pricing_mac ON network_pricing_mac.gcn_seqno = ms.gcn_seqno
WHERE
-- 	pharmacy_network_id = 1
	ended_on IS NULL
-- 	AND end_date is NULL
-- 	AND mac_list = 'BLINK02'
GROUP BY
	ms.gcn,
	ms.gcn_seqno,
	ms.gcn_symphony_2017_rank,
	ms.dosage_form_desc,
	ms.strength,
	branded,
	generic_name_long


-- SELECT
-- 	ms.gcn,
-- 	ms.gcn_seqno,
-- 	ms.gcn_symphony_2017_rank,
-- 	ms.dosage_form_desc,
-- 	ms.strength,
-- -- 	started_on,
-- -- 	ended_on,
-- -- 	branded,
-- -- 	med_price.unit_price as unit_price_sell,
-- 	generic_name_long,
-- 	network_pricing_mac.unit_price as unit_price_mac,
-- -- 	unit_margin,
-- -- 	dispensing_fee_margin,
-- -- 	dispensing_fee_margin::float + 1.75 AS fixed_price,
-- 	mac_list,
-- 	start_date,
-- 	end_date
-- FROM
-- 	(SELECT
-- 			gcn,
-- 			gcn_seqno,
-- 			gcn_symphony_2017_rank,
-- 			strength,
-- 			dosage_form_desc,
-- 			generic_name_long
-- 		FROM
-- 			dwh.dim_gcn_seqno_hierarchy
-- 		WHERE
-- 			gcn = 98921 ) AS ms
-- --INNER JOIN transactional.med_price ON ms.gcn = med_price.gcn
-- INNER JOIN drugs_etl.network_pricing_mac ON network_pricing_mac.gcn_seqno = ms.gcn_seqno
-- WHERE
-- 	pharmacy_network_id = 1
-- 	ended_on IS NULL
-- 	end_date is NULL
-- 	AND mac_list = 'BLINK02'
-- 	;

-- select * from drugs_etl.network_pricing_mac where gcn_seqno = 63164;



SELECT
	ms.gcn,
	ms.gcn_seqno,
	ms.medid,
	ms.gcn_symphony_2017_rank,
	generic_name_long,
	ms.dosage_form_desc,
	ms.strength,
	started_on,
	ended_on,
	branded,
	med_price.unit_price as unit_price_sell,
-- 	network_pricing_mac.unit_price as unit_price_mac,
	unit_margin,
	dispensing_fee_margin,
	dispensing_fee_margin::float + 1.75 AS fixed_price,
    case
    	when pharmacy_network_id is null then 'edlp'
        when pharmacy_network_id = 1 then 'edlp'
        when pharmacy_network_id = 2 then 'bsd'
        when pharmacy_network_id = 3 then 'hd'
        when pharmacy_network_id = 4 then 'quicksave'
        else 'unknown'
    end as pharmacy_network_name
FROM
	(SELECT
			gcn,
			medid,
			gcn_seqno,
			gcn_symphony_2017_rank,
			strength,
			dosage_form_desc,
			generic_name_long
		FROM
			dwh.dim_gcn_seqno_hierarchy
		WHERE
			(generic_name_long like '%levothyroxine sodium%%')) AS ms
INNER JOIN
	transactional.med_price
ON
	ms.gcn = med_price.gcn
	AND ms.medid = med_price.medid
-- 	INNER JOIN drugs_etl.network_pricing_mac ON network_pricing_mac.gcn_seqno = ms.gcn_seqno
WHERE
	started_on >= '2017-01-01'
-- 	pharmacy_network_id = 1
-- 	ended_on IS NULL
-- 	AND end_date is NULL
-- 	AND mac_list = 'BLINK02'
-- 	AND gcn='98921'
ORDER BY
	gcn_symphony_2017_rank ASC, ms.medid ASC, started_on DESC





	SELECT
	ms.gcn,
	ms.medid,
	ms.gcn_seqno,
	ms.gcn_symphony_2017_rank,
	ms.gcn_symphony_2017_fills,
	ms.default_quantity,
	ms.strength,
	ms.dosage_form_desc,
	ms.generic_name_long,
	price.edlp_unit_price,
	price.bsd_unit_price,
	price.hd_unit_price,
	price.magiccard_berkshire_unit_price,
	price.magiccard_heb_unit_price
	price.edlp_dispensing_fee_margin,
	price.bsd_dispensing_fee_margin,
	price.hd_dispensing_fee_margin,
	price.magiccard_berkshire_dispensing_fee_margin,
	price.magiccard_heb_dispensing_fee_margin
FROM
	(SELECT
			d1.gcn,
			d1.medid,
			d1.gcn_seqno,
			d1.gcn_symphony_2017_rank,
			d1.gcn_symphony_2017_fills,
			d2.default_quantity,
			d1.strength,
			d1.dosage_form_desc,
			d1.generic_name_long
		FROM
			dwh.dim_gcn_seqno_hierarchy as d1
		INNER JOIN
			dwh.dim_medid_hierarchy as d2
		ON
			d1.medid = d2.medid
			AND d1.gcn_seqno = d2.gcn_seqno) AS ms
INNER JOIN
	(SELECT
		gcn,
		medid,
		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.unit_price ELSE 0 END  ) AS edlp_unit_price,
		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.unit_price ELSE 0 END  ) AS bsd_unit_price,
		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.unit_price ELSE 0 END  ) AS hd_unit_price,
		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.unit_price ELSE 0 END  ) AS magiccard_berkshire_unit_price,
		MAX(CASE WHEN pharmacy_network_id = 5 THEN med_price.unit_price ELSE 0 END  ) AS magiccard_heb_unit_price,
		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS edlp_dispensing_fee_margin,
		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS bsd_dispensing_fee_margin,
		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS hd_dispensing_fee_margin,
		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS magiccard_berkshire_dispensing_fee_margin,
		MAX(CASE WHEN pharmacy_network_id = 5 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS magiccard_heb_dispensing_fee_margin
	FROM
		transactional.med_price
	WHERE
		ended_on IS NULL
	GROUP BY
		gcn,medid) price
ON
	ms.gcn = price.gcn
	AND ms.medid = price.medid
ORDER BY
	gcn_symphony_2017_rank ASC, ms.medid ASC


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


-- Peso Median

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

-- select data_name, rank() over ( order by data_name) , data_count ,
-- 100*sum(data_count) over (order by data_name asc rows between UNBOUNDED PRECEDING and current row) / sum(data_count) over ()
-- from mktg_dev.tmp_1;




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
-- 	price.magiccard_berkshire_unit_price,
-- 	price.magiccard_heb_unit_price
-- 	price.edlp_dispensing_fee_margin,
-- 	price.bsd_dispensing_fee_margin,
-- 	price.hd_dispensing_fee_margin,
-- 	price.magiccard_berkshire_dispensing_fee_margin,
-- 	price.magiccard_heb_dispensing_fee_margin
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
-- 		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.unit_price ELSE 0 END  ) AS magiccard_berkshire_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id = 5 THEN med_price.unit_price ELSE 0 END  ) AS magiccard_heb_unit_price,
-- 		MAX(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS edlp_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 2 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS bsd_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 3 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS hd_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 4 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS magiccard_berkshire_dispensing_fee_margin,
-- 		MAX(CASE WHEN pharmacy_network_id = 5 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS magiccard_heb_dispensing_fee_margin
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

-- -- select mp.gcn,dgsh.gcn_seqno,started_on::timestamp::date,
-- --     case
-- --     	when pharmacy_network_id is null then 'edlp'
-- --         when pharmacy_network_id = 1 then 'edlp'
-- --         when pharmacy_network_id = 2 then 'bsd'
-- --         when pharmacy_network_id = 3 then 'hd'
-- --         when pharmacy_network_id = 4 then 'quicksave'
-- --         else 'unknown'
-- --     end as pharmacy_network_name
-- -- from transactional.med_price as mp 
-- -- inner join dwh.dim_gcn_seqno_hierarchy as dgsh
-- -- on 
-- -- 	mp.gcn = dgsh.gcn AND mp.medid = dgsh.medid
-- -- where 
-- -- ended_on is null and
-- -- mp.gcn in ('10810',
-- -- '10857',
-- -- '16513',
-- -- '18010',
-- -- '18996',
-- -- '18996',
-- -- '25940',
-- -- '25940',
-- -- '26320',
-- -- '26322',
-- -- '26323',
-- -- '26324',
-- -- '26326',
-- -- '26327',
-- -- '47631',
-- -- '47632',
-- -- '26328',
-- -- '7221',
-- -- '31164',
-- -- '98921')
-- 	
-- 	
-- -- Peso Median

-- -- SELECT  AVG(1.0E * x)
-- -- FROM    (
-- --             SELECT  x,
-- --                     2 * ROW_NUMBER() OVER (ORDER BY x) - COUNT(*) OVER () AS y
-- --             FROM    @Foo
-- --         ) AS d
-- -- WHERE   y BETWEEN 0 AND 2
-- -- 	
-- -- 		
-- -- -- Peso Weighted Median
-- -- SELECT  SUM(1.0E * y) / SUM(1.0E * t)
-- -- FROM    (
-- --             SELECT  SUM(x) OVER (PARTITION BY x) AS y,
-- --                     2 * ROW_NUMBER() OVER (ORDER BY x) - COUNT(*) OVER () AS z,
-- --                     COUNT(*) OVER (PARTITION BY x) AS t
-- --             FROM    @Foo
-- --         ) AS d
-- -- WHERE   z BETWEEN 0 AND 2


-- -- -- select data,rank() over ( order by data), count(*) over (), 2 * rank() over ( order by data) - count(*) over () as y from mktg_dev.tmp

-- -- select data_name, rank() over ( order by data_name) , data_count ,
-- -- 100*sum(data_count) over (order by data_name asc rows between UNBOUNDED PRECEDING and current row) / sum(data_count) over ()
-- -- from mktg_dev.tmp_1;