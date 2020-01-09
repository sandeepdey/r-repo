drop table if EXISTS mktg_dev.sdey_weighted_competitive_index;
create table mktg_dev.sdey_weighted_competitive_index as
with comp_data_with_pharmacy_count AS (
	SELECT
		cp.gcn,
		cp.quantity,
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
		AND date = '2020-01-03'
		AND (gcn=1431 OR gcn=10857)
		AND quantity=30
	GROUP BY
		1,2,3
), comp_data_2 AS (
	SELECT
		gcn,
		quantity,
		pharmacy,
		store_count,
		default_quantity,
		store_price,
		dense_rank() OVER (PARTITION BY gcn,quantity ORDER BY store_price ASC ,store_count DESC) as store_rank,
		sum(store_count) OVER (PARTITION BY gcn,quantity ORDER BY store_price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)*100/sum(store_count) OVER (PARTITION BY gcn,quantity) AS store_price_weighted_percentile
	FROM
		comp_data_with_pharmacy_count
), comp_data_3 AS (
	SELECT
		a.gcn,
		a.quantity,
		a.pharmacy,
		a.store_price,
		a.default_quantity
	FROM
		comp_data_2 AS a
	JOIN
		(SELECT gcn, quantity, min(store_rank) AS min_rank FROM	comp_data_2 WHERE store_price_weighted_percentile >= 10 GROUP BY 1,2) AS b
	ON
		a.gcn = b.gcn
		AND a.quantity = b.quantity
		AND a.store_rank = b.min_rank
)
	SELECT * from comp_data_3

;




-- select * from mktg_dev.sdey_magic_card_pricing_top_300_20191121;
-- select count(*) from mktg_dev.sdey_magic_card_pricing_top_300_20191121;
-- drop table mktg_dev.sdey_magic_card_pricing_top_300_20191121;
-- CREATE TABLE mktg_dev.sdey_magic_card_pricing_top_300_20191121 AS
-- SELECT
-- 	med_price.gcn,
-- 	gcnseqno,
-- 	description,
-- 	med_price.medid,
-- 	pharmacy_network_id,
-- 	started_on,
-- 	branded,
-- 	med_price.unit_price as unit_price_sell,
-- 	network_pricing_mac.unit_price as unit_price_mac,
-- 	unit_margin,
-- 	dispensing_fee_margin,
-- 	dispensing_fee_margin::float + 1.75 AS fixed_price,
-- 	brandmedname,
-- 	hicldesc,
-- 	gtcdesc,
-- 	stcdesc,
-- 	top_300_drugs_20191121_1.strength,
-- 	form
-- FROM
-- 	transactional.med_price
-- 	INNER JOIN mktg_dev.top_300_drugs_20191121_1 ON med_price.gcn = top_300_drugs_20191121_1.gcn
-- 	INNER JOIN transactional.med ON med_price.medid = med.medid
-- 	INNER JOIN drugs_etl.network_pricing_mac ON network_pricing_mac.gcn_seqno = med.gcn_seqno
-- WHERE
-- 	pharmacy_network_id = 4 -- 	OR pharmacy_network_id = 5
-- 	AND ended_on IS NULL
-- 	AND end_date is NULL
-- 	AND mac_list = 'BLINK02';


-- drop table mktg_dev.sdey_magic_card_pricing_and_competitive_data_top_300_20191122;

-- CREATE TABLE mktg_dev.sdey_magic_card_pricing_and_competitive_data_top_300_20191122 as
-- SELECT
-- 	sdey_magic_card_pricing_top_300_20191121.gcn,
-- 	gcnseqno,
-- 	description,
-- 	medid,
-- 	pharmacy_network_id,
-- 	started_on,
-- 	branded,
-- 	unit_price_sell,
-- 	unit_price_mac,
-- 	unit_margin,
-- 	dispensing_fee_margin,
-- 	fixed_price,
-- 	brandmedname,
-- 	hicldesc,
-- 	gtcdesc,
-- 	stcdesc,
-- 	strength,
-- 	form,
-- 	quantity,
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
-- 	brookshire_price,
-- 	filled_orders as filled_orders_2019
-- FROM
-- 	mktg_dev.sdey_magic_card_pricing_top_300_20191121
-- LEFT OUTER JOIN
-- 	mktg_dev.sdey_competitve_pricing_top_300_20191121
-- ON
-- 	sdey_magic_card_pricing_top_300_20191121.gcn = sdey_competitve_pricing_top_300_20191121.gcn
-- LEFT OUTER JOIN
-- 	mktg_dev.sdey_filled_orders_2019_gcn
-- ON
-- 	sdey_magic_card_pricing_top_300_20191121.gcnseqno = sdey_filled_orders_2019_gcn.gcn_seqno

-- select cpr_geo_pct_00_price from mktg_dev.sdey_magic_card_pricing_and_competitive_data_top_300_20191122


-- Magic Card Fills SQL
-- SELECT
-- 	gcn_seqno,
-- 	count(*) AS fills
-- FROM
-- 	dwh.fact_order_item foi
-- 	JOIN dwh.dim_user du ON du.dw_user_id = foi.dw_user_id
-- 		AND foi.account_id = du.account_id
-- WHERE
-- 	foi.fill_sequence IS NOT NULL
-- 	AND foi.last_pbm_adjudication_timestamp_approved > '2019-08-01'
-- 	AND du.is_internal = FALSE
-- 	AND du.is_phantom = FALSE
-- 	AND foi.is_fraud = FALSE
-- 	AND foi.pharmacy_network_id = 4
-- GROUP BY
-- 	1


-- select *
-- from transactional.med_price
-- where ended_on is null -- and gcn = 64324
-- order by gcn
