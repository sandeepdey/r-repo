select * from mktg_dev.sdey_weighted_competitive_index;

drop table if EXISTS mktg_dev.sdey_weighted_competitive_index;
create table mktg_dev.sdey_weighted_competitive_index as 

with comp_data_with_pharmacy_count AS ( 
	SELECT
		cp.gcn,
		cp.quantity,
		cp.pharmacy,
		MEDIAN(cp.default_quantity) AS default_quantity,
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
		(SELECT gcn, quantity, min(store_rank) AS min_rank FROM	 comp_data_2 WHERE store_price_weighted_percentile >= 10 GROUP BY 1,2) AS b 
	ON
		a.gcn = b.gcn
		AND a.quantity = b.quantity
		AND a.store_rank = b.min_rank
)
	SELECT * from comp_data_3

;



-- SELECT
-- 	cp.gcn,
-- 	cp.quantity,
-- 	min_price_pharmacy,
-- 	count(*) AS cnt,
-- 	PERCENTILE_CONT(0)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_00_price,
-- 	PERCENTILE_CONT(0.1)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_10_price,
-- 	PERCENTILE_CONT(0.2)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_20_price,
-- 	PERCENTILE_CONT(0.25)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_25_price,
-- 	PERCENTILE_CONT(0.5)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_50_price,
-- 	PERCENTILE_CONT(0.75)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_75_price,
-- 	PERCENTILE_CONT(1)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_100_price,
-- 	MIN(
-- 		CASE WHEN pharmacy = 'walmart' THEN
-- 			price
-- 		ELSE
-- 			100000000000
-- 		END) AS wmt_grx,
-- 	MIN(
-- 		CASE WHEN pharmacy = 'cvs' THEN
-- 			price
-- 		ELSE
-- 			100000000000
-- 		END) AS cvs_grx,
-- 	MIN(
-- 		CASE WHEN pharmacy = 'walgreens' THEN
-- 			price
-- 		ELSE
-- 			100000000000
-- 		END) AS wag_grx
-- FROM
-- 	api_scraper_external.competitor_pricing AS cp
-- 	INNER JOIN (
-- 		SELECT
-- 			gcn,
-- 			quantity,
-- 			pharmacy AS min_price_pharmacy
-- 		FROM (
-- 			SELECT
-- 				gcn,
-- 				quantity,
-- 				pharmacy,
-- 				DENSE_RANK() OVER (PARTITION BY gcn,
-- 					quantity ORDER BY price ASC,
-- 					date DESC,
-- 					pharmacy DESC) AS price_rank
-- 			FROM
-- 				api_scraper_external.competitor_pricing
-- 			WHERE
-- 				geo != 'all'
-- 				AND pharmacy NOT LIKE '%all%'
-- 				AND pharmacy != 'other_pharmacies'
-- 				AND site = 'goodrx'
-- 				AND date >= '2019-10-01' ORDER BY
-- 					gcn, quantity, price_rank, pharmacy)
-- 			WHERE
-- 				price_rank = 1
-- 			GROUP BY
-- 				gcn, quantity, pharmacy, price_rank) price_rank ON cp.gcn = price_rank.gcn
-- 	AND cp.quantity = price_rank.quantity
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-10-01'
-- GROUP BY
-- 	cp.gcn, cp.quantity, min_price_pharmacy
-- HAVING
-- 	cnt > 10;


-- select * from mktg_dev.sdey_weighted_competitive_index;

-- drop table if EXISTS mktg_dev.sdey_weighted_competitive_index ;
-- create table mktg_dev.sdey_weighted_competitive_index as 
-- with comp_data_with_pharmacy_count AS ( 
-- 	SELECT
-- 		cp.gcn,
-- 		cp.quantity,
-- 		cp.pharmacy,
-- 		AVG(cp.default_quantity) AS default_quantity,
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
-- -- 		AND (gcn=1431 OR gcn=10857) 
-- -- 		AND quantity=30
-- 	GROUP BY
-- 		1,2,3
-- ), comp_data_2 AS (
-- 	SELECT	
-- 		gcn,
-- 		quantity,
-- 		pharmacy,
-- 		store_count,
-- 		default_quantity,
-- 		store_price,
-- 		dense_rank() OVER (PARTITION BY gcn,quantity ORDER BY store_price ASC ,store_count DESC) as store_rank,
-- 		sum(store_count) OVER (PARTITION BY gcn,quantity ORDER BY store_price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)*100/sum(store_count) OVER (PARTITION BY gcn,quantity) AS store_price_weighted_percentile
-- 	FROM
-- 		comp_data_with_pharmacy_count
-- ), comp_data_3 AS (
-- 	SELECT
-- 		a.gcn,
-- 		a.quantity,
-- 		a.pharmacy,
-- 		a.store_price,
-- 		a.default_quantity
-- 	FROM
-- 		comp_data_2 AS a
-- 	JOIN
-- 		(SELECT gcn, quantity, min(store_rank) AS min_rank FROM	comp_data_2 WHERE store_price_weighted_percentile >= 10 GROUP BY 1,2) AS b 
-- 	ON
-- 		a.gcn = b.gcn
-- 		AND a.quantity = b.quantity
-- 		AND a.store_rank = b.min_rank
-- )
-- 	SELECT * from comp_data_3

-- ;



-- SELECT
-- 	cp.gcn,
-- 	cp.quantity,
-- 	min_price_pharmacy,
-- 	count(*) AS cnt,
-- 	PERCENTILE_CONT(0)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_00_price,
-- 	PERCENTILE_CONT(0.1)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_10_price,
-- 	PERCENTILE_CONT(0.2)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_20_price,
-- 	PERCENTILE_CONT(0.25)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_25_price,
-- 	PERCENTILE_CONT(0.5)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_50_price,
-- 	PERCENTILE_CONT(0.75)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_75_price,
-- 	PERCENTILE_CONT(1)
-- 	WITHIN GROUP (ORDER BY price) AS cpr_pct_100_price,
-- 	MIN(
-- 		CASE WHEN pharmacy = 'walmart' THEN
-- 			price
-- 		ELSE
-- 			100000000000
-- 		END) AS wmt_grx,
-- 	MIN(
-- 		CASE WHEN pharmacy = 'cvs' THEN
-- 			price
-- 		ELSE
-- 			100000000000
-- 		END) AS cvs_grx,
-- 	MIN(
-- 		CASE WHEN pharmacy = 'walgreens' THEN
-- 			price
-- 		ELSE
-- 			100000000000
-- 		END) AS wag_grx
-- FROM
-- 	api_scraper_external.competitor_pricing AS cp
-- 	INNER JOIN (
-- 		SELECT
-- 			gcn,
-- 			quantity,
-- 			pharmacy AS min_price_pharmacy
-- 		FROM (
-- 			SELECT
-- 				gcn,
-- 				quantity,
-- 				pharmacy,
-- 				DENSE_RANK() OVER (PARTITION BY gcn,
-- 					quantity ORDER BY price ASC,
-- 					date DESC,
-- 					pharmacy DESC) AS price_rank
-- 			FROM
-- 				api_scraper_external.competitor_pricing
-- 			WHERE
-- 				geo != 'all'
-- 				AND pharmacy NOT LIKE '%all%'
-- 				AND pharmacy != 'other_pharmacies'
-- 				AND site = 'goodrx'
-- 				AND date >= '2019-10-01' ORDER BY
-- 					gcn, quantity, price_rank, pharmacy)
-- 			WHERE
-- 				price_rank = 1
-- 			GROUP BY
-- 				gcn, quantity, pharmacy, price_rank) price_rank ON cp.gcn = price_rank.gcn
-- 	AND cp.quantity = price_rank.quantity
-- WHERE
-- 	site != 'all'
-- 	AND geo != 'all'
-- 	AND pharmacy != 'all'
-- 	AND pharmacy != 'all_major'
-- 	AND pharmacy != 'all_preferred'
-- 	AND pharmacy != 'other_pharmacies'
-- 	AND site = 'goodrx'
-- 	AND date >= '2019-10-01'
-- GROUP BY
-- 	cp.gcn, cp.quantity, min_price_pharmacy
-- HAVING
-- 	cnt > 10;



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

select date,geo,count(*) from api_scraper_external.competitor_pricing 
where date>'2019-11-01' and geo not like '%all%' group by 1,2 order by 1 asc,2;


SELECT
	gcn,
	quantity,
	date,
	count(*) AS all_cnt,
	PERCENTILE_CONT(0)
	WITHIN GROUP (ORDER BY price) AS cpr_pct_00_price,
	PERCENTILE_CONT(0.25)
	WITHIN GROUP (ORDER BY price) AS cpr_pct_25_price,
	PERCENTILE_CONT(0.5)
	WITHIN GROUP (ORDER BY price) AS cpr_pct_50_price,
	PERCENTILE_CONT(0.75)
	WITHIN GROUP (ORDER BY price) AS cpr_pct_75_price,
	PERCENTILE_CONT(1)
	WITHIN GROUP (ORDER BY price) AS cpr_pct_100_price,
	MIN(
		CASE WHEN price_rank = 1 THEN
			pharmacy
		ELSE
			'zzzzzzzzzzzz'
		END) AS lowest_price_pharmacy
FROM
	api_scraper_external.competitor_pricing
	INNER JOIN (
		SELECT
			*
		FROM (
			SELECT
				gcn AS gcn_1,
				quantity AS quantity_1,
				date AS date_1,
				pharmacy AS pharmacy_1,
				rank() OVER (PARTITION BY gcn,
					quantity,
					date ORDER BY price ASC) AS price_rank
			FROM
				api_scraper_external.competitor_pricing
			WHERE
				site != 'all'
				AND geo != 'all'
				-- 			AND geo LIKE '%tx%'
				AND pharmacy != 'all'
				AND pharmacy != 'all_major'
				AND pharmacy != 'all_preferred'
				AND pharmacy != 'other_pharmacies'
				AND site = 'goodrx'
				AND date >= '2019-10-01'
				AND gcn in(26324, 26320, 26328, 26327, 26326, 47631, 47632, 26323, 26321, 26322)) mins
		WHERE
			price_rank = 1) AS cp ON gcn = gcn_1
	AND quantity = quantity_1
	AND date = date_1
WHERE
	site != 'all'
	AND geo != 'all'
	AND pharmacy != 'all'
	AND pharmacy != 'all_major'
	AND pharmacy != 'all_preferred'
	AND pharmacy != 'other_pharmacies'
	AND site = 'goodrx'
	AND date >= '2019-10-01'
	AND gcn in(26324, 26320, 26328, 26327, 26326, 47631, 47632, 26323, 26321, 26322)
GROUP BY
	competitor_pricing.gcn, quantity;



-- SELECT
-- 	ms.gcn,
-- 	ms.gcn_seqno,
-- 	ms.gcn_symphony_2017_rank,
-- 	ms.dosage_form_desc,
-- 	ms.strength,
-- 	branded,
-- 	generic_name_long,	
-- 	sum(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.unit_price ELSE 0 END  ) AS edlp_unit_price,
-- 	sum(CASE WHEN pharmacy_network_id = 2 THEN med_price.unit_price ELSE 0 END  ) AS bsd_unit_price,
-- 	sum(CASE WHEN pharmacy_network_id = 3 THEN med_price.unit_price ELSE 0 END  ) AS hd_unit_price,
-- 	sum(CASE WHEN pharmacy_network_id = 4 THEN med_price.unit_price ELSE 0 END  ) AS quicksave_unit_price,
-- -- 	network_pricing_mac.unit_price as unit_price_mac,
-- -- 	unit_margin,
-- 	sum(CASE WHEN pharmacy_network_id is null or pharmacy_network_id = 1 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS edlp_dispensing_fee_margin,
-- 	sum(CASE WHEN pharmacy_network_id = 2 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS bsd_dispensing_fee_margin,
-- 	sum(CASE WHEN pharmacy_network_id = 3 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS hd_dispensing_fee_margin,
-- 	sum(CASE WHEN pharmacy_network_id = 4 THEN med_price.dispensing_fee_margin ELSE 0 END  ) AS quicksave_dispensing_fee_margin
-- FROM
-- 	(SELECT
-- 			gcn,
-- 			medid,
-- 			gcn_seqno,
-- 			gcn_symphony_2017_rank,
-- 			strength,
-- 			dosage_form_desc,
-- 			generic_name_long
-- 		FROM
-- 			dwh.dim_gcn_seqno_hierarchy
-- 		WHERE 
-- 			generic_name_long like '%levothyroxine sodium%' AND gcn_symphony_2017_rank<300) AS ms 
-- 	INNER JOIN transactional.med_price ON ms.gcn = med_price.gcn AND ms.medid = med_price.medid
-- -- 	LEFT JOIN drugs_etl.network_pricing_mac ON network_pricing_mac.gcn_seqno = ms.gcn_seqno
-- WHERE
-- -- 	pharmacy_network_id = 1 
-- 	ended_on IS NULL
-- -- 	AND end_date is NULL
-- -- 	AND mac_list = 'BLINK02'
-- GROUP BY
-- 	ms.gcn,
-- 	ms.gcn_seqno,
-- 	ms.gcn_symphony_2017_rank,
-- 	ms.dosage_form_desc,
-- 	ms.strength,
-- 	branded,
-- 	generic_name_long


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
