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
select scrape_date , count(*) from mktg_dev.sdey_weighted_competitive_index group by 1 order by 1 desc;