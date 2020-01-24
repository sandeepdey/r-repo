------------------------Default Quantity ----------------

drop table if EXISTS mktg_dev.sdey_gcn_default_quantity_mapping;
create table mktg_dev.sdey_gcn_default_quantity_mapping as
WITH most_recent_grx_default1 AS (
	SELECT
		gcn,
		quantity,
		row_number() OVER (PARTITION BY gcn ORDER BY date DESC) AS qty_rn
	FROM
		api_scraper_external.competitor_pricing
	WHERE
		default_quantity = quantity
		AND site != 'all'
		AND geo != 'all'
),
most_recent_grx_default2 AS (
SELECT
	gcn,
	quantity AS default_quantity_1
FROM
	most_recent_grx_default1
WHERE
	qty_rn = 1
),
default_qty_blink_1 AS (
SELECT
	y.gcn,
	x.quantity,
	sum(x.count) AS gcn_ranking_count
FROM
	transactional.medication_quantity x
	LEFT JOIN transactional.med y ON x.medid = y.medid
GROUP BY
	1,
	2
),
default_qty_blink_2 AS (
SELECT
	*,
	row_number() OVER (PARTITION BY gcn ORDER BY gcn_ranking_count DESC) AS rn
FROM
	default_qty_blink_1
),
default_qty_blink_3 AS ( SELECT DISTINCT
	gcn,
	quantity AS default_quantity_2
FROM
	default_qty_blink_2
WHERE
	rn = 1
),
null_gcn_list AS ( SELECT DISTINCT
	gcn,
	30 AS default_quantity_3
FROM
	api_scraper_external.competitor_pricing
WHERE
	default_quantity IS NULL
),
default_quantity_mega_list_1 AS ( SELECT DISTINCT
	gcn
FROM
	api_scraper_external.competitor_pricing
),
default_quantity_mega_list_2 AS (
SELECT
	a.gcn,
	coalesce(b.default_quantity_1,c.default_quantity_2,d.default_quantity_3) AS quantity
FROM
	default_quantity_mega_list_1 a
	LEFT JOIN most_recent_grx_default2 b ON a.gcn = b.gcn
	LEFT JOIN default_qty_blink_3 c ON a.gcn = c.gcn
	LEFT JOIN null_gcn_list d ON a.gcn = d.gcn
) SELECT
	*
FROM
	default_quantity_mega_list_2
;

GRANT SELECT ON mktg_dev.sdey_gcn_default_quantity_mapping TO "public";
select count(*),count(distinct(gcn)),count(distinct(quantity)) from mktg_dev.sdey_gcn_default_quantity_mapping;

	
