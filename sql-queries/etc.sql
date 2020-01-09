-- select * from pg_tables where tablename like '%top_300_drugs%'

-- drop table mktg_dev.top_300_drugs_20191121

-- GRANT SELECT ON mktg_dev.sdey_magic_card_pricing_and_competitive_data_top_300_20191122 TO "public";


-- select * from fifo.bsd_retirement_input order by fill_date desc;


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
