


select * from fifo.magic_fact_order_claim 
where action_type='approval' and action_sequence = 1
	AND action_timestamp::date > '2020-01-01'



-- Utilization Statistics For Magic Card ---
WITH heb AS (
	SELECT
		action_type,
-- 		medid,
-- 		gcn,
-- 		sum(quantity,
		count(*) AS fills
	FROM
		
		fifo.magic_fact_order_claim
	WHERE
		action_timestamp::date + 365 > CURRENT_DATE
		AND action_type in('reversal','approval','denied')
-- 		AND action_type in('denied')
		AND action_sequence = 1
	GROUP BY
		1
)
SELECT
-- 	heb.action_type,
	am.gcn,
-- 	heb.medid,
	heb.fills
-- 	dmh.medid IS NOT NULL AS found_in_fdb,
-- 	active_price.mid IS NOT NULL AS found_in_price,
-- 	am.medid IS NOT NULL AS found_on_site
FROM
	heb
	inner join transactional.available_med am ON am.medid = heb.medid
	
-- 	LEFT OUTER JOIN dwh.dim_medid_hierarchy dmh ON heb.medid = dmh.medid
-- 	LEFT OUTER JOIN ( SELECT DISTINCT
-- 			(medid) AS mid
-- 		FROM
-- 			transactional.med_price
-- 		WHERE
-- 			ended_on IS NULL) active_price ON active_price.mid = dmh.medid
-- 	LEFT OUTER JOIN transactional.available_med am ON am.medid = dmh.medid
-- WHERE
-- 	dmh.is_branded_price = TRUE
;



-- Utilization GCN Statistics For Magic Card ---

WITH heb AS (
	SELECT
		gcn,
		gcn_seqno,
		count(*) AS fills
	FROM
		fifo.magic_fact_order_claim
	WHERE
		action_timestamp::date + 365 > CURRENT_DATE
-- 		AND action_type in('reversal','approval','denied')
		AND action_type in('denied')
		AND action_sequence = 1
	GROUP BY
		1,2
)
SELECT
-- 	heb.action_type,
-- 	heb.medid,
	heb.gcn,
	heb.fills,
	dmh.gcn_seqno IS NOT NULL AS found_in_fdb,
	active_price.gcn IS NOT NULL AS found_in_price,
	am.gcn IS NOT NULL AS found_on_site,
	heb_price.gcn IS NOT NULL AND am.gcn IS NOT NULL as found_on_heb
FROM
	heb
LEFT OUTER JOIN 
	( SELECT DISTINCT(gcn_seqno) AS gcn_seqno from dwh.dim_medid_hierarchy) dmh ON heb.gcn_seqno = dmh.gcn_seqno
LEFT OUTER JOIN 
	( SELECT DISTINCT(gcn) AS gcn FROM transactional.med_price WHERE ended_on IS NULL) active_price ON active_price.gcn = heb.gcn
LEFT OUTER JOIN ( SELECT DISTINCT(gcn) AS gcn FROM transactional.med_price WHERE ended_on IS NULL AND pharmacy_network_id=5) heb_price ON heb_price.gcn = heb.gcn
LEFT OUTER JOIN 
	( SELECT DISTINCT(gcn) AS gcn FROM transactional.available_med) am ON am.gcn = heb.gcn
order by fills desc 
;



SELECT
	active_price.mid IS NOT NULL AS found_in_price,
	am.medid IS NOT NULL AS found_on_site,
         
FROM
	LEFT OUTER JOIN transactional.available_med am ON am.medid = dmh.medid





SELECT
	pharmacy_network.name,
	count(DISTINCT (med_price.medid)) as medids,
	count(DISTINCT (med_price.gcn)) as gcns,
	count(DISTINCT (available_med.medid)) as medids_available,
	count(DISTINCT (available_med.gcn)) as gcns_available
FROM
	transactional.med_price
INNER JOIN
	transactional.pharmacy_network ON pharmacy_network.id=pharmacy_network_id
LEFT OUTER JOIN
	transactional.available_med ON available_med.medid = med_price.medid
WHERE
	ended_on IS NULL
GROUP BY
	1;



with heb as (
	SELECT
		foi.med_id as medid,
		sum(1) as fills
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
	WHERE foi.fill_sequence IS NOT NULL
	AND foi.is_fraud = FALSE
	AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '1000 day' >= CURRENT_DATE
	GROUP BY
		1
)
SELECT
-- 	heb.action_type,
	am.gcn,
-- 	heb.medid,
	heb.fills
-- 	dmh.medid IS NOT NULL AS found_in_fdb,
-- 	active_price.mid IS NOT NULL AS found_in_price,
-- 	am.medid IS NOT NULL AS found_on_site
FROM
	heb
	inner join transactional.available_med am ON am.medid = heb.medid
	INNER join dwh.dim_medid_hierarchy dmh ON heb.medid = dmh.medid
where 
	dmh.is_branded_price = TRUE
-- 	LEFT OUTER JOIN dwh.dim_medid_hierarchy dmh ON heb.medid = dmh.medid
-- 	LEFT OUTER JOIN ( SELECT DISTINCT
-- 			(medid) AS mid
-- 		FROM
-- 			transactional.med_price
-- 		WHERE
-- 			ended_on IS NULL) active_price ON active_price.mid = dmh.medid
-- 	LEFT OUTER JOIN transactional.available_med am ON am.medid = dmh.medid
-- WHERE
-- 	dmh.is_branded_price = TRUE
;

select gcn,gcn_symphony_2017_rank from dwh.dim_gcn_seqno_hierarchy where gcn_symphony_2017_rank<1000;



	SELECT
		action_timestamp::date,
		action_type,
		case 
			when claim_days_supply<=34 then '30d supply'
			else '90d supply'
		end as supply,
-- 		medid,
-- 		gcn,
-- 		sum(quantity,
		count(*) AS fills
	FROM
		
		fifo.magic_fact_order_claim
	WHERE
		action_timestamp::date + 365 > CURRENT_DATE
		AND action_type in('reversal','approval','denied')
		AND action_sequence = 1
	GROUP BY
		1,2,3;





