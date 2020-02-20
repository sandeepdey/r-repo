
-- Utilization Statistics For Magic Card ---
WITH heb AS (
	SELECT
-- 		action_type,
-- 		medid,
		gcn,
		sum(quantity,
		count(*) AS fills
	FROM
		fifo.magic_fact_order_claim
	WHERE
		action_timestamp::date + 365 > CURRENT_DATE
		AND action_type in('reversal','approval','denied')
		AND action_sequence = 1
	GROUP BY
		1,2
	ORDER BY
		count_total DESC		
)
SELECT
	heb.action_type,
	heb.medid,
	heb.count_total,
	dmh.medid IS NOT NULL AS found_in_fdb,
	active_price.mid IS NOT NULL AS found_in_price,
	am.medid IS NOT NULL AS found_on_site
FROM
	heb
	LEFT OUTER JOIN dwh.dim_medid_hierarchy dmh ON heb.medid = dmh.medid
	LEFT OUTER JOIN ( SELECT DISTINCT
			(medid) AS mid
		FROM
			transactional.med_price
		WHERE
			ended_on IS NULL) active_price ON active_price.mid = dmh.medid
	LEFT OUTER JOIN transactional.available_med am ON am.medid = dmh.medid;



-- Utilization GCN Statistics For Magic Card ---

WITH heb AS (
	SELECT
		gcn,
		gcn_seqno,
		count(*) AS fills
	FROM
		fifo.magic_fact_order_claim
	WHERE
		action_timestamp::date + 90 > CURRENT_DATE
		AND action_type in('reversal','approval','denied')
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
	( SELECT DISTINCT(gcn) AS gcn FROM transactional.available_med) am ON am.gcn = heb.gcn;



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


