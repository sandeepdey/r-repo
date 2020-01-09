-- Current Pricing
SELECT
	unit_price * 30 + dispensing_fee_margin + 1.75 AS current_default_qty_price,
	created_on,
	unit_price,
	dispensing_fee_margin,
	medid
FROM
	transactional.med_price
WHERE
	medid = 471239
	AND ended_on IS NULL
	AND pharmacy_network_id = 1;


SELECT
	am.medid,
	am.name,
	alternate_name,
	nh.gcn_seqno,
	nh.ndc,
	ms.*,
	
FROM
	transactional.available_med am
	JOIN dwh.dim_ndc_hierarchy nh ON nh.gcn_seqno = am.gcn_seqno
	JOIN medispan.mf2ndc ms ON ms.ndc_upc_hri = nh.ndc
WHERE
	am.name LIKE '%Glucophag%';



select * from transactional.available_med;


SELECT
	am.id,
	ms.ndc_upc_hri,
	am.medid,
	am.name,
	alternate_name,
	nh.gcn_seqno,
	nh.ndc,
	ms.*,
	am.*
FROM
	transactional.available_med am
	JOIN dwh.dim_ndc_hierarchy nh ON nh.gcn_seqno = am.gcn_seqno
	JOIN medispan.mf2ndc ms ON ms.ndc_upc_hri = nh.ndc
WHERE
	am.medid = 252504
order BY
	1,2
	;

