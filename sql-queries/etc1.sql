

select * from 
(select dm.gcn,dm.gcn_seqno,hicl_desc, brand_med_name,strength,form,default_quantity,blink_edlp_price,min_major_retail_grx,blink_edlp_price-min_major_retail_grx as d,count(distinct order_id) as fill_count from fifo.generic_price_portfolio_datamart dm join dwh.fact_order_item foi on foi.gcn_seqno=dm.gcn_seqno and foi.fill_sequence is not null and ordered_timestamp>='2020-01-01' where min_major_retail_grx is not null and blink_edlp_price is not null group by 1,2,3,4,5,6,7,8,9,10 having count(distinct order_id)>10 order by d desc limit 10) a
left outer JOIN
(select * from ( select gcn,quantity,geo,pharmacy,price,ROW_NUMBER() OVER (PARTITION BY gcn,quantity ORDER BY price ASC) rn FROM  pricing_external_dev.goodrx_raw_data where date>'2020-05-01' ) where rn=1 ) b
ON a.gcn = b.gcn and a.default_quantity = b.quantity;



select * from ( select gcn,quantity,geo,pharmacy,price,ROW_NUMBER() OVER (PARTITION BY gcn,quantity ORDER BY price ASC) rn FROM  pricing_external_dev.goodrx_raw_data where date>'2020-05-01' ) where rn=1;


 by price asc ;


SELECT * from pricing_external_dev.goodrx_raw_data where date>'2020-01-01' and quantity=180 and gcn=16300 
order by price asc limit 1;





select * from fifo.generic_price_portfolio_datamart limit 10;


select * from dwh.dim_gcn_seqno_hierarchy where gcn=97061;

select * from transactional.available_med where gcn_seqno=61267;

select * from transactional.med where gcn_seqno=61267;

select * from transactional.med_price where gcn=97061 and ended_on is null and pharmacy_network_id=1;


WITH digital_utilization AS (
	SELECT
		CASE WHEN length(mf2name.generic_product_identifier::varchar) <> 14 THEN
			LPAD(mf2name.generic_product_identifier::varchar,
				14,
				'0')
		ELSE
			mf2name.generic_product_identifier::varchar
		END AS gpi,
		coalesce(coalesce(awp_hist_claim.unit_price,
				mf2prc.unit_price)::float * foi.last_claim_quantity_approved::float,
			0) AS awp_amount,
		last_claim_quantity_approved AS claim_quantity
	FROM
		dwh.fact_order_item foi
	LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
		AND du.is_internal = FALSE
		AND du.is_phantom = FALSE
	LEFT JOIN dwh.dim_ndc_hierarchy ndc ON CASE WHEN length(foi.last_claim_ndc_approved::varchar) <> 11 THEN
		LPAD(foi.last_claim_ndc_approved::varchar,
			11,
			'0')
	ELSE
		foi.last_claim_ndc_approved::varchar
	END = CASE WHEN length(ndc.ndc::varchar) <> 11 THEN
		LPAD(ndc.ndc::varchar,
			11,
			'0')
	ELSE
		ndc.ndc::varchar
	END
	LEFT JOIN dwh.dim_pharmacy_hierarchy dph ON foi.last_claim_pharmacy_npi_approved = dph.pharmacy_npi
	LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON CASE WHEN length(foi.last_claim_ndc_approved::varchar) <> 11 THEN
		LPAD(foi.last_claim_ndc_approved::varchar,
			11,
			'0')
	ELSE
		foi.last_claim_ndc_approved::varchar
	END = CASE WHEN length(awp_hist_claim.ndc_upc_hri::varchar) <> 11 THEN
		LPAD(awp_hist_claim.ndc_upc_hri::varchar,
			11,
			'0')
	ELSE
		awp_hist_claim.ndc_upc_hri::varchar
	END
		AND foi.last_pbm_adjudication_timestamp_approved > awp_hist_claim.started_at
		AND foi.last_pbm_adjudication_timestamp_approved < awp_hist_claim.ended_at
	LEFT JOIN medispan.mf2prc mf2prc ON CASE WHEN length(foi.last_claim_ndc_approved::varchar) <> 11 THEN
		LPAD(foi.last_claim_ndc_approved::varchar,
			11,
			'0')
	ELSE
		foi.last_claim_ndc_approved::varchar
	END = CASE WHEN length(mf2prc.ndc_upc_hri::varchar) <> 11 THEN
		LPAD(mf2prc.ndc_upc_hri::varchar,
			11,
			'0')
	ELSE
		mf2prc.ndc_upc_hri::varchar
	END
		AND mf2prc.price_code = 'A'
	LEFT JOIN dwh.dim_pac_price_hist pac_hist_claim ON CASE WHEN length(foi.last_claim_ndc_approved::varchar) <> 11 THEN
		LPAD(foi.last_claim_ndc_approved::varchar,
			11,
			'0')
	ELSE
		foi.last_claim_ndc_approved::varchar
	END = CASE WHEN length(pac_hist_claim.ndc_drug_identifier::varchar) <> 11 THEN
		LPAD(pac_hist_claim.ndc_drug_identifier::varchar,
			11,
			'0')
	ELSE
		pac_hist_claim.ndc_drug_identifier::varchar
	END
		AND foi.last_pbm_adjudication_timestamp_approved >= pac_hist_claim.started_at
		AND foi.last_pbm_adjudication_timestamp_approved < pac_hist_claim.ended_at
	LEFT JOIN medispan.mf2ndc mf2ndc ON CASE WHEN length(foi.last_claim_ndc_approved::varchar) <> 11 THEN
		LPAD(foi.last_claim_ndc_approved::varchar,
			11,
			'0')
	ELSE
		foi.last_claim_ndc_approved::varchar
	END = CASE WHEN length(mf2ndc.ndc_upc_hri::varchar) <> 11 THEN
		LPAD(mf2ndc.ndc_upc_hri::varchar,
			11,
			'0')
	ELSE
		mf2ndc.ndc_upc_hri::varchar
	END
	LEFT JOIN medispan.mf2name mf2name ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
WHERE
	foi.last_pbm_adjudication_timestamp_approved IS NOT NULL
	AND foi.is_fraud = FALSE
	AND mf2ndc.multi_source_code = 'Y'
	AND date_trunc('day',
		convert_timezone ('UTC',
			'America/New_York',
			foi.last_pbm_adjudication_timestamp_approved))::date + INTERVAL '120 day' >= CURRENT_DATE
	AND(foi.pharmacy_network_id != 3
		AND dph.ncpdp_relationship_id != '229'
		AND lower(dph.pharmacy_name)
		NOT ILIKE '%walmart%') -- NON HD & WMT
	--               AND foi.pharmacy_network_id in (4,5,6) -- quicksave - digital
),
counter_utilization AS (
	SELECT
		CASE WHEN length(mf2name.generic_product_identifier::varchar) <> 14 THEN
			LPAD(mf2name.generic_product_identifier::varchar,
				14,
				'0')
		ELSE
			mf2name.generic_product_identifier::varchar
		END gpi,
		COALESCE(mfoi.quantity::float * coalesce(awp_hist_claim.unit_price,
				mf2prc.unit_price,
				0)::float,
			0) AS awp_amount,
		mfoi.quantity::float AS claim_quantity
	FROM
		fifo.magic_fact_order_claim mfoi
	LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON COALESCE(mfoi.ndc,
		'0')::float = awp_hist_claim.ndc_upc_hri::float
		AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
	LEFT JOIN medispan.mf2prc mf2prc ON COALESCE(mfoi.ndc,
		'0')::float = mf2prc.ndc_upc_hri::float
		AND mf2prc.price_code = 'A'
	LEFT JOIN medispan.mf2ndc mf2ndc ON COALESCE(mfoi.ndc,
		'0')::float = mf2ndc.ndc_upc_hri::float
	LEFT JOIN medispan.mf2name mf2name ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
WHERE (mfoi.action_type = 'approval')
AND(mfoi.action_sequence = 1)
AND(date_trunc('day',
		convert_timezone ('UTC',
			'America/New_York',
			mfoi.action_timestamp))::date + INTERVAL '120 day' >= CURRENT_DATE)
AND mf2ndc.multi_source_code = 'Y'
),
combined_utilization AS (
	SELECT
		*
	FROM
		digital_utilization
	UNION ALL
	SELECT
		*
	FROM
		counter_utilization
)
SELECT
    gpi,
    sum(coalesce(claim_quantity,0)) AS sum_last_claim_qty_approved,
    sum(coalesce(awp_amount,0)) AS sum_realized_awp_amount
FROM 
	combined_utilization
GROUP BY gpi
;	






select * from transactional.transactional_claim where claim_prescription_number like '%9000106182%'

