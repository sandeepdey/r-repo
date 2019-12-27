SELECT
-- 	date_trunc('day', CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)) AS fill_date,
	CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date AS fill_date,
	foi.med_id AS purchased_medid,
	foi.last_claim_medid_approved AS filled_medid,
	p_medid.med_name AS purchased_med_name,
	f_medid.med_name AS filled_med_name,
	foi.gcn AS purchased_gcn,
	foi.last_claim_gcn_approved AS filled_gcn,
	p_gcn.generic_name_short AS purchased_generic_name_short,
	f_gcn.generic_name_short AS filled_generic_name_short,
	foi.quantity AS purchased_quantity,
	foi.last_claim_quantity_approved AS filled_quantity,
	foi.price AS purchased_price,
	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float AS filled_price_paid_aka_realized_gross_revenue,
	foi.pharmacy_network_id AS purchased_pharmacy_network_id,
	CASE WHEN pharmacy_network_id IS NULL
		OR pharmacy_network_id = 1 THEN
		'EDLP'
	WHEN pharmacy_network_id = 2 THEN
		'BSD'
	WHEN pharmacy_network_id = 3 THEN
		'HD'
	WHEN pharmacy_network_id = 4
		OR pharmacy_network_id = 5 THEN
		'quicksave'
	ELSE
		'NONE'
	END AS purchased_pharmacy_network,
	foi.last_claim_days_supply_approved AS filled_days_supply,
	coalesce(last_pricing_total_cost_approved, 0)::float + coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS filled_ingredient_plus_dispensing_costs_aka_realized_cogs,
	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float - coalesce(last_pricing_total_cost_approved, 0)::float - coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS realized_gross_profit,
	coalesce(foi.order_medication_discount_amount, 0)::float + coalesce(foi.allocated_order_discount_amount, 0)::float + coalesce(foi.allocated_wallet_payment_amount, 0)::float AS filled_discounts
FROM
	dwh.fact_order_item foi
	LEFT JOIN dwh.dim_user du ON foi.account_id = du.account_id
	-- i've also see a foi.dw_user_id = du.dw_user_id, should that be added?
		AND du.is_internal = FALSE -- removes internal users
		AND du.is_phantom = FALSE -- remove phantom users
	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno -- add’l info for med purchased on the order, (e.g. generic med name)
	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno -- add’l info for med actually filled, (e.g. generic med name)
WHERE
	foi.is_fraud = FALSE
	AND foi.last_pbm_adjudication_timestamp_approved IS NOT NULL
	AND foi.quantity != foi.last_claim_quantity_approved
	AND foi.gcn != foi.last_claim_gcn_approved
	AND CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)::timestamp::date >= '2019-01-01'
;
