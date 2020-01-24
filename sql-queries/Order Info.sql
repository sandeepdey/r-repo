SELECT
	foi.last_pbm_adjudication_timestamp_approved,
	foi.ordered_timestamp,
	foi.order_id,
	foi.med_id AS purchased_medid,
	p_medid.med_name AS purchased_med_name,
	foi.gcn AS purchased_gcn,
	p_gcn.generic_name_short AS purchased_generic_name_short,
	p_gcn.strength AS purchased_strength,
	p_gcn.dosage_form_code_desc AS purchased_form,
	foi.quantity AS purchased_quantity,
	foi.price AS purchased_price,
	coalesce(foi.order_medication_discount_amount, 0)::float + coalesce(foi.allocated_order_discount_amount, 0)::float + coalesce(foi.allocated_wallet_payment_amount, 0)::float AS filled_discounts,
	foi.pharmacy_network_id AS purchased_pharmacy_network_id,
	foi.last_claim_ndc_approved AS filled_ndc,
	ndc.label_name AS filled_ndc_label_name,
	ndc.obsolete_date AS filled_ndc_obsolete_date,
	ndc.orange_book_code AS filled_ndc_orange_book_code,
	CASE WHEN ndc.orange_book_code in('AA', 'AB', 'AN', 'AO', 'AP', 'AT', 'ZA', 'ZC') THEN
		TRUE
	ELSE
		FALSE
	END AS filled_ndc_orange_book_legal_interchangeable_ind,
	ms.multi_source_code AS filled_ndc_multi_source_code,
	ndc.multi_single_source_ind AS filled_ndc_multi_single_source_ind,
	ndc.package_size AS filled_ndc_package_size,
	ndc.drug_form AS filled_ndc_drug_form,
	foi.last_claim_medid_approved AS filled_medid,
	f_medid.med_name AS filled_med_name,
	foi.last_claim_gcn_approved AS filled_gcn,
	f_gcn.generic_name_short AS filled_generic_name_short,
	f_gcn.strength AS filled_strength,
	f_gcn.dosage_form_code_desc AS filled_form,
	foi.last_claim_quantity_approved AS filled_quantity,
	foi.last_claim_days_supply_approved AS filled_days_supply
	--,awp_hist_claim.unit_price as filled_ndc_awp
,
	coalesce(foi.last_claim_med_price_approved, 0)::float + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0)::float AS filled_price_paid,
	coalesce(awp_hist_claim.unit_price, 0)::float * foi.last_claim_quantity_approved::float AS filled_ndc_awp_amt,
	foi.last_pricing_ingredient_cost_approved AS filled_ingredient_cost,
	CASE WHEN awp_hist_claim.unit_price > 0 THEN
		1 - (foi.last_pricing_ingredient_cost_approved::float / (coalesce(awp_hist_claim.unit_price, 0)::float * foi.last_claim_quantity_approved::float))
	ELSE
		0
	END AS inferred_contracted_awp_discount_rate,
	foi.last_pricing_dispensing_fee_approved AS filled_dispensing_fee_cost,
	coalesce(foi.last_pricing_total_cost_approved, 0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)::float AS filled_total_cost,
	coalesce(foi.last_claim_med_price_approved, 0)::float + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0)::float - (coalesce(foi.last_pricing_total_cost_approved, 0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)::float) AS filled_gross_profit,
	tc.pricing_strategy,
	p.pharmacy_name,
	p.ncpdp_relationship_name,
	p.parent_organization_name,
	foi.total_balance_billing_amount,
	foi.total_balance_billing_amount_intended,
	foi.last_balance_billing_reason,
	foi.last_balance_billing_status
FROM
	dwh.fact_order_item foi
	LEFT JOIN dwh.dim_user du ON foi.account_id = du.account_id
	LEFT JOIN dwh.dim_medid_hierarchy p_medid ON foi.med_id = p_medid.medid
	LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid
	LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn ON foi.gcn_seqno = p_gcn.gcn_seqno
	LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
	LEFT JOIN dwh.dim_pharmacy_hierarchy p ON foi.last_claim_pharmacy_npi_approved = p.pharmacy_npi
	LEFT JOIN transactional.transactional_claim tc ON foi.last_claim_transactional_claim_id = tc.id
	LEFT JOIN dwh.dim_ndc_hierarchy ndc ON foi.last_claim_ndc_approved = ndc.ndc
	LEFT JOIN medispan.mf2ndc ms ON foi.last_claim_ndc_approved = ms.ndc_upc_hri
	LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON COALESCE(foi.last_claim_ndc_approved, '0') = awp_hist_claim.ndc_upc_hri
		AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
WHERE
	du.is_internal = FALSE
	AND du.is_phantom = FALSE
	AND foi.is_fraud = FALSE
	AND foi.last_pbm_adjudication_timestamp_approved IS NOT NULL
	AND foi.order_id = 5305717137339412479
ORDER BY
	1 DESC
LIMIT 5