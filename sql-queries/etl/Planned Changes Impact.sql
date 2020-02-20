
------ Get Info on Changes by Planned Data
with fills as (
	SELECT
		foi.last_pbm_adjudication_timestamp_approved::timestamp::date as order_date,
		foi.med_id,
		f_gcn.gcn,
		case 
			when foi.pharmacy_network_name = 'blink' then 1
			when foi.pharmacy_network_name = 'supersaver' then 2
			when foi.pharmacy_network_name = 'delivery' then 3
			else -1
		end as pharmacy_network_id,
		sum(1) as fills,
		sum(quantity) as quantities,
		sum(coalesce(last_claim_med_price_approved, 0) ::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0) ::float) AS revenue,	
		sum(coalesce(foi.last_pricing_total_cost_approved, 0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)::float) AS cogs
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
	AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '90 day' >= CURRENT_DATE
	GROUP BY
		1,2,3,4), 
changes as (
	SELECT
		apr.gcn,
		apr.change_label,
		apr.med_id,
		apr.pricing_strategy,
		apr.dispensing_fee_margin,
		apr.unit_price,
		apr.pharmacy_network_id,
		cpcs.date_activated,
		cpcs.status
	FROM
		mktg_dev.sdey_automated_pricing_recommendations as apr
		INNER JOIN mktg_dev.sdey_consumer_pricing_changelist_status as cpcs ON apr.change_label = cpcs.change_label) 
SELECT
		order_date,
		fills,
		quantities,
		revenue,	
		cogs,
		changes.*
FROM
	changes INNER join fills 
	ON fills.gcn = changes.gcn 
	and fills.med_id = changes.med_id 
	and fills.pharmacy_network_id = changes.pharmacy_network_id