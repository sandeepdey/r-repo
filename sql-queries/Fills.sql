with fills as (
	SELECT
		foi.last_pbm_adjudication_timestamp_approved::timestamp::date as order_date,
		foi.order_id,
		foi.med_id,
		f_gcn.gcn,
		f_gcn.gcn_seqno,
		foi.pharmacy_network_name, -- blink , supersaver or delivery 
		foi.last_claim_pharmacy_name_approved as pharmacy_name,
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
	AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '10 day' >= CURRENT_DATE
	AND f_gcn.gcn_seqno = 16879na
	GROUP BY
		1,2,3,4,5,6,7
)



SELECT
	foi.last_pbm_adjudication_timestamp_approved::timestamp::date as order_date,
	foi.order_id,
	foi.med_id,
	foi.pharmacy_network_name, -- blink , supersaver or delivery 
	foi.last_claim_pharmacy_name_approved as pharmacy_name,
	quantity,
	coalesce(last_claim_med_price_approved, 0) ::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0) ::float AS revenue,	
	coalesce(foi.last_pricing_total_cost_approved, 0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)::float AS cogs
FROM
	dwh.fact_order_item foi
WHERE
	foi.order_id = 5304315940095380907

select * from api_scraper_external.goodrx_price_raw;


select * from transactional.available_med limit 100;

select mf2ndc.*,mf2prc.*  from medispan.mf2prc join medispan.mf2ndc on mf2ndc.ndc_upc_hri = mf2prc.ndc_upc_hri where mf2prc.last_change_date::TIMESTAMP::date > '2019-12-01' limit 100;









SELECT
	date_trunc('day', CONVERT_TIMEZONE ('UTC', 'America/New_York', foi.last_pbm_adjudication_timestamp_approved)) AS fill_date
	-- it would be tremendously easier if we ran the business on UTC time or if the main table used to get these data offered this in ET
,
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
	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float AS filled_price_paid_aka_realized_gross_revenue
	-- what is reimnburse program discount amount?
,
	foi.pharmacy_network_id AS purchased_pharmacy_network_id
	-- null or 1 is edlp, 2 is bsd, 3 is hd, 4 is brookshires_quicksave, 5 is heb_quicksave …
	-- is there a join we can add which would turn this into a name?
,
	foi.last_claim_days_supply_approved AS filled_days_supply
	-- to compute a 30-day supply equivalent, divide the above by 30
,
	coalesce(last_pricing_total_cost_approved, 0)::float + coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS filled_ingredient_plus_dispensing_costs_aka_realized_cogs,
	coalesce(last_claim_med_price_approved, 0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0)::float - coalesce(last_pricing_total_cost_approved, 0)::float - coalesce(last_claim_wmt_true_up_amount_approved, 0)::float AS realized_gross_profit
	-- is literally the price paid from above - the cogs from above
,
	coalesce(foi.order_medication_discount_amount, 0)::float + coalesce(foi.allocated_order_discount_amount, 0)::float + coalesce(foi.allocated_wallet_payment_amount, 0)::float AS filled_discounts
	-- sum of discounts applied on the order (and presumably carried through to the fill)
FROM
	dwh.fact_order_item foi
	LEFT JOIN dwh.dim_user du -- why a left join? does it ever happen we don't have a user account?
	ON foi.account_id = du.account_id
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
	AND fill_date > '2019-10-01'
	
	
	
SELECT
	foi.last_pbm_adjudication_timestamp_approved::timestamp::date as order_date,
	foi.order_id,
	foi.dw_user_id,
	foi.med_id,
	foi.account_id,
	quantity,
	coalesce(last_claim_med_price_approved, 0) ::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0) ::float AS revenue,	
	coalesce(foi.last_pricing_total_cost_approved, 0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)::float AS cogs
FROM
	dwh.fact_order_item foi
	LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
		AND du.is_internal = FALSE
		AND du.is_phantom = FALSE
WHERE foi.fill_sequence IS NOT NULL
AND foi.is_fraud = FALSE
AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '180 day' >= CURRENT_DATE
AND foi.med_id in (579341,587566)

	
