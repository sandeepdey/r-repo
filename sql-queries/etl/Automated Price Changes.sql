DROP TABLE IF EXISTS mktg_dev.sdey_target_gcns_with_neg_revenue;
CREATE TABLE mktg_dev.sdey_target_gcns_with_neg_revenue AS
SELECT
	foi.pharmacy_network_name,
	foi.med_id,
	f_gcn.gcn,
	f_gcn.gcn_seqno,
	sum(
		1) AS fills,
	count(
		DISTINCT foi.dw_user_id) AS users,
	count(
		DISTINCT order_id) AS orders,
	sum(
		coalesce(last_claim_med_price_approved, 0) ::float + coalesce(last_claim_reimburse_program_discount_amount_approved, 0) ::float) AS revenue,
	sum(
		coalesce(foi.last_claim_med_price_approved, 0) + coalesce(foi.last_claim_reimburse_program_discount_amount_approved, 0) - coalesce(foi.last_pricing_total_cost_approved, 0) - coalesce(foi.last_claim_wmt_true_up_amount_approved, 0)) AS margin
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
WHERE (
	foi.fill_sequence IS NOT NULL)
AND foi.is_fraud = FALSE
AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '90 day' >= CURRENT_DATE
GROUP BY
	1,
	2,
	3,
	4
HAVING
	margin < 0
;


DROP TABLE IF EXISTS mktg_dev.sdey_target_gcns_with_updated_history;
CREATE TABLE mktg_dev.sdey_target_gcns_with_updated_history AS
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
			ELSE 'N/A'
		END AS in_consideration_reason
	FROM
		tgnr_updated AS tu
	LEFT OUTER JOIN
		gcns_with_pricing_changes as gwpc
	ON
		tu.gcn = gwpc.gcn
		AND tu.med_id = gwpc.medid
		AND tu.pharmacy_network_id = gwpc.pharmacy_network_id
	LEFT OUTER JOIN
		planned_changes as pc
	ON
		tu.gcn = pc.gcn
		AND tu.med_id = pc.med_id
		AND tu.pharmacy_network_id = pc.pharmacy_network_id
)
select * from target_gcns_changedata
;


DROP TABLE IF EXISTS mktg_dev.sdey_collect_data_about_gcns;
CREATE TABLE mktg_dev.sdey_collect_data_about_gcns AS
SELECT
	tguh.gcn,
	tguh.med_id,
	tguh.gcn_seqno,
	tguh.pharmacy_network_id,
	tguh.pharmacy_network_name,
	tguh.in_consideration,
	tguh.in_consideration_reason,
	tguh.fills,
	tguh.margin,
	tguh.orders,
	tguh.revenue,
	tguh.users,
	gppd.gcn AS gcn_gppd,
	gppd.hicl_desc,
	gppd.brand_med_name,
	gppd.strength,
	gppd.form,
	gppd.default_quantity,
	gppd.blink_hicl_rank,
	gppd.top_30ds_quantity,
	gppd.top_90ds_quantity,
	gppd.edlp_unit_price,
	gppd.bsd_unit_price,
	gppd.hd_unit_price,
	gppd.edlp_fixed_price,
	gppd.bsd_fixed_price,
	gppd.hd_fixed_price,
	gppd.bh01_mac_price,
	gppd.bh02_mac_price,
	gppd.bh03_mac_price,
	gppd.wmt_mac_price,
	gppd.hd_syr_mac_price,
	gppd.bh01_dispensing_fee,
	gppd.bh02_dispensing_fee,
	gppd.bh03_dispensing_fee,
	gppd.wmt_dispensing_fee,
	gppd.hd_syr_dispensing_fee,
	gppd.wmt_2018_11_28_qty1,
	gppd.wmt_2018_11_28_qty2,
	gppd.wmt_2018_11_28_price1,
	gppd.wmt_2018_11_28_price2,
	gppd.wmt_2018_11_28_flg,
	gppd.last_30ds_qty,
	gppd.last_90ds_qty,
	gppd.min_grx_30ds,
	gppd.min_grx_90ds,
	gppd.min_major_retail_grx_30ds,
	gppd.min_major_retail_grx_90ds,
	gppd.min_retail_grx_30ds,
	gppd.min_retail_grx_90ds,
	gppd.last_30ds_qty_scrape_date,
	gppd.ltd_30_day_scripts,
	gppd.ltd_90_day_scripts,
	gppd.ltd_30_day_scripts_pct,
	gppd.ltd_90_day_scripts_pct,
	gppd.r30_30_day_scripts,
	gppd.r30_90_day_scripts,
	gppd.r30_30_day_script_pct,
	gppd.r30_90_day_script_pct
FROM
	mktg_dev.sdey_target_gcns_with_updated_history AS tguh
LEFT OUTER JOIN 
	fifo.generic_price_portfolio_datamart AS gppd 
ON 
	tguh.gcn = gppd.gcn
	AND tguh.gcn_seqno = gppd.gcn_seqno
WHERE
	margin < -300;
	
	
GRANT SELECT ON mktg_dev.sdey_automated_pricing_recommendations TO "public";
GRANT SELECT ON mktg_dev.sdey_consumer_pricing_changelist_status TO "public";
GRANT SELECT ON mktg_dev.sdey_collect_data_about_gcns TO "public";


-- select * from mktg_dev.sdey_collect_data_about_gcns;
-- select * from mktg_dev.sdey_automated_pricing_recommendations;