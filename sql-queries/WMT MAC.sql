with ecomm_claims_1 as (
SELECT    date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date as fill_date
, dph.pharmacy_npi
, dph.pharmacy_name
, dph.ncpdp_relationship_id
, dph.ncpdp_relationship_name
, f_medid.medid               as filled_medid
, f_medid.med_name            as filled_med_name
, f_gcn.generic_name_short    as filled_generic_name_short
, f_gcn.strength              as filled_strength
, f_gcn.dosage_form_desc      as filled_form
, f_gcn.gcn                   as filled_gcn
, f_gcn.gcn_seqno             as filled_gcn_seqno
, case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end as filled_ndc
, ndc.label_name
, ndc.brand_name
, ndc.orange_book_code
, case when length(mf2name.generic_product_identifier::varchar) <> 14 then LPAD(mf2name.generic_product_identifier::varchar, 14, '0') ELSE mf2name.generic_product_identifier::varchar end as filled_gpi
, mf2ndc.multi_source_code
, foi.last_claim_quantity_approved as filled_quantity
, foi.last_claim_days_supply_approved as filled_days_supply
, case when foi.last_claim_days_supply_approved <84 then 30 else 90 end as days_supply_normalized
, 1 as fills
, foi.account_id::varchar
, foi.order_id::varchar
, foi.last_claim_transactional_claim_id::varchar
, foi.last_pricing_total_cost_approved
, foi.last_pricing_ingredient_cost_approved::float
, foi.last_pricing_dispensing_fee_approved::float
, foi.last_pricing_unc_cost_approved
, tc.pricing_strategy
, case when round(foi.last_pricing_total_cost_approved,2) >= round(foi.last_pricing_unc_cost_approved,2) then 1 else 0 end as unc_reimbursement_claim
, coalesce(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float,0) as awp_amount --AWP amount for GER eligible WMT claims


FROM dwh.fact_order_item foi
LEFT JOIN dwh.dim_ndc_hierarchy ndc ON
case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
case when length(ndc.ndc::varchar) <> 11 then LPAD(ndc.ndc::varchar, 11, '0') ELSE ndc.ndc::varchar end
LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn
ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
LEFT JOIN dwh.dim_medid_hierarchy f_medid
ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
LEFT JOIN transactional.transactional_claim tc
ON foi.last_claim_transactional_claim_id=tc.id
LEFT JOIN dwh.dim_pharmacy_hierarchy dph
ON foi.last_claim_pharmacy_npi_approved=dph.pharmacy_npi
LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON
case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
case when length(awp_hist_claim.ndc_upc_hri::varchar) <> 11 then LPAD(awp_hist_claim.ndc_upc_hri::varchar, 11, '0') ELSE awp_hist_claim.ndc_upc_hri::varchar end
AND foi.last_pbm_adjudication_timestamp_approved > awp_hist_claim.started_at AND foi.last_pbm_adjudication_timestamp_approved < awp_hist_claim.ended_at
LEFT JOIN medispan.mf2prc mf2prc ON
case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
case when length(mf2prc.ndc_upc_hri::varchar) <> 11 then LPAD(mf2prc.ndc_upc_hri::varchar, 11, '0') ELSE mf2prc.ndc_upc_hri::varchar end
AND mf2prc.price_code = 'A'
LEFT JOIN medispan.mf2ndc mf2ndc ON
case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
case when length(mf2ndc.ndc_upc_hri::varchar) <> 11 then LPAD(mf2ndc.ndc_upc_hri::varchar, 11, '0') ELSE mf2ndc.ndc_upc_hri::varchar end
LEFT JOIN medispan.mf2name mf2name
ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
WHERE foi.last_pbm_adjudication_timestamp_approved is not null
and dph.ncpdp_relationship_id = '229'


and date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date < '2020-06-23'  
and date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date >= '2020-06-08')
--getdate()::date-15)

, ecomm_claims_2 as (
select
x.*
,mac.price as updated_wmt_mac_price
,mac.price::float*x.filled_quantity::float as updated_wmt_mac_amount
,case when round(coalesce(coalesce(mac.price::float,0)*x.filled_quantity::float,x.last_pricing_ingredient_cost_approved::float)::float+x.last_pricing_dispensing_fee_approved::float,2)::float
>= round(x.last_pricing_unc_cost_approved::float,2)::float then 1 else 0 end as new_mac_unc_reimbursement_claim
from ecomm_claims_1 x
left join mac_pricing.mac_list_prices mac on x.filled_gpi=mac.gpi and mac.update_id=5 and mac.mac_list='BLINKWMT01')

, ecomm_claims_3 as (
select
sum(case when multi_source_code = 'Y' then fills end) as r14_generic_scripts
,sum(case when multi_source_code = 'Y' and unc_reimbursement_claim = 0 then fills end) as r14_ger_eligible_scripts_current
,sum(case when multi_source_code = 'Y' and unc_reimbursement_claim = 0 and awp_amount > 0 then fills end) as r14_ger_eligible_scripts_with_awp_current
,sum(case when multi_source_code = 'Y' and unc_reimbursement_claim = 0 then last_pricing_ingredient_cost_approved else 0 end) as r14_ger_eligible_ingredient_cost_current
,sum(case when multi_source_code = 'Y' and unc_reimbursement_claim = 0 and awp_amount > 0 then awp_amount else 0 end) as r14_ger_eligible_awp_amount_current

,sum(case when multi_source_code = 'Y' and new_mac_unc_reimbursement_claim = 0 then fills end) as r14_ger_eligible_scripts_new
,sum(case when multi_source_code = 'Y' and new_mac_unc_reimbursement_claim = 0 and awp_amount > 0 then fills end) as r14_ger_eligible_scripts_with_awp_new
,sum(case when multi_source_code = 'Y' and new_mac_unc_reimbursement_claim = 0 then coalesce(updated_wmt_mac_amount,last_pricing_ingredient_cost_approved) else 0 end) as r14_ger_eligible_ingredient_cost_new
,sum(case when multi_source_code = 'Y' and new_mac_unc_reimbursement_claim = 0 and awp_amount > 0 then awp_amount else 0 end) as r14_ger_eligible_awp_amount_new

from ecomm_claims_2)

select
*
,(r14_ger_eligible_awp_amount_current::float-r14_ger_eligible_ingredient_cost_current::float)::float/r14_ger_eligible_awp_amount_current::float as r14_ger_with_current_mac
,(r14_ger_eligible_awp_amount_new::float-r14_ger_eligible_ingredient_cost_new::float)::float/r14_ger_eligible_awp_amount_new::float as r14_ger_with_new_mac
from ecomm_claims_3;