WITH utilization AS (
SELECT
CASE
WHEN length(mf2name.generic_product_identifier::varchar) <> 14 then LPAD(mf2name.generic_product_identifier::varchar,14,'0')
ELSE mf2name.generic_product_identifier::varchar
END gpi,
COALESCE( mfoi.quantity::float * coalesce(awp_hist_claim.unit_price,mf2prc.unit_price,0)::float ,0) awp_amount,
mfoi.quantity::float AS claim_quantity
FROM fifo.magic_fact_order_claim mfoi
LEFT JOIN transactional.transactional_claim tc on mfoi.transactional_claim_id=tc.id
LEFT JOIN dwh.dim_pharmacy_hierarchy dph on mfoi.pharmacy_npi=dph.pharmacy_npi
LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn on mfoi.gcn_seqno=gcn.gcn_seqno
LEFT JOIN dwh.dim_medid_hierarchy medid on mfoi.medid=medid.medid
LEFT JOIN dwh.dim_ndc_hierarchy ndc on COALESCE(mfoi.ndc,'0')::float=ndc.ndc::float
LEFT JOIN mh_tem_sub_flag mh_tem_sub_flag on mfoi.gcn=mh_tem_sub_flag.gcn
LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim on COALESCE(mfoi.ndc,'0')::float = awp_hist_claim.ndc_upc_hri::float AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
LEFT JOIN medispan.mf2prc mf2prc on COALESCE(mfoi.ndc,'0')::float = mf2prc.ndc_upc_hri::float AND mf2prc.price_code = 'A'
LEFT JOIN medispan.mf2ndc mf2ndc on COALESCE(mfoi.ndc,'0')::float = mf2ndc.ndc_upc_hri::float
LEFT JOIN medispan.mf2name mf2name on mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
WHERE (mfoi.action_type = 'approval')
AND (mfoi.action_sequence = 1)
AND (date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date + INTERVAL '120 day' >= current_date)
AND mf2ndc.multi_source_code = 'Y'
--         AND brand_generic_type = 'Generic'
)
SELECT
    gpi,
    sum(coalesce(claim_quantity,0)) AS sum_last_claim_qty_approved,
    sum(coalesce(awp_amount,0)) AS sum_realized_awp_amount
FROM utilization
GROUP BY gpi
;

SELECT * from mac_pricing.mac_ger_network_targets;






select
--  Note that any field added to this query should be added to the group
--  by clause.  See the note there for more information.
    m.gcn_seqno,
    n.id as med_name_id,
    max(m.medid) as medid,
    n.name,
    lower(hicl.GNN) as alternate_name,
    price.gcn as gcn,
    m.route,
    m.dosage_form as form,
    m.gcn_str as strength,
    n.type_description,
    pack.drug_form_desc as billing_unit
from med med_base
--  Time for a quirky one.  Some medids have been replaced, but both exist in the med table.
--  We're going to use the history table to select only the new one, when we have a conflict.
--  The sub query is a performance tweak, for some reason the query is significantly faster with it
left join (select * from med_history limit 9999999) hist on med_base.medid=hist.previous_medid
inner join med m on m.medid=med_base.medid and
    --  Also condition this on this not being one that was replaced
    (hist.previous_medid is null or  m.medid = hist.replacement_medid)
-- Collect the name data.
inner join med_name n on n.id=m.med_name_id -- Link to the base name data
inner join fdb.RGCNSEQ4_GCNSEQNO_MSTR gcn on gcn.GCN_SEQNO=m.gcn_seqno -- Link through here to get to:
inner join fdb.RHICLSQ1_HICLSEQNO_MSTR hicl on hicl.HICL_SEQNO=gcn.HICL_SEQNO -- Alternate name data
--  At this point we should have our peak table.  Now we start losing
--  items.
--  Available meds should have a current price.
inner join med_price price on
    price.medid=m.medid and
    price.ended_on is null and -- Prices have to be current
    price.dispensing_fee_margin is not null and -- The price has to have a margin
    price.unit_margin is not null -- And the price must have margin on the unit.
-- Available meds should also have a current auth cost.
-- Using candle_med_authorization_cost since we get a list from candle
-- med_authorization_cost table uses med impact list which is old and
-- haven't been updated since March 2018
-- Also ended_on on candle_med_authorization doesn't have a date and instead
-- is Null instead of date on med_authorization_cost which dates to year 9999
inner join candle_med_authorization_cost cmac
    on cmac.med_id = m.medid
    and cmac.ended_on is NULL
--  Time to get the package data. If we don't have a valid package it can't be sold.
inner join valid_packages pack on pack.gcn_seqno=m.gcn_seqno
where
    m.gcn_str != ''  --   And anything that is strength-less, isn't valid
    -- exclude unauthorizable hicls
    and (price.branded = 1 or m.medid in (
      select authable_med.medid
      from med authable_med
      where authable_med.hicl in (
        select authable_hicl.hicl
        from med authable_hicl join med_package mp2 on mp2.medid = authable_hicl.medid
        where coalesce(mp2.obsolete_date, '2050-01-01') > date_sub(utc_timestamp, interval 3 year) and mp2.is_branded = 0
      )
    ))
    -- manually excluded medids
    and m.medid not in (
      select exclude_id from medication_exclusion where exclude_by = 'medid'
    )
    -- manually excluded med_name_ids
    and m.med_name_id not in (
      select exclude_id from medication_exclusion where exclude_by = 'med_name_id'
    )
    -- manually excluded gcns
    and m.med_name_id not in (
      select m.med_name_id
      from medication_exclusion me
      inner join med m
        on m.gcn = me.exclude_id
        and me.exclude_by = 'gcn'
    )
    -- NOTE (anji 2019-08-13): hardcoded to manually exclude this out-of-date gcn_seqno/gcn pair that is breaking Drugs ETL. SORRY!
    -- TODO (anji): maybe figure out a way to reconcile the old med_price rows or put this into medication_exclusion instead of hardcoding?
    and not (m.gcn_seqno=79405 and price.gcn=11474)
group by
--  You'll notice that nearly all the fields are here.  This is because
--  all EXCEPT the medid should be.  With only one field here, we can
--  use the aggregate function to remove duplicate medids that are
--  otherwise identical.  If a second field gets another aggregate
--  function, we need to rethink how this works so that we don't select
--  the medid from one row and a new field from the other row.  Then
--  we'd break some of out requirements.
    m.gcn_seqno,
    n.id ,
    n.name,
    alternate_name,
    price.gcn,
    m.route,
    m.dosage_form,
    m.gcn_str,
    n.type_description,
    pack.drug_form_desc
;










select * from fdb.RGCNSEQ4_GCNSEQNO_MSTR limit 10;




select * from dwh.dim_gcn_seqno_hierarchy where gcn in (20069);


select distinct coupon_network FROM pricing_external_dev.goodrx_raw_data;


select * from api_scraper.medid_drug_id_mapping where medid=281575;

SELECT * from pricing_external_dev.goodrx_raw_data where date='2020-05-12' and geo='houston' and pharmacy='other_pharmacies' and slug='atorvastatin' and dosage='40mg' and quantity=30;


select * from transactional.network_pricing_mac where gcn_seqno=48703 and end_date is null;



select * from transactional.transactional_claim where claim_pharmacy_npi = '1093271231' and claim_ndc = '00093830501'

;




with mh_tem_sub_flag as 
(select distinct gcn,1 as mh_tem_sub_flag
from git_data_import.telemed_prescribable_med 
where custom_therapeutic_class in ('Hair Loss','Erectile Dysfunction'))

,  ecomm_claims as (
SELECT    date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date as "fill date"
                , date_trunc('day',convert_timezone('UTC','America/New_York',foi.ordered_timestamp))::date as "order date"
                , case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') then 'walmart'
                       when foi.pharmacy_network_id is null then 'edlp'
                       when foi.pharmacy_network_id = 1 then 'edlp'
                       when foi.pharmacy_network_id = 2 then 'bsd'
                       when foi.pharmacy_network_id = 3 then 'hd'
                       when foi.pharmacy_network_id in (4,5,6) then 'quicksave - digital'
                       else 'unknown'
                       end as "pharmacy network name"
                , dph.pharmacy_npi as "pharmacy npi"
                , dph.pharmacy_name as "pharmacy name"
                , dph.ncpdp_relationship_id as "ncpdp relationship id"
                , dph.ncpdp_relationship_name as "ncpdp relationship name"                
                , foi.script_source_type      as "script source type" 
                , foi.med_id                  as "purchased med id"
                , f_medid.medid               as "filled med id"
                , p_medid.med_name            as "purchased med name"
                , f_medid.med_name            as "filled med name"
                , p_gcn.generic_name_short    as "purchased generic name short"
                , f_gcn.generic_name_short    as "filled generic name short"
                , p_gcn.strength              as "purchased strength"
                , f_gcn.strength              as "filled strength"
                , p_gcn.dosage_form_desc      as "purchased form"
                , f_gcn.dosage_form_desc      as "filled form"
                , p_gcn.gcn                   as "purchased gcn"
                , p_gcn.gcn_seqno             as "purchased gcn seqno"
                , f_gcn.gcn                   as "filled gcn"
                , f_gcn.gcn_seqno             as "filled gcn seqno"
                , case when length(foi.last_claim_ndc_approved::varchar) <> 14 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end as "filled ndc"
                , ndc.label_name as "ndc label name"
                , ndc.brand_name as "ndc brand name"
                , ndc.orange_book_code as "orange book code"
                , case when length(mf2name.generic_product_identifier::varchar) <> 14 then LPAD(mf2name.generic_product_identifier::varchar, 14, '0') ELSE mf2name.generic_product_identifier::varchar end as "filled gpi"
                , case when foi.subscription_id is not null then 'subscription' else 'ala carte' end as "is subscription"
                , coalesce(mh_tem_sub_flag.mh_tem_sub_flag,0) as "mens health telehealth flag"
                ,f_gcn.maint                  as "maintenance indicator"
                ,f_gcn.therapeutic_class_desc_generic as "generic therapeutic class"
                ,f_gcn.therapeutic_class_desc_standard as "specific therapeutic class"
                ,dmh.custom_therapeutic_class as "custom therapeutic class"
                ,mf2ndc.multi_source_code as "multi source code"
                ,case when mf2ndc.multi_source_code = 'Y' then 'Generic' else 'Brand' end as "multi source generic brand indicator"
                ,last_claim_quantity_approved    as "quantity"
                ,last_claim_days_supply_approved as "days supply"
                ,case when last_claim_days_supply_approved <84 then 30 else 90 end as "days supply normalized"
                ,1 as fills
                ,foi.account_id::varchar as "user id"
                ,order_id::varchar as "order id"
                ,foi.last_claim_transactional_claim_id::varchar as "transactional claim id"
                ,case when foi.order_date_ny_sequence =1 then 'new' when foi.order_date_ny_sequence>1 then 'returning' else 'other' end as "new or returning"
                ,coalesce(last_claim_med_price_approved,0)::float 
                 + coalesce(last_claim_reimburse_program_discount_amount_approved,0)::float  
                 as "gross revenue"
                ,coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) 
                 AS "profit pool"
                ,coalesce( foi.last_pricing_total_cost_approved,0)+coalesce( foi.last_claim_wmt_true_up_amount_approved,0) as "total cost"
                ,(coalesce ( foi.last_claim_med_price_approved,0)::float+.3)*0.024+.3 as "stripe credit card processing fee"
                ,case when foi.pharmacy_network_id != 3 then 1.1 else 0 end as "candle claims processing fee"
                ,coalesce( foi.total_balance_billing_amount,0) as "actual balance billing amount"
                ,coalesce( foi.total_balance_billing_amount_intended,0) as "intended balance billing amount"
                ,foi.last_balance_billing_reason as "balance billing reason"
                ,foi.last_balance_billing_status as "balance billing charge status"
                ,coalesce( foi.last_pricing_unc_cost_approved,0) as "unc price"

                ,tc.pricing_strategy as "pharmacy reimbursement pricing strategy"
                ,case when foi.last_pricing_unc_cost_approved > 0 and foi.last_claim_med_price_approved > 0 and round(foi.last_claim_med_price_approved,2) > round(foi.last_pricing_unc_cost_approved,2) then 1 else 0 end as "consumer price paid exceeds unc"

                ,case when round(foi.last_pricing_total_cost_approved,2) >= round(foi.last_pricing_unc_cost_approved,2) then 1 else 0 end as "unc reimbursement claim"
								 
                ,coalesce(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float,0) as "awp amount" --AWP amount for GER eligible WMT claims
								
								,coalesce(coalesce(pac_hist_claim.pac)::float*foi.last_claim_quantity_approved::float,0) as "pac amount"
								,coalesce(coalesce(pac_hist_claim.pac_low)::float*foi.last_claim_quantity_approved::float,0) as "pac low amount"
								,coalesce(coalesce(pac_hist_claim.pac_high)::float*foi.last_claim_quantity_approved::float,0) as "pac high amount"
								,coalesce(coalesce(pac_hist_claim.pac_retail)::float*foi.last_claim_quantity_approved::float,0) as "pac retail amount"
                ,coalesce(coalesce(ks.lowest_keysource_unit_acquisition_cost)::float*foi.last_claim_quantity_approved::float::float,0) as "keysource acquisition amount"


		            ,coalesce(last_pricing_ingredient_cost_approved::float,0) as "ingredient cost amount" --AWP amount for GER eligible WMT claims
                
		            ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y')  -- Generic MAC list eligible 
                then 
                ((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)) end as "wmt ger rate" -- GER rate for GER eligible WMT claims

                ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y') -- Generic MAC list eligible 
                then 
                (((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float))-.92)
                *(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)
                else 0 end as "estimated wmt 92 pct ger true up amount"

                ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y') -- Generic MAC list eligible 
                then coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) 
                 -(((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float))-.92)
                *(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)
                else coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) end as "profit pool wmt adjusted"

                ,coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) 
                 AS "gross margin"

                ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y') -- Generic MAC list eligible 
                then coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) 
                 -(((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float))-.92)
                *(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)
                else coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) end as "gross margin wmt adjusted"


              FROM dwh.fact_order_item foi
         LEFT JOIN dwh.dim_user AS du
                ON foi.account_id = du.account_id
               AND du.is_internal = false 
               AND du.is_phantom = false 
         LEFT JOIN dwh.dim_ndc_hierarchy ndc ON 
             case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
             case when length(ndc.ndc::varchar) <> 11 then LPAD(ndc.ndc::varchar, 11, '0') ELSE ndc.ndc::varchar end
         LEFT JOIN dwh.dim_medid_hierarchy dmh
                ON foi.generic_medid = dmh.medid
         LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn 
                ON foi.gcn_seqno=p_gcn.gcn_seqno 
         LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn 
                ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno 
         LEFT JOIN dwh.dim_medid_hierarchy p_medid 
                ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
         LEFT JOIN dwh.dim_medid_hierarchy f_medid 
                ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name) 
         LEFT JOIN mh_tem_sub_flag mh_tem_sub_flag 
                ON foi.last_claim_gcn_approved=mh_tem_sub_flag.gcn
         LEFT JOIN transactional.transactional_claim tc 
                ON foi.last_claim_transactional_claim_id=tc.id
         LEFT JOIN dwh.dim_pharmacy_hierarchy dph 
                ON foi.last_claim_pharmacy_npi_approved=dph.pharmacy_npi 
         LEFT JOIN pricing_dev.keysource_min_gcn_acquisition_unit_cost_2020_04_07 ks on f_gcn.gcn=ks.gcn 
         LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON 
             case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
             case when length(awp_hist_claim.ndc_upc_hri::varchar) <> 11 then LPAD(awp_hist_claim.ndc_upc_hri::varchar, 11, '0') ELSE awp_hist_claim.ndc_upc_hri::varchar end
             AND foi.last_pbm_adjudication_timestamp_approved > awp_hist_claim.started_at AND foi.last_pbm_adjudication_timestamp_approved < awp_hist_claim.ended_at
  --             ON COALESCE(foi.last_claim_ndc_approved,'0') = awp_hist_claim.ndc_upc_hri AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
         LEFT JOIN medispan.mf2prc mf2prc ON
             case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
             case when length(mf2prc.ndc_upc_hri::varchar) <> 11 then LPAD(mf2prc.ndc_upc_hri::varchar, 11, '0') ELSE mf2prc.ndc_upc_hri::varchar end
             AND mf2prc.price_code = 'A'
         LEFT JOIN dwh.dim_pac_price_hist pac_hist_claim ON 
             case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
             case when length(pac_hist_claim.ndc_drug_identifier::varchar) <> 11 then LPAD(pac_hist_claim.ndc_drug_identifier::varchar, 11, '0') ELSE pac_hist_claim.ndc_drug_identifier::varchar end
             AND foi.last_pbm_adjudication_timestamp_approved >= pac_hist_claim.started_at AND foi.last_pbm_adjudication_timestamp_approved < pac_hist_claim.ended_at
         LEFT JOIN medispan.mf2ndc mf2ndc ON
             case when length(foi.last_claim_ndc_approved::varchar) <> 11 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end  =
             case when length(mf2ndc.ndc_upc_hri::varchar) <> 11 then LPAD(mf2ndc.ndc_upc_hri::varchar, 11, '0') ELSE mf2ndc.ndc_upc_hri::varchar end
         LEFT JOIN medispan.mf2name mf2name 
                ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id

             WHERE foi.last_pbm_adjudication_timestamp_approved is not null
               AND foi.is_fraud = FALSE
               AND date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date + INTERVAL '200 day' >= current_date)

, magic_claims as (
SELECT
       date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date AS "fill date"
     , date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date AS "order date"
     , 'quicksave - counter'        as "pharmacy network name"
     , dph.pharmacy_npi             as "pharmacy npi"
     , dph.pharmacy_name            as "pharmacy name"
     , dph.ncpdp_relationship_id    as "ncpdp relationship id"
     , dph.ncpdp_relationship_name  as "ncpdp relationship name"
     , 'quicksave'                  as "script source type"
     , mfoi.medid                   as "purchased med id"
     , mfoi.medid                   as "filled med id"
     , mfoi.med_name                as "purchased med name"
     , mfoi.med_name                as "filled med name" 
     , mfoi.generic_name_short      as "purchased generic name short"
     , mfoi.generic_name_short      as "filled generic name short"
     , mfoi.med_dosage              as "purchased strength"
     , mfoi.med_dosage              as "filled strength"
     , mfoi.med_form                as "purchased form"
     , mfoi.med_form                as "filled form"
     , mfoi.gcn                     as "purchased gcn" 
     , mfoi.gcn_seqno               as "purchased gcn seqno"
     , mfoi.gcn                     as "filled gcn" 
     , mfoi.gcn_seqno               as "filled gcn seqno"

     , case when length(mfoi.ndc::varchar) <> 14 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end as "filled ndc"
     , ndc.label_name as "ndc label name"
     , ndc.brand_name as "ndc brand name"
     , ndc.orange_book_code as "orange book code"
     , case when length(mf2name.generic_product_identifier::varchar) <> 14 then LPAD(mf2name.generic_product_identifier::varchar, 14, '0') ELSE mf2name.generic_product_identifier::varchar end as "filled gpi"

     , 'ala carte'                  as "is subscription"
     , coalesce(mh_tem_sub_flag.mh_tem_sub_flag,0) as "mens health telehealth flag"
     , gcn.maint                    as "maintenance indicator"
     , gcn.therapeutic_class_desc_generic as "generic therapeutic class"
     , gcn.therapeutic_class_desc_standard as "specific therapeutic class"
     , medid.custom_therapeutic_class as "custom therapeutic class"
     , mf2ndc.multi_source_code as "multi source code"
     , case when mf2ndc.multi_source_code = 'Y' then 'Generic' else 'Brand' end as "multi source generic brand indicator"
     , mfoi.quantity                as "quantity" 
     , mfoi.claim_days_supply       as "days supply"
     , case when mfoi.claim_days_supply < 84 then 30 else 90 end as "days supply normalized"
     , 1                            as fills 
     , patient_id::varchar          as "user id"
     , magic_prior_auth_id::varchar as "order id" 
     , transactional_claim_id::varchar as "transactional claim id"
     , case when is_first_claim_date = TRUE then 'new' else 'returning' end as new_returning_fill_flag     
     , coalesce(mfoi.patient_pay_amount,0)::float  as "gross revenue"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float) 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float) 
                 end as "profit pool"
--, COALESCE(case when mfoi.pricing_total_cost < 0 then abs(mfoi.pricing_total_cost) else 0.0 end, 0) as "revenue share"
--, COALESCE(case when mfoi.pricing_total_cost > 0 then abs(mfoi.pricing_total_cost) else 0.0 end, 0) as "true up cost"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float 
                 end as "total cost"
     ,0                             as "stripe credit card processing fee"
     ,1.1                           as "candle claims processing fee"
     ,0                             as "actual balance billing amount"
     ,0                             as "intended balance billing amount"
     ,''                            as "balance billing reason"
     ,''                            as "balance billing charge status"
     , tc.pricing_unc_cost          as "unc price"
     , tc.pricing_strategy          as "pharmacy reimbursement pricing strategy"
     , case when tc.pricing_unc_cost > 0 and mfoi.patient_pay_amount > 0 and round(mfoi.patient_pay_amount,2) > round(tc.pricing_unc_cost,2) then 1 else 0 end as "price paid exceeds unc flg"
     , case when mfoi.copay_revenue_pool is not null and tc.pricing_unc_cost > 0
                 and round(tc.pricing_unc_cost,2) <= round(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float,2) then 1 
            when mfoi.copay_revenue_pool is null and tc.pricing_unc_cost > 0
                 and round(tc.pricing_unc_cost,2) <= round(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float,2) then 1 
                 else 0 end as "unc claim flg"
     , coalesce(mfoi.quantity::float*coalesce(awp_hist_claim.unit_price,mf2prc.unit_price,0)::float,0) as "awp amount"
		 , coalesce(coalesce(pac_hist_claim.pac)::float*mfoi.quantity::float::float,0) as "pac amount"
		 , coalesce(coalesce(pac_hist_claim.pac_low)::float*mfoi.quantity::float::float,0) as "pac low amount"
		 , coalesce(coalesce(pac_hist_claim.pac_high)::float*mfoi.quantity::float::float,0) as "pac high amount"
		 , coalesce(coalesce(pac_hist_claim.pac_retail)::float*mfoi.quantity::float::float,0) as "pac retail amount"
     , coalesce(coalesce(ks.lowest_keysource_unit_acquisition_cost)::float*mfoi.quantity::float::float,0) as "keysource acquisition amount"

     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float-coalesce(mfoi.pricing_dispensing_fee::float,0)::float 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float-coalesce(mfoi.pricing_dispensing_fee::float,0)::float 
                 end as "ingredient cost amount"
     , 0                            as "wmt ger rate"
     , 0                            as "estimated wmt 92 pct ger true up amount"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float) 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float) 
                 end as "profit pool wmt adjusted" 
     , case when mfoi.copay_revenue_for_payer is not null 
                 then coalesce(mfoi.copay_revenue_for_payer::float,0)::float 
            when mfoi.copay_revenue_for_payer is null and dph.ncpdp_relationship_id = '453' and coalesce(mfoi.pricing_total_cost::float,0)::float*-1 < 0 
                 then coalesce(mfoi.pricing_total_cost::float,0)::float*-1 
            when mfoi.copay_revenue_for_payer is null and dph.ncpdp_relationship_id = '453' and coalesce(mfoi.pricing_total_cost::float,0)::float*-1 >= 0 
                 then 0
            when mfoi.copay_revenue_for_payer is null and dph.ncpdp_relationship_id != '453' then coalesce(mfoi.pricing_total_cost::float,0)::float*-1*.5
                 end as "gross margin"

     , case when mfoi.copay_revenue_for_payer is not null 
                 then coalesce(mfoi.copay_revenue_for_payer::float,0)::float 
            when mfoi.copay_revenue_for_payer is null and dph.ncpdp_relationship_id = '453' and coalesce(mfoi.pricing_total_cost::float,0)::float*-1 < 0 
                 then coalesce(mfoi.pricing_total_cost::float,0)::float*-1 
            when mfoi.copay_revenue_for_payer is null and dph.ncpdp_relationship_id = '453' and coalesce(mfoi.pricing_total_cost::float,0)::float*-1 >= 0 
                 then 0
            when mfoi.copay_revenue_for_payer is null and dph.ncpdp_relationship_id != '453' then coalesce(mfoi.pricing_total_cost::float,0)::float*-1*.5
                 end as "gross margin wmt adjusted"

     FROM fifo.magic_fact_order_claim mfoi 
LEFT JOIN transactional.transactional_claim tc on mfoi.transactional_claim_id=tc.id 
LEFT JOIN dwh.dim_pharmacy_hierarchy dph on mfoi.pharmacy_npi=dph.pharmacy_npi
LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn on mfoi.gcn_seqno=gcn.gcn_seqno
LEFT JOIN dwh.dim_medid_hierarchy medid on mfoi.medid=medid.medid
LEFT JOIN dwh.dim_ndc_hierarchy ndc ON 
             case when length(mfoi.ndc::varchar) <> 11 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end  =
             case when length(ndc.ndc::varchar) <> 11 then LPAD(ndc.ndc::varchar, 11, '0') ELSE ndc.ndc::varchar end
LEFT JOIN pricing_dev.keysource_min_gcn_acquisition_unit_cost_2020_04_07 ks on gcn.gcn=ks.gcn 
LEFT JOIN mh_tem_sub_flag mh_tem_sub_flag on mfoi.gcn=mh_tem_sub_flag.gcn
LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON 
             case when length(mfoi.ndc::varchar) <> 11 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end  =
             case when length(awp_hist_claim.ndc_upc_hri::varchar) <> 11 then LPAD(awp_hist_claim.ndc_upc_hri::varchar, 11, '0') ELSE awp_hist_claim.ndc_upc_hri::varchar end
             AND mfoi.action_timestamp > awp_hist_claim.started_at AND mfoi.action_timestamp < awp_hist_claim.ended_at
LEFT JOIN medispan.mf2prc mf2prc ON
             case when length(mfoi.ndc::varchar) <> 11 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end  =
             case when length(mf2prc.ndc_upc_hri::varchar) <> 11 then LPAD(mf2prc.ndc_upc_hri::varchar, 11, '0') ELSE mf2prc.ndc_upc_hri::varchar end
             AND mf2prc.price_code = 'A'
LEFT JOIN dwh.dim_pac_price_hist pac_hist_claim ON 
             case when length(mfoi.ndc::varchar) <> 11 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end  =
             case when length(pac_hist_claim.ndc_drug_identifier::varchar) <> 11 then LPAD(pac_hist_claim.ndc_drug_identifier::varchar, 11, '0') ELSE pac_hist_claim.ndc_drug_identifier::varchar end
             AND mfoi.action_timestamp >= pac_hist_claim.started_at AND mfoi.action_timestamp < pac_hist_claim.ended_at
LEFT JOIN medispan.mf2ndc mf2ndc ON
             case when length(mfoi.ndc::varchar) <> 11 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end  =
             case when length(mf2ndc.ndc_upc_hri::varchar) <> 11 then LPAD(mf2ndc.ndc_upc_hri::varchar, 11, '0') ELSE mf2ndc.ndc_upc_hri::varchar end
LEFT JOIN medispan.mf2name mf2name 
       ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
    WHERE (mfoi.action_type = 'approval') 
      AND (mfoi.action_sequence = 1)
      AND date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date + INTERVAL '200 day' >= current_date)

, integrated_stack as (
select *,case when "pharmacy name" ilike '%serve%you%rx%' then 'Serve You Rx' else "ncpdp relationship name" end as "custom pharmacy chain name" from ecomm_claims 
UNION ALL 
select *,case when "pharmacy name" ilike '%serve%you%rx%' then 'Serve You Rx' else "ncpdp relationship name" end as "custom pharmacy chain name" from magic_claims
)

, most_common_mac_strategy_1 as (
select
"custom pharmacy chain name"
,"pharmacy reimbursement pricing strategy"
,sum(fills) as fills
from integrated_stack where "multi source code" = 'Y' and "custom pharmacy chain name" is not null
group by 1,2)

, most_common_mac_strategy_2 as (
select 
"custom pharmacy chain name"
,"pharmacy reimbursement pricing strategy"
,fills
,row_number() over (partition by "custom pharmacy chain name" order by coalesce(fills,0) desc) as rn
from most_common_mac_strategy_1)																						

, most_common_mac_strategy_3 as (
select
"custom pharmacy chain name"
,"pharmacy reimbursement pricing strategy" 
from most_common_mac_strategy_2 where rn = 1)

select 
x.*
,coalesce(x."pharmacy reimbursement pricing strategy",y."pharmacy reimbursement pricing strategy") as "final pharmacy reimbursement pricing strategy"
from integrated_stack x
left join most_common_mac_strategy_3 y on x."custom pharmacy chain name"=y."custom pharmacy chain name" and x."multi source code" = 'Y'












SELECT
       date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date AS "fill date"
     , date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date AS "order date"
     , 'quicksave - counter'        as "pharmacy network name"
     , dph.pharmacy_name            as "pharmacy name"
     , 'quicksave'                  as "script source type"
     , mfoi.med_name                as "filled med name" 
     , mfoi.quantity                as "quantity" 
     , mfoi.claim_days_supply       as "days supply"
     , 1                            as fills 
     , magic_prior_auth_id::varchar as "order id" 
     , transactional_claim_id::varchar as "transactional claim id"
     , coalesce(mfoi.patient_pay_amount,0)::float  as "gross revenue"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float) 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float) 
                 end as "profit pool"
     , tc.pricing_strategy          as "pharmacy reimbursement pricing strategy"
     FROM fifo.magic_fact_order_claim mfoi 
LEFT JOIN transactional.transactional_claim tc on mfoi.transactional_claim_id=tc.id 
LEFT JOIN dwh.dim_pharmacy_hierarchy dph on mfoi.pharmacy_npi=dph.pharmacy_npi
    WHERE (mfoi.action_type = 'approval') 
      AND (mfoi.action_sequence = 1)
      AND date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date + INTERVAL '5 day' >= current_date
;


SELECT
	last_pbm_adjudication_timestamp_approved::DATE , 
	count(*)
FROM
	dwh.fact_order_claim	order_claim
WHERE order_claim.last_pbm_adjudication_timestamp_approved >= 
    CONVERT_TIMEZONE('America/New_York','UTC','2020-01-19')
  
  AND order_claim.last_pbm_adjudication_timestamp_approved <= 
    CONVERT_TIMEZONE('America/New_York','UTC','2020-05-17')
  
group by 1;




select * from fifo.magic_fact_order_claim where gcn =28959 and action_type = 'approval' and action_sequence=1 and ncpdp_relationship_id='025' ;


grant all on pricing_prod.ochsner_mac_gcn_2020_03_26 to scott;
grant all on pricing_prod.ochsner_mac_gcn_2020_03_26 to doo;



SELECT
	tc.claim_prescription_number, -- S
	tc.claim_pharmacy_npi, -- S
	tc.header_date_of_service, -- F
	mfoi.transactional_claim_id,
	mfoi.action_timestamp,
	mfoi.action_sequence,
	mfoi.action_type,
	mfoi.gcn,
	mfoi.gcn_seqno,
	mfoi.pharmacy_name,
	mfoi.ncpdp_relationship_id,
	tc.pricing_strategy,
	mfoi.quantity,
	mfoi.pharmacy_ingredient_cost,
	mfoi.quantity::float * mac.unit_price AS correct_ingredient_cost
FROM
	fifo.magic_fact_order_claim mfoi
	LEFT JOIN transactional.transactional_claim tc ON mfoi.transactional_claim_id = tc.id
	LEFT JOIN drugs_etl.network_pricing_mac mac ON mfoi.gcn_seqno = mac.gcn_seqno
		AND mac.mac_list = 'BLINK02'
		AND tc.header_date_of_service >= mac.start_date
		and(tc.header_date_of_service < mac.end_date
			OR mac.end_date IS NULL)
WHERE
	mfoi.action_sequence = 1
	AND mfoi.action_type = 'approval'
	AND tc.pricing_strategy ILIKE '%generic%'
ORDER BY
	mfoi.action_timestamp DESC
LIMIT 100;















SELECT
	update_list.drug_price_list,
	coalesce(update_list.gpi,'') as gpi,
	coalesce(update_list.ndc,'') as ndc,
	mac.unit_price as unit_price,
	update_list.eff_date as effective_date,
	'' as termination_date
FROM
	pricing_external_dev.candle_mac_drug_price_20200513 as mac
inner join
	(select drug_price_list,gpi,ndc,max(effective_date) as eff_date from pricing_external_dev.candle_mac_drug_price_20200513 
	 group by 1,2,3) as update_list
ON
	mac.drug_price_list = update_list.drug_price_list
	and coalesce(mac.gpi,'') = coalesce(update_list.gpi,'')
	and coalesce(mac.ndc,'') = coalesce(update_list.ndc,'')
	and mac.effective_date = update_list.eff_date
-- 	and mac.drug_price_list in ('BLINKHEB01','BLINKBKSH01')
;



select * from mac_pricing.mac_frozen_list where mac_list='BLINKSYRx01' ; 



SELECT
	*
FROM
	dwh.dim_ndc_current_price_data
where gpi in 
	('65200010208240')
;


GRANT SELECT ON pricing_dev.mac_automation_gpi_medispan_list TO "public";
GRANT ALL ON pricing_dev.mac_automation_gpi_medispan_list TO scott, sandeepd;

select count(*) from pricing_dev.mac_automation_gpi_medispan_list;


-- Get PAC Data from Gold Standard / Glassbox

drop table if exists pricing_dev.mac_automation_pac_data_gpi;
create table pricing_dev.mac_automation_pac_data_gpi as 
SELECT
	gpi,
	label_name,
	pac,
	pac_low,
	pac_high,
	pac_retail
FROM pricing_dev.mac_automation_gpi_medispan_list  
INNER JOIN gold_standard.pac_ms 
	ON pac_ms.drug_identifier = mac_automation_gpi_medispan_list.gpi 
	AND brand_generic = 'Generic'
	AND identifier_type = 'GPI'
	AND downloaded_date = CURRENT_DATE-1
GROUP BY 1,2,3,4,5,6
;


SELECT
	gpi,
	max(label_name) as label_name
FROM
	dwh.dim_ndc_current_price_data
WHERE
	orange_book_code IN('AA', 'AB', 'AN', 'AO', 'AP', 'AT', 'ZA', 'ZC')
	AND repackager_indicator = '0'
	AND (obsolete_date >= CURRENT_DATE - 730 OR obsolete_date IS NULL)
	AND multi_source_code = 'Y'
-- 	AND NOT lower(gpi) like  '%e%'
	AND label_name like '%SUMATRIPTAN%'
GROUP BY
	1;













select count(distinct gcn) from transactional.available_med where type_description = 'generic';



select gcn,pac_low_unit from dwh.dim_gcn_seqno_hierarchy where pac_low_unit is not null;

select distinct gcn, unit_price,dispensing_fee_margin from transactional.med_price where pharmacy_network_id=6 and ended_on is null;


select 
	last_claim_ndc_approved as ndc,
	AVG(last_claim_pac_approved::float) as pac,
	AVG(last_claim_pac_low_approved::float) as pac_low,
	AVG(last_claim_pac_high_approved::float) as pac_high,
	AVG(last_claim_pac_retail_approved::float) as pac_retail
from 
	dwh.fact_order_item
WHERE
	last_pbm_adjudication_timestamp is not NULL
	AND (last_pbm_adjudication_timestamp_approved::date = '2020-05-04')
	AND last_claim_ndc_approved in 
		('70069005101')
GROUP BY
	1
;


WITH mh_tem_sub_flag AS (
	SELECT DISTINCT
		gcn,
		1 AS mh_tem_sub_flag
	FROM
		git_data_import.telemed_prescribable_med
	WHERE
		custom_therapeutic_class in('Hair Loss',
			'Erectile Dysfunction')
),
integrated AS (
	SELECT
		date_trunc('month',
			convert_timezone ('UTC',
				'America/New_York',
				foi.last_pbm_adjudication_timestamp_approved))::date AS fill_month,
		date_trunc('week',
			convert_timezone ('UTC',
				'America/New_York',
				foi.last_pbm_adjudication_timestamp_approved))::date AS fill_week
		--,ndc.gpi
,
		gcn.gcn,
		gcn.gcn_seqno,
		gcn.hicl_seqno,
		gcn.generic_name_short,
		gcn.dosage_form_desc AS form,
		gcn.strength,
		gcn.maint,
		gcn.therapeutic_class_desc_generic,
		gcn.therapeutic_class_desc_standard,
		coalesce(mh_tem_sub_flag.mh_tem_sub_flag,
			0) AS mh_tem_sub_flag,
		tc.pricing_strategy,
		ms.multi_source_code,
		tc.id AS transactional_claim_id,
		foi.order_id,
		foi.last_claim_quantity_approved::float AS quantity,
		foi.last_claim_days_supply_approved::float AS days_supply,
		foi.last_pricing_ingredient_cost_approved::float AS ingredient_cost,
		foi.last_pricing_dispensing_fee_approved::float AS dispensing_fee,
		foi.last_pricing_total_cost_approved::float AS total_cost,
		foi.last_claim_med_price_approved::float AS realized_gross_revenue,
		foi.last_claim_med_price_approved::float - foi.last_pricing_total_cost_approved::float AS realized_gross_profit,
		foi.last_claim_pac_approved::float * foi.last_claim_quantity_approved::float AS pac_amount,
		foi.last_claim_pac_low_approved::float * foi.last_claim_quantity_approved::float AS pac_low_amount,
		foi.last_claim_pac_high_approved::float * foi.last_claim_quantity_approved::float AS pac_high_amount,
		foi.last_claim_pac_retail_approved::float * foi.last_claim_quantity_approved::float AS pac_retail_amount,
		coalesce(foi.last_claim_awp_amount_approved::float,
			msp.extended_unit_price::float * foi.last_claim_quantity_approved::float,
			0) AS awp_amount
	FROM
		dwh.fact_order_item foi
	LEFT JOIN transactional.transactional_claim tc ON foi.transactional_claim_id = tc.id
	LEFT JOIN dwh.dim_ndc_hierarchy ndc ON foi.last_claim_ndc_approved = ndc.ndc
	LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn ON ndc.gcn_seqno = gcn.gcn_seqno
	LEFT JOIN medispan.mf2ndc ms ON ndc.ndc = ms.ndc_upc_hri
	LEFT JOIN medispan.mf2prc msp ON ndc.ndc = msp.ndc_upc_hri
		AND msp.price_code = 'A'
	LEFT JOIN mh_tem_sub_flag mh_tem_sub_flag ON gcn.gcn = mh_tem_sub_flag.gcn
WHERE
	foi.pharmacy_network_id = 3
	AND foi.last_claim_pac_approved IS NOT NULL
	AND foi.last_pbm_adjudication_timestamp_approved IS NOT NULL
	AND tc.pricing_strategy = 'delivery: serve_you_rx'
	AND ms.multi_source_code = 'Y'
),
fractional_orders AS (
	SELECT
		order_id,
		count(DISTINCT transactional_claim_id) AS order_script_count
	FROM
		integrated
	GROUP BY
		1
),
integrated_plus_fractional_orders AS (
	SELECT
		x.*,
		1 / y.order_script_count::float AS fractional_order_count
	FROM
		integrated x
	LEFT JOIN fractional_orders y ON x.order_id = y.order_id
)
SELECT
	fill_month AS "fill month",
	fill_week AS "fill week",
	gcn,
	gcn_seqno AS "gcn seqno",
	hicl_seqno AS "hicl seqno",
	generic_name_short AS "generic name short",
	form,
	strength,
	maint,
	therapeutic_class_desc_generic AS "therapeutic class desc generic",
	therapeutic_class_desc_standard AS "therapeutic class desc standard",
	mh_tem_sub_flag AS "mh tem sub flag",
	count(DISTINCT transactional_claim_id) AS "filled script count",
	sum(fractional_order_count) AS "filled fractional order count",
	sum(quantity) AS "filled quantity",
	sum(days_supply) AS "filled days supply",
	sum(ingredient_cost) AS "ingredient cost",
	sum(dispensing_fee) AS "dispensing fee",
	sum(total_cost) AS "total cost",
	sum(realized_gross_revenue) AS "realized gross revenue",
	sum(realized_gross_profit) AS "realized gross profit",
	sum(pac_amount) AS "pac amount",
	sum(pac_low_amount) AS "pac low amount",
	sum(pac_high_amount) AS "pac high amount",
	sum(pac_retail_amount) AS "pac retail amount",
	sum(awp_amount) AS "awp amount"
FROM
	integrated_plus_fractional_orders
WHERE
	fill_month >= '2018-07-01' --and fill_month <= '2020-02-01'
GROUP BY
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	11,
	12;










with ndc_gpi as (
	select 
		distinct ndc.ndc_upc_hri as ndc, n.generic_product_identifier as gpi
	from 
		medispan.mf2ndc ndc
		INNER JOIN medispan.mf2name n ON ndc.drug_descriptor_id = n.drug_descriptor_id
)
	select * from ndc_gpi where ndc in (68382000514
,93520006
,16714044802
,16714045001
,68382077305
,68382078501
,23155019201
,67877025001
,16714044802
,68382077305
,57237022005
,16729013401
,781518110);


select 


select distinct gcn,unit_price,dispensing_fee_margin+1.75 as fixed_price from transactional.med_price where ended_on is not null and pharmacy_network_id=6;

select * from transactional.pharmacy_network;


select distinct hicl_seqno,hicl_value from dwh.dim_gcn_seqno_hierarchy limit 10;

select gcn,gcn_seqno,strength, strength_number,generic_name_short from dwh.dim_gcn_seqno_hierarchy ;


select * from dwh.dim_medid_hierarchy where med_medid_desc like '%hydroxychloroquine%';


select * from transactional.network_pricing_mac where end_date is null and mac_list='BLINKBKSH01';

with active_gcn as ( 
	SELECT 
		gcn,
		max(unit_price) as unit_price,
		max(dispensing_fee_margin) as dispensing_fee_margin
	FROM
		transactional.med_price
	WHERE
		pharmacy_network_id = 2
		AND ended_on IS NULL
	GROUP by 1),
mac_prices as (
	select
		gcn_seqno,
		median(unit_price) as unit_cost_price
	FROM
		transactional.network_pricing_mac
	where 
		end_date is NULL
		and mac_list = 'BLINKSYRx01'
	group by 1)
SELECT
	dgsh.gcn,
	dgsh.gcn_seqno,
	generic_name_short,
	strength_long_desc,
	dosage_form_desc,
	gcn_symphony_2017_fills,
	gcn_symphony_2017_rank,
	unit_price,
	dispensing_fee_margin,
	unit_cost_price,
	unit_price_mac_blink01,
	pac_unit,
	pac_low_unit,
	pac_high_unit,
	pac_retail_unit,
	is_contain_opioids,
	dea,
	medid
FROM
	dwh.dim_gcn_seqno_hierarchy as dgsh 
	LEFT JOIN active_gcn ON dgsh.gcn = active_gcn.gcn
	LEFT JOIN mac_prices ON mac_prices.gcn_seqno = dgsh.gcn_seqno
WHERE
	gcn_symphony_2017_rank <= 1000;


select distinct(mac_list) from transactional.network_pricing_mac ;


select * from dwh.dim_ndc_hierarchy where upper(label_name) ilike '%BENAZEPRIL%' limit 10;


select * from pricing_external_dev.healthwarehouse_extract where drug ilike 'Prednisolone Acetate 1% Eye Drops (5ml Bottle)';


select gcn,gcn_seqno,generic_name_long,strength,dosage_form_code_desc from dwh.dim_gcn_seqno_hierarchy;



with gcn_gpi as (
	select 
		ms_n.generic_product_identifier as gpi
		,gcn.gcn
		,sum(coalesce(sh.trx_count,0)) as symphony_health_2017_generic_scripts
	from 
		dwh.dim_ndc_hierarchy n 
	    LEFT JOIN dwh.dim_medid_hierarchy m ON n.branded_medid=m.medid 
	    LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn ON n.gcn_seqno=gcn.gcn_seqno
	    LEFT JOIN medispan.mf2ndc ms_ndc ON n.ndc = ms_ndc.ndc_upc_hri
	    LEFT JOIN medispan.mf2name ms_n ON ms_ndc.drug_descriptor_id = ms_n.drug_descriptor_id
	    LEFT JOIN static_data.symphony_2017_generics_annual sh on n.ndc=sh.ndc
	group by 1,2),
map_gcn_gpi as (
	select gpi,gcn 
	from 
		(select *,row_number() over (partition by gpi order by symphony_health_2017_generic_scripts desc) as rn from gcn_gpi) sorted 
	where 
		sorted.rn = 1
), unit_cost_data as (
	SELECT
		map_gcn_gpi.gcn as gcn,
		iif_(keysource.gcn is not null and lowest_keysource_unit_acquisition_cost::float < pac_low, lowest_keysource_unit_acquisition_cost , pac_low)
		  as unit_cost
	FROM gold_standard.pac_ms
	inner join map_gcn_gpi ON pac_ms.drug_identifier = map_gcn_gpi.gpi
	left join pricing_dev.keysource_min_gcn_acquisition_unit_cost_2020_04_07 as keysource ON keysource.gcn = map_gcn_gpi.gcn
	WHERE 
		brand_generic = 'Generic'
		AND identifier_type = 'GPI'
		AND downloaded_date = CURRENT_DATE-1
), mh_tem_sub_flag as 
	(select distinct gcn,1 as mh_tem_sub_flag
	from git_data_import.telemed_prescribable_med 
	where custom_therapeutic_class in ('Hair Loss','Erectile Dysfunction'))

,  ecomm_claims as (
SELECT    date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date as "fill date"
                , date_trunc('day',convert_timezone('UTC','America/New_York',foi.ordered_timestamp))::date as "order date"
                , case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') then 'walmart'
                       when foi.pharmacy_network_id is null then 'edlp'
                       when foi.pharmacy_network_id = 1 then 'edlp'
                       when foi.pharmacy_network_id = 2 then 'bsd'
                       when foi.pharmacy_network_id = 3 then 'hd'
                       when foi.pharmacy_network_id in (4,5,6) then 'quicksave - digital'
                       else 'unknown'
                       end as "pharmacy network name"
                , dph.pharmacy_npi as "pharmacy npi"
                , dph.pharmacy_name as "pharmacy name"
                , dph.ncpdp_relationship_id as "ncpdp relationship id"
                , dph.ncpdp_relationship_name as "ncpdp relationship name"                
                , foi.script_source_type      as "script source type" 
                , foi.med_id                  as "purchased med id"
                , f_medid.medid               as "filled med id"
                , p_medid.med_name            as "purchased med name"
                , f_medid.med_name            as "filled med name"
                , p_gcn.generic_name_short    as "purchased generic name short"
                , f_gcn.generic_name_short    as "filled generic name short"
                , p_gcn.strength              as "purchased strength"
                , f_gcn.strength              as "filled strength"
                , p_gcn.dosage_form_desc      as "purchased form"
                , f_gcn.dosage_form_desc      as "filled form"
                , p_gcn.gcn                   as "purchased gcn"
                , p_gcn.gcn_seqno             as "purchased gcn seqno"
                , f_gcn.gcn                   as "filled gcn"
                , f_gcn.gcn_seqno             as "filled gcn seqno"
                , case when length(foi.last_claim_ndc_approved::varchar) <> 14 then LPAD(foi.last_claim_ndc_approved::varchar, 11, '0') ELSE foi.last_claim_ndc_approved::varchar end as "filled ndc"
                , ndc.label_name as "ndc label name"
                , ndc.brand_name as "ndc brand name"
                , ndc.orange_book_code as "orange book code"
                , case when length(mf2name.generic_product_identifier::varchar) <> 14 then LPAD(mf2name.generic_product_identifier::varchar, 14, '0') ELSE mf2name.generic_product_identifier::varchar end as "filled gpi"
                , case when foi.subscription_id is not null then 'subscription' else 'ala carte' end as "is subscription"
                , coalesce(mh_tem_sub_flag.mh_tem_sub_flag,0) as "mens health telehealth flag"
                ,f_gcn.maint                  as "maintenance indicator"
                ,f_gcn.therapeutic_class_desc_generic as "generic therapeutic class"
                ,f_gcn.therapeutic_class_desc_standard as "specific therapeutic class"
                ,dmh.custom_therapeutic_class as "custom therapeutic class"
                ,mf2ndc.multi_source_code as "multi source code"
                ,case when mf2ndc.multi_source_code = 'Y' then 'Generic' else 'Brand' end as "multi source generic brand indicator"
                ,last_claim_quantity_approved    as "quantity"
                ,last_claim_days_supply_approved as "days supply"
                ,case when last_claim_days_supply_approved <84 then 30 else 90 end as "days supply normalized"
                ,1 as fills
                ,foi.account_id::varchar as "user id"
                ,order_id::varchar as "order id"
                ,foi.last_claim_transactional_claim_id::varchar as "transactional claim id"
                ,case when foi.order_date_ny_sequence =1 then 'new' when foi.order_date_ny_sequence>1 then 'returning' else 'other' end as "new or returning"
                ,coalesce(last_claim_med_price_approved,0)::float 
                 + coalesce(last_claim_reimburse_program_discount_amount_approved,0)::float  
                 as "gross revenue"
                ,coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) 
                 AS "gross margin"
                ,coalesce( foi.last_pricing_total_cost_approved,0)+coalesce( foi.last_claim_wmt_true_up_amount_approved,0) as "total cost"
                ,(coalesce ( foi.last_claim_med_price_approved,0)::float+.3)*0.024+.3 as "stripe credit card processing fee"
                ,case when foi.pharmacy_network_id != 3 then 1.1 else 0 end as "candle claims processing fee"
                ,coalesce( foi.total_balance_billing_amount,0) as "actual balance billing amount"
                ,coalesce( foi.total_balance_billing_amount_intended,0) as "intended balance billing amount"
                ,foi.last_balance_billing_reason as "balance billing reason"
                ,foi.last_balance_billing_status as "balance billing charge status"
                ,coalesce( foi.last_pricing_unc_cost_approved,0) as "unc price"

                ,tc.pricing_strategy as "pharmacy reimbursement pricing strategy"
                ,case when foi.last_pricing_unc_cost_approved > 0 and foi.last_claim_med_price_approved > 0 and round(foi.last_claim_med_price_approved,2) > round(foi.last_pricing_unc_cost_approved,2) then 1 else 0 end as "consumer price paid exceeds unc"

                ,case when round(foi.last_pricing_total_cost_approved,2) >= round(foi.last_pricing_unc_cost_approved,2) then 1 else 0 end as "unc reimbursement claim"
								 
                ,coalesce(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float,0) as "awp amount" --AWP amount for GER eligible WMT claims
								
		            ,coalesce(last_pricing_ingredient_cost_approved::float,0) as "ingredient cost amount" --AWP amount for GER eligible WMT claims
                
		            ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y')  -- Generic MAC list eligible 
                then 
                ((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)) end as "wmt ger rate" -- GER rate for GER eligible WMT claims

                ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y') -- Generic MAC list eligible 
                then 
                (((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float))-.92)
                *(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)
                else 0 end as "estimated wmt 92 pct ger true up amount"

                ,case when (lower(dph.pharmacy_name) ilike '%walmart%' or dph.ncpdp_relationship_id = '229') -- WMT claim
                      and round(foi.last_pricing_total_cost_approved,2) < round(foi.last_pricing_unc_cost_approved,2) -- non-UNC claim
                      and (foi.last_pbm_adjudication_timestamp_approved)::date >= '2018-09-01' -- subject to the 92% GER / 12% BER contract commenced 9/1/2018
                      and coalesce(awp_hist_claim.unit_price,mf2prc.unit_price) > 0 -- Medi-span AWP price available 
                      and (tc.pricing_strategy = 'drug_price_list: BLINKWMT01' or mf2ndc.multi_source_code = 'Y') -- Generic MAC list eligible 
                then coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) 
                 -(((coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float - foi.last_pricing_ingredient_cost_approved::float)::float 
                / (coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float))-.92)
                *(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float)
                else coalesce(foi.last_claim_med_price_approved,0)
                 +coalesce( foi.last_claim_reimburse_program_discount_amount_approved ,0)
                 -coalesce( foi.last_pricing_total_cost_approved,0)
                 -coalesce( foi.last_claim_wmt_true_up_amount_approved,0) end as "gross margin wmt adjusted",
                 coalesce(unit_cost_data.unit_cost*last_claim_quantity_approved) as "pharmacy acq cost"
              FROM dwh.fact_order_item foi
         LEFT JOIN dwh.dim_user AS du
                ON foi.account_id = du.account_id
               AND du.is_internal = false 
               AND du.is_phantom = false 
         LEFT JOIN dwh.dim_ndc_hierarchy ndc 
                ON foi.last_claim_ndc_approved=ndc.ndc
         LEFT JOIN dwh.dim_medid_hierarchy dmh
                ON foi.generic_medid = dmh.medid
         LEFT JOIN dwh.dim_gcn_seqno_hierarchy p_gcn 
                ON foi.gcn_seqno=p_gcn.gcn_seqno 
         LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn 
                ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno 
         LEFT JOIN dwh.dim_medid_hierarchy p_medid 
                ON foi.med_id = p_medid.medid -- medid for the med purchased in the order (e.g. med name)
         LEFT JOIN dwh.dim_medid_hierarchy f_medid 
                ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name) 
         LEFT JOIN mh_tem_sub_flag mh_tem_sub_flag 
                ON foi.last_claim_gcn_approved=mh_tem_sub_flag.gcn
         LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim 
                ON COALESCE(foi.last_claim_ndc_approved,'0') = awp_hist_claim.ndc_upc_hri AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
         LEFT JOIN medispan.mf2prc mf2prc 
                ON COALESCE(foi.last_claim_ndc_approved,'0') = mf2prc.ndc_upc_hri AND mf2prc.price_code = 'A'
         LEFT JOIN dwh.dim_pharmacy_hierarchy dph 
                ON foi.last_claim_pharmacy_npi_approved=dph.pharmacy_npi 
         LEFT JOIN medispan.mf2ndc mf2ndc 
                ON foi.last_claim_ndc_approved=mf2ndc.ndc_upc_hri
         LEFT JOIN medispan.mf2name mf2name 
                ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
         LEFT JOIN transactional.transactional_claim tc
                ON foi.last_claim_transactional_claim_id=tc.id
         LEFT JOIN unit_cost_data on unit_cost_data.gcn = f_gcn.gcn
             WHERE (foi.fill_sequence is not null)
               AND foi.is_fraud = FALSE
               AND date_trunc('day',convert_timezone('UTC','America/New_York',foi.last_pbm_adjudication_timestamp_approved))::date + INTERVAL '200 day' >= current_date)

, magic_claims as (
SELECT
       date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date AS "fill date"
     , date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date AS "order date"
     , 'quicksave - counter'        as "pharmacy network name"
     , dph.pharmacy_npi             as "pharmacy npi"
     , dph.pharmacy_name            as "pharmacy name"
     , dph.ncpdp_relationship_id    as "ncpdp relationship id"
     , dph.ncpdp_relationship_name  as "ncpdp relationship name"
     , 'quicksave'                  as "script source type"
     , mfoi.medid                   as "purchased med id"
     , mfoi.medid                   as "filled med id"
     , mfoi.med_name                as "purchased med name"
     , mfoi.med_name                as "filled med name" 
     , mfoi.generic_name_short      as "purchased generic name short"
     , mfoi.generic_name_short      as "filled generic name short"
     , mfoi.med_dosage              as "purchased strength"
     , mfoi.med_dosage              as "filled strength"
     , mfoi.med_form                as "purchased form"
     , mfoi.med_form                as "filled form"
     , mfoi.gcn                     as "purchased gcn" 
     , mfoi.gcn_seqno               as "purchased gcn seqno"
     , mfoi.gcn                     as "filled gcn" 
     , mfoi.gcn_seqno               as "filled gcn seqno"

     , case when length(mfoi.ndc::varchar) <> 14 then LPAD(mfoi.ndc::varchar, 11, '0') ELSE mfoi.ndc::varchar end as "filled ndc"
     , ndc.label_name as "ndc label name"
     , ndc.brand_name as "ndc brand name"
     , ndc.orange_book_code as "orange book code"
     , case when length(mf2name.generic_product_identifier::varchar) <> 14 then LPAD(mf2name.generic_product_identifier::varchar, 14, '0') ELSE mf2name.generic_product_identifier::varchar end as "filled gpi"

     , 'ala carte'                  as "is subscription"
     , coalesce(mh_tem_sub_flag.mh_tem_sub_flag,0) as "mens health telehealth flag"
     , gcn.maint                    as "maintenance indicator"
     , gcn.therapeutic_class_desc_generic as "generic therapeutic class"
     , gcn.therapeutic_class_desc_standard as "specific therapeutic class"
     , medid.custom_therapeutic_class as "custom therapeutic class"
     , mf2ndc.multi_source_code as "multi source code"
     , case when mf2ndc.multi_source_code = 'Y' then 'Generic' else 'Brand' end as "multi source generic brand indicator"
     , mfoi.quantity                as "quantity" 
     , mfoi.claim_days_supply       as "days supply"
     , case when mfoi.claim_days_supply < 84 then 30 else 90 end as "days supply normalized"
     , 1                            as fills 
     , patient_id::varchar          as "user id"
     , magic_prior_auth_id::varchar as "order id" 
     , transactional_claim_id::varchar as "transactional claim id"
     , case when is_first_claim_date = TRUE then 'new' else 'returning' end as new_returning_fill_flag     
     , coalesce(mfoi.patient_pay_amount,0)::float  as "gross revenue"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float) 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float) 
                 end as "gross margin"
--, COALESCE(case when mfoi.pricing_total_cost < 0 then abs(mfoi.pricing_total_cost) else 0.0 end, 0) as "revenue share"
--, COALESCE(case when mfoi.pricing_total_cost > 0 then abs(mfoi.pricing_total_cost) else 0.0 end, 0) as "true up cost"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float 
                 end as "total cost"
     ,0                             as "stripe credit card processing fee"
     ,1.1                           as "candle claims processing fee"
     ,0                             as "actual balance billing amount"
     ,0                             as "intended balance billing amount"
     ,''                            as "balance billing reason"
     ,''                            as "balance billing charge status"
     , tc.pricing_unc_cost          as "unc price"
     , tc.pricing_strategy          as "pharmacy reimbursement pricing strategy"
     , case when tc.pricing_unc_cost > 0 and mfoi.patient_pay_amount > 0 and round(mfoi.patient_pay_amount,2) > round(tc.pricing_unc_cost,2) then 1 else 0 end as "price paid exceeds unc flg"
     , case when mfoi.copay_revenue_pool is not null and tc.pricing_unc_cost > 0
                 and round(tc.pricing_unc_cost,2) <= round(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float,2) then 1 
            when mfoi.copay_revenue_pool is null and tc.pricing_unc_cost > 0
                 and round(tc.pricing_unc_cost,2) <= round(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float,2) then 1 
                 else 0 end as "unc claim flg"
     , coalesce(mfoi.quantity::float*coalesce(awp_hist_claim.unit_price,mf2prc.unit_price,0)::float,0) as "awp amount"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float-coalesce(mfoi.pricing_dispensing_fee::float,0)::float 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float-coalesce(mfoi.pricing_dispensing_fee::float,0)::float 
                 end as "ingredient cost amount"
     , 0                            as "wmt ger rate"
     , 0                            as "estimated wmt 92 pct ger true up amount"
     , case when mfoi.copay_revenue_pool is not null 
                 then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float-coalesce(mfoi.copay_revenue_pool::float,0)::float) 
            when mfoi.copay_revenue_pool is null then coalesce(mfoi.patient_pay_amount::float,0)::float-(coalesce(mfoi.patient_pay_amount::float,0)::float+coalesce(mfoi.pricing_total_cost::float,0)::float) 
                 end as "gross margin wmt adjusted"
     ,coalesce(unit_cost_data.unit_cost*mfoi.quantity) as "pharmacy acq cost"
     FROM fifo.magic_fact_order_claim mfoi 
LEFT JOIN transactional.transactional_claim tc on mfoi.transactional_claim_id=tc.id 
LEFT JOIN dwh.dim_pharmacy_hierarchy dph on mfoi.pharmacy_npi=dph.pharmacy_npi
LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn on mfoi.gcn_seqno=gcn.gcn_seqno
LEFT JOIN dwh.dim_medid_hierarchy medid on mfoi.medid=medid.medid
LEFT JOIN dwh.dim_ndc_hierarchy ndc on COALESCE(mfoi.ndc,'0')::float=ndc.ndc::float
LEFT JOIN mh_tem_sub_flag mh_tem_sub_flag on mfoi.gcn=mh_tem_sub_flag.gcn
LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim 
       ON COALESCE(mfoi.ndc,'0')::float = awp_hist_claim.ndc_upc_hri::float AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
LEFT JOIN medispan.mf2prc mf2prc 
       ON COALESCE(mfoi.ndc,'0')::float = mf2prc.ndc_upc_hri::float AND mf2prc.price_code = 'A'
LEFT JOIN medispan.mf2ndc mf2ndc 
       ON COALESCE(mfoi.ndc,'0')::float = mf2ndc.ndc_upc_hri::float
LEFT JOIN medispan.mf2name mf2name 
       ON mf2ndc.drug_descriptor_id = mf2name.drug_descriptor_id
LEFT JOIN unit_cost_data on unit_cost_data.gcn = mfoi.gcn
    WHERE (mfoi.action_type = 'approval') 
      AND (mfoi.action_sequence = 1)
      AND date_trunc('day',convert_timezone('UTC','America/New_York',mfoi.action_timestamp))::date + INTERVAL '200 day' >= current_date)

, integrated_stack as (
select *,case when "pharmacy name" ilike '%serve%you%rx%' then 'Serve You Rx' else "ncpdp relationship name" end as "custom pharmacy chain name" from ecomm_claims 
UNION ALL 
select *,case when "pharmacy name" ilike '%serve%you%rx%' then 'Serve You Rx' else "ncpdp relationship name" end as "custom pharmacy chain name" from magic_claims
)

, most_common_mac_strategy_1 as (
select
"custom pharmacy chain name"
,"pharmacy reimbursement pricing strategy"
,sum(fills) as fills
from integrated_stack where "multi source code" = 'Y' and "custom pharmacy chain name" is not null
group by 1,2)

, most_common_mac_strategy_2 as (
select 
"custom pharmacy chain name"
,"pharmacy reimbursement pricing strategy"
,fills
,row_number() over (partition by "custom pharmacy chain name" order by coalesce(fills,0) desc) as rn
from most_common_mac_strategy_1)																						

, most_common_mac_strategy_3 as (
select
"custom pharmacy chain name"
,"pharmacy reimbursement pricing strategy" 
from most_common_mac_strategy_2 where rn = 1)

select 
x.*
,coalesce(x."pharmacy reimbursement pricing strategy",y."pharmacy reimbursement pricing strategy") as "final pharmacy reimbursement pricing strategy"
from integrated_stack x
left join most_common_mac_strategy_3 y on x."custom pharmacy chain name"=y."custom pharmacy chain name" and x."multi source code" = 'Y'
;


 
select distinct gcn,unit_price,dispensing_fee_margin from transactional.med_price where pharmacy_network_id=5
and ended_on is null and gcn in (57901,45680,39683,4348,10200,26323,27174,10770);


select * from dwh.dim_gcn_seqno_hierarchy limit 10;


select * from dwh.dim_medid_hierarchy  where med_medid_desc ilike '%anusol%';


select * from transactional.med_price where medid = 278350 and ended_on is null and pharmacy_network_id=1;



select * from mktg_dev.privia_scripts limit 100;



select * from fifo.magic_fact_order_claim limit 10;


select * from transactional.med_price where;



create external table pricing_external_dev.keysource_min_gcn_acquisition_unit_cost_2020_04_07(
gcn integer,
lowest_keysource_unit_acquisition_cost float)
row format delimited
fields terminated by ','
stored as textfile
location 's3://blink-dw-data-raw-prod/pricing_adhoc_data/keysource_data_20200417/keysource_min_gcn_acquisition_unit_cost_2020_04_07/';


SELECT
	started_on, unit_price, dispensing_fee_margin , pharmacy_network_id
FROM
	transactional.med_price
WHERE
	medid = 212595
-- 	and(ended_on > '2019-08-01'
-- 		OR ended_on IS NULL)
-- 	AND pharmacy_network_id = 5
order BY
	started_on 
	
	
	
;




select distinct medid, gcn, started_on, ended_on, unit_price, dispensing_fee_margin, branded, pharmacy_network_id from transactional.med_price where gcn = 27941 and ended_on is null order by medid, started_on;

select * from dwh.dim_medid_hierarchy where gcn_seqno = 6858;

select * FROM transactional.available_med where gcn_seqno = 6858;

select * from dwh.dim_gcn_seqno_hierarchy where gcn = 27941 ; 

select * from dwh.dim_ndc_hierarchy where gcn_seqno = 6858 ;



SELECT
	start_date, mac_list, unit_price
FROM
	transactional.network_pricing_mac
WHERE
	gcn_seqno = 1262
	and(end_date > '2019-08-01'
		OR end_date IS NULL)
	AND mac_list like '%02%'
order BY
	start_date 
;

select count(*) from transactional.med_price where ended_on is null and pharmacy_network_id = 4;



select medid,med_name_slug,* from dwh.dim_medid_hierarchy where medid=294452;

select * from fifo.magic_fact_order_claim where action_timestamp::date 

select pharmacy_name,sum(patient_pay_amount),sum(pharmacy_ingredient_cost),count(*) from fifo.magic_fact_order_claim where action_type = 'approval' and action_timestamp::date >'2020-01-01' group by 1;



WITH gcn_gpi AS (
	SELECT
		ms_n.generic_product_identifier AS gpi,
		gcn.gcn,
		sum(coalesce(sh.trx_count,
				0)) AS symphony_health_2017_generic_scripts
	FROM
		dwh.dim_ndc_hierarchy n
	LEFT JOIN dwh.dim_medid_hierarchy m ON n.branded_medid = m.medid
	LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn ON n.gcn_seqno = gcn.gcn_seqno
	LEFT JOIN medispan.mf2ndc ms_ndc ON n.ndc = ms_ndc.ndc_upc_hri
	LEFT JOIN medispan.mf2name ms_n ON ms_ndc.drug_descriptor_id = ms_n.drug_descriptor_id
	LEFT JOIN static_data.symphony_2017_generics_annual sh ON n.ndc = sh.ndc
GROUP BY
	1,
	2
),
map_gcn_gpi AS (
	SELECT
		gpi,
		gcn
	FROM (
		SELECT
			*,
			row_number() OVER (PARTITION BY gpi ORDER BY symphony_health_2017_generic_scripts DESC) AS rn
		FROM
			gcn_gpi) sorted
	WHERE
		sorted.rn = 1
),
ndc_gpi AS ( SELECT DISTINCT
	ndc.ndc_upc_hri AS ndc,
	n.generic_product_identifier AS gpi
FROM
	medispan.mf2ndc ndc
	INNER JOIN medispan.mf2name n ON ndc.drug_descriptor_id = n.drug_descriptor_id
),
gpi_info AS (
SELECT
	gpi,
	max(orange_book_code) AS orange_book_code,
	max(label_name) AS label_name,
	max(multi_source_code) AS multi_source_code
FROM
	dwh.dim_ndc_current_price_data
WHERE
	repackager_indicator = '0'
	AND(obsolete_date >= CURRENT_DATE - 730
	OR obsolete_date IS NULL)
GROUP BY
	1
)
SELECT
	brand_generic,
	smac.ndc,
	unit_price,
	prior_approval,
	billing_units,
	otc_indicator,
	ndc_gpi.gpi,
	map_gcn_gpi.gcn,
	gpi_info.orange_book_code,
	gpi_info.label_name,
	gpi_info.multi_source_code
FROM
	mktg_dev.sdey_ny_smac_20200301 AS smac
	LEFT OUTER JOIN ndc_gpi ON REPLACE(smac.ndc, '-', '') = REPLACE(ndc_gpi.ndc, '-', '')
	LEFT OUTER JOIN map_gcn_gpi ON map_gcn_gpi.gpi = ndc_gpi.gpi
	LEFT OUTER JOIN gpi_info ON gpi_info.gpi = ndc_gpi.gpi;


-- with k as (
-- 	SELECT
-- 		gpi,
-- 		max(label_name) as label_name,
-- 		max(otc_rx_id) as otc
-- 	FROM
-- 		dwh.dim_ndc_current_price_data
-- 	WHERE
-- -- 		orange_book_code IN('ZB')
-- -- 		repackager_indicator = '0'
-- -- 		AND (obsolete_date >= CURRENT_DATE - 730 OR obsolete_date IS NULL)
-- -- 		AND multi_source_code = 'Y'
-- -- 		AND NOT lower(gpi) like  '%e%'
-- 		gpi='04000020100630'
-- 	GROUP BY
-- 		1
-- )
SELECT
*
-- 	gpi,
-- 	label_name,
-- 	otc,
-- 	pac,
-- 	pac_low,
-- 	pac_high,
-- 	pac_retail
FROM gold_standard.pac_ms 
-- 	ON pac_ms.drug_identifier = k.gpi 
-- 	AND brand_generic = 'Generic'
	WHERE
	identifier_type = 'GPI'
	AND pac_ms.drug_identifier='04000020100630'
	AND downloaded_date > '2020-02-01'

-- 	AND downloaded_date = CURRENT_DATE-1
-- GROUP BY 1,2,3,4,5,6,7;
;




with fills as (
	SELECT
		f_gcn.gcn,
		sum(1) as fills
	FROM
		dwh.fact_order_item foi
		LEFT JOIN dwh.dim_user AS du ON foi.account_id = du.account_id
			AND du.is_internal = FALSE
			AND du.is_phantom = FALSE
		LEFT JOIN dwh.dim_medid_hierarchy dmh ON foi.generic_medid = dmh.medid
		LEFT JOIN dwh.dim_gcn_seqno_hierarchy f_gcn ON foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
		LEFT JOIN dwh.dim_medid_hierarchy f_medid ON foi.last_claim_medid_approved = f_medid.medid -- info for the med actually filled (e.g. med name)
	WHERE foi.fill_sequence IS NOT NULL
	AND foi.is_fraud = FALSE
	AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '2000 day' >= CURRENT_DATE
	GROUP BY
		1) 
SELECT
	gcn,fills
FROM
	fills 
where 
	fills > 10;
-- 	gcn in ('17469','32099','20715','20840','19975','97248','35347','32767','22913','45639',
-- 	'94200','22148','25414','10943','26329','34835');


select * from transactional.med_price where medid=167135;


select gcn,unit_price from transactional.network_pricing_mac 
INNER join dwh.dim_gcn_seqno_hierarchy ON network_pricing_mac.gcn_seqno = dim_gcn_seqno_hierarchy.gcn_seqno
where end_date is null and network_pricing_mac.gcn_seqno=65423;

select * from dwh.dim_ndc_hierarchy where gpi=65200010100780;
select * from pricing_dev.mac_automation_pac_data_gpi where gpi = 65200010100780;


-- 	gcn in ('17469','32099','18126','161','20715','20840','27692','22882','19975','97248','4695','18961','35347','11491','4701','27386','32767','99676','7651','47263','35793','10194','27384','98308','22913','98425','45639','94200','22148','9101','25414','11301','97706','10943','26329','56972','34835')
	and pharmacy_network_id = 1
	and ended_on is null;


select * from mktg_dev.units_of_use_phase2;
grant all on mktg_dev.units_of_use_phase2 to scott;

GRANT SELECT ON pricing_prod.ochsner_mac_gcn_2020_03_26 TO "public";
GRANT SELECT ON pricing_prod.ochsner_mac_gpi_2020_03_26 TO "public";



with drug_data AS 
(SELECT
	med_price.medid,
	started_on,
	ended_on,
	unit_price,
	dispensing_fee_margin + 1.75,
	dense_rank() OVER (PARTITION BY med_price.medid,
		pharmacy_network_id ORDER BY started_on DESC) AS store_rank
FROM
	transactional.med_price
	INNER JOIN mktg_dev.sdey_tmp_data_test_889_brands AS branded ON med_price.medid = branded.medid
WHERE
	pharmacy_network_id = 1
	-- 	and ended_on is null
ORDER BY
	med_price.medid,
	started_on)
SELECT
	*
FROM
	drug_data
WHERE
	store_rank = 1
	AND ended_on is null


select med_name_slug,* from dwh.dim_medid_hierarchy left join transactional.available_med on dim_medid_hierarchy.medid = available_med.medid where available_med.medid is null ;

select last_change_date,count(*) from medispan.MF2PRC group by 1 ;

select * from drugs_etl.med_price where medid=561693 and pharmacy_network_id=1;

select * from transactional.med_price where pharmacy_network_id = 5 and medid = 150048;

select 
	others.medid
from
(select distinct medid from transactional.med_price where pharmacy_network_id!=1 and ended_on is null) others
left JOIN
(select distinct medid from transactional.med_price where pharmacy_network_id=1 and ended_on is null) edlp
on others.medid = edlp.medid
where edlp.medid is null ;

select * from mktg_dev.sdey_walmart_list_2019_11_01_raw;

select * from dwh.dim_medid_hierarchy where medid=579341;


with gcn_gpi as (
	select 
	ms_n.generic_product_identifier as gpi
	,gcn.gcn
	,gcn.gcn_seqno
	from dwh.dim_ndc_hierarchy n 
	    LEFT JOIN dwh.dim_medid_hierarchy m ON n.branded_medid=m.medid 
	    LEFT JOIN dwh.dim_gcn_seqno_hierarchy gcn ON n.gcn_seqno=gcn.gcn_seqno
	    LEFT JOIN medispan.mf2ndc ms_ndc ON n.ndc = ms_ndc.ndc_upc_hri
	    LEFT JOIN medispan.mf2name ms_n ON ms_ndc.drug_descriptor_id = ms_n.drug_descriptor_id
	    LEFT JOIN static_data.symphony_2017_generics_annual sh on n.ndc=sh.ndc
	group by 1,2,3)
select 
	distinct med.medid,gcn_gpi.gpi 
from 
	dwh.dim_medid_hierarchy as med
inner JOIN gcn_gpi ON med.gcn_seqno = gcn_gpi.gcn_seqno 
where gcn_gpi.gpi is not null
order by gpi,gcn;

SELECT
	oschner_formulary_medids.medid,
	med_medid_desc,
	med_ref_multi_source_code_desc,
	med_ref_gen_drug_name_code_desc,
	bg_type,
	gcn_seqno,
	gcn,
	dea,
	is_contain_opioids,
	available_med_flag,
	has_med_price_set_flag,
	edlp_priced_flag,
	hd_priced_flag,
	bh01_mac_priced_flag,
	site_med_name_default_quantity,
	edlp_unit_price,
	edlp_fixed_price,
	hd_unit_price,
	hd_fixed_price,
	min_wmt_grx,
	hd_syr_mac_price,
	hd_syr_dispensing_fee,
	bh01_mac_price,
	bh01_dispensing_fee,
	avg_generic_awp_unit_cost,
	min_generic_awp_unit_cost,
	max_generic_awp_unit_cost,
	generic_pac_unit,
	generic_pac_high_unit,
	generic_pac_retail_unit,
	top_30ds_quantity
FROM
	mktg_dev.oschner_formulary_medids
	INNER JOIN fifo.medid_universe_price_and_catalog ON oschner_formulary_medids.medid = medid_universe_price_and_catalog.medid;


select * from fifo.medid_universe_price_and_catalog where medid=551704;



with zb_gpi as (SELECT
	gpi,
	max(label_name) as label_name
FROM
	dwh.dim_ndc_current_price_data
WHERE
-- 	orange_book_code IN('AA', 'AB', 'AN', 'AO', 'AP', 'AT', 'ZA', 'ZC')
	orange_book_code IN('ZB')
	AND repackager_indicator = '0'
	AND (obsolete_date >= CURRENT_DATE - 730 OR obsolete_date IS NULL)
	AND multi_source_code = 'Y'
	AND NOT lower(gpi) like  '%e%'
GROUP BY
	1), 
fills_gpi AS (
	SELECT
		dim_ndc_hierarchy.gpi AS gpi,
		if_varchar(mf2ndc.multi_source_code = 'Y','Generic','Brand') AS brand_generic,
		tc.pricing_strategy AS pharmacy_reimbursement_cost_basis,
		sum(coalesce(coalesce(awp_hist_claim.unit_price,mf2prc.unit_price)::float*foi.last_claim_quantity_approved::float,0))::float as awp_amount_calc	,
		sum(foi.last_claim_quantity_approved::float) as quantity,
		sum(coalesce(awp_hist_last.unit_price::float, 0)::float * foi.last_claim_quantity_approved::float)::float AS awp_amount_calc_foc,
		sum(coalesce(last_claim_awp_amount_approved, 0))::float AS awp_amount_foi,
		sum(coalesce(last_pricing_ingredient_cost_approved::float, 0))::float AS ingredient_cost,
		count(DISTINCT tc.id) AS filled_scripts
	FROM
		dwh.fact_order_item foi
		LEFT JOIN dwh.dim_ndc_hierarchy  AS dim_ndc_hierarchy ON foi.last_claim_ndc_approved = dim_ndc_hierarchy.ndc
		LEFT JOIN dwh.dim_awp_price_hist awp_hist_claim ON COALESCE(foi.last_claim_ndc_approved, '0') = awp_hist_claim.ndc_upc_hri
			AND awp_hist_claim.ended_at = '3000-01-01 00:00:00'
		LEFT JOIN medispan.mf2prc mf2prc ON COALESCE(foi.last_claim_ndc_approved, '0') = mf2prc.ndc_upc_hri
			AND mf2prc.price_code = 'A'
		LEFT JOIN dwh.dim_awp_price_hist awp_hist_last ON COALESCE(foi.last_claim_ndc_approved, '-1') = awp_hist_last.ndc_upc_hri
			AND foi.last_pbm_adjudication_timestamp_approved >= awp_hist_last.started_at
			AND foi.last_pbm_adjudication_timestamp_approved < awp_hist_last.ended_at
		LEFT JOIN dwh.dim_pharmacy_hierarchy dph ON foi.last_claim_pharmacy_npi_approved = dph.pharmacy_npi
		LEFT JOIN medispan.mf2ndc mf2ndc ON foi.last_claim_ndc_approved = mf2ndc.ndc_upc_hri
		LEFT JOIN transactional.transactional_claim tc ON foi.last_claim_transactional_claim_id = tc.id
	WHERE
		foi.last_pbm_adjudication_timestamp_approved::date >= CURRENT_DATE - 365
	GROUP BY
		1,2,3
)
	SELECT
		zb_gpi.gpi,
		label_name,
		fills_gpi.brand_generic,
		pharmacy_reimbursement_cost_basis,
		awp_amount_calc,
		quantity,
		awp_amount_calc/quantity as awp_amount_unit_price,
		ingredient_cost,
		filled_scripts,
		pac_ms.pac_high,
		pac_ms.pac_retail,
		pac_ms.pac_low
	FROM zb_gpi
	INNER join fills_gpi ON zb_gpi.gpi = fills_gpi.gpi
	LEFT OUTER JOIN gold_standard.pac_ms ON pac_ms.drug_identifier = zb_gpi.gpi and fills_gpi.brand_generic = pac_ms.brand_generic
	WHERE
		identifier_type = 'GPI'
		AND downloaded_date = CURRENT_DATE-1
	;
		





	