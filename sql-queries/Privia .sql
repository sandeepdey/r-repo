with mp_edlp as (select 
y.gcn
,1.75 as dispensing_fee
,max(x.unit_price) as unit_price
,max(x.dispensing_fee_margin) as dispensing_fee_margin
,max(x.started_on)::date as max_started_date
from transactional.med_price x
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 1 
group by 1,2)

, mp_hd as (select 
y.gcn
,1.75 as dispensing_fee
,max(x.unit_price) as unit_price
,max(x.dispensing_fee_margin) as dispensing_fee_margin
,max(x.started_on)::date as max_started_date
from transactional.med_price x
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 3 
group by 1,2)

, qs_mp_bksh as (select 
y.gcn
,1.75 as dispensing_fee
,max(x.unit_price) as unit_price
,max(x.dispensing_fee_margin) as dispensing_fee_margin
,max(x.started_on)::date as max_started_date
from transactional.med_price x
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 4 
group by 1,2)

, qs_mp_heb as (select 
y.gcn
,1.75 as dispensing_fee
,max(x.unit_price) as unit_price
,max(x.dispensing_fee_margin) as dispensing_fee_margin
,max(x.started_on)::date as max_started_date
from transactional.med_price x
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 5 
group by 1,2)

, billing_unit as (
select gcn,drug_form as billing_unit from 
(select gcn,drug_form,row_number() over (partition by gcn order by ndc_count desc,drug_form desc) as rn from
(select g.gcn,ndc.drug_form,count(ndc.ndc) as ndc_count from dwh.dim_ndc_hierarchy ndc inner join dwh.dim_gcn_seqno_hierarchy g on ndc.gcn_seqno=g.gcn_seqno
group by 1,2)) rn where rn.rn = 1)

, uou_form_flag as (
select distinct form,1 as uou_form_flag
from fifo.generic_price_portfolio_datamart where form in ('AMPUL',
'CREAM (G)',
'SOLUTION',
'DROPS',
'VIAL',
'JELLY(ML)',
'OINT. (G)',
'GEL (GRAM)',
'LOTION',
'POWDER',
'DROP W/APP',
'ORAL CONC',
'GRAN PACK',
'SHAMPOO',
'CAP DS PK',
'AUTO INJCT',
'PATCH TDWK',
'ORAL SUSP',
'GEL MD PMP',
'FOAM',
'SPRAY',
'SPRAY/PUMP',
'SUSP RECON',
'POWD PACK',
'CREAM/APPL',
'PATCH TDSW',
'SOL MD PMP',
'GEL W/PUMP',
'PASTE (G)',
'PATCH TD24',
'DROPS SUSP',
'SYRINGE',
'TAB DS PK',
'SOLN RECON',
'MED. SWAB',
'SUPP.VAG',
'LIQUID',
'GEL W/APPL',
'BLST W/DEV',
'CPMP 12HR',
'ENEMA KIT',
'MOUTHWASH',
'VIAL-NEB',
'AMPUL-NEB',
'PATCH TD72',
'GRANULES',
'SYRUP',
'CARTRIDGE',
'SUS MC REC',
'ENEMA',
'GEL PACKET',
'SUSPENSION'))

, cpr_data_1 as (
select 
z.gcn
,z.generic_name_short
,z.dosage_form_desc
,z.strength
,x.equiv_name as cpr_generic_name
,x.dosage as cpr_strength
,x.form_name as cpr_form
,x.scrape_date
,x.default_quantity
,x.quantity
,x.uou_form
,x.uou_multiplier
,x.quantity::float*x.uou_multiplier::float as adjusted_quantity
,min(case when x.price > 0 then x.price end) as min_houston_grx
,min(case when x.pharmacy = 'Walgreens' and x.price > 0 then x.price end) as min_wags_grx
,min(case when x.pharmacy = 'Walmart' and x.price > 0 then x.price end) as min_wmt_grx
,min(case when x.pharmacy = 'CVS' and x.price > 0 then x.price end) as min_cvs_grx
,min(case when x.pharmacy = 'Kroger' and x.price > 0 then x.price end) as min_kr_grx
,min(case when x.pharmacy ilike '%sam%club%' and x.price > 0 then x.price end) as min_sams_grx
,min(case when x.pharmacy = 'Costco' and x.price > 0 then x.price end) as min_cstco_grx
,min(case when x.pharmacy = 'H-E-B' and x.price > 0 then x.price end) as min_heb_grx
,min(case when x.pharmacy = 'Randalls Pharmacy' and x.price > 0 then x.price end) as min_rand_grx
,min(case when x.pharmacy = 'Other pharmacies' and x.price > 0 then x.price end) as min_other_pharmacies_grx

from pricing_external_dev.goodrx_raw_data x 
left join dwh.dim_medid_hierarchy y on x.medid=y.medid
left join dwh.dim_gcn_seqno_hierarchy z on y.gcn_seqno=z.gcn_seqno
where x.price_type in ('coupon','cash') and x.geo = 'houston' and x.default_quantity::float=x.quantity::float and z.gcn is not null and x.date::date = '2020-04-14'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)

, cpr_data_2 as (
select * from (
select *
,row_number() over (partition by gcn order by scrape_date desc,adjusted_quantity asc) as rn 
from cpr_data_1) rn 
where rn.rn=1)

, gcns_with_multiple_multipliers as (
select gcn,count(distinct uou_multiplier) as multiplier_count
from pricing_external_dev.goodrx_raw_data 
where price_type in ('coupon','cash') and gcn is not null and uou_multiplier > 0
group by 1)

, cpr_data_3 as (
select 
x.gcn
,x.cpr_generic_name
,x.cpr_strength
,x.cpr_form
,x.scrape_date
,x.default_quantity
,x.quantity
,x.uou_form
,x.uou_multiplier
,case when y.multiplier_count > 1 then 1 else 0 end as gcn_multiple_multiplier_flag
,x.adjusted_quantity
,x.min_houston_grx
,x.min_wags_grx
,x.min_wmt_grx
,x.min_cvs_grx
,x.min_kr_grx
,x.min_sams_grx
,x.min_cstco_grx
,x.min_heb_grx
,x.min_rand_grx
,x.min_other_pharmacies_grx
from cpr_data_2 x
left join gcns_with_multiple_multipliers y on x.gcn=y.gcn)

, privia_scripts as 
(select account_name,provider_full_name,g.gcn as fdb_gcn,g.generic_name_short,g.strength,g.dosage_form_desc,g.maint,g.dea
,count(p.document_id) as rx_count_total
,sum(p.prescription_fill_quantity::float) as rx_quantity_total
,count(case when pharmacy ilike '%walgreen%' then p.document_id end) as rx_count_wags
,count(case when pharmacy ilike '%wal%mart%' then p.document_id end) as rx_count_wmt
,count(case when pharmacy ilike '%cvs%' then p.document_id end) as rx_count_cvs
,count(case when pharmacy ilike '%kroger%' then p.document_id end) as rx_count_kr
,count(case when pharmacy ilike '%sam%club%' then p.document_id end) as rx_count_sams
,count(case when pharmacy ilike '%costco%' then p.document_id end) as rx_count_cstco
,count(case when pharmacy ilike '%heb%' then p.document_id end) as rx_count_heb
,count(case when pharmacy ilike '%randall%' then p.document_id end) as rx_count_rand
,count(case when pharmacy ilike '%delivery%' or pharmacy ilike '%mail%' or pharmacy ilike '%maxor%' or pharmacy ilike '%pillpack%' or pharmacy ilike '%gogomeds%' 
              or pharmacy ilike '%healthwarehouse%' or pharmacy ilike '%health wearhouse%' then p.document_id end) as rx_count_mail_order
--,sum(p.number_of_refills_prescribed+1) as total_fills_prescribed
from pricing_external_dev.privia_rx_export_20200420 p
left join dwh.dim_medid_hierarchy m on p.fbd_med_id=m.medid
left join dwh.dim_gcn_seqno_hierarchy g on m.gcn_seqno=g.gcn_seqno 
group by 1,2,3,4,5,6,7,8)

, generic_cpr_integrated as (
select 
gppd.gcn

-- reimbursement price data
,gppd.bh01_mac_price as privia_bh01_mac_price
,1.25 as privia_dispensing_fee
-- ,gppd.bh02_mac_price
-- ,gppd.bh03_mac_price
-- ,gppd.pac_unit
-- ,gppd.pac_low_unit
-- ,gppd.pac_high_unit
-- ,gppd.pac_retail_unit
-- 
-- consumer price data
,mp_edlp.unit_price as edlp_unit_price
,mp_edlp.dispensing_fee::float+mp_edlp.dispensing_fee_margin::float as edlp_fixed_price
-- ,mp_edlp.max_started_date as edlp_start_date

,mp_hd.unit_price as hd_unit_price
,mp_hd.dispensing_fee::float+mp_hd.dispensing_fee_margin::float as hd_fixed_price
-- ,mp_hd.max_started_date as hd_start_date

-- consumer price data - qs digital
,qs_mp_bksh.unit_price as qs_bksh_unit_price_digital
,qs_mp_bksh.dispensing_fee::float+qs_mp_bksh.dispensing_fee_margin::float as qs_bksh_fixed_price_digital
,qs_mp_heb.unit_price as qs_heb_unit_price_digital
,qs_mp_heb.dispensing_fee::float+qs_mp_heb.dispensing_fee_margin::float as qs_heb_fixed_price_digital

,h1pct.unit_price as qs_houston_1pct_sim_unit_price
,h1pct.fixed_price as qs_houston_1pct_sim_fixed_price

,h5pct.unit_price as qs_houston_5pct_sim_unit_price
,h5pct.fixed_price as qs_houston_5pct_sim_fixed_price



-- ,gppd.hicl_desc as fdb_generic_name_short
-- ,gppd.strength as fdb_strength
-- ,gppd.form as fdb_form
-- ,cpr.cpr_generic_name
-- ,cpr.cpr_strength
-- ,cpr.cpr_form
-- ,gppd.maint_flg
-- ,gppd.dea_flg
-- ,bu.billing_unit as fdb_billing_unit
,case when uou.uou_form_flag = 1 or bu.billing_unit in ('milliliters (liquids)','grams (solids)') then 1 else 0 end as is_unit_of_use
,case when cpr.scrape_date is not null then 1 else 0 end as has_cpr
,case when cpr.uou_multiplier > 1 then 1 else 0 end as has_cpr_multiplier
-- ,case when cpr.gcn_multiple_multiplier_flag = 1 then 1 else 0 end as has_multiple_multipliers

,case when gppd.bh02_mac_price > 0 then 1 else 0 end as has_mac_price
,case when gppd.blink_edlp_price > 0 and gppd.available_med_flg = 1 then 1 else 0 end as has_edlp

,case when qs_mp_bksh.unit_price > 0 then 1 else 0 end as has_qs_bksh_digital_price
,case when qs_mp_heb.unit_price > 0 then 1 else 0 end as has_qs_heb_digital_price

,cpr.scrape_date
,cpr.uou_form
,cpr.default_quantity
,cpr.uou_multiplier
,cpr.adjusted_quantity as multiplier_adjusted_default_quantity

,case when cpr.adjusted_quantity > 0 and mp_edlp.unit_price::float > 0 
      then cpr.adjusted_quantity*mp_edlp.unit_price::float+mp_edlp.dispensing_fee::float+mp_edlp.dispensing_fee_margin::float else 0 end as edlp_price
,case when cpr.adjusted_quantity > 0 and mp_hd.unit_price::float > 0 
      then cpr.adjusted_quantity*mp_hd.unit_price::float+mp_hd.dispensing_fee::float+mp_hd.dispensing_fee_margin::float else 0 end as hd_price
,case when cpr.adjusted_quantity > 0 and qs_mp_bksh.unit_price::float > 0 
      then cpr.adjusted_quantity*qs_mp_bksh.unit_price::float+qs_mp_bksh.dispensing_fee::float+qs_mp_bksh.dispensing_fee_margin::float else 0 end as bksh_qs_price
,case when cpr.adjusted_quantity > 0 and qs_mp_heb.unit_price::float > 0 
      then cpr.adjusted_quantity*qs_mp_heb.unit_price::float+qs_mp_heb.dispensing_fee::float+qs_mp_heb.dispensing_fee_margin::float else 0 end as heb_qs_price

,case when cpr.adjusted_quantity > 0 and mp_hd.unit_price::float > 0
      then cpr.adjusted_quantity*mp_hd.unit_price::float+mp_hd.dispensing_fee::float+mp_hd.dispensing_fee_margin::float
      when cpr.adjusted_quantity > 0 and mp_edlp.unit_price::float > 0 
      then cpr.adjusted_quantity*mp_edlp.unit_price::float+mp_edlp.dispensing_fee::float+mp_edlp.dispensing_fee_margin::float else 0 end as privia_hd_else_edlp_price

,case when cpr.adjusted_quantity > 0 and qs_mp_heb.unit_price::float > 0
      then cpr.adjusted_quantity*qs_mp_heb.unit_price::float+qs_mp_heb.dispensing_fee::float+qs_mp_heb.dispensing_fee_margin::float 
      when cpr.adjusted_quantity > 0 and mp_hd.unit_price::float > 0
      then cpr.adjusted_quantity*mp_hd.unit_price::float+mp_hd.dispensing_fee::float+mp_hd.dispensing_fee_margin::float
      when cpr.adjusted_quantity > 0 and mp_edlp.unit_price::float > 0 
      then cpr.adjusted_quantity*mp_edlp.unit_price::float+mp_edlp.dispensing_fee::float+mp_edlp.dispensing_fee_margin::float else 0 end as privia_heb_else_hd_else_edlp_price

,case when cpr.adjusted_quantity > 0 and h1pct.unit_price > 0 
      then cpr.adjusted_quantity*h1pct.unit_price::float+h1pct.fixed_price::float   
      when cpr.adjusted_quantity > 0 and qs_mp_heb.unit_price::float > 0
      then cpr.adjusted_quantity*qs_mp_heb.unit_price::float+qs_mp_heb.dispensing_fee::float+qs_mp_heb.dispensing_fee_margin::float 
      when cpr.adjusted_quantity > 0 and mp_hd.unit_price::float > 0
      then cpr.adjusted_quantity*mp_hd.unit_price::float+mp_hd.dispensing_fee::float+mp_hd.dispensing_fee_margin::float
      when cpr.adjusted_quantity > 0 and mp_edlp.unit_price::float > 0 
      then cpr.adjusted_quantity*mp_edlp.unit_price::float+mp_edlp.dispensing_fee::float+mp_edlp.dispensing_fee_margin::float else 0 end as privia_hou_1pct_heb_else_hd_else_edlp_price

,case when cpr.adjusted_quantity > 0 and h5pct.unit_price > 0 
      then cpr.adjusted_quantity*h5pct.unit_price::float+h5pct.fixed_price::float   
      when cpr.adjusted_quantity > 0 and qs_mp_heb.unit_price::float > 0
      then cpr.adjusted_quantity*qs_mp_heb.unit_price::float+qs_mp_heb.dispensing_fee::float+qs_mp_heb.dispensing_fee_margin::float 
      when cpr.adjusted_quantity > 0 and mp_hd.unit_price::float > 0
      then cpr.adjusted_quantity*mp_hd.unit_price::float+mp_hd.dispensing_fee::float+mp_hd.dispensing_fee_margin::float
      when cpr.adjusted_quantity > 0 and mp_edlp.unit_price::float > 0 
      then cpr.adjusted_quantity*mp_edlp.unit_price::float+mp_edlp.dispensing_fee::float+mp_edlp.dispensing_fee_margin::float else 0 end as privia_hou_5pct_heb_else_hd_else_edlp_price

,cpr.min_houston_grx
,cpr.min_wags_grx
,cpr.min_wmt_grx
,cpr.min_cvs_grx
,cpr.min_kr_grx
,cpr.min_sams_grx
,cpr.min_cstco_grx
,cpr.min_heb_grx
,cpr.min_rand_grx
,cpr.min_other_pharmacies_grx

-- ,qs_qty.ltd_qs_counter_qty_per_script
-- ,bh_ecomm_qty.ltd_bh_ecomm_qty_per_script
--,mi_qty.mi_mmq_qty_per_script
-- ,medicaid_qty.medicaid_qty_per_script
-- 
-- ,coalesce(qs_qty.ltd_qs_counter_qty_per_script,bh_ecomm_qty.ltd_bh_ecomm_qty_per_script,mi_qty.mi_mmq_qty_per_script,medicaid_qty.medicaid_qty_per_script) as final_qty_per_script
-- 
from fifo.generic_price_portfolio_datamart gppd 
left join mp_edlp mp_edlp on gppd.gcn=mp_edlp.gcn
left join qs_mp_bksh qs_mp_bksh on gppd.gcn=qs_mp_bksh.gcn
left join qs_mp_heb qs_mp_heb on gppd.gcn=qs_mp_heb.gcn
left join mp_hd mp_hd on gppd.gcn=mp_hd.gcn
left join billing_unit bu on gppd.gcn=bu.gcn
left join uou_form_flag uou on gppd.form=uou.form
left join pricing_external_dev.min_houston_grx_1pct_2020_04_22_final h1pct on h1pct.gcn=gppd.gcn
left join pricing_external_dev.min_houston_grx_5pct_2020_04_22_final h5pct on h5pct.gcn=gppd.gcn

-- left join qs_counter_qty_per_script qs_qty on gppd.gcn=qs_qty.gcn
-- left join bh_ecomm_qty_per_script bh_ecomm_qty on gppd.gcn=bh_ecomm_qty.gcn
-- left join mi_qty_export_ref mi_qty on gppd.gcn=mi_qty.gcn
-- left join medicaid_qty_per_script medicaid_qty on gppd.gcn=medicaid_qty.gcn
left join cpr_data_3 cpr on gppd.gcn=cpr.gcn)

, integrated_1 as (
select 
account_name
,provider_full_name
,p.fdb_gcn
,p.generic_name_short
,p.strength
,p.dosage_form_desc
,p.maint
,p.dea
,p.rx_count_total
,p.rx_quantity_total
,p.rx_count_wags
,p.rx_count_wmt
,p.rx_count_cvs
,p.rx_count_kr
,p.rx_count_sams
,p.rx_count_cstco
,p.rx_count_heb
,p.rx_count_rand
--,p.rx_count_mail_order
,coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
                             -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) as rx_count_other_pharmacies

,g.privia_bh01_mac_price
,g.privia_dispensing_fee

,case when g.edlp_unit_price > 0 then p.rx_quantity_total::float*g.privia_bh01_mac_price::float+p.rx_count_total*g.privia_dispensing_fee else 0 end as privia_mac_df_cogs


,case when g.qs_houston_5pct_sim_unit_price > 0 then g.qs_houston_5pct_sim_unit_price::float*p.rx_quantity_total::float+g.qs_houston_5pct_sim_fixed_price::float*p.rx_count_total
      when g.qs_heb_unit_price_digital > 0 then g.qs_heb_unit_price_digital::float*p.rx_quantity_total::float+g.qs_heb_fixed_price_digital::float*p.rx_count_total
      when g.hd_unit_price > 0 then g.hd_unit_price::float*p.rx_quantity_total::float+g.hd_fixed_price::float*p.rx_count_total
      when g.edlp_unit_price > 0 then g.edlp_unit_price::float*p.rx_quantity_total::float+g.edlp_fixed_price::float*p.rx_count_total
      else 0 end as hou_5_pct_heb_else_hd_else_edlp_revenue


,case when g.qs_houston_1pct_sim_unit_price > 0 then g.qs_houston_1pct_sim_unit_price::float*p.rx_quantity_total::float+g.qs_houston_1pct_sim_fixed_price::float*p.rx_count_total
      when g.qs_heb_unit_price_digital > 0 then g.qs_heb_unit_price_digital::float*p.rx_quantity_total::float+g.qs_heb_fixed_price_digital::float*p.rx_count_total
      when g.hd_unit_price > 0 then g.hd_unit_price::float*p.rx_quantity_total::float+g.hd_fixed_price::float*p.rx_count_total
      when g.edlp_unit_price > 0 then g.edlp_unit_price::float*p.rx_quantity_total::float+g.edlp_fixed_price::float*p.rx_count_total
      else 0 end as hou_1_pct_heb_else_hd_else_edlp_revenue

,case when g.qs_heb_unit_price_digital > 0 then g.qs_heb_unit_price_digital::float*p.rx_quantity_total::float+g.qs_heb_fixed_price_digital::float*p.rx_count_total
      when g.hd_unit_price > 0 then g.hd_unit_price::float*p.rx_quantity_total::float+g.hd_fixed_price::float*p.rx_count_total
      when g.edlp_unit_price > 0 then g.edlp_unit_price::float*p.rx_quantity_total::float+g.edlp_fixed_price::float*p.rx_count_total
      else 0 end as heb_else_hd_else_edlp_revenue

,case when g.hd_unit_price > 0 then g.hd_unit_price::float*p.rx_quantity_total::float+g.hd_fixed_price::float*p.rx_count_total
      when g.edlp_unit_price > 0 then g.edlp_unit_price::float*p.rx_quantity_total::float+g.edlp_fixed_price::float*p.rx_count_total
      else 0 end as hd_else_edlp_revenue

,g.edlp_unit_price
,g.edlp_fixed_price

,g.hd_unit_price
,g.hd_fixed_price


,g.qs_bksh_unit_price_digital
,g.qs_bksh_fixed_price_digital
,g.qs_heb_unit_price_digital
,g.qs_heb_fixed_price_digital

,g.qs_houston_1pct_sim_unit_price
,g.qs_houston_1pct_sim_fixed_price

,g.qs_houston_5pct_sim_unit_price
,g.qs_houston_5pct_sim_fixed_price


,case when edlp_unit_price > 0 and qs_heb_unit_price_digital > 0 then qs_heb_unit_price_digital
      when hd_unit_price > 0 then hd_unit_price
      else edlp_unit_price end as heb_hd_edlp_unit_price

,case when edlp_unit_price > 0 and qs_heb_unit_price_digital > 0 then qs_heb_fixed_price_digital
      when hd_unit_price > 0 then hd_fixed_price
      else edlp_fixed_price end as heb_hd_edlp_fixed_price

,case when edlp_unit_price > 0 and qs_houston_1pct_sim_unit_price > 0 then qs_houston_1pct_sim_unit_price
      when qs_heb_unit_price_digital > 0 then qs_heb_unit_price_digital
      when hd_unit_price > 0 then hd_unit_price
      else edlp_unit_price end as hou_1pct_heb_hd_edlp_unit_price

,case when edlp_unit_price > 0 and qs_houston_1pct_sim_unit_price > 0 then qs_houston_1pct_sim_fixed_price
      when qs_heb_unit_price_digital > 0 then qs_heb_fixed_price_digital
      when hd_unit_price > 0 then hd_fixed_price
      else edlp_fixed_price end as hou_1pct_heb_hd_edlp_fixed_price

,case when edlp_unit_price > 0 and qs_houston_1pct_sim_unit_price > 0 then qs_houston_5pct_sim_unit_price
      when qs_heb_unit_price_digital > 0 then qs_heb_unit_price_digital
      when hd_unit_price > 0 then hd_unit_price
      else edlp_unit_price end as hou_5pct_heb_hd_edlp_unit_price

,case when qs_houston_1pct_sim_unit_price > 0 then qs_houston_5pct_sim_fixed_price
      when qs_heb_unit_price_digital > 0 then qs_heb_fixed_price_digital
      when hd_unit_price > 0 then hd_fixed_price
      else edlp_fixed_price end as hou_5pct_heb_hd_edlp_fixed_price


,g.is_unit_of_use
,g.has_cpr
,g.has_cpr_multiplier

,g.scrape_date
,g.multiplier_adjusted_default_quantity

,g.edlp_price
,g.hd_price
,g.privia_hd_else_edlp_price
,g.privia_heb_else_hd_else_edlp_price
,g.privia_hou_1pct_heb_else_hd_else_edlp_price
,g.privia_hou_5pct_heb_else_hd_else_edlp_price
,g.bksh_qs_price
,g.heb_qs_price

,g.min_wags_grx
,g.min_wmt_grx
,g.min_cvs_grx
,g.min_kr_grx
,g.min_sams_grx
,g.min_cstco_grx
,g.min_heb_grx
,g.min_rand_grx
,g.min_other_pharmacies_grx
,g.min_houston_grx


-- Scenario A: HD else EDLP Price vs. Benchmark - No Weighting
,case when g.privia_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then 1 else 0 end as gcn_count_wags_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then 1 else 0 end as gcn_count_wmt_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then 1 else 0 end as gcn_count_cvs_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then 1 else 0 end as gcn_count_kr_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then 1 else 0 end as gcn_count_sams_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then 1 else 0 end as gcn_count_cstco_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then 1 else 0 end as gcn_count_heb_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then 1 else 0 end as gcn_count_rand_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 1 else 0 end as gcn_count_min_houston_cpr_comp_a

,case when g.privia_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then 1 else 0 end as gcn_count_wags_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then 1 else 0 end as gcn_count_wmt_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then 1 else 0 end as gcn_count_cvs_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then 1 else 0 end as gcn_count_kr_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then 1 else 0 end as gcn_count_sams_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then 1 else 0 end as gcn_count_cstco_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then 1 else 0 end as gcn_count_heb_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then 1 else 0 end as gcn_count_rand_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 1 else 0 end as gcn_count_min_houston_cpr_win_a

,case when g.privia_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_comp_a

,case when g.privia_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_win_a

-- Scenario A: HD else EDLP Price vs. Benchmark - Pharmacy Weighting

,case when g.privia_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then p.rx_count_wags else 0 end as rx_count_wags_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then p.rx_count_kr else 0 end as rx_count_kr_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then p.rx_count_sams else 0 end as rx_count_sams_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then p.rx_count_heb else 0 end as rx_count_heb_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then p.rx_count_rand else 0 end as rx_count_rand_cpr_comp_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_comp_a

,case when g.privia_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then p.rx_count_wags else 0 end as rx_count_wags_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then p.rx_count_kr else 0 end as rx_count_kr_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then p.rx_count_sams else 0 end as rx_count_sams_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then p.rx_count_heb else 0 end as rx_count_heb_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then p.rx_count_rand else 0 end as rx_count_rand_cpr_win_a
,case when g.privia_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_win_a


-- Scenario B: HEB else HD else EDLP Price vs. Benchmark - No Weighting
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then 1 else 0 end as gcn_count_wags_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then 1 else 0 end as gcn_count_wmt_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then 1 else 0 end as gcn_count_cvs_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then 1 else 0 end as gcn_count_kr_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then 1 else 0 end as gcn_count_sams_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then 1 else 0 end as gcn_count_cstco_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then 1 else 0 end as gcn_count_heb_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then 1 else 0 end as gcn_count_rand_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 1 else 0 end as gcn_count_min_houston_cpr_comp_b

,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then 1 else 0 end as gcn_count_wags_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then 1 else 0 end as gcn_count_wmt_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then 1 else 0 end as gcn_count_cvs_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then 1 else 0 end as gcn_count_kr_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then 1 else 0 end as gcn_count_sams_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then 1 else 0 end as gcn_count_cstco_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then 1 else 0 end as gcn_count_heb_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then 1 else 0 end as gcn_count_rand_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 1 else 0 end as gcn_count_min_houston_cpr_win_b

,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_comp_b

,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_win_b

-- Scenario B: HEB else HD else EDLP Price vs. Benchmark - Pharmacy Weighting

,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then p.rx_count_wags else 0 end as rx_count_wags_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then p.rx_count_kr else 0 end as rx_count_kr_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then p.rx_count_sams else 0 end as rx_count_sams_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then p.rx_count_heb else 0 end as rx_count_heb_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then p.rx_count_rand else 0 end as rx_count_rand_cpr_comp_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_comp_b

,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then p.rx_count_wags else 0 end as rx_count_wags_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then p.rx_count_kr else 0 end as rx_count_kr_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then p.rx_count_sams else 0 end as rx_count_sams_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then p.rx_count_heb else 0 end as rx_count_heb_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then p.rx_count_rand else 0 end as rx_count_rand_cpr_win_b
,case when g.privia_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_win_b

-- Scenario C: Hou 1pct else HEB else HD else EDLP Price vs. Benchmark - No Weighting
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then 1 else 0 end as gcn_count_wags_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then 1 else 0 end as gcn_count_wmt_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then 1 else 0 end as gcn_count_cvs_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then 1 else 0 end as gcn_count_kr_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then 1 else 0 end as gcn_count_sams_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then 1 else 0 end as gcn_count_cstco_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then 1 else 0 end as gcn_count_heb_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then 1 else 0 end as gcn_count_rand_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 1 else 0 end as gcn_count_min_houston_cpr_comp_c

,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then 1 else 0 end as gcn_count_wags_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then 1 else 0 end as gcn_count_wmt_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then 1 else 0 end as gcn_count_cvs_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then 1 else 0 end as gcn_count_kr_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then 1 else 0 end as gcn_count_sams_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then 1 else 0 end as gcn_count_cstco_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then 1 else 0 end as gcn_count_heb_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then 1 else 0 end as gcn_count_rand_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 1 else 0 end as gcn_count_min_houston_cpr_win_c

,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_comp_c

,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_win_c

-- Scenario C: Hou 1pct else HEB else HD else EDLP Price vs. Benchmark - Pharmacy Weighting

,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then p.rx_count_wags else 0 end as rx_count_wags_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then p.rx_count_kr else 0 end as rx_count_kr_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then p.rx_count_sams else 0 end as rx_count_sams_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then p.rx_count_heb else 0 end as rx_count_heb_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then p.rx_count_rand else 0 end as rx_count_rand_cpr_comp_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_comp_c

,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then p.rx_count_wags else 0 end as rx_count_wags_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then p.rx_count_kr else 0 end as rx_count_kr_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then p.rx_count_sams else 0 end as rx_count_sams_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then p.rx_count_heb else 0 end as rx_count_heb_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then p.rx_count_rand else 0 end as rx_count_rand_cpr_win_c
,case when g.privia_hou_1pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hou_1pct_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_win_c

-- Scenario D: Hou 5pct else HEB else HD else EDLP Price vs. Benchmark - No Weighting
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then 1 else 0 end as gcn_count_wags_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then 1 else 0 end as gcn_count_wmt_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then 1 else 0 end as gcn_count_cvs_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then 1 else 0 end as gcn_count_kr_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then 1 else 0 end as gcn_count_sams_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then 1 else 0 end as gcn_count_cstco_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then 1 else 0 end as gcn_count_heb_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then 1 else 0 end as gcn_count_rand_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 1 else 0 end as gcn_count_min_houston_cpr_comp_d

,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then 1 else 0 end as gcn_count_wags_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then 1 else 0 end as gcn_count_wmt_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then 1 else 0 end as gcn_count_cvs_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then 1 else 0 end as gcn_count_kr_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then 1 else 0 end as gcn_count_sams_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then 1 else 0 end as gcn_count_cstco_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then 1 else 0 end as gcn_count_heb_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then 1 else 0 end as gcn_count_rand_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 1 else 0 end as gcn_count_min_houston_cpr_win_d

,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_comp_d

,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_total else 0 end as full_rx_count_wmt_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then p.rx_count_total else 0 end as full_rx_count_min_houston_cpr_win_d

-- Scenario D: Hou 5pct else HEB else HD else EDLP Price vs. Benchmark - Pharmacy Weighting

,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 then p.rx_count_wags else 0 end as rx_count_wags_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 then p.rx_count_kr else 0 end as rx_count_kr_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 then p.rx_count_sams else 0 end as rx_count_sams_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 then p.rx_count_heb else 0 end as rx_count_heb_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 then p.rx_count_rand else 0 end as rx_count_rand_cpr_comp_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_comp_d

,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wags_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wags_grx,2) then p.rx_count_wags else 0 end as rx_count_wags_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_wmt_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_wmt_grx,2) then p.rx_count_wmt else 0 end as rx_count_wmt_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cvs_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cvs_grx,2) then p.rx_count_cvs else 0 end as rx_count_cvs_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_kr_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_kr_grx,2) then p.rx_count_kr else 0 end as rx_count_kr_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_sams_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_sams_grx,2) then p.rx_count_sams else 0 end as rx_count_sams_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_cstco_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_cstco_grx,2) then p.rx_count_cstco else 0 end as rx_count_cstco_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_heb_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_heb_grx,2) then p.rx_count_heb else 0 end as rx_count_heb_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_rand_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_rand_grx,2) then p.rx_count_rand else 0 end as rx_count_rand_cpr_win_d
,case when g.privia_hou_5pct_heb_else_hd_else_edlp_price > 0 and g.min_houston_grx > 0 and round(g.privia_hou_5pct_heb_else_hd_else_edlp_price,2) <= round(g.min_houston_grx,2) then 
           coalesce(p.rx_count_total,0)-coalesce(p.rx_count_wags,0)-coalesce(p.rx_count_wmt,0)-coalesce(p.rx_count_cvs,0)-coalesce(p.rx_count_kr,0)-coalesce(p.rx_count_sams,0)
           -coalesce(p.rx_count_cstco,0)-coalesce(p.rx_count_heb,0)-coalesce(p.rx_count_rand,0) else 0 end as rx_count_other_pharmacies_cpr_win_d

from privia_scripts p
left join generic_cpr_integrated g on p.fdb_gcn=g.gcn)

-- , integrated_2 as (
-- select *

-- -- Scenario A: Savings Calcs

-- ,case when gcn_count_min_houston_cpr_comp_a then (round(privia_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_comp_a
-- ,case when gcn_count_min_houston_cpr_win_a then (round(privia_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_win_a

-- ,case when gcn_count_wags_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_comp_a
--      ,case when gcn_count_wmt_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_comp_a
--      ,case when gcn_count_cvs_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_comp_a
--      ,case when gcn_count_kr_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_comp_a
--      ,case when gcn_count_sams_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_comp_a 
--      ,case when gcn_count_cstco_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_comp_a
--      ,case when gcn_count_heb_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_comp_a
--      ,case when gcn_count_rand_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_comp_a
--      ,case when gcn_count_min_houston_cpr_comp_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_comp_a

-- ,case when gcn_count_wags_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_win_a 
--      ,case when gcn_count_wmt_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_win_a
--      ,case when gcn_count_cvs_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_win_a
--      ,case when gcn_count_kr_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_win_a
--      ,case when gcn_count_sams_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_win_a
--      ,case when gcn_count_cstco_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_win_a
--      ,case when gcn_count_heb_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_win_a
--      ,case when gcn_count_rand_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_win_a
--      ,case when gcn_count_min_houston_cpr_win_a = 1 then (round(privia_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_win_a

-- ,case when gcn_count_min_houston_cpr_comp_a = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_comp_a
-- ,case when gcn_count_min_houston_cpr_win_a = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_win_a

-- ,case when gcn_count_wags_cpr_comp_a = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_comp_a
--      ,case when gcn_count_wmt_cpr_comp_a = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_comp_a
--      ,case when gcn_count_cvs_cpr_comp_a = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_comp_a
--      ,case when gcn_count_kr_cpr_comp_a = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_comp_a
--      ,case when gcn_count_sams_cpr_comp_a = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_comp_a 
--      ,case when gcn_count_cstco_cpr_comp_a = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_comp_a
--      ,case when gcn_count_heb_cpr_comp_a = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_comp_a
--      ,case when gcn_count_rand_cpr_comp_a = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_comp_a
--      ,case when gcn_count_min_houston_cpr_comp_a = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_comp_a

-- ,case when gcn_count_wags_cpr_win_a = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_win_a 
--      ,case when gcn_count_wmt_cpr_win_a = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_win_a
--      ,case when gcn_count_cvs_cpr_win_a = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_win_a
--      ,case when gcn_count_kr_cpr_win_a = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_win_a
--      ,case when gcn_count_sams_cpr_win_a = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_win_a
--      ,case when gcn_count_cstco_cpr_win_a = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_win_a
--      ,case when gcn_count_heb_cpr_win_a = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_win_a
--      ,case when gcn_count_rand_cpr_win_a = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_win_a
--      ,case when gcn_count_min_houston_cpr_win_a = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_win_a

-- -- Scenario B: Savings Calcs

-- ,case when gcn_count_min_houston_cpr_comp_b then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_comp_b
-- ,case when gcn_count_min_houston_cpr_win_b then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_win_b

-- ,case when gcn_count_wags_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_comp_b
-- ,case when gcn_count_wmt_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_comp_b
--      ,case when gcn_count_cvs_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_comp_b
--      ,case when gcn_count_kr_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_comp_b
--      ,case when gcn_count_sams_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_comp_b 
--      ,case when gcn_count_cstco_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_comp_b
--      ,case when gcn_count_heb_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_comp_b
--      ,case when gcn_count_rand_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_comp_b
--      ,case when gcn_count_min_houston_cpr_comp_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_comp_b

-- ,case when gcn_count_wags_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_win_b 
--      ,case when gcn_count_wmt_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_win_b
--      ,case when gcn_count_cvs_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_win_b
--      ,case when gcn_count_kr_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_win_b
--      ,case when gcn_count_sams_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_win_b
--      ,case when gcn_count_cstco_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_win_b
--      ,case when gcn_count_heb_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_win_b
--      ,case when gcn_count_rand_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_win_b
--      ,case when gcn_count_min_houston_cpr_win_b = 1 then (round(privia_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_win_b

-- ,case when gcn_count_min_houston_cpr_comp_b = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_comp_b
-- ,case when gcn_count_min_houston_cpr_win_b = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_win_b

-- ,case when gcn_count_wags_cpr_comp_b = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_comp_b
--      ,case when gcn_count_wmt_cpr_comp_b = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_comp_b
--      ,case when gcn_count_cvs_cpr_comp_b = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_comp_b
--      ,case when gcn_count_kr_cpr_comp_b = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_comp_b
--      ,case when gcn_count_sams_cpr_comp_b = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_comp_b 
--      ,case when gcn_count_cstco_cpr_comp_b = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_comp_b
--      ,case when gcn_count_heb_cpr_comp_b = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_comp_b
--      ,case when gcn_count_rand_cpr_comp_b = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_comp_b
--      ,case when gcn_count_min_houston_cpr_comp_b = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_comp_b

-- ,case when gcn_count_wags_cpr_win_b = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_win_b 
--      ,case when gcn_count_wmt_cpr_win_b = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_win_b
--      ,case when gcn_count_cvs_cpr_win_b = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_win_b
--      ,case when gcn_count_kr_cpr_win_b = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_win_b
--      ,case when gcn_count_sams_cpr_win_b = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_win_b
--      ,case when gcn_count_cstco_cpr_win_b = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_win_b
--      ,case when gcn_count_heb_cpr_win_b = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_win_b
--      ,case when gcn_count_rand_cpr_win_b = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_win_b
--      ,case when gcn_count_min_houston_cpr_win_b = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_win_b

-- -- Scenario C: Savings Calcs

-- ,case when gcn_count_min_houston_cpr_comp_c then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_comp_c
-- ,case when gcn_count_min_houston_cpr_win_c then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_win_c

-- ,case when gcn_count_wags_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_comp_c
--      ,case when gcn_count_wmt_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_comp_c
--      ,case when gcn_count_cvs_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_comp_c
--      ,case when gcn_count_kr_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_comp_c
--      ,case when gcn_count_sams_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_comp_c 
--      ,case when gcn_count_cstco_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_comp_c
--      ,case when gcn_count_heb_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_comp_c
--      ,case when gcn_count_rand_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_comp_c
--      ,case when gcn_count_min_houston_cpr_comp_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_comp_c

-- ,case when gcn_count_wags_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_win_c 
--      ,case when gcn_count_wmt_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_win_c
--      ,case when gcn_count_cvs_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_win_c
--      ,case when gcn_count_kr_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_win_c
--      ,case when gcn_count_sams_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_win_c
--      ,case when gcn_count_cstco_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_win_c
--      ,case when gcn_count_heb_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_win_c
--      ,case when gcn_count_rand_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_win_c
--      ,case when gcn_count_min_houston_cpr_win_c = 1 then (round(privia_hou_1pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_win_c

-- ,case when gcn_count_min_houston_cpr_comp_c = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_comp_c
-- ,case when gcn_count_min_houston_cpr_win_c = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_win_c

-- ,case when gcn_count_wags_cpr_comp_c = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_comp_c
--      ,case when gcn_count_wmt_cpr_comp_c = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_comp_c
--      ,case when gcn_count_cvs_cpr_comp_c = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_comp_c
--      ,case when gcn_count_kr_cpr_comp_c = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_comp_c
--      ,case when gcn_count_sams_cpr_comp_c = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_comp_c 
--      ,case when gcn_count_cstco_cpr_comp_c = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_comp_c
--      ,case when gcn_count_heb_cpr_comp_c = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_comp_c
--      ,case when gcn_count_rand_cpr_comp_c = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_comp_c
--      ,case when gcn_count_min_houston_cpr_comp_c = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_comp_c

-- ,case when gcn_count_wags_cpr_win_c = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_win_c 
--      ,case when gcn_count_wmt_cpr_win_c = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_win_c
--      ,case when gcn_count_cvs_cpr_win_c = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_win_c
--      ,case when gcn_count_kr_cpr_win_c = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_win_c
--      ,case when gcn_count_sams_cpr_win_c = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_win_c
--      ,case when gcn_count_cstco_cpr_win_c = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_win_c
--      ,case when gcn_count_heb_cpr_win_c = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_win_c
--      ,case when gcn_count_rand_cpr_win_c = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_win_c
--      ,case when gcn_count_min_houston_cpr_win_c = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_win_c

-- -- Scenario D: Savings Calcs

-- ,case when gcn_count_min_houston_cpr_comp_d then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_comp_d
-- ,case when gcn_count_min_houston_cpr_win_d then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_grx_savings_win_d

-- ,case when gcn_count_wags_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_comp_d
--      ,case when gcn_count_wmt_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_comp_d
--      ,case when gcn_count_cvs_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_comp_d
--      ,case when gcn_count_kr_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_comp_d
--      ,case when gcn_count_sams_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_comp_d 
--      ,case when gcn_count_cstco_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_comp_d
--      ,case when gcn_count_heb_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_comp_d
--      ,case when gcn_count_rand_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_comp_d
--      ,case when gcn_count_min_houston_cpr_comp_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_comp_d

-- ,case when gcn_count_wags_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_savings_win_d 
--      ,case when gcn_count_wmt_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_savings_win_d
--      ,case when gcn_count_cvs_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_savings_win_d
--      ,case when gcn_count_kr_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_savings_win_d
--      ,case when gcn_count_sams_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_savings_win_d
--      ,case when gcn_count_cstco_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_savings_win_d
--      ,case when gcn_count_heb_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_savings_win_d
--      ,case when gcn_count_rand_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_savings_win_d
--      ,case when gcn_count_min_houston_cpr_win_d = 1 then (round(privia_hou_5pct_heb_else_hd_else_edlp_price,2)::float - round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_savings_win_d

-- ,case when gcn_count_min_houston_cpr_comp_d = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_comp_d
-- ,case when gcn_count_min_houston_cpr_win_d = 1 then (round(min_houston_grx,2)::float)*rx_count_total::float else 0 end as full_rx_count_houston_cpr_amount_win_d

-- ,case when gcn_count_wags_cpr_comp_d = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_comp_d
--      ,case when gcn_count_wmt_cpr_comp_d = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_comp_d
--      ,case when gcn_count_cvs_cpr_comp_d = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_comp_d
--      ,case when gcn_count_kr_cpr_comp_d = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_comp_d
--      ,case when gcn_count_sams_cpr_comp_d = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_comp_d 
--      ,case when gcn_count_cstco_cpr_comp_d = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_comp_d
--      ,case when gcn_count_heb_cpr_comp_d = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_comp_d
--      ,case when gcn_count_rand_cpr_comp_d = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_comp_d
--      ,case when gcn_count_min_houston_cpr_comp_d = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_comp_d

-- ,case when gcn_count_wags_cpr_win_d = 1 then (round(min_wags_grx,2)::float)*rx_count_wags::float else 0 end as wags_cpr_amount_win_d 
--      ,case when gcn_count_wmt_cpr_win_d = 1 then (round(min_wmt_grx,2)::float)*rx_count_wmt::float else 0 end as wmt_cpr_amount_win_d
--      ,case when gcn_count_cvs_cpr_win_d = 1 then (round(min_cvs_grx,2)::float)*rx_count_cvs::float else 0 end as cvs_cpr_amount_win_d
--      ,case when gcn_count_kr_cpr_win_d = 1 then (round(min_kr_grx,2)::float)*rx_count_kr::float else 0 end as kr_cpr_amount_win_d
--      ,case when gcn_count_sams_cpr_win_d = 1 then (round(min_sams_grx,2)::float)*rx_count_sams::float else 0 end as sams_cpr_amount_win_d
--      ,case when gcn_count_cstco_cpr_win_d = 1 then (round(min_cstco_grx,2)::float)*rx_count_cstco::float else 0 end as cstco_cpr_amount_win_d
--      ,case when gcn_count_heb_cpr_win_d = 1 then (round(min_heb_grx,2)::float)*rx_count_heb::float else 0 end as heb_cpr_amount_win_d
--      ,case when gcn_count_rand_cpr_win_d = 1 then (round(min_rand_grx,2)::float)*rx_count_rand::float else 0 end as rand_cpr_amount_win_d
--      ,case when gcn_count_min_houston_cpr_win_d = 1 then (round(min_houston_grx,2)::float)*rx_count_other_pharmacies::float else 0 end as houston_cpr_amount_win_d

-- from integrated_1)

-- , summary as (
-- select 
-- sum(rx_count_total) as total_rx_count
-- ,sum(rx_quantity_total) as total_rx_quantity
-- -- 
-- -- ,sum(rx_count_wags_cpr_comp+rx_count_wmt_cpr_comp+rx_count_cvs_cpr_comp+rx_count_kr_cpr_comp+rx_count_sams_cpr_comp
-- --     +rx_count_cstco_cpr_comp+rx_count_heb_cpr_comp+rx_count_rand_cpr_comp+rx_count_other_pharmacies_cpr_comp) as total_rx_count_cpr_comp
-- -- 
-- -- ,trunc(sum(rx_count_wags_cpr_comp+rx_count_wmt_cpr_comp+rx_count_cvs_cpr_comp+rx_count_kr_cpr_comp+rx_count_sams_cpr_comp
-- --     +rx_count_cstco_cpr_comp+rx_count_heb_cpr_comp+rx_count_rand_cpr_comp+rx_count_other_pharmacies_cpr_comp)::float
-- -- /sum(rx_count_total)::float,5) as cpr_tracked_and_priced_rate_rx_count
-- -- 
-- -- ,sum(rx_count_wags_cpr_win+rx_count_wmt_cpr_win+rx_count_cvs_cpr_win+rx_count_kr_cpr_win+rx_count_sams_cpr_win
-- --     +rx_count_cstco_cpr_win+rx_count_heb_cpr_win+rx_count_rand_cpr_win+rx_count_other_pharmacies_cpr_win) as total_rx_count_cpr_win
-- -- 
-- -- ,trunc(sum(rx_count_wags_cpr_win+rx_count_wmt_cpr_win+rx_count_cvs_cpr_win+rx_count_kr_cpr_win+rx_count_sams_cpr_win
-- --     +rx_count_cstco_cpr_win+rx_count_heb_cpr_win+rx_count_rand_cpr_win+rx_count_other_pharmacies_cpr_win)::float
-- -- /sum(rx_count_wags_cpr_comp+rx_count_wmt_cpr_comp+rx_count_cvs_cpr_comp+rx_count_kr_cpr_comp+rx_count_sams_cpr_comp
-- --     +rx_count_cstco_cpr_comp+rx_count_heb_cpr_comp+rx_count_rand_cpr_comp+rx_count_other_pharmacies_cpr_comp)::float,5) as cpr_win_rate_rx_count

-- -- Scenario A: Summary

-- ,sum(full_rx_count_wmt_cpr_comp_a) as full_rx_count_wmt_cpr_comp_a
-- ,trunc(sum(full_rx_count_wmt_cpr_comp_a)::float/sum(rx_count_total)::float,5) as wmt_cpr_tracked_and_priced_rate_rx_count_a
-- ,sum(full_rx_count_wmt_cpr_win_a) as full_rx_count_wmt_cpr_win_a
-- ,trunc(sum(full_rx_count_wmt_cpr_win_a)::float/sum(full_rx_count_wmt_cpr_comp_a)::float,5) as full_rx_count_wmt_cpr_win_rate_a

-- ,sum(full_rx_count_min_houston_cpr_comp_a) as full_rx_count_min_houston_cpr_comp_a
-- ,trunc(sum(full_rx_count_min_houston_cpr_comp_a)::float/sum(rx_count_total)::float,5) as houston_cpr_tracked_and_priced_rate_rx_count_a
-- ,sum(full_rx_count_min_houston_cpr_win_a) as full_rx_count_min_houston_cpr_win_a
-- ,trunc(sum(full_rx_count_min_houston_cpr_win_a)::float/sum(full_rx_count_min_houston_cpr_comp_a)::float,5) as full_rx_count_houston_cpr_win_rate_a

-- ,sum(rx_count_wags_cpr_comp_a+rx_count_wmt_cpr_comp_a+rx_count_cvs_cpr_comp_a+rx_count_kr_cpr_comp_a+rx_count_sams_cpr_comp_a
--     +rx_count_cstco_cpr_comp_a+rx_count_heb_cpr_comp_a+rx_count_rand_cpr_comp_a+rx_count_other_pharmacies_cpr_comp_a) as total_rx_count_cpr_comp_a

-- ,trunc(sum(rx_count_wags_cpr_comp_a+rx_count_wmt_cpr_comp_a+rx_count_cvs_cpr_comp_a+rx_count_kr_cpr_comp_a+rx_count_sams_cpr_comp_a
--     +rx_count_cstco_cpr_comp_a+rx_count_heb_cpr_comp_a+rx_count_rand_cpr_comp_a+rx_count_other_pharmacies_cpr_comp_a)::float
-- /sum(rx_count_total)::float,5) as cpr_tracked_and_priced_rate_rx_count_a

-- ,sum(rx_count_wags_cpr_win_a+rx_count_wmt_cpr_win_a+rx_count_cvs_cpr_win_a+rx_count_kr_cpr_win_a+rx_count_sams_cpr_win_a
--     +rx_count_cstco_cpr_win_a+rx_count_heb_cpr_win_a+rx_count_rand_cpr_win_a+rx_count_other_pharmacies_cpr_win_a) as total_rx_count_cpr_win_a

-- ,trunc(sum(rx_count_wags_cpr_win_a+rx_count_wmt_cpr_win_a+rx_count_cvs_cpr_win_a+rx_count_kr_cpr_win_a+rx_count_sams_cpr_win_a
--     +rx_count_cstco_cpr_win_a+rx_count_heb_cpr_win_a+rx_count_rand_cpr_win_a+rx_count_other_pharmacies_cpr_win_a)::float
-- /sum(rx_count_wags_cpr_comp_a+rx_count_wmt_cpr_comp_a+rx_count_cvs_cpr_comp_a+rx_count_kr_cpr_comp_a+rx_count_sams_cpr_comp_a
--     +rx_count_cstco_cpr_comp_a+rx_count_heb_cpr_comp_a+rx_count_rand_cpr_comp_a+rx_count_other_pharmacies_cpr_comp_a)::float,5) as cpr_win_rate_rx_count_a

-- -- Scenario A: Savings

-- ,sum(full_rx_count_houston_grx_savings_comp_a)::float/sum(full_rx_count_houston_cpr_amount_comp_a)::float as full_rx_count_houston_savings_pct_comp_a
-- ,sum(full_rx_count_houston_grx_savings_win_a)::float/sum(full_rx_count_houston_cpr_amount_win_a)::float as full_rx_count_houston_savings_pct_win_a

-- ,sum(full_rx_count_houston_grx_savings_comp_a)::float/sum(full_rx_count_min_houston_cpr_comp_a)::float as full_rx_count_houston_cpr_savings_per_script_comp_a
-- ,sum(full_rx_count_houston_grx_savings_win_a)::float/sum(full_rx_count_min_houston_cpr_win_a)::float as full_rx_count_houston_cpr_savings_per_script_win_a

-- ,sum(wags_cpr_amount_comp_a::float+wmt_cpr_amount_comp_a::float+cvs_cpr_amount_comp_a::float+kr_cpr_amount_comp_a::float
--     +sams_cpr_amount_comp_a::float+cstco_cpr_amount_comp_a::float+heb_cpr_amount_comp_a::float+rand_cpr_amount_comp_a::float+houston_cpr_amount_comp_a::float) as pharmacy_weighted_cpr_amount_comp_a

-- ,sum(wags_savings_comp_a+wmt_savings_comp_a::float+cvs_savings_comp_a::float+kr_savings_comp_a::float
--      +sams_savings_comp_a::float+cstco_savings_comp_a::float+heb_savings_comp_a::float+rand_savings_comp_a::float+houston_savings_comp_a::float)::float as pharmacy_weighted_savings_comp_a

-- ,sum(wags_savings_comp_a+wmt_savings_comp_a::float+cvs_savings_comp_a::float+kr_savings_comp_a::float
--      +sams_savings_comp_a::float+cstco_savings_comp_a::float+heb_savings_comp_a::float+rand_savings_comp_a::float+houston_savings_comp_a::float)::float
-- /sum(wags_cpr_amount_comp_a::float+wmt_cpr_amount_comp_a::float+cvs_cpr_amount_comp_a::float+kr_cpr_amount_comp_a::float
--     +sams_cpr_amount_comp_a::float+cstco_cpr_amount_comp_a::float+heb_cpr_amount_comp_a::float+rand_cpr_amount_comp_a::float+houston_cpr_amount_comp_a::float)::float as pharmacy_weighted_savings_pct_comp_a

-- ,sum(wags_cpr_amount_win_a::float+wmt_cpr_amount_win_a::float+cvs_cpr_amount_win_a::float+kr_cpr_amount_win_a::float
--     +sams_cpr_amount_win_a::float+cstco_cpr_amount_win_a::float+heb_cpr_amount_win_a::float+rand_cpr_amount_win_a::float+houston_cpr_amount_win_a::float) as pharmacy_weighted_cpr_amount_win_a

-- ,sum(rx_count_wags_cpr_win_a+rx_count_wmt_cpr_win_a+rx_count_cvs_cpr_win_a+rx_count_kr_cpr_win_a+rx_count_sams_cpr_win_a
--     +rx_count_cstco_cpr_win_a+rx_count_heb_cpr_win_a+rx_count_rand_cpr_win_a+rx_count_other_pharmacies_cpr_win_a)::float as pharmacy_weighted_savings_win_a

-- ,sum(wags_savings_win_a+wmt_savings_win_a::float+cvs_savings_win_a::float+kr_savings_win_a::float
--      +sams_savings_win_a::float+cstco_savings_win_a::float+heb_savings_win_a::float+rand_savings_win_a::float+houston_savings_win_a::float)::float
-- /sum(wags_cpr_amount_win_a::float+wmt_cpr_amount_win_a::float+cvs_cpr_amount_win_a::float+kr_cpr_amount_win_a::float
--     +sams_cpr_amount_win_a::float+cstco_cpr_amount_win_a::float+heb_cpr_amount_win_a::float+rand_cpr_amount_win_a::float+houston_cpr_amount_win_a::float)::float as pharmacy_weighted_savings_pct_win_a

-- ,sum(wags_savings_comp_a+wmt_savings_comp_a::float+cvs_savings_comp_a::float+kr_savings_comp_a::float
--      +sams_savings_comp_a::float+cstco_savings_comp_a::float+heb_savings_comp_a::float+rand_savings_comp_a::float+houston_savings_comp_a::float)::float/
--  sum(rx_count_wags_cpr_comp_a+rx_count_wmt_cpr_comp_a+rx_count_cvs_cpr_comp_a+rx_count_kr_cpr_comp_a+rx_count_sams_cpr_comp_a
--     +rx_count_cstco_cpr_comp_a+rx_count_heb_cpr_comp_a+rx_count_rand_cpr_comp_a+rx_count_other_pharmacies_cpr_comp_a)::float as pharmacy_weighted_savings_per_script_comp_a

-- ,sum(wags_savings_win_a+wmt_savings_win_a::float+cvs_savings_win_a::float+kr_savings_win_a::float
--      +sams_savings_win_a::float+cstco_savings_win_a::float+heb_savings_win_a::float+rand_savings_win_a::float+houston_savings_win_a::float)::float/
--  sum(rx_count_wags_cpr_win_a+rx_count_wmt_cpr_win_a+rx_count_cvs_cpr_win_a+rx_count_kr_cpr_win_a+rx_count_sams_cpr_win_a
--     +rx_count_cstco_cpr_win_a+rx_count_heb_cpr_win_a+rx_count_rand_cpr_win_a+rx_count_other_pharmacies_cpr_win_a)::float as pharmacy_weighted_savings_per_script_win_a


-- -- Scenario B: Summary

-- ,sum(full_rx_count_wmt_cpr_comp_b) as full_rx_count_wmt_cpr_comp_b
-- ,trunc(sum(full_rx_count_wmt_cpr_comp_b)::float/sum(rx_count_total)::float,5) as wmt_cpr_tracked_and_priced_rate_rx_count_b
-- ,sum(full_rx_count_wmt_cpr_win_b) as full_rx_count_wmt_cpr_win_b
-- ,trunc(sum(full_rx_count_wmt_cpr_win_b)::float/sum(full_rx_count_wmt_cpr_comp_b)::float,5) as full_rx_count_wmt_cpr_win_rate_b

-- ,sum(full_rx_count_min_houston_cpr_comp_b) as full_rx_count_min_houston_cpr_comp_b
-- ,trunc(sum(full_rx_count_min_houston_cpr_comp_b)::float/sum(rx_count_total)::float,5) as houston_cpr_tracked_and_priced_rate_rx_count_b
-- ,sum(full_rx_count_min_houston_cpr_win_b) as full_rx_count_min_houston_cpr_win_b
-- ,trunc(sum(full_rx_count_min_houston_cpr_win_b)::float/sum(full_rx_count_min_houston_cpr_comp_b)::float,5) as full_rx_count_houston_cpr_win_rate_b


-- ,sum(rx_count_wags_cpr_comp_b+rx_count_wmt_cpr_comp_b+rx_count_cvs_cpr_comp_b+rx_count_kr_cpr_comp_b+rx_count_sams_cpr_comp_b
--     +rx_count_cstco_cpr_comp_b+rx_count_heb_cpr_comp_b+rx_count_rand_cpr_comp_b+rx_count_other_pharmacies_cpr_comp_b) as total_rx_count_cpr_comp_b

-- ,trunc(sum(rx_count_wags_cpr_comp_b+rx_count_wmt_cpr_comp_b+rx_count_cvs_cpr_comp_b+rx_count_kr_cpr_comp_b+rx_count_sams_cpr_comp_b
--     +rx_count_cstco_cpr_comp_b+rx_count_heb_cpr_comp_b+rx_count_rand_cpr_comp_b+rx_count_other_pharmacies_cpr_comp_b)::float
-- /sum(rx_count_total)::float,5) as cpr_tracked_bnd_priced_rate_rx_count_b

-- ,sum(rx_count_wags_cpr_win_b+rx_count_wmt_cpr_win_b+rx_count_cvs_cpr_win_b+rx_count_kr_cpr_win_b+rx_count_sams_cpr_win_b
--     +rx_count_cstco_cpr_win_b+rx_count_heb_cpr_win_b+rx_count_rand_cpr_win_b+rx_count_other_pharmacies_cpr_win_b) as total_rx_count_cpr_win_b

-- ,trunc(sum(rx_count_wags_cpr_win_b+rx_count_wmt_cpr_win_b+rx_count_cvs_cpr_win_b+rx_count_kr_cpr_win_b+rx_count_sams_cpr_win_b
--     +rx_count_cstco_cpr_win_b+rx_count_heb_cpr_win_b+rx_count_rand_cpr_win_b+rx_count_other_pharmacies_cpr_win_b)::float
-- /sum(rx_count_wags_cpr_comp_b+rx_count_wmt_cpr_comp_b+rx_count_cvs_cpr_comp_b+rx_count_kr_cpr_comp_b+rx_count_sams_cpr_comp_b
--     +rx_count_cstco_cpr_comp_b+rx_count_heb_cpr_comp_b+rx_count_rand_cpr_comp_b+rx_count_other_pharmacies_cpr_comp_b)::float,5) as cpr_win_rate_rx_count_b

-- -- Scenario B: Savings

-- ,sum(full_rx_count_houston_grx_savings_comp_b)::float/sum(full_rx_count_houston_cpr_amount_comp_b)::float as full_rx_count_houston_savings_pct_comp_b
-- ,sum(full_rx_count_houston_grx_savings_win_b)::float/sum(full_rx_count_houston_cpr_amount_win_b)::float as full_rx_count_houston_savings_pct_win_b

-- ,sum(full_rx_count_houston_grx_savings_comp_b)::float/sum(full_rx_count_min_houston_cpr_comp_b)::float as full_rx_count_houston_cpr_savings_per_script_comp_b
-- ,sum(full_rx_count_houston_grx_savings_win_b)::float/sum(full_rx_count_min_houston_cpr_win_b)::float as full_rx_count_houston_cpr_savings_per_script_win_b

-- ,sum(wags_cpr_amount_comp_b::float+wmt_cpr_amount_comp_b::float+cvs_cpr_amount_comp_b::float+kr_cpr_amount_comp_b::float
--     +sams_cpr_amount_comp_b::float+cstco_cpr_amount_comp_b::float+heb_cpr_amount_comp_b::float+rand_cpr_amount_comp_b::float+houston_cpr_amount_comp_b::float) as pharmacy_weighted_cpr_amount_comp_b

-- ,sum(wags_savings_comp_b+wmt_savings_comp_b::float+cvs_savings_comp_b::float+kr_savings_comp_b::float
--      +sams_savings_comp_b::float+cstco_savings_comp_b::float+heb_savings_comp_b::float+rand_savings_comp_b::float+houston_savings_comp_b::float)::float as pharmacy_weighted_savings_comp_b

-- ,sum(wags_savings_comp_b+wmt_savings_comp_b::float+cvs_savings_comp_b::float+kr_savings_comp_b::float
--      +sams_savings_comp_b::float+cstco_savings_comp_b::float+heb_savings_comp_b::float+rand_savings_comp_b::float+houston_savings_comp_b::float)::float
-- /sum(wags_cpr_amount_comp_b::float+wmt_cpr_amount_comp_b::float+cvs_cpr_amount_comp_b::float+kr_cpr_amount_comp_b::float
--     +sams_cpr_amount_comp_b::float+cstco_cpr_amount_comp_b::float+heb_cpr_amount_comp_b::float+rand_cpr_amount_comp_b::float+houston_cpr_amount_comp_b::float)::float as pharmacy_weighted_savings_pct_comp_b

-- ,sum(wags_cpr_amount_win_b::float+wmt_cpr_amount_win_b::float+cvs_cpr_amount_win_b::float+kr_cpr_amount_win_b::float
--     +sams_cpr_amount_win_b::float+cstco_cpr_amount_win_b::float+heb_cpr_amount_win_b::float+rand_cpr_amount_win_b::float+houston_cpr_amount_win_b::float) as pharmacy_weighted_cpr_amount_win_b

-- ,sum(rx_count_wags_cpr_win_b+rx_count_wmt_cpr_win_b+rx_count_cvs_cpr_win_b+rx_count_kr_cpr_win_b+rx_count_sams_cpr_win_b
--     +rx_count_cstco_cpr_win_b+rx_count_heb_cpr_win_b+rx_count_rand_cpr_win_b+rx_count_other_pharmacies_cpr_win_b)::float as pharmacy_weighted_savings_win_b

-- ,sum(wags_savings_win_b+wmt_savings_win_b::float+cvs_savings_win_b::float+kr_savings_win_b::float
--      +sams_savings_win_b::float+cstco_savings_win_b::float+heb_savings_win_b::float+rand_savings_win_b::float+houston_savings_win_b::float)::float
-- /sum(wags_cpr_amount_win_b::float+wmt_cpr_amount_win_b::float+cvs_cpr_amount_win_b::float+kr_cpr_amount_win_b::float
--     +sams_cpr_amount_win_b::float+cstco_cpr_amount_win_b::float+heb_cpr_amount_win_b::float+rand_cpr_amount_win_b::float+houston_cpr_amount_win_b::float)::float as pharmacy_weighted_savings_pct_win_b

-- ,sum(wags_savings_comp_b+wmt_savings_comp_b::float+cvs_savings_comp_b::float+kr_savings_comp_b::float
--      +sams_savings_comp_b::float+cstco_savings_comp_b::float+heb_savings_comp_b::float+rand_savings_comp_b::float+houston_savings_comp_b::float)::float/
--  sum(rx_count_wags_cpr_comp_b+rx_count_wmt_cpr_comp_b+rx_count_cvs_cpr_comp_b+rx_count_kr_cpr_comp_b+rx_count_sams_cpr_comp_b
--     +rx_count_cstco_cpr_comp_b+rx_count_heb_cpr_comp_b+rx_count_rand_cpr_comp_b+rx_count_other_pharmacies_cpr_comp_b)::float as pharmacy_weighted_savings_per_script_comp_b

-- ,sum(wags_savings_win_b+wmt_savings_win_b::float+cvs_savings_win_b::float+kr_savings_win_b::float
--      +sams_savings_win_b::float+cstco_savings_win_b::float+heb_savings_win_b::float+rand_savings_win_b::float+houston_savings_win_b::float)::float/
--  sum(rx_count_wags_cpr_win_b+rx_count_wmt_cpr_win_b+rx_count_cvs_cpr_win_b+rx_count_kr_cpr_win_b+rx_count_sams_cpr_win_b
--     +rx_count_cstco_cpr_win_b+rx_count_heb_cpr_win_b+rx_count_rand_cpr_win_b+rx_count_other_pharmacies_cpr_win_b)::float as pharmacy_weighted_savings_per_script_win_b

-- -- Scenario C: Summary

-- ,sum(full_rx_count_wmt_cpr_comp_c) as full_rx_count_wmt_cpr_comp_c
-- ,trunc(sum(full_rx_count_wmt_cpr_comp_c)::float/sum(rx_count_total)::float,5) as wmt_cpr_tracked_and_priced_rate_rx_count_c
-- ,sum(full_rx_count_wmt_cpr_win_c) as full_rx_count_wmt_cpr_win_c
-- ,trunc(sum(full_rx_count_wmt_cpr_win_c)::float/sum(full_rx_count_wmt_cpr_comp_c)::float,5) as full_rx_count_wmt_cpr_win_rate_c

-- ,sum(full_rx_count_min_houston_cpr_comp_c) as full_rx_count_min_houston_cpr_comp_c
-- ,trunc(sum(full_rx_count_min_houston_cpr_comp_c)::float/sum(rx_count_total)::float,5) as houston_cpr_tracked_and_priced_rate_rx_count_c
-- ,sum(full_rx_count_min_houston_cpr_win_c) as full_rx_count_min_houston_cpr_win_c
-- ,trunc(sum(full_rx_count_min_houston_cpr_win_c)::float/sum(full_rx_count_min_houston_cpr_comp_c)::float,5) as full_rx_count_houston_cpr_win_rate_c


-- ,sum(rx_count_wags_cpr_comp_c+rx_count_wmt_cpr_comp_c+rx_count_cvs_cpr_comp_c+rx_count_kr_cpr_comp_c+rx_count_sams_cpr_comp_c
--     +rx_count_cstco_cpr_comp_c+rx_count_heb_cpr_comp_c+rx_count_rand_cpr_comp_c+rx_count_other_pharmacies_cpr_comp_c) as total_rx_count_cpr_comp_c

-- ,trunc(sum(rx_count_wags_cpr_comp_c+rx_count_wmt_cpr_comp_c+rx_count_cvs_cpr_comp_c+rx_count_kr_cpr_comp_c+rx_count_sams_cpr_comp_c
--     +rx_count_cstco_cpr_comp_c+rx_count_heb_cpr_comp_c+rx_count_rand_cpr_comp_c+rx_count_other_pharmacies_cpr_comp_c)::float
-- /sum(rx_count_total)::float,5) as cpr_tracked_and_priced_rate_rx_count_c

-- ,sum(rx_count_wags_cpr_win_c+rx_count_wmt_cpr_win_c+rx_count_cvs_cpr_win_c+rx_count_kr_cpr_win_c+rx_count_sams_cpr_win_c
--     +rx_count_cstco_cpr_win_c+rx_count_heb_cpr_win_c+rx_count_rand_cpr_win_c+rx_count_other_pharmacies_cpr_win_c) as total_rx_count_cpr_win_c

-- ,trunc(sum(rx_count_wags_cpr_win_c+rx_count_wmt_cpr_win_c+rx_count_cvs_cpr_win_c+rx_count_kr_cpr_win_c+rx_count_sams_cpr_win_c
--     +rx_count_cstco_cpr_win_c+rx_count_heb_cpr_win_c+rx_count_rand_cpr_win_c+rx_count_other_pharmacies_cpr_win_c)::float
-- /sum(rx_count_wags_cpr_comp_c+rx_count_wmt_cpr_comp_c+rx_count_cvs_cpr_comp_c+rx_count_kr_cpr_comp_c+rx_count_sams_cpr_comp_c
--     +rx_count_cstco_cpr_comp_c+rx_count_heb_cpr_comp_c+rx_count_rand_cpr_comp_c+rx_count_other_pharmacies_cpr_comp_c)::float,5) as cpr_win_rate_rx_count_c

-- -- Scenario C: Savings

-- ,sum(full_rx_count_houston_grx_savings_comp_c)::float/sum(full_rx_count_houston_cpr_amount_comp_c)::float as full_rx_count_houston_savings_pct_comp_c
-- ,sum(full_rx_count_houston_grx_savings_win_c)::float/sum(full_rx_count_houston_cpr_amount_win_c)::float as full_rx_count_houston_savings_pct_win_c

-- ,sum(full_rx_count_houston_grx_savings_comp_c)::float/sum(full_rx_count_min_houston_cpr_comp_c)::float as full_rx_count_houston_cpr_savings_per_script_comp_c
-- ,sum(full_rx_count_houston_grx_savings_win_c)::float/sum(full_rx_count_min_houston_cpr_win_c)::float as full_rx_count_houston_cpr_savings_per_script_win_c

-- ,sum(wags_cpr_amount_comp_c::float+wmt_cpr_amount_comp_c::float+cvs_cpr_amount_comp_c::float+kr_cpr_amount_comp_c::float
--     +sams_cpr_amount_comp_c::float+cstco_cpr_amount_comp_c::float+heb_cpr_amount_comp_c::float+rand_cpr_amount_comp_c::float+houston_cpr_amount_comp_c::float) as pharmacy_weighted_cpr_amount_comp_c

-- ,sum(wags_savings_comp_c+wmt_savings_comp_c::float+cvs_savings_comp_c::float+kr_savings_comp_c::float
--      +sams_savings_comp_c::float+cstco_savings_comp_c::float+heb_savings_comp_c::float+rand_savings_comp_c::float+houston_savings_comp_c::float)::float as pharmacy_weighted_savings_comp_c

-- ,sum(wags_savings_comp_c+wmt_savings_comp_c::float+cvs_savings_comp_c::float+kr_savings_comp_c::float
--      +sams_savings_comp_c::float+cstco_savings_comp_c::float+heb_savings_comp_c::float+rand_savings_comp_c::float+houston_savings_comp_c::float)::float
-- /sum(wags_cpr_amount_comp_c::float+wmt_cpr_amount_comp_c::float+cvs_cpr_amount_comp_c::float+kr_cpr_amount_comp_c::float
--     +sams_cpr_amount_comp_c::float+cstco_cpr_amount_comp_c::float+heb_cpr_amount_comp_c::float+rand_cpr_amount_comp_c::float+houston_cpr_amount_comp_c::float)::float as pharmacy_weighted_savings_pct_comp_c

-- ,sum(wags_cpr_amount_win_c::float+wmt_cpr_amount_win_c::float+cvs_cpr_amount_win_c::float+kr_cpr_amount_win_c::float
--     +sams_cpr_amount_win_c::float+cstco_cpr_amount_win_c::float+heb_cpr_amount_win_c::float+rand_cpr_amount_win_c::float+houston_cpr_amount_win_c::float) as pharmacy_weighted_cpr_amount_win_c

-- ,sum(rx_count_wags_cpr_win_c+rx_count_wmt_cpr_win_c+rx_count_cvs_cpr_win_c+rx_count_kr_cpr_win_c+rx_count_sams_cpr_win_c
--     +rx_count_cstco_cpr_win_c+rx_count_heb_cpr_win_c+rx_count_rand_cpr_win_c+rx_count_other_pharmacies_cpr_win_c)::float as pharmacy_weighted_savings_win_c

-- ,sum(wags_savings_win_c+wmt_savings_win_c::float+cvs_savings_win_c::float+kr_savings_win_c::float
--      +sams_savings_win_c::float+cstco_savings_win_c::float+heb_savings_win_c::float+rand_savings_win_c::float+houston_savings_win_c::float)::float
-- /sum(wags_cpr_amount_win_c::float+wmt_cpr_amount_win_c::float+cvs_cpr_amount_win_c::float+kr_cpr_amount_win_c::float
--     +sams_cpr_amount_win_c::float+cstco_cpr_amount_win_c::float+heb_cpr_amount_win_c::float+rand_cpr_amount_win_c::float+houston_cpr_amount_win_c::float)::float as pharmacy_weighted_savings_pct_win_c

-- ,sum(wags_savings_comp_c+wmt_savings_comp_c::float+cvs_savings_comp_c::float+kr_savings_comp_c::float
--      +sams_savings_comp_c::float+cstco_savings_comp_c::float+heb_savings_comp_c::float+rand_savings_comp_c::float+houston_savings_comp_c::float)::float/
--  sum(rx_count_wags_cpr_comp_c+rx_count_wmt_cpr_comp_c+rx_count_cvs_cpr_comp_c+rx_count_kr_cpr_comp_c+rx_count_sams_cpr_comp_c
--     +rx_count_cstco_cpr_comp_c+rx_count_heb_cpr_comp_c+rx_count_rand_cpr_comp_c+rx_count_other_pharmacies_cpr_comp_c)::float as pharmacy_weighted_savings_per_script_comp_c

-- ,sum(wags_savings_win_c+wmt_savings_win_c::float+cvs_savings_win_c::float+kr_savings_win_c::float
--      +sams_savings_win_c::float+cstco_savings_win_c::float+heb_savings_win_c::float+rand_savings_win_c::float+houston_savings_win_c::float)::float/
--  sum(rx_count_wags_cpr_win_c+rx_count_wmt_cpr_win_c+rx_count_cvs_cpr_win_c+rx_count_kr_cpr_win_c+rx_count_sams_cpr_win_c
--     +rx_count_cstco_cpr_win_c+rx_count_heb_cpr_win_c+rx_count_rand_cpr_win_c+rx_count_other_pharmacies_cpr_win_c)::float as pharmacy_weighted_savings_per_script_win_c

-- -- Scenario D: Summary

-- ,sum(full_rx_count_wmt_cpr_comp_d) as full_rx_count_wmt_cpr_comp_d
-- ,trunc(sum(full_rx_count_wmt_cpr_comp_d)::float/sum(rx_count_total)::float,5) as wmt_cpr_tracked_and_priced_rate_rx_count_d
-- ,sum(full_rx_count_wmt_cpr_win_d) as full_rx_count_wmt_cpr_win_d
-- ,trunc(sum(full_rx_count_wmt_cpr_win_d)::float/sum(full_rx_count_wmt_cpr_comp_d)::float,5) as full_rx_count_wmt_cpr_win_rate_d

-- ,sum(full_rx_count_min_houston_cpr_comp_d) as full_rx_count_min_houston_cpr_comp_d
-- ,trunc(sum(full_rx_count_min_houston_cpr_comp_d)::float/sum(rx_count_total)::float,5) as houston_cpr_tracked_and_priced_rate_rx_count_d
-- ,sum(full_rx_count_min_houston_cpr_win_d) as full_rx_count_min_houston_cpr_win_d
-- ,trunc(sum(full_rx_count_min_houston_cpr_win_d)::float/sum(full_rx_count_min_houston_cpr_comp_d)::float,5) as full_rx_count_houston_cpr_win_rate_d


-- ,sum(rx_count_wags_cpr_comp_d+rx_count_wmt_cpr_comp_d+rx_count_cvs_cpr_comp_d+rx_count_kr_cpr_comp_d+rx_count_sams_cpr_comp_d
--     +rx_count_cstco_cpr_comp_d+rx_count_heb_cpr_comp_d+rx_count_rand_cpr_comp_d+rx_count_other_pharmacies_cpr_comp_d) as total_rx_count_cpr_comp_d

-- ,trunc(sum(rx_count_wags_cpr_comp_d+rx_count_wmt_cpr_comp_d+rx_count_cvs_cpr_comp_d+rx_count_kr_cpr_comp_d+rx_count_sams_cpr_comp_d
--     +rx_count_cstco_cpr_comp_d+rx_count_heb_cpr_comp_d+rx_count_rand_cpr_comp_d+rx_count_other_pharmacies_cpr_comp_d)::float
-- /sum(rx_count_total)::float,5) as cpr_tracked_and_priced_rate_rx_count_c

-- ,sum(rx_count_wags_cpr_win_d+rx_count_wmt_cpr_win_d+rx_count_cvs_cpr_win_d+rx_count_kr_cpr_win_d+rx_count_sams_cpr_win_d
--     +rx_count_cstco_cpr_win_d+rx_count_heb_cpr_win_d+rx_count_rand_cpr_win_d+rx_count_other_pharmacies_cpr_win_d) as total_rx_count_cpr_win_d

-- ,trunc(sum(rx_count_wags_cpr_win_d+rx_count_wmt_cpr_win_d+rx_count_cvs_cpr_win_d+rx_count_kr_cpr_win_d+rx_count_sams_cpr_win_d
--     +rx_count_cstco_cpr_win_d+rx_count_heb_cpr_win_d+rx_count_rand_cpr_win_d+rx_count_other_pharmacies_cpr_win_d)::float
-- /sum(rx_count_wags_cpr_comp_d+rx_count_wmt_cpr_comp_d+rx_count_cvs_cpr_comp_d+rx_count_kr_cpr_comp_d+rx_count_sams_cpr_comp_d
--     +rx_count_cstco_cpr_comp_d+rx_count_heb_cpr_comp_d+rx_count_rand_cpr_comp_d+rx_count_other_pharmacies_cpr_comp_d)::float,5) as cpr_win_rate_rx_count_d

-- -- Scenario D: Savings

-- ,sum(full_rx_count_houston_grx_savings_comp_d)::float/sum(full_rx_count_houston_cpr_amount_comp_d)::float as full_rx_count_houston_savings_pct_comp_d
-- ,sum(full_rx_count_houston_grx_savings_win_d)::float/sum(full_rx_count_houston_cpr_amount_win_d)::float as full_rx_count_houston_savings_pct_win_d

-- ,sum(full_rx_count_houston_grx_savings_comp_d)::float/sum(full_rx_count_min_houston_cpr_comp_d)::float as full_rx_count_houston_cpr_savings_per_script_comp_d
-- ,sum(full_rx_count_houston_grx_savings_win_d)::float/sum(full_rx_count_min_houston_cpr_win_d)::float as full_rx_count_houston_cpr_savings_per_script_win_d

-- ,sum(wags_cpr_amount_comp_d::float+wmt_cpr_amount_comp_d::float+cvs_cpr_amount_comp_d::float+kr_cpr_amount_comp_d::float
--     +sams_cpr_amount_comp_d::float+cstco_cpr_amount_comp_d::float+heb_cpr_amount_comp_d::float+rand_cpr_amount_comp_d::float+houston_cpr_amount_comp_d::float) as pharmacy_weighted_cpr_amount_comp_d

-- ,sum(wags_savings_comp_d+wmt_savings_comp_d::float+cvs_savings_comp_d::float+kr_savings_comp_d::float
--      +sams_savings_comp_d::float+cstco_savings_comp_d::float+heb_savings_comp_d::float+rand_savings_comp_d::float+houston_savings_comp_d::float)::float as pharmacy_weighted_savings_comp_d

-- ,sum(wags_savings_comp_d+wmt_savings_comp_d::float+cvs_savings_comp_d::float+kr_savings_comp_d::float
--      +sams_savings_comp_d::float+cstco_savings_comp_d::float+heb_savings_comp_d::float+rand_savings_comp_d::float+houston_savings_comp_d::float)::float
-- /sum(wags_cpr_amount_comp_d::float+wmt_cpr_amount_comp_d::float+cvs_cpr_amount_comp_d::float+kr_cpr_amount_comp_d::float
--     +sams_cpr_amount_comp_d::float+cstco_cpr_amount_comp_d::float+heb_cpr_amount_comp_d::float+rand_cpr_amount_comp_d::float+houston_cpr_amount_comp_d::float)::float as pharmacy_weighted_savings_pct_comp_d

-- ,sum(wags_cpr_amount_win_d::float+wmt_cpr_amount_win_d::float+cvs_cpr_amount_win_d::float+kr_cpr_amount_win_d::float
--     +sams_cpr_amount_win_d::float+cstco_cpr_amount_win_d::float+heb_cpr_amount_win_d::float+rand_cpr_amount_win_d::float+houston_cpr_amount_win_d::float) as pharmacy_weighted_cpr_amount_win_d

-- ,sum(rx_count_wags_cpr_win_d+rx_count_wmt_cpr_win_d+rx_count_cvs_cpr_win_d+rx_count_kr_cpr_win_d+rx_count_sams_cpr_win_d
--     +rx_count_cstco_cpr_win_d+rx_count_heb_cpr_win_d+rx_count_rand_cpr_win_d+rx_count_other_pharmacies_cpr_win_d)::float as pharmacy_weighted_savings_win_d

-- ,sum(wags_savings_win_d+wmt_savings_win_d::float+cvs_savings_win_d::float+kr_savings_win_d::float
--      +sams_savings_win_d::float+cstco_savings_win_d::float+heb_savings_win_d::float+rand_savings_win_d::float+houston_savings_win_d::float)::float
-- /sum(wags_cpr_amount_win_d::float+wmt_cpr_amount_win_d::float+cvs_cpr_amount_win_d::float+kr_cpr_amount_win_d::float
--     +sams_cpr_amount_win_d::float+cstco_cpr_amount_win_d::float+heb_cpr_amount_win_d::float+rand_cpr_amount_win_d::float+houston_cpr_amount_win_d::float)::float as pharmacy_weighted_savings_pct_win_d

-- ,sum(wags_savings_comp_d+wmt_savings_comp_d::float+cvs_savings_comp_d::float+kr_savings_comp_d::float
--      +sams_savings_comp_d::float+cstco_savings_comp_d::float+heb_savings_comp_d::float+rand_savings_comp_d::float+houston_savings_comp_d::float)::float/
--  sum(rx_count_wags_cpr_comp_d+rx_count_wmt_cpr_comp_d+rx_count_cvs_cpr_comp_d+rx_count_kr_cpr_comp_d+rx_count_sams_cpr_comp_d
--     +rx_count_cstco_cpr_comp_d+rx_count_heb_cpr_comp_d+rx_count_rand_cpr_comp_d+rx_count_other_pharmacies_cpr_comp_d)::float as pharmacy_weighted_savings_per_script_comp_d

-- ,sum(wags_savings_win_d+wmt_savings_win_d::float+cvs_savings_win_d::float+kr_savings_win_d::float
--      +sams_savings_win_d::float+cstco_savings_win_d::float+heb_savings_win_d::float+rand_savings_win_d::float+houston_savings_win_d::float)::float/
--  sum(rx_count_wags_cpr_win_d+rx_count_wmt_cpr_win_d+rx_count_cvs_cpr_win_d+rx_count_kr_cpr_win_d+rx_count_sams_cpr_win_d
--     +rx_count_cstco_cpr_win_d+rx_count_heb_cpr_win_d+rx_count_rand_cpr_win_d+rx_count_other_pharmacies_cpr_win_d)::float as pharmacy_weighted_savings_per_script_win_d

-- -- Economics

-- ,sum(case when edlp_unit_price > 0 then coalesce(rx_count_total,0) else 0 end) as generic_priced_rx_count_total

-- ,sum(case when edlp_unit_price > 0 then coalesce(rx_quantity_total,0) else 0 end) as generic_priced_rx_quantity_total

-- ,sum(case when edlp_unit_price > 0 then coalesce(privia_mac_df_cogs,0) else 0 end) as generic_priced_privia_mac_df_cogs

-- ,sum(case when edlp_unit_price > 0 then coalesce(hou_5_pct_heb_else_hd_else_edlp_revenue,0) else 0 end) as generic_priced_hou_5_pct_heb_else_hd_else_edlp_revenue

-- ,sum(case when edlp_unit_price > 0 then coalesce(hou_1_pct_heb_else_hd_else_edlp_revenue,0) else 0 end) as generic_priced_hou_1_pct_heb_else_hd_else_edlp_revenue

-- ,sum(case when edlp_unit_price > 0 then coalesce(heb_else_hd_else_edlp_revenue,0) else 0 end) as generic_priced_heb_else_hd_else_edlp_revenue

-- ,sum(case when edlp_unit_price > 0 then coalesce(hd_else_edlp_revenue,0) else 0 end) as generic_priced_hd_else_edlp_revenue

-- from 
-- integrated_2)

select * from integrated_1;


select document_class,doc_type,count(*) from pricing_external_dev.privia_rx_export_20200420 group by 1,2;


-- drop table if exists pricing_dev.privia_rx_export_20200420_medid_data;
-- create table pricing_dev.privia_rx_export_20200420_medid_data  as 
-- select 
-- 	fbd_med_id as medid , 
-- -- 	pharmacy as pharmacy_name,
-- 	case when pharmacy ilike '%walgreen%' then 'Walgreens'
-- 		when pharmacy ilike '%wal%mart%' then 'Walmart'
-- 		when pharmacy ilike '%cvs%' then 'CVS'
-- 		when pharmacy ilike '%heb%' then 'HEB'
-- 		when pharmacy ilike '%randall%' then 'Randalls'
-- 		when pharmacy ilike '%kroger%' then 'Kroger'
-- 		when pharmacy ilike '%sam%club%' then 'Sams Club'
-- 		when pharmacy ilike '%costco%' then 'Costco'
-- 		when pharmacy ilike '%home%' or pharmacy ilike '%%mail%' then 'Home Delivery'
-- 		when pharmacy ilike '%blink%' then 'Blink'
-- 		else 'Other Pharmacies' end 
-- 	as pharmacy,
-- 	count(distinct document_id) as fills , 
-- 	sum(prescription_fill_quantity) as quantity
-- from 
-- 	pricing_external_dev.privia_rx_export_20200420
-- group BY
-- 	1,2
-- ;

-- with privia as (
-- 	select 
-- 		medid , 
-- 		sum(fills) as fills, 
-- 		sum(quantity) as quantity 
-- 	from 
-- 		pricing_dev.privia_rx_export_20200420_medid_data
-- 	group by 1
-- )
-- select
-- 	privia.medid,
-- 	privia.fills,
-- 	privia.quantity,
-- 	med.medid is not null as present_blink_medspan_data,
-- 	med.gcn_seqno,
-- 	med.name_source_code,
-- 	med.multi_source_code,
-- 	med.generic_drug_name_code,
-- 	med.generic_medid,
-- 	med.gcn,
-- 	med.gtc_desc,
-- 	available_med.medid is not null as present_blink_edlp,
-- 	available_med.type_description,
-- 	med_medid_desc,
-- 	med_name_type_code_desc,
-- 	med_name_maint,
-- 	med_name_dea,
-- 	med_ref_multi_source_code_desc
-- FROM
-- 	 privia
-- 	left outer join transactional.med on privia.medid = med.medid
-- 	left outer join transactional.available_med on privia.medid = available_med.medid
-- 	left outer join dwh.dim_medid_hierarchy as dmh on privia.medid = dmh.medid
-- ;

-- select * from pricing_external_dev.privia_rx_export_20200420 limit 10; 



-- create external table pricing_external_dev.privia_rx_export_20200420
-- (
--   document_id varchar(65000), 
--   account_acronym varchar(65000),
--   account_name varchar(65000),
--   provider_group_name varchar(65000),
--   department_name varchar(65000),
--   approved_date date,
--   year int,
--   month int,
--   document_id2 varchar(65000),
--   fbd_med_id int,
--   clinical_order_type varchar(65000),
--   display_dosage_units varchar(65000),
--   length_of_course int,
--   dosage_action varchar(65000),
--   dosage_form varchar(65000),
--   dosage_quantity int,
--   dosage_strength varchar(65000),
--   dosage_strength_units varchar(65000),
--   prescription_fill_quantity int,
--   frequency varchar(65000),
--   number_of_refills_prescribed int,
--   doc_type varchar(65000),
--   document_class varchar(65000),
--   pharmacy varchar(65000),
--   pharmacy_type varchar(65000),
--   provider_first_name varchar(65000),
--   provider_last_name varchar(65000),
--   provider_full_name varchar(65000),
--   provider_npi_number varchar(65000),
--   provider_type varchar(65000),
--   provider_type_category varchar(65000)
-- )

-- row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
-- location 's3://blink-dw-data-scrubbed-prod/pricing_team_adhoc_data/privia_rx_export_20200420/';
-- -- TBLPROPERTIES ('skip.header.line.count'='1');



-- DROP TABLE IF EXISTS sdey_privia_utilization_data_202001031;
-- Create table mktg_dev.sdey_privia_utilization_data_202001031 AS 
-- SELECT
-- 	privia.medname,
-- 	dmh.medid,
-- 	dmh.gcn_seqno,
-- 	privia.count_total,
-- 	dmh.medid is not null as found_in_fdb,
-- 	active_price.mid is not null as found_in_price,
-- 	am.medid is not null as found_on_site
-- FROM
-- 	mktg_dev.sdey_privia_data_temp_data privia
-- 	LEFT OUTER JOIN dwh.dim_medid_hierarchy dmh ON lower(privia.medname) = lower(dmh.med_medid_desc)
-- 	LEFT OUTER JOIN ( SELECT DISTINCT
-- 			(medid) AS mid
-- 		FROM
-- 			transactional.med_price
-- 		WHERE
-- 			ended_on IS NULL) active_price ON active_price.mid = dmh.medid
-- 	LEFT OUTER JOIN transactional.available_med am ON am.medid = dmh.medid
-- ;
-- GRANT SELECT ON mktg_dev.sdey_privia_utilization_data_202001031 TO "public";
-- GRANT SELECT ON mktg_dev.sdey_privia_data_temp_data TO "public";


-- select gcn,pac_low_unit from dwh.dim_gcn_seqno_hierarchy where pac_low_unit is not null;

-- -- select * from mktg_dev.sdey_privia_utilization_data_202001031 

-- select gcn,count_total
--  from mktg_dev.sdey_privia_utilization_data_202001031 inner join dwh.dim_medid_hierarchy
-- on sdey_privia_utilization_data_202001031.medid = dim_medid_hierarchy.medid;


-- select gcn,min(branded) from transactional.med_price group by 1 ;


-- with ddd as (
-- SELECT
-- 	mp.medid,
-- 	max(mp.branded) AS branded
-- FROM
-- 	mktg_dev.sdey_privia_utilization_data_202001031 privia
-- 	INNER JOIN transactional.med_price mp ON mp.medid = privia.medid
-- WHERE
-- 	found_in_fdb
-- 	AND found_in_price
-- 	AND NOT found_on_site
-- GROUP BY
-- 	1) select branded,count(*) from ddd group by 1;




-- select  	count(distinct(medname)),
-- 	count(*)
--  from mktg_dev.sdey_privia_data_temp_data


-- SELECT
-- 	pharmacy_network_id,
-- 	count(DISTINCT(med_price.medid)) count_of_common_drugs,
-- 	sum("count") as sum_of_matched_fills
--  FROM
-- 	mktg_dev.sdey_privia_utilization_data
-- INNER JOIN
-- transactional.med_price
-- ON
-- 	sdey_privia_utilization_data.medid = med_price.medid
-- WHERE
-- 	ended_on is null
-- group by 1


-- select
-- 	med_desc,medid,gcn_seqno
--  from
--  	mktg_dev.sdey_privia_temp_key_table
--  left outer JOIN
--  	dwh.dim_medid_hierarchy
--  ON
--  	lower(sdey_privia_temp_key_table.med_desc) = replace(lower(dim_medid_hierarchy.med_medid_desc),',',':')






-- SELECT
-- 	*
-- FROM
-- 	dwh.dim_medid_hierarchy
-- WHERE
-- 	lower(med_medid_desc) like lower('%Fluconazole%') and med_dosage_form_abbr='tab'


-- select * from mktg_dev.sdey_privia_utilization_data left JOIN transactional.med on sdey_privia_utilization_data.medid = med.medid;




