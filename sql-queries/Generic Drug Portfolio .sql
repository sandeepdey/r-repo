begin;
drop table if exists mktg_dev.sdey_generic_price_portfolio_datamart;
create table mktg_dev.sdey_generic_price_portfolio_datamart as  

with top_30ds_qty_1 as (
select last_claim_gcn_approved as gcn
,last_claim_quantity_approved as quantity
,count(*) as scripts
from dwh.fact_order_item 
where last_pbm_adjudication_timestamp_approved is not null and getdate()::date-last_pbm_adjudication_timestamp_approved::date <= 181 and last_claim_days_supply_approved = 30
group by 1,2)

, top_30ds_qty_2 as (select *
,row_number() over (partition by gcn order by scripts desc,quantity desc) as rn
from top_30ds_qty_1)

, top_30ds_qty_3 as (select * from top_30ds_qty_2 where rn = 1)

, top_90ds_qty_1 as (
select last_claim_gcn_approved as gcn
,last_claim_quantity_approved as quantity
,count(*) as scripts
from dwh.fact_order_item 
where last_pbm_adjudication_timestamp_approved is not null and getdate()::date-last_pbm_adjudication_timestamp_approved::date <= 181 and last_claim_days_supply_approved = 90
group by 1,2)

, top_90ds_qty_2 as (select *
,row_number() over (partition by gcn order by scripts desc,quantity desc) as rn
from top_90ds_qty_1)

, top_90ds_qty_3 as (select * from top_90ds_qty_2 where rn = 1)

, top_30ds_90ds_qty_stack as (
select gcn,quantity from top_30ds_qty_3
UNION
select gcn,quantity from top_90ds_qty_3)
 
, most_recent_30ds_90ds_qty_scrape_date_1 as 
(select 
a.gcn
,a.date::date as scrape_date
,max(case when c.quantity > 0 then 1 else 0 end) as qty_30ds_flg
,max(case when d.quantity > 0 then 1 else 0 end) as qty_90ds_flg
,count(*) as scrape_count
from api_scraper_external.competitor_pricing a
inner join top_30ds_90ds_qty_stack b on a.gcn=b.gcn and a.quantity=b.quantity
left join top_30ds_qty_3 c on b.gcn=c.gcn and b.quantity=c.quantity
left join top_90ds_qty_3 d on b.gcn=d.gcn and b.quantity=d.quantity
group by 1,2)

, most_recent_30ds_90ds_qty_scrape_date_2 as 
(select 
gcn
,scrape_date
,scrape_count
,row_number() over (partition by gcn order by scrape_date desc) as rn
from most_recent_30ds_90ds_qty_scrape_date_1 where qty_30ds_flg = 1 and qty_90ds_flg = 1)

, most_recent_30ds_90ds_qty_scrape_date_3 as 
(select * from most_recent_30ds_90ds_qty_scrape_date_2 where rn = 1)

, most_recent_30ds_qty_scrape_1 as (
select x.*
from api_scraper_external.competitor_pricing x
inner join top_30ds_qty_3 y on x.gcn=y.gcn and x.quantity=y.quantity 
inner join most_recent_30ds_90ds_qty_scrape_date_3 sd on x.gcn=sd.gcn and x.date=sd.scrape_date)

, most_recent_30ds_qty_scrape_2 as (
select 
x.gcn
,max(x.date)::date as last_30ds_qty_scrape_date
,max(x.quantity) as last_30ds_qty

,min(case when x.site = 'goodrx' and x.price > 0.1 then x.price end) as min_grx_30ds
,min(case when x.site = 'goodrx' and y.pharmacy_type != 'mailorder' and x.price > 0.1 then x.price end) as min_retail_grx_30ds
,min(case when x.site = 'goodrx' and y.pharmacy_type = 'major' and x.price > 0.1 then x.price end) as min_major_retail_grx_30ds
,min(case when x.site = 'goodrx' and (x.pharmacy = 'walmart' or x.pharmacy = 'h_e_b' or x.pharmacy = 'giant_eagle') and x.price > 0.1 then x.price end) as min_bh_retail_index_grx_30ds

,min(case when x.site = 'goodrx' and x.pharmacy = 'healthwarehouse' and x.price > 0.1 then x.price end) as min_hwh_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'walmart' and x.price > 0.1 then x.price end) as min_wmt_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'kroger' and x.price > 0.1 then x.price end) as min_kr_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'safeway' and x.price > 0.1 then x.price end) as min_sfwy_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'publix' and x.price > 0.1 then x.price end) as min_pblx_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'brookshires' and x.price > 0.1 then x.price end) as min_bksh_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'giant_eagle' and x.price > 0.1 then x.price end) as min_geagle_grx_30ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'h_e_b' and x.price > 0.1 then x.price end) as min_heb_grx_30ds

from most_recent_30ds_qty_scrape_1 x 
left join box.pharmacy_type_scraper y on x.pharmacy=y.pharmacy where x.site != 'all' and x.geo != 'all' and x.site = 'goodrx' and (x.pharmacy = 'brookshires' or (y.pharmacy_type is not null and (y.pharmacy_type = 'mailorder' or y.is_preferred = 1)))
group by 1)

, most_recent_90ds_qty_scrape_1 as (
select x.*
from api_scraper_external.competitor_pricing x
inner join top_90ds_qty_3 y on x.gcn=y.gcn and x.quantity=y.quantity 
inner join most_recent_30ds_90ds_qty_scrape_date_3 sd on x.gcn=sd.gcn and x.date=sd.scrape_date)

, most_recent_90ds_qty_scrape_2 as (
select 
x.gcn
,max(x.date)::date as last_90ds_qty_scrape_date
,max(x.quantity) as last_90ds_qty

,min(case when x.site = 'goodrx' and x.price > 0.1 then x.price end) as min_grx_90ds
,min(case when x.site = 'goodrx' and y.pharmacy_type != 'mailorder' and x.price > 0.1 then x.price end) as min_retail_grx_90ds
,min(case when x.site = 'goodrx' and y.pharmacy_type = 'major' and x.price > 0.1 then x.price   end) as min_major_retail_grx_90ds
,min(case when x.site = 'goodrx' and (x.pharmacy = 'walmart' or x.pharmacy = 'h_e_b' or x.pharmacy = 'giant_eagle') and x.price > 0.1 then x.price end) as min_bh_retail_index_grx_90ds

,min(case when x.site = 'goodrx' and x.pharmacy = 'healthwarehouse' and x.price > 0.1 then x.price end) as min_hwh_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'walmart' and x.price > 0.1 then x.price end) as min_wmt_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'kroger' and x.price > 0.1 then x.price end) as min_kr_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'safeway' and x.price > 0.1 then x.price end) as min_sfwy_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'publix' and x.price > 0.1 then x.price end) as min_pblx_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'brookshires' and x.price > 0.1 then x.price end) as min_bksh_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'giant_eagle' and x.price > 0.1 then x.price end) as min_geagle_grx_90ds
,min(case when x.site = 'goodrx' and x.pharmacy = 'h_e_b' and x.price > 0.1 then x.price end) as min_heb_grx_90ds

from most_recent_90ds_qty_scrape_1 x 
left join box.pharmacy_type_scraper y on x.pharmacy=y.pharmacy where x.site != 'all' and x.geo != 'all' and x.site = 'goodrx' and (x.pharmacy = 'brookshires' or (y.pharmacy_type is not null and (y.pharmacy_type = 'mailorder' or y.is_preferred = 1)))
group by 1)

, most_recent_grx_default1 as (
select gcn,quantity
,row_number() over (partition by gcn order by date desc) as qty_rn
from api_scraper_external.competitor_pricing where default_quantity=quantity and site != 'all' and geo != 'all')

, most_recent_grx_default2 as (
select gcn,quantity as default_quantity_1 from most_recent_grx_default1 where qty_rn = 1)

, default_qty_blink_1 as (
select y.gcn,x.quantity,sum(x.count) as gcn_ranking_count 
from transactional.medication_quantity x
left join transactional.med y on x.medid=y.medid
group by 1,2)

, default_qty_blink_2 as (
select *
,row_number() over (partition by gcn order by gcn_ranking_count desc) as rn
from default_qty_blink_1)

, default_qty_blink_3 as (
select distinct gcn,quantity as default_quantity_2 from default_qty_blink_2 where rn = 1)

, null_gcn_list as (
select distinct gcn,30 as default_quantity_3 from api_scraper_external.competitor_pricing where default_quantity is null)

, default_quantity_mega_list_1 as (
select distinct gcn from api_scraper_external.competitor_pricing)

, default_quantity_mega_list_2 as (
select a.gcn
,coalesce(b.default_quantity_1,c.default_quantity_2,d.default_quantity_3) as quantity
from default_quantity_mega_list_1 a
left join most_recent_grx_default2 b on a.gcn=b.gcn
left join default_qty_blink_3 c on a.gcn=c.gcn
left join null_gcn_list d on a.gcn=d.gcn)

, scraper_data_1 as (
select x.*
from api_scraper_external.competitor_pricing x
inner join default_quantity_mega_list_2 y on x.gcn=y.gcn and x.quantity=y.quantity and x.date >= getdate()::date-181)

, scraper_data_2 as (
select 
x.gcn
,x.date::date as scrape_date
,max(x.quantity) as default_quantity
,min(case when x.site = 'goodrx' and x.price > 0.1 then x.price end) as min_grx
,min(case when x.site = 'goodrx' and y.pharmacy_type != 'mailorder' and x.price > 0.1 then x.price end) as min_retail_grx
,min(case when x.site = 'goodrx' and y.pharmacy_type = 'major' and x.price > 0.1 then x.price end) as min_major_retail_grx
,min(case when x.site = 'goodrx' and (x.pharmacy = 'walmart' or x.pharmacy = 'h_e_b' or x.pharmacy = 'giant_eagle') and x.price > 0.1 then x.price end) as min_bh_retail_index_grx

,min(case when x.site = 'goodrx' and x.geo in ('greenwich_ct','menlo_park_ca') and x.price > 0.1 then x.price end) as min_grx_sf_nyc_suburbs
,min(case when x.site = 'goodrx' and x.region = 'northeast' and x.price > 0.1 then x.price end) as min_grx_northeast
,min(case when x.site = 'goodrx' and x.region = 'south' and x.price > 0.1 then x.price end) as min_grx_south
,min(case when x.site = 'goodrx' and x.region = 'midwest' and x.price > 0.1 then x.price end) as min_grx_midwest
,min(case when x.site = 'goodrx' and x.region = 'west' and x.price > 0.1 then x.price end) as min_grx_west

,min(case when x.site = 'goodrx' and x.pharmacy = 'healthwarehouse' and x.price > 0.1 then x.price end) as min_hwh_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'cvs' and x.price > 0.1 then x.price end) as min_cvs_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'walgreens' and x.price > 0.1 then x.price end) as min_wag_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'walmart' and x.price > 0.1 then x.price end) as min_wmt_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'rite_aid' and x.price > 0.1 then x.price end) as min_rad_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'kroger' and x.price > 0.1 then x.price end) as min_kr_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'safeway' and x.price > 0.1 then x.price end) as min_sfwy_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'publix' and x.price > 0.1 then x.price end) as min_pblx_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'brookshires' and x.price > 0.1 then x.price end) as min_bksh_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'giant_eagle' and x.price > 0.1 then x.price end) as min_geagle_grx
,min(case when x.site = 'goodrx' and x.pharmacy = 'h_e_b' and x.price > 0.1 then x.price end) as min_heb_grx

from scraper_data_1 x 
left join box.pharmacy_type_scraper y on x.pharmacy=y.pharmacy where x.site != 'all' and x.geo != 'all' and x.site = 'goodrx' and (x.pharmacy = 'brookshires' or (y.pharmacy_type is not null and (y.pharmacy_type = 'mailorder' or y.is_preferred = 1)))
group by 1,2)

, scraper_data_3 as (
select * from (
select *
,row_number() over (partition by gcn order by scrape_date desc) as date_rn
from scraper_data_2) sorted 
where sorted.date_rn = 1)

, scraper_data_lowest_grx_1 as (
select x.*
,y.pharmacy_type
,row_number() over (partition by x.gcn order by x.price asc) as price_rn
from scraper_data_1 x
left join box.pharmacy_type_scraper y on x.pharmacy=y.pharmacy where x.site != 'all' and x.geo != 'all' and x.site = 'goodrx' 
and (x.pharmacy = 'brookshires' or (y.pharmacy_type is not null and (y.pharmacy_type = 'mailorder' or y.is_preferred = 1))))

, scraper_data_lowest_grx_2 as (
select * from scraper_data_lowest_grx_1 where price_rn = 1)

, fdb_flags1 as (
select 
drg_gcn.gcn
,drg_gcn.gcn_seqno
,drg_gcn.hicl_seqno
,lower(drg_gcn.generic_name_short) as hicl_desc
,drg_gcn.therapeutic_class_desc_generic as gtc_desc
,drg_gcn.therapeutic_class_desc_standard as stc_desc
,drg_medid.custom_therapeutic_class as ctc_desc
,drg_gcn.strength
,drg_gcn.dosage_form_desc as form
,1 as maint_flg
,1 as dea_flg
-- ,max(case when mstr.maint > 0 then mstr.maint else 0 end) as maint_flg
-- ,coalesce(min(case when mstr.dea is not null and mstr.dea > 0 then mstr.dea end),0) as dea_flg
,count(*) as ndc_count
from dwh.dim_ndc_hierarchy drg_ndc 
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno
left join dwh.dim_medid_hierarchy drg_medid on drg_ndc.branded_medid=drg_medid.medid
-- left join fdb.RNDC14_NDC_MSTR mstr on drg_ndc.ndc=mstr.ndc
group by 1,2,3,4,5,6,7,8,9)

, fdb_flags2 as (select *
,row_number() over (partition by gcn order by ndc_count desc,hicl_desc asc) as rn
from fdb_flags1)

, fdb_flags3 as (select * from fdb_flags2 where rn = 1)

-- WMT MAC
, wmt_mac as 
(select y.gcn,max(x.unit_price) as wmt_mac_price_raw
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.end_date is null and x.mac_list = 'BLINKWMT01'  
group by 1)

, wmt_mac_r30_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as r30_wmt_mac_price
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINKWMT01' and x.start_date::date >= '2018-09-06' and x.start_date::date <= getdate()::date-31
group by 1,2)

, wmt_mac_r30_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date desc) as rn
from wmt_mac_r30_a)

, wmt_mac_r30_c as (select * from wmt_mac_r30_b where rn = 1)

, wmt_mac_ltd_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as ltd_wmt_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINKWMT01' and x.start_date::date >= '2018-09-06'
group by 1,2)

, wmt_mac_ltd_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date asc) as rn
from wmt_mac_ltd_a)

, wmt_mac_ltd_c as (select * from wmt_mac_ltd_b where rn = 1)

-- SYR (HD) MAC
, hd_syr_mac as 
(select y.gcn,max(x.unit_price) as hd_syr_mac_price_raw 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.end_date is null and x.mac_list = 'BLINKSYRx01' 
group by 1)

, hd_syr_mac_r30_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as r30_hd_syr_mac_price
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINKSYRx01' and x.start_date::date >= '2018-08-09' and x.start_date::date <= getdate()::date-31
group by 1,2)

, hd_syr_mac_r30_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date desc) as rn
from hd_syr_mac_r30_a)

, hd_syr_mac_r30_c as (select * from hd_syr_mac_r30_b where rn = 1)

 , hd_syr_mac_ltd_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as ltd_hd_syr_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINKSYRx01' and x.start_date::date >= '2018-08-09' 
group by 1,2)

, hd_syr_mac_ltd_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date asc) as rn
from hd_syr_mac_ltd_a)

, hd_syr_mac_ltd_c as (select * from hd_syr_mac_ltd_b where rn = 1)

-- BLINK01 MAC
, bh01_mac as 
(select y.gcn,max(x.unit_price) as bh01_mac_price_raw 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.end_date is null and x.mac_list = 'BLINK01' 
group by 1)

, bh01_mac_r30_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as r30_bh01_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINK01' and x.start_date::date >= '2018-03-23' and x.start_date::date <= getdate()::date-31
group by 1,2)

, bh01_mac_r30_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date desc) as rn
from bh01_mac_r30_a)

, bh01_mac_r30_c as (select * from bh01_mac_r30_b where rn = 1)

 , bh01_mac_ltd_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as ltd_bh01_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINK01' and x.start_date::date >= '2018-03-23'  
group by 1,2)

, bh01_mac_ltd_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date asc) as rn
from bh01_mac_ltd_a)

, bh01_mac_ltd_c as (select * from bh01_mac_ltd_b where rn = 1)

-- BLINK02 MAC
, bh02_mac as 
(select y.gcn,max(x.unit_price) as bh02_mac_price_raw 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.end_date is null and x.mac_list = 'BLINK02' 
group by 1)

, bh02_mac_r30_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as r30_bh02_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINK02' and x.start_date::date >= '2019-01-31' and x.start_date::date <= getdate()::date-31
group by 1,2)

, bh02_mac_r30_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date desc) as rn
from bh02_mac_r30_a)

, bh02_mac_r30_c as (select * from bh02_mac_r30_b where rn = 1)

 , bh02_mac_ltd_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as ltd_bh02_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINK02' and x.start_date::date >= '2019-01-31'  
group by 1,2)

, bh02_mac_ltd_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date asc) as rn
from bh02_mac_ltd_a)

, bh02_mac_ltd_c as (select * from bh02_mac_ltd_b where rn = 1)

-- BLINK03 MAC
, bh03_mac as 
(select y.gcn,max(x.unit_price) as bh03_mac_price_raw 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.end_date is null and x.mac_list = 'BLINK03' 
group by 1)

, bh03_mac_r30_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as r30_bh03_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINK03' and x.start_date::date >= '2019-01-31' and x.start_date::date <= getdate()::date-31
group by 1,2)

, bh03_mac_r30_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date desc) as rn
from bh03_mac_r30_a)

, bh03_mac_r30_c as (select * from bh03_mac_r30_b where rn = 1)

 , bh03_mac_ltd_a as 
(select y.gcn,x.start_date::date,max(x.unit_price) as ltd_bh03_mac_price 
from drugs_etl.network_pricing_mac x
left join dwh.dim_gcn_seqno_hierarchy y on x.gcn_seqno=y.gcn_seqno 
where x.mac_list = 'BLINK03' and x.start_date::date >= '2019-01-31'  
group by 1,2)

, bh03_mac_ltd_b as (select 
*
,row_number() over (partition by gcn::bigint order by start_date asc) as rn
from bh03_mac_ltd_a)

, bh03_mac_ltd_c as (select * from bh03_mac_ltd_b where rn = 1)

-- AWP (Medi-span)

, medispan_awp as (
select 
ndc_upc_hri
,max(x.unit_price) as awp_unit_price 
from medispan.mf2prc x where price_code = 'A' and x.unit_price is not null
group by 1)

-- PAC (Glassbox)

, pac_data_1 as (select
drug_identifier as gcn,
pac::float,
pac_low::float,
pac_high::float,
pac_retail::float,
downloaded_date::date as start_date
from gold_standard.pac_fdb where identifier_type = 'GCN' and brand_generic = 'Generic' and end_date is null)

, pac_data_2 as (select 
*
,row_number() over (partition by gcn::bigint order by start_date desc) as rn
from pac_data_1)

, pac_data_3 as (select * from pac_data_2 where rn = 1)

-- Blink Site Price Inputs

, site_unc as (
select 
y.gcn
,max(x.unit_price) as unc_unit_site_price
from transactional.med_unc_price x 
inner join transactional.med y on x.medid=y.medid where y.gcn is not null and x.unit_price > 0
group by 1)

, mp_edlp as (select 
y.gcn
,1.75 as edlp_dispensing_fee
,max(x.unit_price) as edlp_unit_price
,max(x.dispensing_fee_margin) as edlp_dispensing_fee_margin
from transactional.med_price x
inner join transactional.available_med am on x.medid=am.medid
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 1 and branded=0
group by 1,2)

, mp_bsd as (select 
y.gcn
,1.75 as bsd_dispensing_fee
,max(x.unit_price) as bsd_unit_price
,max(x.dispensing_fee_margin) as bsd_dispensing_fee_margin
from transactional.med_price x
inner join transactional.available_med am on x.medid=am.medid
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 2 and branded=0
group by 1,2)

, mp_hd as (select 
y.gcn
,1.75 as hd_dispensing_fee
,max(x.unit_price) as hd_unit_price
,max(x.dispensing_fee_margin) as hd_dispensing_fee_margin
from transactional.med_price x
inner join transactional.available_med am on x.medid=am.medid
left join transactional.med y on x.medid=y.medid where x.ended_on is null and x.pharmacy_network_id = 3 and branded=0
group by 1,2)

-- SymphonyHealth 2017 U.S. national generic NDC utilization dataset

, symphony_2017_fills_hicl_gcn as 
(select
fdb.hicl_seqno
,drg_gcn.gcn
,sum(x.trx_count::float) as symphony_2017_hicl_gcn_scripts
,sum(case when drg_gcn.therapeutic_class_desc_standard != 'OPIOID ANALGESICS' then x.trx_count::float else 0 end) as symphony_2017_non_opioid_scripts
from box.symphony_2017_generics_annual_temp x
left join dwh.dim_ndc_hierarchy drg_ndc on x.ndc::bigint=drg_ndc.ndc::bigint
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno 
left join fdb_flags3 fdb on drg_gcn.gcn=fdb.gcn
where drg_gcn.gcn is not null and drg_gcn.hicl_seqno is not null and x.trx_count is not null
group by 1,2)

, symphony_2017_fills_hicl as 
(select 
hicl_seqno
,sum(symphony_2017_hicl_gcn_scripts) as symphony_2017_hicl_scripts
from symphony_2017_fills_hicl_gcn
group by 1)

, symphony_2017_fills_final as 
(select 
x.hicl_seqno 
,x.gcn
,x.symphony_2017_hicl_gcn_scripts
,x.symphony_2017_non_opioid_scripts
,y.symphony_2017_hicl_scripts
,trunc(x.symphony_2017_hicl_gcn_scripts::float/y.symphony_2017_hicl_scripts::float,4) as symphony_2017_pct_of_hicl_scripts
from symphony_2017_fills_hicl_gcn x
left join symphony_2017_fills_hicl y on x.hicl_seqno=y.hicl_seqno)

-- Blink Site Default Quantities

, mq1 as (select 
y.gcn
,x.quantity
,sum(x."count"::float) as fills
from transactional.medication_quantity x
left join transactional.med y on x.medid=y.medid where y.gcn is not null 
group by 1,2)

, mq2 as (select gcn,quantity,fills
,row_number() over (partition by gcn order by fills desc) as rn
from mq1)

, mq3 as (select gcn,quantity,fills,1 as default_qty from mq2 where rn = 1)

, available_med_flg as (select
distinct y.gcn,1 as available_med_flg from transactional.available_med x
left join transactional.med y on x.medid=y.medid where y.gcn is not null)

, tem_available_med_flg as (select gcn,min(start_date) as min_start_date,1 as tem_available_med_flg 
from git_data_import.telemed_prescribable_med group by 1 order by 2 asc)

-- Data Integration #1

, core1 as (
select 
drg_gcn.gcn
,fdb.gcn_seqno
,fdb.hicl_seqno
,fdb.hicl_desc
,fdb.gtc_desc
,fdb.stc_desc
,fdb.ctc_desc
,fdb.strength
,fdb.form
,fdb.maint_flg
,fdb.dea_flg
,max(symphony.symphony_2017_hicl_gcn_scripts) as symphony_2017_scripts
,max(symphony.symphony_2017_non_opioid_scripts) as symphony_2017_non_opioid_scripts
,max(symphony.symphony_2017_hicl_scripts) as symphony_2017_hicl_scripts
,max(symphony.symphony_2017_pct_of_hicl_scripts) as symphony_2017_pct_of_hicl_scripts

,max(bh01_mac.unit_price) as bh01_mac_price_raw 
,max(bh02_mac.bh02_mac_price_raw) as bh02_mac_price_raw 
,max(bh03_mac.bh03_mac_price_raw) as bh03_mac_price_raw
,max(wmt_mac.wmt_mac_price_raw)::float as wmt_mac_price_raw
,max(hd_syr_mac.hd_syr_mac_price_raw)::float as hd_syr_mac_price_raw
,max(pac.pac::float) as pac_unit
,max(pac.pac_low::float) as pac_low_unit
,max(pac.pac_high::float) as pac_high_unit
,max(pac.pac_retail::float) as pac_retail_unit

,max(edlp.edlp_dispensing_fee) as edlp_dispensing_fee
,max(edlp.edlp_unit_price) as edlp_unit_price
,max(edlp.edlp_dispensing_fee_margin) as edlp_dispensing_fee_margin

,max(bsd.bsd_dispensing_fee) as bsd_dispensing_fee 
,max(bsd.bsd_unit_price) as bsd_unit_price
,max(bsd.bsd_dispensing_fee_margin) as bsd_dispensing_fee_margin

,max(hd.hd_dispensing_fee) as hd_dispensing_fee 
,max(hd.hd_unit_price) as hd_unit_price
,max(hd.hd_dispensing_fee_margin) as hd_dispensing_fee_margin

from drugs_etl.network_pricing_mac bh01_mac
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on bh01_mac.gcn_seqno=drg_gcn.gcn_seqno 
left join fdb_flags3 fdb on drg_gcn.gcn=fdb.gcn
left join pac_data_3 pac on drg_gcn.gcn=pac.gcn
left join bh02_mac bh02_mac on drg_gcn.gcn=bh02_mac.gcn 
left join bh03_mac bh03_mac on drg_gcn.gcn=bh03_mac.gcn 
left join wmt_mac wmt_mac on drg_gcn.gcn=wmt_mac.gcn
left join hd_syr_mac hd_syr_mac on drg_gcn.gcn=hd_syr_mac.gcn
left join symphony_2017_fills_final symphony on drg_gcn.gcn=symphony.gcn
left join mp_edlp edlp on drg_gcn.gcn=edlp.gcn
left join mp_bsd bsd on drg_gcn.gcn=bsd.gcn
left join mp_hd hd on drg_gcn.gcn=hd.gcn

where bh01_mac.end_date is null and bh01_mac.mac_list = 'BLINK01' 
group by 1,2,3,4,5,6,7,8,9,10,11)

-- Blink claims utilization run rates

, claims_base as (
select 
drg_gcn.gcn

-- LTD
,count(distinct a.account_id) as ltd_filling_patients
,count(a.last_pbm_adjudication_timestamp_approved) as ltd_scripts
,sum(a.last_claim_quantity_approved) as ltd_qty

,count(distinct(case when a.fill_date_ny_sequence = 1 then a.account_id end)) as ltd_nfp
,count(case when a.fill_date_ny_sequence = 1 then a.last_pbm_adjudication_timestamp_approved end) as ltd_nfp_scripts
,sum(case when a.fill_date_ny_sequence = 1 then a.last_claim_quantity_approved end) as ltd_nfp_qty

-- WMT UNC + WTD WMT AWP

,sum(case when getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 91 and (lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') then a.last_pricing_unc_cost_approved::float else 0 end)::float/(nullif(sum(case when getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 91 and (lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') then a.last_claim_quantity_approved::float else 0 end)::float,0)) as r90_wmt_unc_unit_cost
,sum(case when getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 91 and lower(p.pharmacy_name) ilike '%publix%' then a.last_pricing_unc_cost_approved::float else 0 end)::float/nullif(sum(case when getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 91 and (lower(p.pharmacy_name) ilike '%publix%') then a.last_claim_quantity_approved::float else 0 end)::float,0) as r90_pblx_unc_unit_cost
,sum(case when pp.in_candle = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 91 then a.last_pricing_unc_cost_approved::float else 0 end)::float/nullif(sum(case when pp.in_candle = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 91 then a.last_claim_quantity_approved::float else 0 end)::float,0) as r90_beh_unc_unit_cost
,sum(awp.awp_unit_price::float*a.last_claim_quantity_approved::float)::float/sum(a.last_claim_quantity_approved::float)::float as awp_unit_cost

-- R30 ALL
,count(case when ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_scripts
,sum(case when ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_qty

,count(case when ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_nfp_scripts
,sum(case when ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_nfp_qty

-- CORE 4 CHANNELS

-- R30 WMT
,count(case when ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_wmt_scripts
,sum(case when ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_wmt_qty

,count(case when ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01') and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_wmt_nfp_scripts
,sum(case when ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01') and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_wmt_nfp_qty


-- R30 EDLP NON-WMT
,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_edlp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_edlp_qty

,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_edlp_nfp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_edlp_nfp_qty

-- R30 BSD

,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bsd_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bsd_qty

,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bsd_nfp_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229')) and (tc.pricing_strategy is null or tc.pricing_strategy != 'drug_price_list: BLINKWMT01')
 and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bsd_nfp_qty

-- R30 HD
,count(case when ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_hd_scripts
,sum(case when ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx')  and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_hd_qty

,count(case when ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx')  and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_hd_nfp_scripts
,sum(case when ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_hd_nfp_qty

-- R30 CLAIMS-BASED MAC CHECKS

,sum(case when tc.pricing_strategy = 'drug_price_list: BLINKWMT01' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pricing_ingredient_cost_approved::float end)::float/
sum(case when tc.pricing_strategy = 'drug_price_list: BLINKWMT01' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_wmt_clm_mac_check

,sum(case when tc.pricing_strategy = 'drug_price_list: BLINK01' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pricing_ingredient_cost_approved::float end)::float/
sum(case when tc.pricing_strategy = 'drug_price_list: BLINK01' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_bh01_clm_mac_check

,sum(case when tc.pricing_strategy = 'drug_price_list: BLINK02' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pricing_ingredient_cost_approved::float end)::float/
sum(case when tc.pricing_strategy = 'drug_price_list: BLINK02' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_bh02_clm_mac_check

,sum(case when tc.pricing_strategy = 'drug_price_list: BLINK03' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pricing_ingredient_cost_approved::float end)::float/
sum(case when tc.pricing_strategy = 'drug_price_list: BLINK03' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_bh03_clm_mac_check

,sum(case when tc.pricing_strategy = 'delivery: serve_you_rx' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pricing_ingredient_cost_approved::float end)::float/
sum(case when tc.pricing_strategy = 'delivery: serve_you_rx' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_hd_syr_clm_mac_check 

-- PRICING STRATEGY SUBSETS

-- R30 EDLP BH01
,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and (tc.pricing_strategy is null or tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03','drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh01_edlp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
  and (tc.pricing_strategy is null or tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03','drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh01_edlp_qty

,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and (tc.pricing_strategy is null or tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03','drug_price_list: BLINKWMT01')) and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh01_edlp_nfp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and (tc.pricing_strategy is null or tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03','drug_price_list: BLINKWMT01')) and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh01_edlp_nfp_qty


-- R30 EDLP BH02
,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK02' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh02_edlp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
  and tc.pricing_strategy = 'drug_price_list: BLINK02' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh02_edlp_qty


,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK02' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh02_edlp_nfp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK02' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh02_edlp_nfp_qty

-- R30 EDLP BH03
,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK03' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh03_edlp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
  and tc.pricing_strategy = 'drug_price_list: BLINK03' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh03_edlp_qty

,count(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK03' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh03_edlp_nfp_scripts
,sum(case when pp.in_candle = 1 and (a.pharmacy_network_id = 1 or a.pharmacy_network_id is null) and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK03' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh03_edlp_nfp_qty

-- R30 BSD BH01
,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh01_bsd_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
  and tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh01_bsd_qty

,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03') and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh01_bsd_nfp_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy not in ('drug_price_list: BLINK02','drug_price_list: BLINK03') and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh01_bsd_nfp_qty

-- R30 BSD BH02
,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK02' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh02_bsd_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
  and tc.pricing_strategy = 'drug_price_list: BLINK02' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh02_bsd_qty

,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK02' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh02_bsd_nfp_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK02' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh02_bsd_nfp_qty

-- R30 BSD BH03
,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK03' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh03_bsd_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
  and tc.pricing_strategy = 'drug_price_list: BLINK03' and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh03_bsd_qty

,count(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK03' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_bh03_bsd_nfp_scripts
,sum(case when pp.in_candle = 1 and a.pharmacy_network_id = 2 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
 and tc.pricing_strategy = 'drug_price_list: BLINK03' and a.fill_date_ny_sequence = 1 and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end) as r30_bh03_bsd_nfp_qty

-- LTD 30 vs. 90 Day Scripts
,count(case when a.last_claim_days_supply_approved < 84 then a.last_pbm_adjudication_timestamp_approved end) as ltd_30_day_scripts
,count(case when a.last_claim_days_supply_approved >= 84 then a.last_pbm_adjudication_timestamp_approved end) as ltd_90_day_scripts
,count(case when a.last_claim_days_supply_approved < 84 then a.last_pbm_adjudication_timestamp_approved end)::float/nullif(
count(a.last_pbm_adjudication_timestamp_approved)::float,0) as ltd_30_day_scripts_pct
,count(case when a.last_claim_days_supply_approved >= 84 then a.last_pbm_adjudication_timestamp_approved end)::float/nullif(
count(a.last_pbm_adjudication_timestamp_approved)::float,0) as ltd_90_day_scripts_pct

-- R30 ALL 30 vs. 90 Day Scripts
,count(case when a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_30_day_scripts
,count(case when a.last_claim_days_supply_approved >= 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_90_day_scripts

,count(case when a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_30_day_script_pct

,count(case when a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_90_day_script_pct

-- R30 NFP ALL 30 vs. 90 Day Scripts

,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_30_day_nfp_scripts
,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved >= 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_90_day_nfp_scripts

,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when a.fill_date_ny_sequence = 1 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_30_day_nfp_script_pct

,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when a.fill_date_ny_sequence = 1 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_90_day_nfp_script_pct

-- R30 HD 30 vs. 90 Day Scripts
,count(case when a.last_claim_days_supply_approved < 84 and ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_hd_30_day_scripts
,count(case when a.last_claim_days_supply_approved >= 84 and ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_hd_90_day_scripts

,count(case when a.last_claim_days_supply_approved < 84 and ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_hd_30_day_script_pct

,count(case when a.last_claim_days_supply_approved < 84 and ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_hd_90_day_script_pct

-- R30 NFP HD 30 vs. 90 Day Scripts

,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved < 84 and ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_hd_30_day_nfp_scripts
,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved >= 84 and ((a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) or tc.pricing_strategy = 'delivery: serve_you_rx') and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end) as r30_hd_90_day_nfp_scripts

,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when a.fill_date_ny_sequence = 1 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_hd_30_day_nfp_script_pct

,count(case when a.fill_date_ny_sequence = 1 and a.last_claim_days_supply_approved < 84 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float/
nullif(count(case when a.fill_date_ny_sequence = 1 and ((pp.in_candle = 1 or pp.npi_number = 1811906720) or ((lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') or tc.pricing_strategy = 'drug_price_list: BLINKWMT01')) and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_pbm_adjudication_timestamp_approved end)::float,0)
as r30_hd_90_day_nfp_script_pct

-- R30 WMT MAC PAID

-- ,sum(case when pp.in_candle = 1 and (lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') 
-- and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_incredient_cost_approved::float end)::float
-- /sum(case when pp.in_candle = 1 and (lower(p.pharmacy_name) ilike '%walmart%' or p.ncpdp_relationship_id = '229') 
-- and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_wmt_mac_paid

-- R30 BH01 MAC PAID

-- ,sum(case when pp.in_candle = 1 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
--  and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_incredient_cost_approved::float end)::float
-- /sum(case when pp.in_candle = 1 and (lower(p.pharmacy_name) not like '%walmart%' and (p.ncpdp_relationship_id is null or p.ncpdp_relationship_id != '229'))
--  and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_bh01_mac_paid

-- R30 HD PAID

-- ,sum(case when (a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) 
--  and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_ingredient_cost_approved::float end)::float
-- /sum(case when (a.pharmacy_network_id = 3 and pp.npi_number = 1811906720) 
--  and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31 then a.last_claim_quantity_approved::float end)::float as r30_mail_order_mac_paid


from dwh.fact_order_item a
left join dwh.dim_user b on a.account_id=b.account_id 
left join dwh.dim_ndc_hierarchy drg_ndc on a.last_claim_ndc_approved=drg_ndc.ndc
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno
left join fdb.RNDC14_NDC_MSTR mstr on a.last_claim_ndc_approved=mstr.ndc
left join medispan.mf2ndc ms on a.last_claim_ndc_approved=ms.ndc_upc_hri
left join dwh.dim_pharmacy_hierarchy p on a.last_claim_pharmacy_npi_approved=p.pharmacy_npi
left join transactional.pharmacy pp on a.last_claim_pharmacy_npi_approved=pp.npi_number
left join transactional.transactional_claim tc on a.last_claim_transactional_claim_id=tc.id
left join medispan_awp awp on a.last_claim_ndc_approved=awp.ndc_upc_hri
where a.last_pbm_adjudication_timestamp_approved is not null
--and b.is_internal = FALSE and 
and ms.multi_source_code = 'Y' 
group by 1)

-- Related brand medname data

, brand_medname1 as (
select 
m.gcn
,am.name as med_name
,am.med_name_id
,count(a.last_pbm_adjudication_timestamp_approved) as fills 
from dwh.fact_order_item a
left join dwh.dim_user b on a.account_id=b.account_id 
left join dwh.dim_ndc_hierarchy drg_ndc on a.last_claim_ndc_approved=drg_ndc.ndc
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno
left join fdb.RNDC14_NDC_MSTR mstr on a.last_claim_ndc_approved=mstr.ndc
left join medispan.mf2ndc ms on a.last_claim_ndc_approved=ms.ndc_upc_hri
left join transactional.available_med am on a.med_id=am.medid
left join transactional.med m on am.medid=m.medid
where a.last_pbm_adjudication_timestamp_approved is not null and ms.multi_source_code = 'Y' and am.type_description = 'brand' and am.name is not null and m.gcn is not null
group by 1,2,3)

, brand_medname2 as (select *
,row_number() over (partition by gcn order by fills desc) as rn
from brand_medname1)

, brand_medname3 as (select * from brand_medname2 where rn = 1)

-- Blink internal ranking data

, blink_rank as (
select 
drg_gcn.gcn
,fdb.hicl_seqno
,fdb.hicl_desc
,count(a.last_pbm_adjudication_timestamp_approved) as blink_scripts
,count(case when fdb.stc_desc != 'OPIOID ANALGESICS' then a.last_pbm_adjudication_timestamp_approved end) as blink_non_opioid_scripts
from dwh.fact_order_item a
left join dwh.dim_user b on a.account_id=b.account_id 
left join dwh.dim_ndc_hierarchy drg_ndc on a.last_claim_ndc_approved=drg_ndc.ndc
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno
left join fdb_flags3 fdb on drg_gcn.gcn=fdb.gcn
left join medispan.mf2ndc ms on a.last_claim_ndc_approved=ms.ndc_upc_hri
left join transactional.available_med am on a.last_claim_medid_approved=am.medid
where a.last_pbm_adjudication_timestamp_approved is not null and ms.multi_source_code = 'Y' and b.is_internal = FALSE and drg_gcn.gcn is not null and drg_gcn.generic_name_short is not null and drg_gcn.hicl_seqno is not null
group by 1,2,3)

, blink_hicl_gcn_rank1 as (select *
,row_number() over (partition by hicl_desc order by blink_scripts desc) as blink_hicl_gcn_rank
,row_number() over (partition by gcn order by blink_scripts desc) as rn
from blink_rank)

, blink_hicl_gcn_rank2 as (select * from blink_hicl_gcn_rank1 where rn = 1)

, blink_hicl_rank1 as (
select 
hicl_seqno
,sum(blink_scripts) as blink_scripts
,sum(blink_non_opioid_scripts) as blink_non_opioid_scripts
from blink_rank
group by 1)

, blink_hicl_rank2 as 
(select 
hicl_seqno
,row_number() over (ORDER BY coalesce(blink_non_opioid_scripts,0) DESC) AS blink_non_opioid_hicl_rank
,row_number() over (ORDER BY blink_scripts DESC) AS blink_hicl_rank
from blink_hicl_rank1)

, blink_pct_of_hicl_scripts as (
select x.hicl_seqno
,x.gcn
,trunc(x.blink_scripts::float/y.blink_scripts::float,4) as blink_pct_of_hicl_scripts
from blink_rank x
left join blink_hicl_rank1 y on x.hicl_seqno=y.hicl_seqno where y.blink_scripts > 0)

, blink_gcn_rank1 as (
select 
gcn
,sum(blink_scripts) as blink_scripts
,sum(blink_non_opioid_scripts) as blink_non_opioid_scripts
from blink_rank
group by 1)

, blink_gcn_rank2 as 
(select 
gcn
,row_number() over (ORDER BY coalesce(blink_non_opioid_scripts,0) DESC) AS blink_non_opioid_gcn_rank
,row_number() over (ORDER BY blink_scripts DESC) AS blink_gcn_rank
from blink_gcn_rank1)

, blink_r30_rank as (
select 
drg_gcn.gcn
,fdb.hicl_seqno
,fdb.hicl_desc
,count(a.last_pbm_adjudication_timestamp_approved) as blink_scripts
,count(case when fdb.stc_desc != 'OPIOID ANALGESICS' then a.last_pbm_adjudication_timestamp_approved end) as blink_non_opioid_scripts
from dwh.fact_order_item a
left join dwh.dim_user b on a.account_id=b.account_id 
left join dwh.dim_ndc_hierarchy drg_ndc on a.last_claim_ndc_approved=drg_ndc.ndc
left join dwh.dim_gcn_seqno_hierarchy drg_gcn on drg_ndc.gcn_seqno=drg_gcn.gcn_seqno
left join fdb_flags3 fdb on drg_gcn.gcn=fdb.gcn
left join medispan.mf2ndc ms on a.last_claim_ndc_approved=ms.ndc_upc_hri
left join transactional.available_med am on a.last_claim_medid_approved=am.medid
where a.last_pbm_adjudication_timestamp_approved is not null 
and getdate()::date-a.last_pbm_adjudication_timestamp_approved::date <= 31
and ms.multi_source_code = 'Y' and b.is_internal = FALSE and drg_gcn.gcn is not null and drg_gcn.generic_name_short is not null and drg_gcn.hicl_seqno is not null
group by 1,2,3)

, blink_r30_gcn_rank1 as (
select 
gcn
,sum(blink_scripts) as blink_r30_scripts
from blink_r30_rank
group by 1)

, blink_r30_gcn_rank2 as 
(select 
gcn
,row_number() over (ORDER BY blink_r30_scripts DESC) AS blink_r30_gcn_rank
from blink_r30_gcn_rank1)

, blink_r30_hicl_rank1 as (
select 
hicl_seqno
,sum(blink_scripts) as blink_r30_scripts
from blink_r30_rank
group by 1)

, blink_r30_hicl_rank2 as 
(select 
hicl_seqno
,row_number() over (ORDER BY blink_r30_scripts DESC) AS blink_r30_hicl_rank
from blink_r30_hicl_rank1)


-- Site CVR by GCN and HICL (Segment)

, r30_filled_orders_cvr as (
select distinct order_id,1 as fill_flag
from dwh.fact_order_item where fill_sequence > 0 and ordered_timestamp::date >= getdate()::date-181)


, cvr_events1 as (

    select
        coalesce(gcn.gcn,p_gcn.gcn) as gcn
        ,coalesce(fdb.hicl_seqno,p_fdb.hicl_seqno) as hicl_seqno
        ,coalesce(fdb.hicl_desc,p_fdb.hicl_desc) as hicl_desc
        ,coalesce(med.med_name_id,p_medid.med_name_id) as med_name_id
        ,a.session_start_time
        ,a.global_session_id
        ,a.touchpoint1
        ,case when a.touchpoint1 = 'purchased_product' and a.detail1 > 0 then a.detail1 end as pharmacy_network_id
        ,f.fill_flag
    from journey.event a
    left join dwh.dim_medid_hierarchy med on a.detail3=med.medid
    left join dwh.dim_gcn_seqno_hierarchy gcn on med.gcn_seqno=gcn.gcn_seqno
    left join fdb_flags3 fdb on gcn.gcn=fdb.gcn

    left join dwh.dim_medid_hierarchy p_medid on a.detail3::int = p_medid.medid and a.touchpoint1 = 'purchased_product'
    left join dwh.dim_gcn_seqno_hierarchy p_gcn on p_medid.gcn_seqno=p_gcn.gcn_seqno
    left join fdb_flags3 p_fdb on p_gcn.gcn=p_fdb.gcn 
    left join r30_filled_orders_cvr f on a.detail2::bigint=f.order_id::bigint and a.touchpoint1 = 'purchased_product'

    where a.session_start_time::date >= getdate()::date-31 and a.session_start_time::date < getdate()::date 
       and a.touchpoint1 in ('viewed_product','purchased_product')
                and a.purchasing_patient = FALSE
                    and a.ip_category = 'visitor')

, cvr_events_gcn as (
select 
hicl_seqno
,gcn
,count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end)) as r30_gcn_viewed_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end)) as r30_gcn_purchased_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 then global_session_id end)) as r30_gcn_filled_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 1 then global_session_id end)) as r30_gcn_purchased_edlp_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 2 then global_session_id end)) as r30_gcn_purchased_bsd_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 3 then global_session_id end)) as r30_gcn_purchased_hd_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 1 then global_session_id end)) as r30_gcn_filled_edlp_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 2 then global_session_id end)) as r30_gcn_filled_bsd_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 3 then global_session_id end)) as r30_gcn_filled_hd_product_sessions

,trunc(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_purchased_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 1 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_purchased_edlp_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 2 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_purchased_bsd_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 3 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_purchased_hd_cvr

,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_filled_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  and pharmacy_network_id = 1 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_filled_edlp_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 2 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_filled_bsd_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 3 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_gcn_filled_hd_cvr

,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_gcn_purchase_to_fill_rate
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  and pharmacy_network_id = 1 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_gcn_edlp_purchase_to_fill_rate
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 2 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_gcn_bsd_purchase_to_fill_rate
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 3 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_gcn_hd_purchase_to_fill_rate
from cvr_events1 where hicl_seqno is not null and gcn is not null

group by 1,2)

, cvr_events_hicl as (
select 
hicl_seqno
,count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end)) as r30_hicl_viewed_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end)) as r30_hicl_purchased_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 then global_session_id end)) as r30_hicl_filled_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 1 then global_session_id end)) as r30_hicl_purchased_edlp_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 2 then global_session_id end)) as r30_hicl_purchased_bsd_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 3 then global_session_id end)) as r30_hicl_purchased_hd_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 1 then global_session_id end)) as r30_hicl_filled_edlp_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 2 then global_session_id end)) as r30_hicl_filled_bsd_product_sessions
,count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 3 then global_session_id end)) as r30_hicl_filled_hd_product_sessions

,trunc(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_purchased_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 1 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_purchased_edlp_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 2 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_purchased_bsd_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and pharmacy_network_id = 3 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_purchased_hd_cvr

,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_filled_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  and pharmacy_network_id = 1 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_filled_edlp_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 2 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_filled_bsd_cvr
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 3 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end))::float,0),5) as r30_hicl_filled_hd_cvr

,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_hicl_purchase_to_fill_rate
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1  and pharmacy_network_id = 1 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_hicl_edlp_purchase_to_fill_rate
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 2 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_hicl_bsd_purchase_to_fill_rate
,trunc(count(distinct(case when touchpoint1 = 'purchased_product' and fill_flag = 1 and pharmacy_network_id = 3 then global_session_id end))::float
/nullif(count(distinct(case when touchpoint1 = 'purchased_product' then global_session_id end))::float,0),5) as r30_hicl_hd_purchase_to_fill_rate

from cvr_events1 x where hicl_seqno is not null
group by 1)

-- Default GCN flags

, med_name_id_visits as (
select hicl_seqno,med_name_id
,count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end)) as med_name_viewed_product_sessions
from cvr_events1
group by 1,2)

, hicl_seqno_visits as (
select hicl_seqno
,count(distinct(case when touchpoint1 = 'viewed_product' then global_session_id end)) as hicl_viewed_product_sessions
from cvr_events1
group by 1)

, medname_vs_hicl_visits1 as (
select x.hicl_seqno
,x.med_name_id
,x.med_name_viewed_product_sessions
,y.hicl_viewed_product_sessions
,x.med_name_viewed_product_sessions::float/nullif(y.hicl_viewed_product_sessions::float,0) as med_name_hicl_traffic_pct
from med_name_id_visits x
left join hicl_seqno_visits y on x.hicl_seqno=y.hicl_seqno)

, medname_vs_hicl_visits2 as (
select distinct med_name_id from medname_vs_hicl_visits1 where med_name_hicl_traffic_pct::float >= .25 and med_name_id is not null)

, formulation as (
select
  m.gcn
  ,am."default" as default_flag
from transactional.available_med am
inner join transactional.med m on m.medid = am.medid
inner join medname_vs_hicl_visits2 mhv on am.med_name_id=mhv.med_name_id)

, default_flag as (select distinct gcn,1 as default_flag from formulation where default_flag = 1)

, cvr_events3 as (
select 
x.*
,coalesce(z.default_flag,0) as default_flag
from cvr_events_gcn x
left join default_flag z on x.gcn=z.gcn)

, cvr_events4 as (select *
,row_number() over (partition by hicl_seqno order by default_flag desc, r30_gcn_viewed_product_sessions desc) as final_default_flag
from cvr_events3)

, final_default_flag as (select gcn,final_default_flag from cvr_events4 where final_default_flag = 1)

, cvr_events5 as (
select 
x.hicl_seqno
,x.gcn
,x.r30_gcn_viewed_product_sessions
,x.r30_gcn_purchased_product_sessions
,x.r30_gcn_filled_product_sessions
,x.r30_gcn_purchased_edlp_product_sessions
,x.r30_gcn_purchased_bsd_product_sessions
,x.r30_gcn_purchased_hd_product_sessions
,x.r30_gcn_filled_edlp_product_sessions
,x.r30_gcn_filled_bsd_product_sessions
,x.r30_gcn_filled_hd_product_sessions
,x.r30_gcn_purchased_cvr
,x.r30_gcn_purchased_edlp_cvr
,x.r30_gcn_purchased_bsd_cvr
,x.r30_gcn_purchased_hd_cvr
,x.r30_gcn_filled_cvr
,x.r30_gcn_filled_edlp_cvr
,x.r30_gcn_filled_bsd_cvr
,x.r30_gcn_filled_hd_cvr
,x.r30_gcn_purchase_to_fill_rate
,x.r30_gcn_edlp_purchase_to_fill_rate
,x.r30_gcn_bsd_purchase_to_fill_rate
,x.r30_gcn_hd_purchase_to_fill_rate
,y.r30_hicl_viewed_product_sessions
,y.r30_hicl_purchased_product_sessions
,y.r30_hicl_filled_product_sessions
,y.r30_hicl_purchased_edlp_product_sessions
,y.r30_hicl_purchased_bsd_product_sessions
,y.r30_hicl_purchased_hd_product_sessions
,y.r30_hicl_filled_edlp_product_sessions
,y.r30_hicl_filled_bsd_product_sessions
,y.r30_hicl_filled_hd_product_sessions
,y.r30_hicl_purchased_cvr
,y.r30_hicl_purchased_edlp_cvr
,y.r30_hicl_purchased_bsd_cvr
,y.r30_hicl_purchased_hd_cvr
,y.r30_hicl_filled_cvr
,y.r30_hicl_filled_edlp_cvr
,y.r30_hicl_filled_bsd_cvr
,y.r30_hicl_filled_hd_cvr
,y.r30_hicl_purchase_to_fill_rate
,y.r30_hicl_edlp_purchase_to_fill_rate
,y.r30_hicl_bsd_purchase_to_fill_rate
,y.r30_hicl_hd_purchase_to_fill_rate

,z.final_default_flag as default_gcn_flag
from cvr_events_gcn x
left join cvr_events_hicl y on x.hicl_seqno=y.hicl_seqno
left join final_default_flag z on x.gcn=z.gcn
inner join bh01_mac mac on x.gcn=mac.gcn)

, ffm_rn_1 as (select distinct first_fill_month::date from fifo.fta_gcn_seqno_12_month_value where first_fill_month is not null)

, ffm_rn_2 as (select first_fill_month,row_number() over (order by first_fill_month desc) as ffm_rn from ffm_rn_1)

, gcn_12mo_value_1 as (
select y.gcn as first_fill_gcn
,x.first_fill_month::date
,max(x.accounts)::float as accounts
,max(x.accounts)::float*max(x.normalized_scripts_per_account)::float as wtd_normalized_scripts
,max(x.accounts)::float*max(x.normalized_days_supply_30s_per_account)::float as wtd_normalized_30ds_scripts
,max(x.accounts)::float*max(x.normalized_margin_per_account)::float as wtd_normalized_gp
,max(x.accounts)::float*max(x.normalized_revenue_per_account)::float as wtd_normalized_revenue
,max(x.accounts)::float*max(x.normalized_count_of_gcn_seqno_per_account)::float as wtd_normalized_count_of_gcn_seqnos
from fifo.fta_gcn_seqno_12_month_value x
left join dwh.dim_gcn_seqno_hierarchy y on x.first_fill_gcn_seqno=y.gcn_seqno 
left join ffm_rn_2 z on x.first_fill_month::date=z.first_fill_month::date where z.ffm_rn >= 1 and z.ffm_rn <= 6
group by 1,2)

, gcn_12mo_value_2 as (
select first_fill_gcn
,count(distinct first_fill_month) as first_fill_month_count
,sum(accounts) as nfp_sample_size
,sum(wtd_normalized_scripts)::float/sum(accounts)::float as normalized_12_mos_scripts_per_nfp
,sum(wtd_normalized_30ds_scripts)::float/sum(accounts)::float as normalized_12_mos_30ds_scripts_per_nfp
,sum(wtd_normalized_revenue)::float/sum(accounts)::float as normalized_12_mos_revenue_per_nfp
,sum(wtd_normalized_gp)::float/sum(accounts)::float as normalized_12_mos_gp_per_nfp
,sum(wtd_normalized_count_of_gcn_seqnos)::float/sum(accounts)::float as normalized_count_of_gcn_seqno_per_nfp
from gcn_12mo_value_1
group by 1)

, hicl_seqno_form_12mo_value_1 as (
select y.hicl_seqno||y.dosage_form_desc as first_fill_hicl_seqno_form
,x.first_fill_month::date
,max(x.accounts)::float as accounts
,max(x.accounts)::float*max(x.normalized_scripts_per_account)::float as wtd_normalized_scripts
,max(x.accounts)::float*max(x.normalized_days_supply_30s_per_account)::float as wtd_normalized_30ds_scripts
,max(x.accounts)::float*max(x.normalized_margin_per_account)::float as wtd_normalized_gp
,max(x.accounts)::float*max(x.normalized_revenue_per_account)::float as wtd_normalized_revenue
,max(x.accounts)::float*max(x.normalized_count_of_gcn_seqno_per_account)::float as wtd_normalized_count_of_gcn_seqnos
from fifo.fta_gcn_seqno_12_month_value x
left join dwh.dim_gcn_seqno_hierarchy y on x.first_fill_gcn_seqno=y.gcn_seqno 
left join ffm_rn_2 z on x.first_fill_month::date=z.first_fill_month::date where z.ffm_rn >= 1 and z.ffm_rn <= 6
group by 1,2)

, hicl_seqno_form_12mo_value_2 as (
select first_fill_hicl_seqno_form
,count(distinct first_fill_month) as first_fill_month_count
,sum(accounts) as nfp_sample_size
,sum(wtd_normalized_scripts)::float/sum(accounts)::float as normalized_12_mos_scripts_per_nfp
,sum(wtd_normalized_30ds_scripts)::float/sum(accounts)::float as normalized_12_mos_30ds_scripts_per_nfp
,sum(wtd_normalized_revenue)::float/sum(accounts)::float as normalized_12_mos_revenue_per_nfp
,sum(wtd_normalized_gp)::float/sum(accounts)::float as normalized_12_mos_gp_per_nfp
,sum(wtd_normalized_count_of_gcn_seqnos)::float/sum(accounts)::float as normalized_count_of_gcn_seqno_per_nfp
from hicl_seqno_form_12mo_value_1
group by 1)


, hicl_12mo_value_1 as (
select y.hicl_seqno as first_fill_hicl_seqno
,x.first_fill_month::date
,max(x.accounts)::float as accounts
,max(x.accounts)::float*max(x.normalized_scripts_per_account)::float as wtd_normalized_scripts
,max(x.accounts)::float*max(x.normalized_days_supply_30s_per_account)::float as wtd_normalized_30ds_scripts
,max(x.accounts)::float*max(x.normalized_margin_per_account)::float as wtd_normalized_gp
,max(x.accounts)::float*max(x.normalized_revenue_per_account)::float as wtd_normalized_revenue
,max(x.accounts)::float*max(x.normalized_count_of_gcn_seqno_per_account)::float as wtd_normalized_count_of_gcn_seqnos
from fifo.fta_gcn_seqno_12_month_value x
left join dwh.dim_gcn_seqno_hierarchy y on x.first_fill_gcn_seqno=y.gcn_seqno 
left join ffm_rn_2 z on x.first_fill_month::date=z.first_fill_month::date where z.ffm_rn >= 1 and z.ffm_rn <= 6
group by 1,2)

, hicl_12mo_value_2 as (
select first_fill_hicl_seqno
,count(distinct first_fill_month) as first_fill_month_count
,sum(accounts) as nfp_sample_size
,sum(wtd_normalized_scripts)::float/sum(accounts)::float as normalized_12_mos_scripts_per_nfp
,sum(wtd_normalized_30ds_scripts)::float/sum(accounts)::float as normalized_12_mos_30ds_scripts_per_nfp
,sum(wtd_normalized_revenue)::float/sum(accounts)::float as normalized_12_mos_revenue_per_nfp
,sum(wtd_normalized_gp)::float/sum(accounts)::float as normalized_12_mos_gp_per_nfp
,sum(wtd_normalized_count_of_gcn_seqnos)::float/sum(accounts)::float as normalized_count_of_gcn_seqno_per_nfp
from hicl_12mo_value_1
group by 1)

, gcn_gpi_mac_conflict_list as (select 
distinct gcn,1 as gpi_gcn_mac_conflict_flg from dwh.dim_gcn_seqno_hierarchy where gcn in (1697,
2221,
2222,
2226,
2227,
2324,
2326,
2371,
2962,
3020,
3034,
3321,
3510,
3512,
3513,
3515,
7462,
7463,
10320,
10772,
10843,
10844,
11700,
15721,
15731,
16386,
17290,
17291,
17795,
17912,
18040,
18754,
18992,
19753,
19757,
20661,
20693,
20706,
20713,
21328,
25665,
25839,
25888,
26637,
27056,
28360,
28478,
28851,
28880,
28882,
30370,
30841,
30842,
30880,
30941,
30942,
31060,
31070,
32806,
32807,
33038,
33456,
33457,
33654,
33665,
33820,
33823,
34995,
35350,
35351,
35793,
35956,
36992,
37172,
37499,
40410,
40411,
40843,
40850,
41820,
43636,
44520,
45360,
46032,
47131,
48831,
49001,
49291,
60521,
60563,
69500,
70140,
71150,
86211,
86212,
91071,
92420,
92421,
96850,
98590,
98592,
98694,
99040,
99042))

, wmt_4_dollar_list_gcn as (select distinct 
gcn
,wmt_flg
,qty1
,price1
,qty2
,price2
,effective_date
from git_data_import.wmt_4_dollar_list_gcn 
where effective_date::date = '2018-07-27')

, core2 as (
select
a.gcn
,a.gcn_seqno
,a.hicl_seqno
,a.hicl_seqno||a.form as hicl_seqno_form
,a.hicl_desc
,bmn.med_name as brand_med_name
,a.gtc_desc
,a.stc_desc
,a.ctc_desc
,a.strength
,a.form
,a.maint_flg
,a.dea_flg
,case when a.stc_desc = 'OPIOID ANALGESICS' then 1 else 0 end as opioid_flg
,coalesce(am.available_med_flg,0) as available_med_flg
,coalesce(tem.tem_available_med_flg,0) as tem_available_med_flg
,a.symphony_2017_scripts
,a.symphony_2017_non_opioid_scripts
,a.symphony_2017_hicl_scripts
,coalesce(a.symphony_2017_pct_of_hicl_scripts,0) as symphony_2017_pct_of_hicl_scripts
,coalesce(bs.blink_pct_of_hicl_scripts,0) as blink_pct_of_hicl_scripts

,grk.blink_gcn_rank
,hgrnk.blink_hicl_gcn_rank
,grk.blink_non_opioid_gcn_rank
,hrk.blink_hicl_rank
,hrk.blink_non_opioid_hicl_rank

,r30_grk.blink_r30_gcn_rank
,r30_hrk.blink_r30_hicl_rank

,coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) as gpi_gcn_mac_list_conflict_flg

,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh01_clm_mac_check,a.bh01_mac_price_raw) else a.bh01_mac_price_raw end as bh01_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac.r30_bh01_mac_price is not null then mac.r30_bh01_mac_price else 0 end as r30_bh01_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac.r30_bh01_mac_price is not null and a.bh01_mac_price_raw is not null then (a.bh01_mac_price_raw::float/nullif(mac.r30_bh01_mac_price::float,0))-1 else 0 end as r30_bh01_mac_shift_pct
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac_ltd.ltd_bh01_mac_price is not null then mac_ltd.ltd_bh01_mac_price else 0 end ltd_bh01_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac_ltd.ltd_bh01_mac_price is not null and a.bh01_mac_price_raw is not null then (a.bh01_mac_price_raw::float/nullif(mac_ltd.ltd_bh01_mac_price::float,0))-1 else 0 end as ltd_bh01_mac_shift_pct

,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh02_clm_mac_check,a.bh02_mac_price_raw) else a.bh02_mac_price_raw end as bh02_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac02.r30_bh02_mac_price is not null then mac02.r30_bh02_mac_price else 0 end as r30_bh02_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac02.r30_bh02_mac_price is not null and a.bh02_mac_price_raw is not null then (a.bh02_mac_price_raw::float/nullif(mac02.r30_bh02_mac_price::float,0))-1 else 0 end as r30_bh02_mac_shift_pct
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac02_ltd.ltd_bh02_mac_price is not null then mac02_ltd.ltd_bh02_mac_price else 0 end ltd_bh02_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac02_ltd.ltd_bh02_mac_price is not null and a.bh02_mac_price_raw is not null then (a.bh02_mac_price_raw::float/nullif(mac02_ltd.ltd_bh02_mac_price::float,0))-1 else 0 end as ltd_bh02_mac_shift_pct

,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh03_clm_mac_check,a.bh03_mac_price_raw) else a.bh03_mac_price_raw end as bh03_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac03.r30_bh03_mac_price is not null then mac03.r30_bh03_mac_price else 0 end as r30_bh03_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac03.r30_bh03_mac_price is not null and a.bh03_mac_price_raw is not null then (a.bh03_mac_price_raw::float/nullif(mac03.r30_bh03_mac_price::float,0))-1 else 0 end as r30_bh03_mac_shift_pct
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac03_ltd.ltd_bh03_mac_price is not null then mac03_ltd.ltd_bh03_mac_price else 0 end ltd_bh03_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and mac03_ltd.ltd_bh03_mac_price is not null and a.bh03_mac_price_raw is not null then (a.bh03_mac_price_raw::float/nullif(mac03_ltd.ltd_bh03_mac_price::float,0))-1 else 0 end as ltd_bh03_mac_shift_pct

,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end as wmt_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and wmt_mac.r30_wmt_mac_price is not null then wmt_mac.r30_wmt_mac_price else 0 end as r30_wmt_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and wmt_mac.r30_wmt_mac_price is not null and a.wmt_mac_price_raw is not null then (a.wmt_mac_price_raw::float/nullif(wmt_mac.r30_wmt_mac_price::float,0))-1 else 0 end as r30_wmt_mac_shift_pct
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and wmt_mac_ltd.ltd_wmt_mac_price is not null then wmt_mac_ltd.ltd_wmt_mac_price else 0 end ltd_wmt_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and wmt_mac_ltd.ltd_wmt_mac_price is not null and a.wmt_mac_price_raw is not null then (a.wmt_mac_price_raw::float/nullif(wmt_mac_ltd.ltd_wmt_mac_price::float,0))-1 else 0 end as ltd_wmt_mac_shift_pct

,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_hd_syr_clm_mac_check,a.hd_syr_mac_price_raw) else a.hd_syr_mac_price_raw end as hd_syr_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and hd_syr_mac.r30_hd_syr_mac_price is not null then hd_syr_mac.r30_hd_syr_mac_price else 0 end as r30_hd_syr_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and hd_syr_mac.r30_hd_syr_mac_price is not null and a.hd_syr_mac_price_raw is not null then (a.hd_syr_mac_price_raw::float/nullif(hd_syr_mac.r30_hd_syr_mac_price::float,0))-1 else 0 end as r30_hd_syr_mac_shift_pct
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and hd_syr_mac_ltd.ltd_hd_syr_mac_price is not null then hd_syr_mac_ltd.ltd_hd_syr_mac_price else 0 end ltd_hd_syr_mac_price
,case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 0 and hd_syr_mac_ltd.ltd_hd_syr_mac_price is not null and a.hd_syr_mac_price_raw is not null then (a.hd_syr_mac_price_raw::float/nullif(hd_syr_mac_ltd.ltd_hd_syr_mac_price::float,0))-1 else 0 end as ltd_hd_syr_mac_shift_pct

,a.pac_unit
,a.pac_low_unit
,a.pac_high_unit
,a.pac_retail_unit

,coalesce(b.r30_bh01_clm_mac_check,0) as r30_bh01_clm_mac_check
,coalesce(b.r30_bh02_clm_mac_check,0) as r30_bh02_clm_mac_check
,coalesce(b.r30_bh03_clm_mac_check,0) as r30_bh03_clm_mac_check
,coalesce(b.r30_wmt_clm_mac_check,0) as r30_wmt_clm_mac_check
,coalesce(b.r30_hd_syr_clm_mac_check,0) as r30_hd_syr_clm_mac_check

,b.r90_wmt_unc_unit_cost
,b.r90_pblx_unc_unit_cost 
,b.r90_beh_unc_unit_cost
,unc.unc_unit_site_price
,b.awp_unit_cost
,case when b.awp_unit_cost is not null then ((b.awp_unit_cost::float-(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh01_clm_mac_check,a.bh01_mac_price_raw) else a.bh01_mac_price_raw end)::float)::float/nullif(b.awp_unit_cost::float,0))::float else 0 end as ger_bh01_mac

,1.00 as bh01_dispensing_fee
,1.00 as bh02_dispensing_fee
,1.00 as bh03_dispensing_fee
,1.00 as wmt_dispensing_fee
,1.50 as hd_syr_dispensing_fee

,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh01_clm_mac_check,a.bh01_mac_price_raw) else a.bh01_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float as bh01_mac_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh01_clm_mac_check,a.bh01_mac_price_raw) else a.bh01_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00 as bh01_cogs_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh01_clm_mac_check,a.bh01_mac_price_raw) else a.bh01_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00+.3+((coalesce(c.default_quantity,d.quantity)::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)*0.024 as bh01_edlp_cogs_cc_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_bh01_clm_mac_check,a.bh01_mac_price_raw) else a.bh01_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00+.3+((coalesce(c.default_quantity,d.quantity)::float*a.bsd_unit_price::float)::float+a.bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float)*0.024 as bh01_bsd_cogs_cc_per_script

,case when b.awp_unit_cost is not null then ((b.awp_unit_cost::float-(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float)::float/nullif(b.awp_unit_cost::float,0))::float else 0 end as ger_wmt_mac
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float as wmt_mac_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00 as wmt_cogs_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00+.3+((coalesce(c.default_quantity,d.quantity)::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)*0.024 as wmt_cogs_cc_per_script

,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_hd_syr_clm_mac_check,a.hd_syr_mac_price_raw) else a.hd_syr_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float as hd_syr_mac_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_hd_syr_clm_mac_check,a.hd_syr_mac_price_raw) else a.hd_syr_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00 as hd_syr_cogs_per_script
,(case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_hd_syr_clm_mac_check,a.hd_syr_mac_price_raw) else a.hd_syr_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float+1.00+.3+((coalesce(c.default_quantity,d.quantity)::float*a.hd_unit_price::float)::float+a.hd_dispensing_fee::float+hd_dispensing_fee_margin::float)*0.024 as hd_syr_cogs_cc_per_script


-- ,case when wmt.price2 is not null or wmt.price1 is not null then ((coalesce(wmt.price2,wmt.price1)::float-1.00)::float/nullif(coalesce(wmt.qty2,wmt.qty1)::float,0)) 
--       when b.r90_wmt_unc_unit_cost > 0 and b.r90_wmt_unc_unit_cost::float < (case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float then b.r90_wmt_unc_unit_cost::float
--       else (case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float end as wmt_mac_unc_adjusted

-- ,case when wmt.price2 is not null or wmt.price1 is not null then ((coalesce(wmt.price2,wmt.price1)::float-1.00)::float/nullif(coalesce(wmt.qty2,wmt.qty1)::float,0))*coalesce(c.default_quantity,d.quantity)::float 
--       when b.r90_wmt_unc_unit_cost > 0 and b.r90_wmt_unc_unit_cost::float < (case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float then b.r90_wmt_unc_unit_cost::float*coalesce(c.default_quantity,d.quantity)::float
--       else (case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float end as wmt_mac_per_script_unc_adjusted

-- ,case when wmt.price2 is not null or wmt.price1 is not null then (((coalesce(wmt.price2,wmt.price1)::float-1.00)::float/nullif(coalesce(wmt.qty2,wmt.qty1)::float,0))*coalesce(c.default_quantity,d.quantity)::float)+1.00 
--       when b.r90_wmt_unc_unit_cost > 0 and b.r90_wmt_unc_unit_cost::float < (case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float then (b.r90_wmt_unc_unit_cost::float*coalesce(c.default_quantity,d.quantity)::float)+1.00
--       else ((case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float)+1.00 end as wmt_cogs_per_script_unc_adjusted

-- ,case when wmt.price2 is not null or wmt.price1 is not null then (((coalesce(wmt.price2,wmt.price1)::float-1.05)::float/nullif(coalesce(wmt.qty2,wmt.qty1)::float,0))*coalesce(c.default_quantity,d.quantity)::float)+1.00+.3+((coalesce(c.default_quantity,d.quantity)::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)*0.024
--       when b.r90_wmt_unc_unit_cost > 0 and b.r90_wmt_unc_unit_cost::float < (case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float then (b.r90_wmt_unc_unit_cost::float*coalesce(c.default_quantity,d.quantity)::float)+1.05+.3+((coalesce(c.default_quantity,d.quantity)::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)*0.024
--       else ((case when coalesce(gpi_gcn_mac.gpi_gcn_mac_conflict_flg,0) = 1 then coalesce(b.r30_wmt_clm_mac_check,a.wmt_mac_price_raw) else a.wmt_mac_price_raw end)::float*coalesce(c.default_quantity,d.quantity)::float)+1.00+.3+((coalesce(c.default_quantity,d.quantity)::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)*0.024 end as wmt_cogs_cc_per_script_unc_adjusted

,(coalesce(b.r30_bh01_bsd_scripts,0)::float+coalesce(b.r30_bh02_bsd_scripts,0)::float+coalesce(b.r30_bh03_bsd_scripts,0)::float)/nullif(b.r30_scripts,0)::float as bsd_script_pct
,coalesce(b.r30_hd_scripts,0)::float/nullif(b.r30_scripts,0)::float as hd_script_pct
,coalesce(b.r30_wmt_scripts,0)::float/nullif(b.r30_scripts,0)::float as wmt_script_pct
,(coalesce(b.r30_scripts,0)::float-coalesce(b.r30_bh01_bsd_scripts,0)::float-coalesce(b.r30_bh02_bsd_scripts,0)::float-coalesce(b.r30_bh03_bsd_scripts,0)::float-coalesce(b.r30_hd_scripts,0)::float-coalesce(b.r30_wmt_scripts,0)::float)/nullif((coalesce(b.r30_scripts,0)::float),0) as edlp_script_pct

,(coalesce(b.r30_bh01_bsd_nfp_scripts,0)::float+coalesce(b.r30_bh02_bsd_nfp_scripts,0)::float+coalesce(b.r30_bh03_bsd_nfp_scripts,0)::float)/nullif(b.r30_nfp_scripts,0)::float as bsd_nfp_script_pct
,coalesce(b.r30_hd_nfp_scripts,0)::float/nullif(b.r30_nfp_scripts,0)::float as hd_nfp_script_pct
,coalesce(b.r30_wmt_nfp_scripts,0)::float/nullif(b.r30_nfp_scripts,0)::float as wmt_nfp_script_pct
,(coalesce(b.r30_nfp_scripts,0)::float-coalesce(b.r30_bh01_bsd_nfp_scripts,0)::float-coalesce(b.r30_bh02_bsd_nfp_scripts,0)::float-coalesce(b.r30_bh03_bsd_nfp_scripts,0)::float-coalesce(b.r30_hd_nfp_scripts,0)::float-coalesce(b.r30_wmt_nfp_scripts,0)::float)/nullif((coalesce(b.r30_nfp_scripts,0)::float),0) as edlp_nfp_script_pct

,coalesce(a.pac_unit,0)*coalesce(c.default_quantity,d.quantity)::float as pac_per_script
,coalesce(a.pac_low_unit,0)*coalesce(c.default_quantity,d.quantity)::float as pac_low_per_script
,coalesce(a.pac_high_unit,0)*coalesce(c.default_quantity,d.quantity)::float as pac_high_per_script
,coalesce(a.pac_retail_unit,0)*coalesce(c.default_quantity,d.quantity)::float as pac_retail_per_script

,coalesce(b.awp_unit_cost,0)*coalesce(c.default_quantity,d.quantity)::float as awp_per_script

,a.edlp_dispensing_fee
,a.edlp_unit_price
,a.edlp_dispensing_fee_margin

,a.bsd_dispensing_fee 
,a.bsd_unit_price
,a.bsd_dispensing_fee_margin

,a.hd_dispensing_fee 
,a.hd_unit_price
,a.hd_dispensing_fee_margin

,.30 as fixed_stripe_charge_cost
,.024 as variable_stripe_charge_cost
,1.85 as fixed_retail_claims_processing_cost

,coalesce(b.ltd_filling_patients,0) as ltd_filling_patients
,coalesce(b.ltd_scripts,0) as ltd_scripts
,coalesce(b.ltd_qty,0) as ltd_qty

,coalesce(b.ltd_nfp,0) as ltd_nfp
,coalesce(b.ltd_nfp_scripts,0) as ltd_nfp_scripts
,coalesce(b.ltd_nfp_qty,0) as ltd_nfp_qty

,coalesce(b.r30_scripts,0) as r30_scripts
,coalesce(b.r30_qty,0) as r30_qty
,coalesce(b.r30_nfp_scripts,0) as r30_nfp_scripts
,coalesce(b.r30_nfp_qty,0) as r30_nfp_qty

-- Core 4 Channels

,coalesce(b.r30_wmt_scripts,0) as r30_wmt_scripts
,coalesce(b.r30_wmt_qty,0) as r30_wmt_qty
,coalesce(b.r30_wmt_nfp_scripts,0) as r30_wmt_nfp_scripts
,coalesce(b.r30_wmt_nfp_qty,0) as r30_wmt_nfp_qty

,coalesce(b.r30_edlp_scripts,0) as r30_edlp_scripts
,coalesce(b.r30_edlp_qty,0) as r30_edlp_qty
,coalesce(b.r30_edlp_nfp_scripts,0) as r30_edlp_nfp_scripts
,coalesce(b.r30_edlp_nfp_qty,0) as r30_edlp_nfp_qty

,coalesce(b.r30_bsd_scripts,0) as r30_bsd_scripts
,coalesce(b.r30_bsd_qty,0) as r30_bsd_qty
,coalesce(b.r30_bsd_nfp_scripts,0) as r30_bsd_nfp_scripts
,coalesce(b.r30_bsd_nfp_qty,0) as r30_bsd_nfp_qty

,coalesce(b.r30_hd_scripts,0) as r30_hd_scripts
,coalesce(b.r30_hd_qty,0) as r30_hd_qty
,coalesce(b.r30_hd_nfp_scripts,0) as r30_hd_nfp_scripts
,coalesce(b.r30_hd_nfp_qty,0) as r30_hd_nfp_qty

-- Pricing Strategy Subsets 
,coalesce(b.r30_bh01_edlp_scripts,0) as r30_bh01_edlp_scripts
,coalesce(b.r30_bh01_edlp_qty,0) as r30_bh01_edlp_qty
,coalesce(b.r30_bh01_edlp_nfp_scripts,0) as r30_bh01_edlp_nfp_scripts
,coalesce(b.r30_bh01_edlp_nfp_qty,0) as r30_bh01_edlp_nfp_qty

,coalesce(b.r30_bh02_edlp_scripts,0) as r30_bh02_edlp_scripts
,coalesce(b.r30_bh02_edlp_qty,0) as r30_bh02_edlp_qty
,coalesce(b.r30_bh02_edlp_nfp_scripts,0) as r30_bh02_edlp_nfp_scripts
,coalesce(b.r30_bh02_edlp_nfp_qty,0) as r30_bh02_edlp_nfp_qty

,coalesce(b.r30_bh03_edlp_scripts,0) as r30_bh03_edlp_scripts
,coalesce(b.r30_bh03_edlp_qty,0) as r30_bh03_edlp_qty
,coalesce(b.r30_bh03_edlp_nfp_scripts,0) as r30_bh03_edlp_nfp_scripts
,coalesce(b.r30_bh03_edlp_nfp_qty,0) as r30_bh03_edlp_nfp_qty

,coalesce(b.r30_bh01_bsd_scripts,0) as r30_bh01_bsd_scripts
,coalesce(b.r30_bh01_bsd_qty,0) as r30_bh01_bsd_qty
,coalesce(b.r30_bh01_bsd_nfp_scripts,0) as r30_bh01_bsd_nfp_scripts
,coalesce(b.r30_bh01_bsd_nfp_qty,0) as r30_bh01_bsd_nfp_qty

,coalesce(b.r30_bh02_bsd_scripts,0) as r30_bh02_bsd_scripts
,coalesce(b.r30_bh02_bsd_qty,0) as r30_bh02_bsd_qty
,coalesce(b.r30_bh02_bsd_nfp_scripts,0) as r30_bh02_bsd_nfp_scripts
,coalesce(b.r30_bh02_bsd_nfp_qty,0) as r30_bh02_bsd_nfp_qty

,coalesce(b.r30_bh03_bsd_scripts,0) as r30_bh03_bsd_scripts
,coalesce(b.r30_bh03_bsd_qty,0) as r30_bh03_bsd_qty
,coalesce(b.r30_bh03_bsd_nfp_scripts,0) as r30_bh03_bsd_nfp_scripts
,coalesce(b.r30_bh03_bsd_nfp_qty,0) as r30_bh03_bsd_nfp_qty

,coalesce(b.ltd_30_day_scripts,0) as ltd_30_day_scripts
,coalesce(b.ltd_90_day_scripts,0) as ltd_90_day_scripts
,coalesce(b.ltd_30_day_scripts_pct,0) as ltd_30_day_scripts_pct
,coalesce(b.ltd_90_day_scripts_pct,0) as ltd_90_day_scripts_pct

,coalesce(b.r30_30_day_scripts,0) as r30_30_day_scripts
,coalesce(b.r30_90_day_scripts,0) as r30_90_day_scripts
,coalesce(b.r30_30_day_script_pct,0) as r30_30_day_script_pct
,coalesce(b.r30_90_day_script_pct,0) as r30_90_day_script_pct

,coalesce(b.r30_30_day_nfp_scripts,0) as r30_30_day_nfp_scripts
,coalesce(b.r30_90_day_nfp_scripts,0) as r30_90_day_nfp_scripts
,coalesce(b.r30_30_day_nfp_script_pct,0) as r30_30_day_nfp_script_pct
,coalesce(b.r30_90_day_nfp_script_pct,0) as r30_90_day_nfp_script_pct

,coalesce(b.r30_hd_30_day_scripts,0) as r30_hd_30_day_scripts
,coalesce(b.r30_hd_90_day_scripts,0) as r30_hd_90_day_scripts
,coalesce(b.r30_hd_30_day_script_pct,0) as r30_hd_30_day_script_pct
,coalesce(b.r30_hd_90_day_script_pct,0) as r30_hd_90_day_script_pct

,coalesce(b.r30_hd_30_day_nfp_scripts,0) as r30_hd_30_day_nfp_scripts
,coalesce(b.r30_hd_90_day_nfp_scripts,0) as r30_hd_90_day_nfp_scripts
,coalesce(b.r30_hd_30_day_nfp_script_pct,0) as r30_hd_30_day_nfp_script_pct
,coalesce(b.r30_hd_90_day_nfp_script_pct,0) as r30_hd_90_day_nfp_script_pct

,coalesce(c.default_quantity,d.quantity) as default_quantity

,trunc((coalesce(c.default_quantity,d.quantity)::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float,4) as blink_edlp_price
,trunc((coalesce(c.default_quantity,d.quantity)::float*a.bsd_unit_price::float)::float+a.bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float,4) as blink_bsd_price
,trunc((coalesce(c.default_quantity,d.quantity)::float*a.hd_unit_price::float)::float+a.hd_dispensing_fee::float+hd_dispensing_fee_margin::float,4) as blink_hd_price
,trunc((coalesce(c.default_quantity,d.quantity)::float*unc.unc_unit_site_price::float)::float,4) as blink_unc_site_price

,wmt_1.wmt_flg as wmt_2018_07_27_flg
,wmt_1.qty1 as wmt_2018_07_27_qty1
,wmt_1.price1 as wmt_2018_07_27_price1
,wmt_1.qty2 as wmt_2018_07_27_qty2
,wmt_1.price2 as wmt_2018_07_27_price2

,wmt_2.wmt_flg as wmt_2018_11_28_flg
,wmt_2.qty1 as wmt_2018_11_28_qty1
,wmt_2.price1 as wmt_2018_11_28_price1
,wmt_2.qty2 as wmt_2018_11_28_qty2
,wmt_2.price2 as wmt_2018_11_28_price2

,pblx.pblx_flg as pblx_2018_10_12_flg
,pblx.qty1 as pblx_2018_10_12_qty1
,pblx.price1 as pblx_2018_10_12_price1
,pblx.qty2 as pblx_2018_10_12_qty2
,pblx.price2 as pblx_2018_10_12_price2

,case when wmt_2.qty1::float=coalesce(c.default_quantity,d.quantity)::float then wmt_2.price1
      when wmt_2.qty2::float=coalesce(c.default_quantity,d.quantity)::float then wmt_2.price2
        end as wmt_retail_list_comp_price

,coalesce(b.r90_wmt_unc_unit_cost,0)*coalesce(c.default_quantity,d.quantity)::float as wmt_est_unc_price
,coalesce(b.r90_pblx_unc_unit_cost,0)*coalesce(c.default_quantity,d.quantity)::float as pblx_est_unc_price
,coalesce(b.r90_beh_unc_unit_cost,0)*coalesce(c.default_quantity,d.quantity)::float as beh_est_unc_price

,c.scrape_date
,c.min_grx
,c.min_retail_grx
,c.min_major_retail_grx
,c.min_bh_retail_index_grx

,c.min_grx_sf_nyc_suburbs
,c.min_grx_northeast
,c.min_grx_south
,c.min_grx_midwest
,c.min_grx_west

,c.min_hwh_grx
,c.min_cvs_grx
,c.min_wag_grx
,c.min_wmt_grx
,c.min_rad_grx
,c.min_kr_grx
,c.min_sfwy_grx
,c.min_pblx_grx
,c.min_bksh_grx
,c.min_geagle_grx
,c.min_heb_grx

,grx_low.pharmacy as lowest_tracked_price_grx_pharmacy
,grx_low.price as lowest_tracked_grx_price
,grx_low.pharmacy_type as lowest_tracked_grx_pharmacy_type

,qty30ds.quantity as top_30ds_quantity
,case when qty30ds.quantity is not null then trunc((qty30ds.quantity::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float,4) end as blink_edlp_price_30ds
,case when qty30ds.quantity is not null then trunc((qty30ds.quantity::float*a.bsd_unit_price::float)::float+a.bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float,4) end as blink_bsd_price_30ds
,case when qty30ds.quantity is not null then trunc((qty30ds.quantity::float*a.hd_unit_price::float)::float+a.hd_dispensing_fee::float+hd_dispensing_fee_margin::float,4) end as blink_hd_price_30ds

,qty90ds.quantity as top_90ds_quantity
,case when qty90ds.quantity is not null then trunc((qty90ds.quantity::float*a.edlp_unit_price::float)::float+a.edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float,4) end as blink_edlp_price_90ds
,case when qty90ds.quantity is not null then trunc((qty90ds.quantity::float*a.bsd_unit_price::float)::float+a.bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float,4) end as blink_bsd_price_90ds
,case when qty90ds.quantity is not null then trunc((qty90ds.quantity::float*a.hd_unit_price::float)::float+a.hd_dispensing_fee::float+hd_dispensing_fee_margin::float,4) end as blink_hd_price_90ds

,mrs_30ds.last_30ds_qty_scrape_date
,mrs_30ds.last_30ds_qty
,mrs_30ds.min_grx_30ds
,mrs_30ds.min_retail_grx_30ds
,mrs_30ds.min_major_retail_grx_30ds
,mrs_30ds.min_bh_retail_index_grx_30ds
,mrs_30ds.min_hwh_grx_30ds
,mrs_30ds.min_wmt_grx_30ds
,mrs_30ds.min_kr_grx_30ds
,mrs_30ds.min_sfwy_grx_30ds
,mrs_30ds.min_pblx_grx_30ds
,mrs_30ds.min_bksh_grx_30ds
,mrs_30ds.min_geagle_grx_30ds
,mrs_30ds.min_heb_grx_30ds

,mrs_90ds.last_90ds_qty_scrape_date
,mrs_90ds.last_90ds_qty
,mrs_90ds.min_grx_90ds
,mrs_90ds.min_retail_grx_90ds
,mrs_90ds.min_major_retail_grx_90ds
,mrs_90ds.min_bh_retail_index_grx_90ds
,mrs_90ds.min_hwh_grx_90ds
,mrs_90ds.min_wmt_grx_90ds
,mrs_90ds.min_kr_grx_90ds
,mrs_90ds.min_sfwy_grx_90ds
,mrs_90ds.min_pblx_grx_90ds
,mrs_90ds.min_bksh_grx_90ds
,mrs_90ds.min_geagle_grx_90ds
,mrs_90ds.min_heb_grx_90ds

--,case when c.min_grx is not null and ((coalesce(c.default_quantity,d.quantity)::float*a.bsd_unit_price::float)::float+a.bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float)::float > c.min_grx::float then 1 else 0 end as bsd_grx_competitor_alert_flg
--,case when c.min_hwh_grx is not null and c.min_hwh_grx < 999999 and ((coalesce(c.default_quantity,d.quantity)::float*a.hd_unit_price::float)::float+a.hd_dispensing_fee::float+hd_dispensing_fee_margin::float)::float > c.min_hwh_grx::float then 1 else 0 end as hd_hwh_grx_competitor_alert_flg

,gv.nfp_sample_size as gcn_nfp_sample_size
,gv.normalized_12_mos_scripts_per_nfp as gcn_normalized_12_mos_scripts_per_nfp
,gv.normalized_12_mos_30ds_scripts_per_nfp as gcn_normalized_12_mos_30ds_scripts_per_nfp
,gv.normalized_12_mos_revenue_per_nfp as gcn_normalized_12_mos_revenue_per_nfp
,gv.normalized_12_mos_gp_per_nfp as gcn_normalized_12_mos_gp_per_nfp
,gv.normalized_count_of_gcn_seqno_per_nfp as gcn_normalized_count_of_gcn_seqno_per_nfp

,hsf.nfp_sample_size as hicl_form_nfp_sample_size
,hsf.normalized_12_mos_scripts_per_nfp as hicl_form_normalized_12_mos_scripts_per_nfp
,hsf.normalized_12_mos_30ds_scripts_per_nfp as hicl_form_normalized_12_mos_30ds_scripts_per_nfp
,hsf.normalized_12_mos_revenue_per_nfp as hicl_form_normalized_12_mos_revenue_per_nfp
,hsf.normalized_12_mos_gp_per_nfp as hicl_form_normalized_12_mos_gp_per_nfp
,hsf.normalized_count_of_gcn_seqno_per_nfp as hicl_form_normalized_count_of_gcn_seqno_per_nfp

,hv.nfp_sample_size as hicl_nfp_sample_size
,hv.normalized_12_mos_scripts_per_nfp as hicl_normalized_12_mos_scripts_per_nfp
,hv.normalized_12_mos_30ds_scripts_per_nfp as hicl_normalized_12_mos_30ds_scripts_per_nfp
,hv.normalized_12_mos_revenue_per_nfp as hicl_normalized_12_mos_revenue_per_nfp
,hv.normalized_12_mos_gp_per_nfp as hicl_normalized_12_mos_gp_per_nfp
,hv.normalized_count_of_gcn_seqno_per_nfp as hicl_normalized_count_of_gcn_seqno_per_nfp

,cvr.r30_gcn_viewed_product_sessions
,cvr.r30_gcn_purchased_product_sessions
,cvr.r30_gcn_filled_product_sessions

,cvr.r30_gcn_purchased_edlp_product_sessions
,cvr.r30_gcn_purchased_bsd_product_sessions
,cvr.r30_gcn_purchased_hd_product_sessions
,cvr.r30_gcn_filled_edlp_product_sessions
,cvr.r30_gcn_filled_bsd_product_sessions
,cvr.r30_gcn_filled_hd_product_sessions

,cvr.r30_gcn_purchased_cvr
,cvr.r30_gcn_purchased_edlp_cvr
,cvr.r30_gcn_purchased_bsd_cvr
,cvr.r30_gcn_purchased_hd_cvr
,cvr.r30_gcn_filled_cvr
,cvr.r30_gcn_filled_edlp_cvr
,cvr.r30_gcn_filled_bsd_cvr
,cvr.r30_gcn_filled_hd_cvr

,cvr.r30_gcn_purchase_to_fill_rate
,cvr.r30_gcn_edlp_purchase_to_fill_rate
,cvr.r30_gcn_bsd_purchase_to_fill_rate
,cvr.r30_gcn_hd_purchase_to_fill_rate

,cvr.r30_hicl_viewed_product_sessions
,cvr.r30_hicl_purchased_product_sessions
,cvr.r30_hicl_filled_product_sessions

,cvr.r30_hicl_purchased_edlp_product_sessions
,cvr.r30_hicl_purchased_bsd_product_sessions
,cvr.r30_hicl_purchased_hd_product_sessions
,cvr.r30_hicl_filled_edlp_product_sessions
,cvr.r30_hicl_filled_bsd_product_sessions
,cvr.r30_hicl_filled_hd_product_sessions

,cvr.r30_hicl_purchased_cvr
,cvr.r30_hicl_purchased_edlp_cvr
,cvr.r30_hicl_purchased_bsd_cvr
,cvr.r30_hicl_purchased_hd_cvr
,cvr.r30_hicl_filled_cvr
,cvr.r30_hicl_filled_edlp_cvr
,cvr.r30_hicl_filled_bsd_cvr
,cvr.r30_hicl_filled_hd_cvr

,cvr.r30_hicl_purchase_to_fill_rate
,cvr.r30_hicl_edlp_purchase_to_fill_rate
,cvr.r30_hicl_bsd_purchase_to_fill_rate
,cvr.r30_hicl_hd_purchase_to_fill_rate

,cvr.default_gcn_flag

from core1 a
left join claims_base b on a.gcn=b.gcn
left join available_med_flg am on a.gcn=am.gcn
left join tem_available_med_flg tem on a.gcn=tem.gcn
left join scraper_data_3 c on a.gcn=c.gcn
left join top_30ds_qty_3 qty30ds on a.gcn=qty30ds.gcn
left join top_90ds_qty_3 qty90ds on a.gcn=qty90ds.gcn
left join site_unc unc on a.gcn=unc.gcn
left join gcn_gpi_mac_conflict_list gpi_gcn_mac on a.gcn=gpi_gcn_mac.gcn
left join bh01_mac_r30_c mac on a.gcn=mac.gcn
left join bh01_mac_ltd_c mac_ltd on a.gcn=mac_ltd.gcn

left join bh02_mac_r30_c mac02 on a.gcn=mac02.gcn
left join bh02_mac_ltd_c mac02_ltd on a.gcn=mac02_ltd.gcn
left join bh03_mac_r30_c mac03 on a.gcn=mac03.gcn
left join bh03_mac_ltd_c mac03_ltd on a.gcn=mac03_ltd.gcn

left join wmt_mac_r30_c wmt_mac on a.gcn=wmt_mac.gcn
left join wmt_mac_ltd_c wmt_mac_ltd on a.gcn=wmt_mac_ltd.gcn
left join hd_syr_mac_r30_c hd_syr_mac on a.gcn=hd_syr_mac.gcn
left join hd_syr_mac_ltd_c hd_syr_mac_ltd on a.gcn=hd_syr_mac_ltd.gcn
left join mq3 d on a.gcn=d.gcn
left join blink_pct_of_hicl_scripts bs on a.gcn=bs.gcn

left join wmt_4_dollar_list_gcn wmt_1 on a.gcn=wmt_1.gcn and wmt_1.effective_date::date = '2018-07-27'
left join git_data_import.wmt_4_dollar_list_gcn wmt_2 on a.gcn=wmt_2.gcn and wmt_2.effective_date::date = '2018-11-28'
left join git_data_import.pblx_free_750_list_gcn pblx on a.gcn=pblx.gcn and pblx.effective_date::date = '2018-10-12'

left join brand_medname3 bmn on a.gcn=bmn.gcn
left join blink_gcn_rank2 grk on a.gcn=grk.gcn
left join blink_hicl_rank2 hrk on a.hicl_seqno=hrk.hicl_seqno
left join blink_hicl_gcn_rank2 hgrnk on a.gcn=hgrnk.gcn
left join blink_r30_gcn_rank2 r30_grk on a.gcn=r30_grk.gcn
left join blink_r30_hicl_rank2 r30_hrk on a.hicl_seqno=r30_hrk.hicl_seqno
left join scraper_data_lowest_grx_2 grx_low on a.gcn=grx_low.gcn
left join most_recent_30ds_qty_scrape_2 mrs_30ds on a.gcn=mrs_30ds.gcn
left join most_recent_90ds_qty_scrape_2 mrs_90ds on a.gcn=mrs_90ds.gcn
left join gcn_12mo_value_2 gv on a.gcn=gv.first_fill_gcn
left join hicl_seqno_form_12mo_value_2 hsf on a.hicl_seqno||a.form=hsf.first_fill_hicl_seqno_form
left join hicl_12mo_value_2 hv on a.hicl_seqno=hv.first_fill_hicl_seqno
left join cvr_events5 cvr on a.gcn=cvr.gcn)

, core3 as (
select 
gcn
,gcn_seqno
,hicl_seqno
,hicl_seqno_form
,hicl_desc
,brand_med_name
,gtc_desc
,stc_desc
,ctc_desc
,strength
,form
,maint_flg
,dea_flg
,opioid_flg
,available_med_flg
,tem_available_med_flg
,symphony_2017_scripts
,symphony_2017_non_opioid_scripts
,symphony_2017_hicl_scripts
,symphony_2017_pct_of_hicl_scripts
,blink_pct_of_hicl_scripts

,blink_gcn_rank
,blink_hicl_gcn_rank
,blink_non_opioid_gcn_rank
,blink_hicl_rank
,blink_non_opioid_hicl_rank

,blink_r30_gcn_rank
,blink_r30_hicl_rank

,gpi_gcn_mac_list_conflict_flg

,bh01_mac_price 
,r30_bh01_mac_price
,r30_bh01_mac_shift_pct
,ltd_bh01_mac_price
,ltd_bh01_mac_shift_pct

,bh02_mac_price 
,r30_bh02_mac_price
,r30_bh02_mac_shift_pct
,ltd_bh02_mac_price
,ltd_bh02_mac_shift_pct

,bh03_mac_price 
,r30_bh03_mac_price
,r30_bh03_mac_shift_pct
,ltd_bh03_mac_price
,ltd_bh03_mac_shift_pct

,wmt_mac_price 
,r30_wmt_mac_price
,r30_wmt_mac_shift_pct
,ltd_wmt_mac_price
,ltd_wmt_mac_shift_pct

,hd_syr_mac_price
,r30_hd_syr_mac_price
,r30_hd_syr_mac_shift_pct
,ltd_hd_syr_mac_price
,ltd_hd_syr_mac_shift_pct

,r30_bh01_clm_mac_check
,r30_bh02_clm_mac_check
,r30_bh03_clm_mac_check
,r30_wmt_clm_mac_check
,r30_hd_syr_clm_mac_check

,pac_unit
,pac_low_unit
,pac_high_unit
,pac_retail_unit

,r90_wmt_unc_unit_cost
,r90_pblx_unc_unit_cost 
,r90_beh_unc_unit_cost
,awp_unit_cost

,bh01_dispensing_fee
,bh02_dispensing_fee
,bh03_dispensing_fee
,wmt_dispensing_fee
,hd_syr_dispensing_fee

,bh01_mac_per_script
,bh01_cogs_per_script
,bh01_edlp_cogs_cc_per_script
,bh01_bsd_cogs_cc_per_script

,ger_wmt_mac
,wmt_mac_per_script
,wmt_cogs_per_script
,wmt_cogs_cc_per_script

,hd_syr_mac_per_script
,hd_syr_cogs_per_script
,hd_syr_cogs_cc_per_script

-- ,case when wmt_mac_unc_adjusted > wmt_mac_price then 1 else 0 end as wmt_mac_exceeds_unc_flg
-- ,wmt_mac_unc_adjusted
-- ,wmt_mac_per_script_unc_adjusted
-- ,wmt_cogs_per_script_unc_adjusted
-- ,wmt_cogs_cc_per_script_unc_adjusted
 
,pac_per_script
,pac_low_per_script
,pac_high_per_script
,pac_retail_per_script

,awp_per_script

,edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float as edlp_fixed_price
,edlp_unit_price

,bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float as bsd_fixed_price
,bsd_unit_price

,hd_dispensing_fee::float+hd_dispensing_fee_margin::float as hd_fixed_price
,hd_unit_price

,fixed_stripe_charge_cost
,variable_stripe_charge_cost
,fixed_retail_claims_processing_cost

,ltd_filling_patients
,ltd_scripts
,ltd_qty

,ltd_nfp
,ltd_nfp_scripts
,ltd_nfp_qty

,r30_scripts
,r30_qty
,r30_nfp_scripts
,r30_nfp_qty

-- CORE 4 CHANNELS

,r30_wmt_scripts
,r30_wmt_qty
,r30_wmt_nfp_scripts
,r30_wmt_nfp_qty

,r30_edlp_scripts
,r30_edlp_qty
,r30_edlp_nfp_scripts
,r30_edlp_nfp_qty

,r30_bsd_scripts
,r30_bsd_qty
,r30_bsd_nfp_scripts
,r30_bsd_nfp_qty

,r30_hd_scripts
,r30_hd_qty
,r30_hd_nfp_scripts
,r30_hd_nfp_qty

-- PRICING STRATEGY SUBSETS

,r30_bh01_edlp_scripts
,r30_bh01_edlp_qty
,r30_bh01_edlp_nfp_scripts
,r30_bh01_edlp_nfp_qty

,r30_bh02_edlp_scripts
,r30_bh02_edlp_qty
,r30_bh02_edlp_nfp_scripts
,r30_bh02_edlp_nfp_qty

,r30_bh03_edlp_scripts
,r30_bh03_edlp_qty
,r30_bh03_edlp_nfp_scripts
,r30_bh03_edlp_nfp_qty

,r30_bh01_bsd_scripts
,r30_bh01_bsd_qty
,r30_bh01_bsd_nfp_scripts
,r30_bh01_bsd_nfp_qty

,r30_bh02_bsd_scripts
,r30_bh02_bsd_qty
,r30_bh02_bsd_nfp_scripts
,r30_bh02_bsd_nfp_qty

,r30_bh03_bsd_scripts
,r30_bh03_bsd_qty
,r30_bh03_bsd_nfp_scripts
,r30_bh03_bsd_nfp_qty

,ltd_30_day_scripts
,ltd_90_day_scripts
,ltd_30_day_scripts_pct
,ltd_90_day_scripts_pct

,r30_30_day_scripts
,r30_90_day_scripts
,r30_30_day_script_pct
,r30_90_day_script_pct

,r30_30_day_nfp_scripts
,r30_90_day_nfp_scripts
,r30_30_day_nfp_script_pct
,r30_90_day_nfp_script_pct

,r30_hd_30_day_scripts
,r30_hd_90_day_scripts
,r30_hd_30_day_script_pct
,r30_hd_90_day_script_pct

,r30_hd_30_day_nfp_scripts
,r30_hd_90_day_nfp_scripts
,r30_hd_30_day_nfp_script_pct
,r30_hd_90_day_nfp_script_pct

,default_gcn_flag
,default_quantity

,blink_edlp_price
,blink_bsd_price
,blink_hd_price
,blink_unc_site_price

,case when blink_edlp_price is not null and blink_bsd_price is not null then blink_edlp_price::float-blink_bsd_price::float else 0 end as edlp_vs_bsd_gap
,case when blink_edlp_price is not null and blink_hd_price is not null then blink_edlp_price::float-blink_hd_price::float else 0 end as edlp_vs_hd_gap
,case when blink_bsd_price is not null and blink_hd_price is not null then blink_bsd_price::float-blink_hd_price::float else 0 end as bsd_vs_hd_gap

,blink_edlp_price::float-wmt_cogs_cc_per_script::float as edlp_wmt_gp_net_cc
,blink_edlp_price::float-bh01_edlp_cogs_cc_per_script::float as edlp_bh01_gp_net_cc
,blink_bsd_price::float-bh01_bsd_cogs_cc_per_script::float as bsd_bh01_gp_net_cc
,blink_hd_price::float-hd_syr_cogs_cc_per_script::float as hd_gp_net_cc

,wmt_script_pct
,edlp_script_pct
,bsd_script_pct
,hd_script_pct

,wmt_nfp_script_pct
,edlp_nfp_script_pct
,bsd_nfp_script_pct
,hd_nfp_script_pct

,scrape_date
,min_grx
,min_retail_grx
,min_major_retail_grx
,min_bh_retail_index_grx

,min_grx_sf_nyc_suburbs
,min_grx_northeast
,min_grx_south
,min_grx_midwest
,min_grx_west

,min_hwh_grx
,min_cvs_grx
,min_wag_grx
,min_wmt_grx
,min_rad_grx
,min_kr_grx
,min_sfwy_grx
,min_pblx_grx
,min_bksh_grx
,min_geagle_grx
,min_heb_grx

,lowest_tracked_price_grx_pharmacy
,lowest_tracked_grx_price
,lowest_tracked_grx_pharmacy_type

,wmt_2018_07_27_flg
,wmt_2018_07_27_qty1
,wmt_2018_07_27_price1
,wmt_2018_07_27_qty2
,wmt_2018_07_27_price2
 
,wmt_2018_11_28_flg
,wmt_2018_11_28_qty1
,wmt_2018_11_28_price1
,wmt_2018_11_28_qty2
,wmt_2018_11_28_price2

,wmt_retail_list_comp_price

,pblx_2018_10_12_flg
,pblx_2018_10_12_qty1
,pblx_2018_10_12_price1
,pblx_2018_10_12_qty2
,pblx_2018_10_12_price2

,wmt_est_unc_price
,pblx_est_unc_price
,beh_est_unc_price

,case when blink_edlp_price is not null and min_grx is not null and round(blink_edlp_price,2) <= round(min_grx,2) then 1 else 0 end as edlp_grx_price_leader
,case when blink_edlp_price is not null and min_retail_grx is not null and round(blink_edlp_price,2) <= round(min_retail_grx,2) then 1 else 0 end as edlp_min_retail_grx_price_leader
,case when blink_edlp_price is not null and min_major_retail_grx is not null and round(blink_edlp_price,2) <= round(min_major_retail_grx,2) then 1 else 0 end as edlp_min_major_retail_grx_price_leader
,case when blink_edlp_price is not null and min_bh_retail_index_grx is not null and round(blink_edlp_price,2) <= round(min_bh_retail_index_grx,2) then 1 else 0 end as edlp_min_bh_retail_index_grx_price_leader
,case when blink_edlp_price is not null and min_wmt_grx is not null and min_wmt_grx < 999999 and round(blink_edlp_price,2) <= round(min_wmt_grx,2) then 1 else 0 end as edlp_min_wmt_grx_price_leader
,case when blink_edlp_price is not null and wmt_2018_11_28_flg = 1 and wmt_retail_list_comp_price > 0 and round(blink_edlp_price,2) <= round(wmt_retail_list_comp_price,2) then 1 else 0 end as edlp_wmt_retail_leader

,case when blink_edlp_price is not null and min_grx is not null then blink_edlp_price::float-min_grx::float  else 0 end as edlp_vs_min_grx_gap
,case when blink_edlp_price is not null and min_retail_grx is not null then blink_edlp_price::float-min_retail_grx::float  else 0 end as edlp_vs_min_retail_grx_gap
,case when blink_edlp_price is not null and min_major_retail_grx is not null then blink_edlp_price::float-min_major_retail_grx::float  else 0 end as edlp_vs_min_major_retail_grx_gap
,case when blink_edlp_price is not null and min_bh_retail_index_grx is not null then blink_edlp_price::float-min_bh_retail_index_grx::float else 0 end as edlp_vs_min_bh_retail_index_grx_gap
,case when blink_edlp_price is not null and min_wmt_grx is not null and min_wmt_grx < 999999 then blink_edlp_price::float-min_wmt_grx::float else 0 end as edlp_vs_min_wmt_grx_gap

,case when blink_bsd_price is not null and min_grx is not null then blink_bsd_price::float-min_grx::float  else 0 end as bsd_vs_min_grx_gap
,case when blink_bsd_price is not null and min_bh_retail_index_grx is not null then blink_bsd_price::float-min_bh_retail_index_grx::float  else 0 end as bsd_vs_min_bh_retail_index_grx_gap
,case when blink_bsd_price is not null and min_wmt_grx is not null and min_wmt_grx < 999999 then blink_bsd_price::float-min_wmt_grx::float else 0 end as bsd_vs_min_wmt_grx_gap

,case when blink_hd_price is not null and min_grx is not null then blink_hd_price::float-min_grx::float  else 0 end as hd_vs_min_grx_gap
,case when blink_hd_price is not null and min_bh_retail_index_grx is not null then blink_hd_price::float-min_bh_retail_index_grx::float  else 0 end as hd_vs_min_bh_retail_index_grx_gap
,case when blink_hd_price is not null and min_wmt_grx is not null and min_wmt_grx < 999999 then blink_hd_price::float-min_wmt_grx::float else 0 end as hd_vs_min_wmt_grx_gap

,case when blink_hd_price is not null and min_hwh_grx is not null and min_hwh_grx < 999999 then blink_hd_price::float-min_hwh_grx::float else 0 end as hd_vs_hwh_grx_gap

,case when blink_edlp_price is not null and blink_bsd_price is not null then blink_bsd_price::float-blink_edlp_price::float else 0 end as bsd_vs_edlp_price_gap
,case when blink_edlp_price is not null and blink_hd_price is not null then blink_hd_price::float-blink_edlp_price::float else 0 end as hd_vs_edlp_price_gap
,case when blink_bsd_price is not null and blink_hd_price is not null then blink_hd_price::float-blink_bsd_price::float else 0 end as hd_vs_bsd_price_gap

,top_30ds_quantity
,blink_edlp_price_30ds
,blink_bsd_price_30ds
,blink_hd_price_30ds

,top_90ds_quantity
,blink_edlp_price_90ds
,blink_bsd_price_90ds
,blink_hd_price_90ds

,last_30ds_qty_scrape_date
,last_30ds_qty
,min_grx_30ds
,min_retail_grx_30ds
,min_major_retail_grx_30ds
,min_bh_retail_index_grx_30ds
,min_hwh_grx_30ds
,min_wmt_grx_30ds
,min_kr_grx_30ds
,min_sfwy_grx_30ds
,min_pblx_grx_30ds
,min_bksh_grx_30ds
,min_geagle_grx_30ds
,min_heb_grx_30ds

,last_90ds_qty_scrape_date
,last_90ds_qty
,min_grx_90ds
,min_retail_grx_90ds
,min_major_retail_grx_90ds
,min_bh_retail_index_grx_90ds
,min_hwh_grx_90ds
,min_wmt_grx_90ds
,min_kr_grx_90ds
,min_sfwy_grx_90ds
,min_pblx_grx_90ds
,min_bksh_grx_90ds
,min_geagle_grx_90ds
,min_heb_grx_90ds

,gcn_nfp_sample_size
,gcn_normalized_12_mos_scripts_per_nfp
,gcn_normalized_12_mos_30ds_scripts_per_nfp
,gcn_normalized_12_mos_revenue_per_nfp
,gcn_normalized_12_mos_gp_per_nfp
,gcn_normalized_count_of_gcn_seqno_per_nfp

,hicl_form_nfp_sample_size
,hicl_form_normalized_12_mos_scripts_per_nfp
,hicl_form_normalized_12_mos_30ds_scripts_per_nfp
,hicl_form_normalized_12_mos_revenue_per_nfp
,hicl_form_normalized_12_mos_gp_per_nfp
,hicl_form_normalized_count_of_gcn_seqno_per_nfp

,hicl_nfp_sample_size
,hicl_normalized_12_mos_scripts_per_nfp
,hicl_normalized_12_mos_30ds_scripts_per_nfp
,hicl_normalized_12_mos_revenue_per_nfp
,hicl_normalized_12_mos_gp_per_nfp
,hicl_normalized_count_of_gcn_seqno_per_nfp

,gcn_normalized_12_mos_scripts_per_nfp::float*r30_nfp_scripts::float as projected_gcn_12_mos_scripts_from_nfp_scripts
,gcn_normalized_12_mos_30ds_scripts_per_nfp::float*r30_nfp_scripts::float as projected_gcn_12_mos_30ds_normalized_scripts_from_nfp_scripts
,gcn_normalized_12_mos_revenue_per_nfp::float*r30_nfp_scripts::float as projected_gcn_12_mos_revenue_from_nfp_scripts
,gcn_normalized_12_mos_gp_per_nfp::float*r30_nfp_scripts::float as projected_gcn_12_mos_gp_from_nfp_scripts

,r30_gcn_viewed_product_sessions
,r30_gcn_purchased_product_sessions
,r30_gcn_filled_product_sessions

,r30_gcn_purchased_edlp_product_sessions
,r30_gcn_purchased_bsd_product_sessions
,r30_gcn_purchased_hd_product_sessions
,r30_gcn_filled_edlp_product_sessions
,r30_gcn_filled_bsd_product_sessions
,r30_gcn_filled_hd_product_sessions

,r30_gcn_purchased_cvr
,r30_gcn_purchased_edlp_cvr
,r30_gcn_purchased_bsd_cvr
,r30_gcn_purchased_hd_cvr
,r30_gcn_filled_cvr
,r30_gcn_filled_edlp_cvr
,r30_gcn_filled_bsd_cvr
,r30_gcn_filled_hd_cvr

,r30_gcn_purchase_to_fill_rate
,r30_gcn_edlp_purchase_to_fill_rate
,r30_gcn_bsd_purchase_to_fill_rate
,r30_gcn_hd_purchase_to_fill_rate

,r30_hicl_viewed_product_sessions
,r30_hicl_purchased_product_sessions
,r30_hicl_filled_product_sessions

,r30_hicl_purchased_edlp_product_sessions
,r30_hicl_purchased_bsd_product_sessions
,r30_hicl_purchased_hd_product_sessions
,r30_hicl_filled_edlp_product_sessions
,r30_hicl_filled_bsd_product_sessions
,r30_hicl_filled_hd_product_sessions

,r30_hicl_purchased_cvr
,r30_hicl_purchased_edlp_cvr
,r30_hicl_purchased_bsd_cvr
,r30_hicl_purchased_hd_cvr
,r30_hicl_filled_cvr
,r30_hicl_filled_edlp_cvr
,r30_hicl_filled_bsd_cvr
,r30_hicl_filled_hd_cvr

,r30_hicl_purchase_to_fill_rate
,r30_hicl_edlp_purchase_to_fill_rate
,r30_hicl_bsd_purchase_to_fill_rate
,r30_hicl_hd_purchase_to_fill_rate

-- R30 REVENUE

,case when edlp_unit_price > 0 and available_med_flg = 1 then 1 else 0 end as edlp_priced_flg
,case when bsd_unit_price > 0 and available_med_flg = 1 then 1 else 0 end as bsd_priced_flg
,case when (gcn in (10857,10810,10811) or hd_unit_price > 0) and available_med_flg then 1 else 0 end as hd_priced_flg
,case when min_grx > 0 and min_grx < 999999 then 1 else 0 end as grx_tracked_flg

,case when edlp_unit_price > 0 then ((coalesce(r30_wmt_qty,0)::float*edlp_unit_price::float+coalesce(r30_wmt_scripts,0)::float*(edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)::float)) else 0 end as r30_wmt_gross_revenue

,case when edlp_unit_price > 0 then (((coalesce(r30_bh01_edlp_qty,0)::float+coalesce(r30_bh02_edlp_qty,0)::float+coalesce(r30_bh03_edlp_qty,0)::float)*edlp_unit_price::float+
 (coalesce(r30_bh01_edlp_scripts,0)::float+coalesce(r30_bh02_edlp_scripts,0)::float+coalesce(r30_bh03_edlp_scripts,0)::float)*(edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)::float)) else 0 end as r30_edlp_gross_revenue

,case when bsd_unit_price > 0 then (((coalesce(r30_bh01_bsd_qty,0)::float+coalesce(r30_bh02_bsd_qty,0)::float+coalesce(r30_bh03_bsd_qty,0)::float)*bsd_unit_price::float+
 (coalesce(r30_bh01_bsd_scripts,0)::float+coalesce(r30_bh02_bsd_scripts,0)::float+coalesce(r30_bh03_bsd_scripts,0)::float)*(bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float))) else 0 end as r30_bsd_gross_revenue

,case when gcn in (10857,10810,10811) or hd_unit_price > 0 then ((coalesce(r30_hd_qty,0)::float*hd_unit_price::float+
 coalesce(r30_hd_scripts,0)::float*(hd_dispensing_fee::float+hd_dispensing_fee_margin::float)::float)) else 0 end as r30_hd_gross_revenue


,case when edlp_unit_price > 0 then ((coalesce(r30_bh01_edlp_qty,0)::float*edlp_unit_price::float+
 (coalesce(r30_bh01_edlp_scripts,0)::float)*(edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)::float)) else 0 end as r30_edlp_bh01_gross_revenue

,case when edlp_unit_price > 0 then ((coalesce(r30_bh02_edlp_qty,0)::float*edlp_unit_price::float+
 (coalesce(r30_bh02_edlp_scripts,0)::float)*(edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)::float)) else 0 end as r30_edlp_bh02_gross_revenue

,case when edlp_unit_price > 0 then ((coalesce(r30_bh03_edlp_qty,0)::float*edlp_unit_price::float+
 (coalesce(r30_bh03_edlp_scripts,0)::float)*(edlp_dispensing_fee::float+edlp_dispensing_fee_margin::float)::float)) else 0 end as r30_edlp_bh03_gross_revenue

,case when bsd_unit_price > 0 then ((coalesce(r30_bh01_bsd_qty,0)::float*bsd_unit_price::float+
 (coalesce(r30_bh01_bsd_scripts,0)::float)*(bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float)::float)) else 0 end as r30_bsd_bh01_gross_revenue

,case when bsd_unit_price > 0 then ((coalesce(r30_bh02_bsd_qty,0)::float*bsd_unit_price::float+
 (coalesce(r30_bh02_bsd_scripts,0)::float)*(bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float)::float)) else 0 end as r30_bsd_bh02_gross_revenue

,case when bsd_unit_price > 0 then ((coalesce(r30_bh03_bsd_qty,0)::float*bsd_unit_price::float+
 (coalesce(r30_bh03_bsd_scripts,0)::float)*(bsd_dispensing_fee::float+bsd_dispensing_fee_margin::float)::float)) else 0 end as r30_bsd_bh03_gross_revenue


-- R30 COGS


,case when edlp_unit_price > 0 then (coalesce(r30_wmt_qty,0)::float*wmt_mac_price::float+coalesce(r30_wmt_scripts,0)::float*(wmt_dispensing_fee::float)::float) else 0 end as r30_wmt_cogs 

,case when edlp_unit_price > 0 then coalesce(r30_wmt_qty,0)::float*wmt_mac_price::float else 0 end as r30_wmt_mac_paid
,case when edlp_unit_price > 0 and awp_unit_cost > 0 then coalesce(r30_wmt_qty,0)::float*awp_unit_cost::float else 0 end as r30_wmt_awp_filled

,case when edlp_unit_price > 0 and awp_unit_cost > 0 then ((((coalesce(r30_wmt_qty,0)::float*awp_unit_cost::float-coalesce(r30_wmt_qty,0)::float*wmt_mac_price::float)
/nullif((coalesce(r30_wmt_qty,0)::float*awp_unit_cost::float),0))::float-.92)*(coalesce(r30_wmt_qty,0)::float*awp_unit_cost::float)) else 0 end as r30_wmt_ger_true_up

,case when edlp_unit_price > 0 then ((coalesce(r30_bh01_edlp_qty,0)::float*bh01_mac_price::float+coalesce(r30_bh02_edlp_qty,0)::float*bh02_mac_price::float+coalesce(r30_bh03_edlp_qty,0)::float*bh03_mac_price::float)+
 (coalesce(r30_bh01_edlp_scripts,0)::float+coalesce(r30_bh02_edlp_scripts,0)::float+coalesce(r30_bh03_edlp_scripts,0)::float)*(bh01_dispensing_fee::float)::float) else 0 end as r30_edlp_cogs

,case when bsd_unit_price > 0 then ((coalesce(r30_bh01_bsd_qty,0)::float*bh01_mac_price::float+coalesce(r30_bh02_bsd_qty,0)::float*bh02_mac_price::float+coalesce(r30_bh03_bsd_qty,0)::float*bh03_mac_price::float)+
 (coalesce(r30_bh01_bsd_scripts,0)::float+coalesce(r30_bh02_bsd_scripts,0)::float+coalesce(r30_bh03_bsd_scripts,0)::float)*(bh01_dispensing_fee::float)::float) else 0 end as r30_bsd_cogs

,case when gcn in (10857,10810,10811) or hd_unit_price > 0 then (coalesce(r30_hd_qty,0)::float*hd_syr_mac_price::float+
 coalesce(r30_hd_scripts,0)::float*(hd_syr_dispensing_fee::float)::float) else 0 end as r30_hd_cogs

,case when edlp_unit_price > 0 then ((coalesce(r30_bh01_edlp_qty,0)::float*bh01_mac_price::float)+
 coalesce(r30_bh01_edlp_scripts,0)::float*(bh01_dispensing_fee::float)::float) else 0 end as r30_edlp_bh01_cogs

,case when edlp_unit_price > 0 then ((coalesce(r30_bh02_edlp_qty,0)::float*bh02_mac_price::float)+
 coalesce(r30_bh02_edlp_scripts,0)::float*(bh01_dispensing_fee::float)::float) else 0 end as r30_edlp_bh02_cogs

,case when edlp_unit_price > 0 then ((coalesce(r30_bh03_edlp_qty,0)::float*bh03_mac_price::float)+
 coalesce(r30_bh03_edlp_scripts,0)::float*(bh01_dispensing_fee::float)::float) else 0 end as r30_edlp_bh03_cogs

,case when bsd_unit_price > 0 then ((coalesce(r30_bh01_bsd_qty,0)::float*bh01_mac_price::float)+
 coalesce(r30_bh01_bsd_scripts,0)::float*(bh01_dispensing_fee::float)::float) else 0 end as r30_bsd_bh01_cogs

,case when bsd_unit_price > 0 then ((coalesce(r30_bh02_bsd_qty,0)::float*bh02_mac_price::float)+
 coalesce(r30_bh02_bsd_scripts,0)::float*(bh01_dispensing_fee::float)::float) else 0 end as r30_bsd_bh02_cogs

,case when bsd_unit_price > 0 then ((coalesce(r30_bh03_bsd_qty,0)::float*bh03_mac_price::float)+
 coalesce(r30_bh03_bsd_scripts,0)::float*(bh01_dispensing_fee::float)::float) else 0 end as r30_bsd_bh03_cogs

from core2)

, core4 as (
select *
-- R30 GP

,coalesce(r30_wmt_gross_revenue,0)::float-coalesce(r30_wmt_cogs,0)::float as r30_wmt_gp
,coalesce(r30_wmt_gross_revenue,0)::float-coalesce(r30_wmt_cogs,0)::float-coalesce(r30_wmt_ger_true_up,0)::float as r30_wmt_gp_net_tu

,coalesce(r30_edlp_gross_revenue,0)::float-coalesce(r30_edlp_cogs,0)::float as r30_edlp_gp
,coalesce(r30_bsd_gross_revenue,0)::float-coalesce(r30_bsd_cogs,0)::float as r30_bsd_gp
,coalesce(r30_hd_gross_revenue,0)::float-coalesce(r30_hd_cogs,0)::float as r30_hd_gp

,coalesce(r30_edlp_bh01_gross_revenue,0)::float-coalesce(r30_edlp_bh01_cogs,0)::float as r30_edlp_bh01_gp
,coalesce(r30_edlp_bh02_gross_revenue,0)::float-coalesce(r30_edlp_bh02_cogs,0)::float as r30_edlp_bh02_gp
,coalesce(r30_edlp_bh03_gross_revenue,0)::float-coalesce(r30_edlp_bh03_cogs,0)::float as r30_edlp_bh03_gp

,coalesce(r30_bsd_bh01_gross_revenue,0)::float-coalesce(r30_bsd_bh01_cogs,0)::float as r30_bsd_bh01_gp
,coalesce(r30_bsd_bh02_gross_revenue,0)::float-coalesce(r30_bsd_bh02_cogs,0)::float as r30_bsd_bh02_gp
,coalesce(r30_bsd_bh03_gross_revenue,0)::float-coalesce(r30_bsd_bh03_cogs,0)::float as r30_bsd_bh03_gp

from core3)


, core5 as (
select 
*
,coalesce(r30_wmt_gross_revenue,0)::float+coalesce(r30_edlp_gross_revenue,0)::float+coalesce(r30_bsd_gross_revenue,0)::float+coalesce(r30_hd_gross_revenue,0)::float as r30_gross_revenue
,coalesce(r30_wmt_cogs,0)::float+coalesce(r30_edlp_cogs,0)::float+coalesce(r30_bsd_cogs,0)::float+coalesce(r30_hd_cogs,0)::float as r30_cogs 
,coalesce(r30_wmt_cogs,0)::float+coalesce(r30_wmt_ger_true_up,0)::float+coalesce(r30_edlp_cogs,0)::float+coalesce(r30_bsd_cogs,0)::float+coalesce(r30_hd_cogs,0)::float as r30_cogs_plus_tu

,coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float as r30_gp
,coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float as r30_gp_net_ger_tu

,(coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) as r30_gp_per_script
,(coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) as r30_gp_net_tu_per_script

,coalesce(r30_wmt_gp,0)::float/nullif(coalesce(r30_wmt_scripts,0)::float,0) as r30_wmt_gp_per_script
,coalesce(r30_edlp_gp,0)::float/nullif((coalesce(r30_bh01_edlp_scripts,0)::float+coalesce(r30_bh02_edlp_scripts,0)::float+coalesce(r30_bh01_edlp_scripts,0)::float),0) as r30_edlp_gp_per_script
,coalesce(r30_bsd_gp,0)::float/nullif((coalesce(r30_bh01_bsd_scripts,0)::float+coalesce(r30_bh02_bsd_scripts,0)::float+coalesce(r30_bh01_bsd_scripts,0)::float),0) as r30_bsd_gp_per_script
,coalesce(r30_hd_gp,0)::float/nullif(coalesce(r30_hd_scripts,0)::float,0) as r30_hd_gp_per_script

,coalesce(r30_edlp_bh01_gp,0)::float/nullif(coalesce(r30_bh01_edlp_scripts,0)::float,0) as r30_edlp_bh01_gp_per_script
,coalesce(r30_edlp_bh02_gp,0)::float/nullif(coalesce(r30_bh02_edlp_scripts,0)::float,0) as r30_edlp_bh02_gp_per_script
,coalesce(r30_edlp_bh03_gp,0)::float/nullif(coalesce(r30_bh03_edlp_scripts,0)::float,0) as r30_edlp_bh03_gp_per_script

,coalesce(r30_bsd_bh01_gp,0)::float/nullif(coalesce(r30_bh01_bsd_scripts,0)::float,0) as r30_bsd_bh01_gp_per_script
,coalesce(r30_bsd_bh02_gp,0)::float/nullif(coalesce(r30_bh02_bsd_scripts,0)::float,0) as r30_bsd_bh02_gp_per_script
,coalesce(r30_bsd_bh03_gp,0)::float/nullif(coalesce(r30_bh03_bsd_scripts,0)::float,0) as r30_bsd_bh03_gp_per_script
,coalesce(r30_wmt_gp_net_tu,0)::float/nullif(coalesce(r30_wmt_scripts,0)::float,0) as r30_wmt_gp_net_tu_per_script

, case when coalesce(available_med_flg,0) = 1 and coalesce(r30_scripts,0) = 0 then 'I: Available - No Scripts'
       when coalesce(available_med_flg,0) = 0 and coalesce(r30_scripts,0) = 0 then 'J: Not Available - No Scripts'
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 25 then 'A: More than +$25'
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 10 and (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 25 then 'B: Between +$10 and +$25' 
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 5 and (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 10 then 'C: Between +$5 and +$10' 
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 0 and (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 5 then 'D: Between $0 and +$5' 

       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 0 and (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) > -5 then 'E: Between $0 and -$5' 
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) <= -5 and (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) > -10 then 'F: Between -$5 and -$10' 
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) <= -10 and (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) > -25 then 'G: Between -$10 and -$25' 
       when (coalesce(r30_wmt_gp,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) <= -25 then 'H: Less than -$25'
       else 'NA' end as gp_per_script_bins

, case when coalesce(available_med_flg,0) = 1 and coalesce(r30_scripts,0) = 0 then 'I: Available - No Scripts'
       when coalesce(available_med_flg,0) = 0 and coalesce(r30_scripts,0) = 0 then 'J: Not Available - No Scripts'
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 25 then 'A: More than +$25'
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 10 and (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 25 then 'B: Between +$10 and +$25' 
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 5 and (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 10 then 'C: Between +$5 and +$10' 
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) >= 0 and (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 5 then 'D: Between $0 and +$5' 

       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) < 0 and (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) > -5 then 'E: Between $0 and -$5' 
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) <= -5 and (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) > -10 then 'F: Between -$5 and -$10' 
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) <= -10 and (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) > -25 then 'G: Between -$10 and -$25' 
       when (coalesce(r30_wmt_gp_net_tu,0)::float+coalesce(r30_edlp_gp,0)::float+coalesce(r30_bsd_gp,0)::float+coalesce(r30_hd_gp,0)::float)/nullif(coalesce(r30_scripts::float,0),0) <= -25 then 'H: Less than -$25'
       else 'NA' end as gp_net_tu_per_script_bins

 , case when edlp_priced_flg = 1 and grx_tracked_flg = 1 and round(blink_edlp_price,2) <= round(min_grx,2) then 'Price Leader - Min GRX'
              when edlp_priced_flg = 1 and grx_tracked_flg = 1 and min_wmt_grx > 0 and min_wmt_grx < 999999 and round(blink_edlp_price,2) < round(min_wmt_grx,2) then 'Price Leader - WMT GRX'
              when edlp_priced_flg = 1 and grx_tracked_flg = 1 and min_wmt_grx > 0 and min_wmt_grx < 999999 and edlp_vs_min_wmt_grx_gap < 5 then 'Inside $5 Price Corridor - WMT GRX'
              when edlp_priced_flg = 1 and grx_tracked_flg = 1 and min_wmt_grx > 0 and min_wmt_grx < 999999 and edlp_vs_min_wmt_grx_gap >= 5 then 'Outside $5 Price Corridor - WMT GRX'
              else 'NA'
        end as edlp_price_competitiveness_v1

  , case when edlp_priced_flg = 1 and grx_tracked_flg = 1 and round(blink_edlp_price,2) <= round(min_grx,2) then 'Price Leader - Min GRX'
              when edlp_priced_flg = 1 and grx_tracked_flg = 1 and min_bh_retail_index_grx > 0 and min_bh_retail_index_grx < 999999 and round(blink_edlp_price,2) <= round(min_bh_retail_index_grx,2) then 'Price Leader -  GRX'
              when edlp_priced_flg = 1 and grx_tracked_flg = 1 and min_bh_retail_index_grx > 0 and min_bh_retail_index_grx < 999999 and edlp_vs_min_bh_retail_index_grx_gap < 5 then 'Inside $5 Price Corridor - Big 5 GRX'
              when edlp_priced_flg = 1 and grx_tracked_flg = 1 and min_bh_retail_index_grx > 0 and min_bh_retail_index_grx < 999999 and edlp_vs_min_bh_retail_index_grx_gap >= 5 then 'Outside $5 Price Corridor - Big 5 GRX'
              else 'NA'
        end as edlp_price_competitiveness_v2

 , case when bsd_priced_flg = 1 and grx_tracked_flg = 1 and round(blink_bsd_price,2) < round(min_grx,2) then 'Price Leader - Min GRX'
              when bsd_priced_flg = 1 and grx_tracked_flg = 1 and bsd_vs_min_grx_gap < 5 then 'Inside $5 Price Corridor - Min GRX'
              when bsd_priced_flg = 1 and grx_tracked_flg = 1 and bsd_vs_min_grx_gap >= 5 then 'Outside $5 Price Corridor - Min GRX'
              else 'NA'
        end as bsd_price_competitiveness

 , case when hd_priced_flg = 1 and grx_tracked_flg = 1 and round(blink_hd_price,2) < round(min_grx,2) then 'Price Leader - Min GRX'
              when hd_priced_flg = 1 and grx_tracked_flg = 1 and hd_vs_min_grx_gap < 5 then 'Inside $5 Price Corridor - Min GRX'
              when hd_priced_flg = 1 and grx_tracked_flg = 1 and hd_vs_min_grx_gap >= 5 then 'Outside $5 Price Corridor - Min GRX'
              else 'NA'
        end as hd_price_competitiveness

 ,case when hicl_normalized_12_mos_30ds_scripts_per_nfp::float > 0 and hicl_normalized_12_mos_30ds_scripts_per_nfp::float < 4 then 'Low: Under 4'
              when hicl_normalized_12_mos_30ds_scripts_per_nfp::float >= 4 and hicl_normalized_12_mos_30ds_scripts_per_nfp::float < 8 then 'Upper Mid: Between 4 and 8'
              when hicl_normalized_12_mos_30ds_scripts_per_nfp::float >= 8 and hicl_normalized_12_mos_30ds_scripts_per_nfp::float < 12 then 'Lower Mid: Between 8 and 12'
              when hicl_normalized_12_mos_30ds_scripts_per_nfp::float >= 12 then 'High: Over 12'
              else 'NA'
        end as projected_hicl_12_mos_30ds_script_value_bins

 ,case when hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float > 0 and hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float < 4 then 'Low: Under 4'
              when hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float >= 4 and hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float < 8 then 'Lower Mid: Between 4 and 8'
              when hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float >= 8 and hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float < 12 then 'Upper Mid: Between 8 and 12'
              when hicl_form_normalized_12_mos_30ds_scripts_per_nfp::float >= 12 then 'High: Over 12'
              else 'NA'
        end as projected_hicl_form_12_mos_30ds_script_value_bins

 ,case when gcn_normalized_12_mos_30ds_scripts_per_nfp::float > 0 and gcn_normalized_12_mos_30ds_scripts_per_nfp::float < 4 then 'Low: Under 4'
              when gcn_normalized_12_mos_30ds_scripts_per_nfp::float >= 4 and gcn_normalized_12_mos_30ds_scripts_per_nfp::float < 8 then 'Upper Mid: Between 4 and 8'
              when gcn_normalized_12_mos_30ds_scripts_per_nfp::float >= 8 and gcn_normalized_12_mos_30ds_scripts_per_nfp::float < 12 then 'Lower Mid: Between 8 and 12'
              when gcn_normalized_12_mos_30ds_scripts_per_nfp::float >= 12 then 'High: Over 12'
              else 'NA'
        end as projected_gcn_12_mos_30ds_script_value_bins

, case when coalesce(available_med_flg,0) = 0 then 'Not Available'
       when coalesce(available_med_flg,0) = 1 and coalesce(r30_hicl_viewed_product_sessions,0) = 0 then 'Available - No Sessions'
       when coalesce(r30_hicl_viewed_product_sessions,0) > 0 and coalesce(r30_hicl_viewed_product_sessions,0) < 10000 then 'Under 10K Sessions'
       when coalesce(r30_hicl_viewed_product_sessions,0) >= 10000 then hicl_desc
       else 'NA' end as r30_hicl_viewed_product_sessions_bins_1

, case when coalesce(available_med_flg,0) = 0 then 'F: Not Available'
       when coalesce(available_med_flg,0) = 1 and coalesce(r30_hicl_viewed_product_sessions,0) = 0 then 'E:Available - No Sessions'
       when coalesce(r30_hicl_viewed_product_sessions,0) > 0 and coalesce(r30_hicl_viewed_product_sessions,0) < 1000 then 'A: Under 1K Sessions'
       when coalesce(r30_hicl_viewed_product_sessions,0) >= 1000 and coalesce(r30_hicl_viewed_product_sessions,0) < 5000 then 'B: Between 1K and 5K Sessions'
       when coalesce(r30_hicl_viewed_product_sessions,0) >= 5000 and coalesce(r30_hicl_viewed_product_sessions,0) < 10000 then 'C: Between 5K and 10K Sessions'
       when coalesce(r30_hicl_viewed_product_sessions,0) >= 10000 then 'D: Over 10K Sessions'
       else 'NA' end as r30_hicl_viewed_product_sessions_bins_2

from core4)

, top_gcn_price_competitiveness as (
select distinct hicl_seqno,edlp_price_competitiveness_v1,edlp_price_competitiveness_v2,bsd_price_competitiveness,hd_price_competitiveness from core5 where blink_hicl_gcn_rank = 1)


select 
x.*
,y.edlp_price_competitiveness_v1 as top_gcn_edlp_price_competitiveness_v1
,y.edlp_price_competitiveness_v2 as top_gcn_edlp_price_competitiveness_v2
,y.bsd_price_competitiveness as top_gcn_bsd_price_competitiveness
,y.hd_price_competitiveness as top_gcn_hd_price_competitiveness
from core5 x
left join top_gcn_price_competitiveness y on x.hicl_seqno=y.hicl_seqno
order by x.blink_gcn_rank asc;

GRANT SELECT ON mktg_dev.sdey_generic_price_portfolio_datamart TO "public";

end;
