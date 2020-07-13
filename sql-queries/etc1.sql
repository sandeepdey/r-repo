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
)


SELECT DISTINCT drug_description,
    rewards.gpi,
    gcn_gpi.gcn,
    "30day_qty",
    "90day_qty",
    dispensing_fee_margin,
    unit_price
from 
    pricing_dev.sav_mor_rewards_20200611 rewards
inner join 
    gcn_gpi on gcn_gpi.gpi = rewards.gpi
left outer JOIN
    transactional.med_price on med_price.gcn = gcn_gpi.gcn
    and ended_on is NULL
    and pharmacy_network_id = 6
    and branded = 0 
;



    





create table pricing_dev.sav_mor_rewards_20200611 as 

SELECT DISTINCT
	drug_description,
	"30day_qty",
	"90day_qty",
	tcgpi_id AS gpi
FROM
	pricing_external_dev.sav_mor_rewards_20200611 rewards
	INNER JOIN medispan.mf2tcgpi ON tcgpi_name ILIKE '%' || rewards.drug_description || '%'
	INNER JOIN medispan.mf2name ON mf2tcgpi.tcgpi_id = mf2name.generic_product_identifier;



select 
    generic_name_short,
    strength,

    dgsh.gcn,
    dgsh.medid,
    
    pharmacy_network_id,
    dispensing_fee_margin,
    unit_price    

FROM
    dwh.dim_gcn_seqno_hierarchy dgsh 
INNER JOIN
    transactional.med_price mp
ON
    dgsh.gcn = mp.gcn
    AND dgsh.medid = mp.medid
where 
    mp.ended_on is NULL
    and pharmacy_network_id in (1,5)
    and generic_name_short ilike '%duloxetine%'
    and strength ilike '60 MG'
order by 
    pharmacy_network_id
;

select * from transactional.pharmacy_network;



select * from dwh.fact_cloud_item where program='Optinose';





;
WITH hd_fills AS (
    SELECT
        foi.last_pbm_adjudication_timestamp_approved::timestamp::date AS order_date,
        foi.order_id,
        foi.med_id,
        f_gcn.gcn,
        f_gcn.gcn_seqno,
        foi.pharmacy_network_name,
        -- blink , supersaver or delivery
        foi.last_claim_pharmacy_name_approved AS pharmacy_name,
        sum(1) AS fills,
        sum(quantity) AS quantities,
        sum(coalesce(last_claim_med_price_approved,
                0)::float + coalesce(last_claim_reimburse_program_discount_amount_approved,
                0)::float) AS revenue,
        sum(coalesce(foi.last_pricing_total_cost_approved,
                0)::float + coalesce(foi.last_claim_wmt_true_up_amount_approved,
                0)::float) AS cogs
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
WHERE
    foi.fill_sequence IS NOT NULL
    AND foi.is_fraud = FALSE
    AND foi.last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '180 day' >= CURRENT_DATE
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
), totals as (
    SELECT
        gcn,
        sum(fills) as scripts
    FROM
        hd_fills
    WHERE
        pharmacy_network_name = 'delivery'
    GROUP BY
        1
    ORDER BY
        scripts DESC )

;

select * from mac_pricing.mac_ger_network_targets where  mac_list='BLINKWMT01';


select * from dwh.dim_pharmacy_hierarchy where pharmacy_name ilike '%safewa%';



select * from dwh.dim_pharmacy_hierarchy where pharmacy_npi=1346883881 limit 10;


select pharmacy,count(distinct(gcn)) as gcns,count(*) as rows from pricing_external_dev.goodrx_raw_data 
WHERE
    date = '2020-05-12' 
    AND gcn IS NOT NULL 
--     AND pharmacy ilike '%albert%' or pharmacy ilike '%safeway%'
group by 1 
;

SELECT * from pricing_external_dev.goodrx_raw_data where date='2020-05-12' and geo='houston' and pharmacy='other_pharmacies' and slug='atorvastatin' and dosage='40mg' and quantity=30;



select * from transactional.pharmacy_network;


with gcn_gpi as (
	select 
		ms_n.generic_product_identifier as gpi
		,gcn.gcn as gcn 
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
), gcns as (
select distinct gcn from gcn_gpi where gpi in ('12604075002120','21100040002105','93000045002010','21101020002130','44201040102005','28100010102112','66100030102105','21100028002130','13000040000310','28100010102107','21101020002125','21101020002120','30908050102060','11500050102130','21300003001920','21700008102030','21300020002105','83337015002020','28100010102103','11500050102120','09000030000105','16270030002130','21755040102170','21300034102160','21200010102115','75200010102105','21300025102120','01300050102131','01300040102127','21300050102150','21500005002040','79993002202020','77204030002010','60100055102010','21500005001317','02200060102115','21300025102020','01200020302132','01990002702150','84100010000320','35100020102020','37100010102105','21500005002030','21200055001320','21200055001325','21200055001330','21300007002015','86330015002020','16280080102125','21200030102025','21550080102020','29000020102005','65100050102005','21500005002050','21700020002110','21755050102021','21755050102030','49270025202140','50250035102010','01300050102120','90550070003710','64991002120105','16220010102005','34000030102005','01300040102115','16100010102105','01300050102115','35100020102010','01300040102105','49102030002010','49102030002012','49102030002013','49102030002014','74200060102120','16280080102115','02200060102110','59200055202010','79400010402040','21101020000110','75100080102005','36400010102005','22100050101810','83100020202045','74200060102105','21101020000105','02200060102105','83100020202034','22100020202011','33100040102005','16280080102110','49101010102070','21101025002025','30042060102012','07000010102011','01200020302115','13000010200305','77101010102005','07000010102013','36100025102210','21101025002030','57200030002005','30042060102006','65100045102010','65100055102058','79600020102030','65100055102060','65100055102059','31200010002040','79050020002010','01200020302110','79100010002010','66100037102015','21300034102040','21300034102060','21300034102020','21755040102056','79600010052030','60201025102005','74200010102020','74200010102010','16990002302010','93200040002025','93200040002030','13000030100310','21300010002011','60201025102006','35400005002030','60201025102010','38000020102010','12405010102030','44300010002010','60201025102011','69100070102040','84100010002005','35400005002040','35400005002050','65100025102037','65100025102012','72200030052005','65100025102022','07000070102034','07000070102038','65100025102042','72600043052040','65100055100315','49200030002015','60201025102003','33300010102005','60201025102008','72600043052030','31100030102040','31100030102030','31100030102050','60201025102004','34000010102025','34000010102030','34000010102040','79400010402065','83100020202015','72600043052020','38000010102005','79400010402045','60201025102002','21100020002020','21100020002025','79700010002020','79400010412032','38000010112040','79050020002025','79700030002005','83100020356420','79400010402020','79750010002050','79050010002010','31100030112060','38000010112010','31100030112040','38000010112020','38000020112030','37400030002020','38000020112020','05000034112024','79400010402050','11407015012010','38000020112010','07000020112025','79400010402055','83100020302030','35200020112030','79993003102050','79750010002030','79993002102020','83100020222033','79993003102027','35200020112020','79993003102038','79993003102015','79993003102020','83100020222037','79750010002040','79992002102015','79993002202025','79992001302010','99750015002000','80100020002060','43995702301210','37400030002025','11407015012020','80100020002020','80100020002050','05000020112024','56700040002005','79750010002021','80100020002015','79992002102020','79992001202010','79993002202030','79993003102025','79993002302020','79993002202035')) 

select * from dwh.dim_gcn_seqno_hierarchy dgsh INNER join gcns on gcns.gcn = dgsh.gcn ;

SELECT
    gcn,
    quantity,quantity*uou_multiplier,
    min(iif_(pharmacy='brookshires',price,10000000)) brookshires_prices,
    max(iif_(pharmacy='other_pharmacies',price,-1)) independents_prices
FROM
    pricing_external_dev.goodrx_raw_data
WHERE
    date > '2020-05-01'
    AND geo = 'tyler_tx'
    AND gcn in ('1772',
'2363',
'4750',
'10194',
'10194',
'10194',
'10194',
'14007',
'14280',
'14853',
'20736',
'22880',
'22913',
'23831',
'26322',
'26322',
'26323',
'26323',
'26324',
'26324',
'26587',
'31630',
'31640',
'31640',
'50272',
'68811',
'95347')
GROUP BY
    1,
    2,
    3
ORDER BY
    1,
    2,
    3;
	
select gcn,quantity,uou_multiplier,min(price) from pricing_external_dev.goodrx_raw_data where date > '2020-05-01'
    AND geo = 'tyler_tx' and pharmacy='brookshires'
    AND gcn in ('26324',
'10194',
'26322',
'10194',
'22913',
'26322',
'14007',
'31640',
'26324',
'23831',
'13975',
'1772',
'54201',
'22880',
'86211',
'32480',
'32481',
'31630',
'14280',
'26587') group by 1,2,3;



(select distinct gcn,quantity
	FROM
		fifo.magic_fact_order_claim where quantity> 0)
		




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

