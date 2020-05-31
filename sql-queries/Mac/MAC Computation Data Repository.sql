-- Get Medispan Data At GCI Level

drop table if exists pricing_dev.mac_automation_gpi_medispan_list;
create table pricing_dev.mac_automation_gpi_medispan_list AS 
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
	AND NOT lower(gpi) like  '%e%'
GROUP BY
	1
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
GRANT SELECT ON pricing_dev.mac_automation_pac_data_gpi TO "public";
GRANT ALL ON pricing_dev.mac_automation_pac_data_gpi TO scott, sandeepd;
select count(*) from pricing_dev.mac_automation_pac_data_gpi;

-- Get Initial Blended Mac Data for Generic Drugs

drop table if exists pricing_dev.mac_automation_init_mac_blended_weights;
create table pricing_dev.mac_automation_init_mac_blended_weights as 
SELECT 
	gpi,
	label_name,
	pac,
	pac_low,
	pac_high,
	pac_retail,
	mac_list,
	pac_low*pac_low_wt + pac*pac_wt + pac_high*pac_high_wt + pac_retail*pac_retail_wt  as initial_blend_mac
FROM
	pricing_dev.mac_automation_pac_data_gpi
	LEFT OUTER JOIN pricing_dev.mac_blended_weights ON 1=1 -- Makes a Full Outer Join
;
GRANT SELECT ON pricing_dev.mac_automation_init_mac_blended_weights TO "public";
GRANT ALL ON pricing_dev.mac_automation_init_mac_blended_weights TO scott, sandeepd;
select count(*) from pricing_dev.mac_automation_init_mac_blended_weights;


-- Get Utilization Data for Generic Drugs

drop table if exists pricing_dev.mac_automation_claim_data;
create table pricing_dev.mac_automation_claim_data as 
SELECT
	transactional_claim_id,
	last_pbm_adjudication_timestamp_approved::date,
	last_claim_pharmacy_npi_approved,
	ncpdp_relationship_id,
	ncpdp_relationship_name,
	dim_ndc_hierarchy.gpi as gpi,
	last_claim_quantity_approved,
	CASE 
		WHEN last_claim_pharmacy_npi_approved=1811906720  THEN 'BLINKSYRx01'
		WHEN drug_price_list is null THEN 'BLINK01'
		ELSE drug_price_list
	END AS mac_list,
	last_claim_days_supply_approved,
	last_pricing_total_cost_approved,
	order_claim.last_claim_gcn_approved,
	last_claim_awp_unit_price_approved,
	last_claim_awp_amount_approved,
	last_pricing_ingredient_cost_approved,
	last_pricing_unc_cost_approved,
	last_pricing_dispensing_fee_approved,
	case when medispan_multi_source_code = 'Y' then 'Generic' else 'Brand' end  AS brand_generic_type,
	CASE
		WHEN COALESCE(last_pricing_ingredient_cost_approved,0)+COALESCE(last_pricing_dispensing_fee_approved,0)=COALESCE(last_pricing_unc_cost_approved,0) THEN TRUE
		ELSE FALSE
	END AS usual_and_customary,
	NOT(mac_automation_gpi_medispan_list.gpi is NULL) AS is_mac_gpi
FROM dwh.fact_order_item  AS order_claim
LEFT JOIN dwh.dim_ndc_hierarchy  AS dim_ndc_hierarchy ON order_claim.last_claim_ndc_approved = dim_ndc_hierarchy.ndc
LEFT JOIN dwh.dim_user  AS dim_user ON order_claim.dw_user_id = dim_user.dw_user_id and order_claim.account_id = dim_user.account_id
LEFT JOIN dwh.dim_pharmacy_hierarchy  AS pharmacy ON order_claim.last_claim_pharmacy_npi_approved = pharmacy.pharmacy_npi
LEFT JOIN pricing_dev.blink_network_20200127v2 ON ncpdp_relationship_id = chain_code
LEFT JOIN pricing_dev.mac_automation_gpi_medispan_list ON mac_automation_gpi_medispan_list.gpi = dim_ndc_hierarchy.gpi
WHERE order_claim.last_pbm_adjudication_timestamp_approved::date >= '2019-01-01' --CURRENT_DATE - 180 
 	AND dim_user.is_internal = false 
 	AND dim_user.is_phantom = false 
 	AND order_claim.is_fraud = FALSE
;
GRANT SELECT ON pricing_dev.mac_automation_claim_data TO "public";
GRANT ALL ON pricing_dev.mac_automation_claim_data TO scott, sandeepd;
select count(*) from pricing_dev.mac_automation_claim_data;

select last_pbm_adjudication_timestamp_approved::date, count(*) from pricing_dev.mac_automation_claim_data group by 1 order by 1 desc;


-- For each Network get Sum AWP, GER Perf Target , Frozen _ Blended Init Macs, None Frozen _ Blended Init Macs, 
-- Formulae : Adjusted Discount = 1 -  [sum_awp ( 1 - ger perf target ) - Sum _ Frozen _ Mac] / [Sum _ Non Frozen _ Mac]
drop table if exists pricing_dev.mac_automation_applicable_utilization;
create table pricing_dev.mac_automation_applicable_utilization as
WITH applicable_utlization AS (
	SELECT
		gpi,	
		brand_generic_type,
	 	usual_and_customary,
	 	is_mac_gpi,
	 	False AS is_walmart_4_10,
		CASE 
			WHEN mac_list IN ('BLINK01','BLINK02','BLINK03') THEN 'BLINK' 
			ELSE mac_list
		END AS utilization,
		sum(coalesce(last_claim_quantity_approved,0)) claim_quantity,
		sum(coalesce(last_claim_awp_amount_approved,0)) awp_amount
	FROM
		pricing_dev.mac_automation_claim_data
	group BY
		1,2,3,4,5,6)
SELECT
		mac_blended_weights.mac_list,
		applicable_utlization.gpi,	
		brand_generic_type,
	 	usual_and_customary,
	 	is_mac_gpi,
	 	is_walmart_4_10,
		claim_quantity,
		awp_amount,
		mac_frozen_list.gpi is NOT NULL AS is_frozen,
		CASE
			WHEN mac_frozen_list.gpi is NULL THEN mac_automation_init_mac_blended_weights.initial_blend_mac
			ELSE mac_frozen_list.mac
		END AS initial_blend_mac
FROM
	pricing_dev.mac_blended_weights
	LEFT OUTER JOIN applicable_utlization ON 1=1 
	LEFT OUTER JOIN pricing_dev.mac_automation_init_mac_blended_weights 
		ON mac_automation_init_mac_blended_weights.mac_list = mac_blended_weights.mac_list
		AND mac_automation_init_mac_blended_weights.gpi = applicable_utlization.gpi
	LEFT OUTER JOIN pricing_dev.mac_frozen_list 
		ON mac_frozen_list.mac_list = mac_frozen_list.mac_list
		AND mac_automation_init_mac_blended_weights.gpi = mac_frozen_list.gpi
	WHERE
		( (utilization='BLINK' AND mac_blended_weights.mac_list in ('BLINK01','BLINK02','BLINK03')) 
		OR mac_blended_weights.mac_list=utilization)
		AND is_mac_gpi
		AND brand_generic_type = 'Generic'
;
GRANT SELECT ON pricing_dev.mac_automation_applicable_utilization TO "public";
select count(*) from pricing_dev.mac_automation_applicable_utilization;


with data as (
	SELECT
		mac_automation_applicable_utilization.mac_list, 
		ger_target,
		sum(awp_amount) as AWP, 
		sum(iif_(is_frozen,initial_blend_mac*claim_quantity,0)) as frozen_mac_rev , 		
		sum(iif_(not is_frozen,initial_blend_mac*claim_quantity,0)) as non_frozen_mac_rev
	FROM
		pricing_dev.mac_automation_applicable_utilization
	inner JOIN
		pricing_dev.mac_ger_network_targets ON mac_ger_network_targets.mac_list = mac_automation_applicable_utilization.mac_list
	group by 
		1,2)
select 
	*, 1 -  (( AWP * ( 1 - ger_target ) - frozen_mac_rev) / non_frozen_mac_rev) as adjustment
from 
	data
;






-- DROP TABLE IF EXISTS pricing_dev.mac_computation_gpi_medispan_list;
-- CREATE TABLE pricing_dev.mac_computation_gpi_medispan_list AS
-- SELECT
-- 	dim_ndc_current_price_data.gpi AS "dim_ndc_current_price_data.gpi",
-- 	dim_ndc_current_price_data.label_name AS "dim_ndc_current_price_data.label_name",
-- 	dim_ndc_current_price_data.multi_source_code AS "dim_ndc_current_price_data.multi_source_code",
-- 	dim_ndc_current_price_data.repackager_indicator AS "dim_ndc_current_price_data.repackager_indicator",
-- 	dim_ndc_current_price_data.orange_book_code AS "dim_ndc_current_price_data.orange_book_code",
-- 	DATE(CONVERT_TIMEZONE ('UTC', 'America/New_York', dim_ndc_current_price_data.obsolete_date)) AS "dim_ndc_current_price_data.obsolete_date_date"
-- FROM
-- 	dwh.dim_ndc_current_price_data AS dim_ndc_current_price_data
-- WHERE
-- 	dim_ndc_current_price_data.orange_book_code IN('AA', 'AB', 'AN', 'AO', 'AP', 'AT', 'ZA', 'ZC')
-- 	AND dim_ndc_current_price_data.repackager_indicator = '0'
-- 	AND(dim_ndc_current_price_data.obsolete_date >= CURRENT_DATE - 730
-- 		OR dim_ndc_current_price_data.obsolete_date IS NULL)
-- 	AND dim_ndc_current_price_data.multi_source_code = 'Y'
-- GROUP BY
-- 	1,2,3,4,5,6
-- ;


-- select * from dwh.dim_ndc_current_price_data where med_id=206813;

-- GRANT SELECT ON pricing_dev.mac_computation_gpi_medispan_list TO "public";
-- select count(*) from pricing_dev.mac_computation_gpi_medispan_list;


-- 
-- DROP TABLE IF EXISTS pricing_dev.mac_computation_pac_data_gold_standard;
-- CREATE TABLE pricing_dev.mac_computation_pac_data_gold_standard AS
-- SELECT
-- 	gold_standard_pac_ms.drug_identifier  AS "gold_standard_pac_ms.drug_identifier",
-- 	gold_standard_pac_ms.identifier_type  AS "gold_standard_pac_ms.identifier_type",
-- 	gold_standard_pac_ms.brand_generic  AS "gold_standard_pac_ms.brand_generic",
-- 	gold_standard_pac_ms.pac  AS "gold_standard_pac_ms.pac",
-- 	gold_standard_pac_ms.pac_low  AS "gold_standard_pac_ms.pac_low",
-- 	gold_standard_pac_ms.pac_high  AS "gold_standard_pac_ms.pac_high",
-- 	gold_standard_pac_ms.pac_retail  AS "gold_standard_pac_ms.pac_retail",
-- 	gold_standard_pac_ms.error_code  AS "gold_standard_pac_ms.error_code",
-- 	DATE(gold_standard_pac_ms.effective_date ) AS "gold_standard_pac_ms.effective_date",
-- 	DATE(gold_standard_pac_ms.downloaded_date ) AS "gold_standard_pac_ms.downloaded_date",
-- 	gold_standard_pac_ms.pac_model_version  AS "gold_standard_pac_ms.pac_model_version",
-- 	gold_standard_pac_ms.end_date  AS "gold_standard_pac_ms.end_date"
-- FROM gold_standard.pac_ms AS gold_standard_pac_ms
-- WHERE (gold_standard_pac_ms.brand_generic = 'Generic') 
-- 	AND gold_standard_pac_ms.downloaded_date = CURRENT_DATE-1
-- GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;
-- GRANT SELECT ON pricing_dev.mac_computation_pac_data_gold_standard TO "public";
-- select count(*) from pricing_dev.mac_computation_pac_data_gold_standard;



-- DROP TABLE IF EXISTS pricing_dev.mac_computation_utilization_info;
-- CREATE TABLE pricing_dev.mac_computation_utilization_info AS
-- SELECT
-- 	order_claim.transactional_claim_id  AS "order_claim.transactional_claim_id",
-- 	DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', order_claim.last_pbm_adjudication_timestamp_approved )) AS "order_claim.last_pbm_adjudication_timestamp_approved_date",
-- 	TO_CHAR(DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/New_York', order_claim.last_pbm_adjudication_timestamp_approved )), 'YYYY-MM') AS "order_claim.last_pbm_adjudication_timestamp_approved_month",
-- 	order_claim.last_claim_pharmacy_npi_approved  AS "order_claim.last_claim_pharmacy_npi_approved",
-- 	order_claim.last_claim_pharmacy_name_approved  AS "order_claim.last_claim_pharmacy_name_approved",
-- 	pharmacy.ncpdp_relationship_id  AS "pharmacy.ncpdp_relationship_id",
-- 	pharmacy.ncpdp_relationship_name  AS "pharmacy.ncpdp_relationship_name",
-- 	order_claim.last_claim_quantity_approved  AS "order_claim.last_claim_quantity_approved_actual",
-- 	order_claim.last_claim_days_supply_approved  AS "order_claim.last_claim_days_supply_approved",
-- 	order_claim.last_pricing_total_cost_approved  AS "order_claim.last_pricing_total_cost_approved",
-- 	order_claim.last_claim_gcn_approved  AS "order_claim.last_claim_gcn_approved",
-- 	dim_ndc_hierarchy.gpi  AS "dim_ndc_hierarchy.gpi_1",
-- 	case when length(order_claim.last_claim_ndc_approved) < 10 then LPAD (order_claim.last_claim_ndc_approved, 10, '0') else order_claim.last_claim_ndc_approved::varchar end  AS "order_claim.last_claim_ndc_approved_display",
-- 	dim_ndc_hierarchy.label_name  AS "dim_ndc_hierarchy.label_name",
-- 	dim_ndc_hierarchy.brand_name  AS "dim_ndc_hierarchy.brand_name",
-- 	gcn1.generic_name_short  AS "gcn1.generic_name_short",
-- 	gcn1.strength  AS "gcn1.strength",
-- 	gcn1.dosage_form_desc  AS "gcn1.dosage_form_desc",
-- 	dim_ndc_hierarchy.medispan_multi_source_code AS "dim_ndc_hierarchy.medispan_multi_source_code",
-- 	case when dim_ndc_hierarchy.medispan_multi_source_code = 'Y' then 'Generic' else 'Brand' end  AS "dim_ndc_hierarchy.bg_type",
-- 	order_claim.last_claim_awp_unit_price_approved  AS "order_claim.realized_awp_unit_price",
-- 	order_claim.last_claim_awp_unit_price_approved  AS "order_claim.realized_awp_unit_price_decimal",
-- 	COALESCE(SUM(order_claim.last_pricing_ingredient_cost_approved ), 0) AS "order_claim.last_pricing_ingredient_cost_approved",
-- 	COALESCE(SUM(order_claim.last_pricing_dispensing_fee_approved ), 0) AS "order_claim.last_pricing_dispensing_fee_approved",
-- 	COALESCE(SUM(order_claim.last_claim_awp_amount_approved ), 0) AS "order_claim.realized_awp_amount",
-- 	COALESCE(SUM(order_claim.last_claim_awp_amount_approved ), 0) AS "order_claim.realized_awp_amount_decimal",
-- 	COALESCE(SUM(order_claim.last_pricing_unc_cost_approved ), 0) AS "order_claim.last_pricing_unc_cost_approved_decimal"
-- FROM dwh.fact_order_item  AS order_claim
-- LEFT JOIN dwh.dim_gcn_seqno_hierarchy AS gcn1 ON order_claim.last_claim_gcn_seqno_approved = gcn1.gcn_seqno
-- LEFT JOIN dwh.dim_ndc_hierarchy  AS dim_ndc_hierarchy ON order_claim.last_claim_ndc_approved = dim_ndc_hierarchy.ndc
-- LEFT JOIN dwh.dim_user  AS dim_user ON order_claim.dw_user_id = dim_user.dw_user_id and order_claim.account_id = dim_user.account_id
-- LEFT JOIN dwh.dim_pharmacy_hierarchy  AS pharmacy ON order_claim.last_claim_pharmacy_npi_approved = pharmacy.pharmacy_npi
-- WHERE ((((order_claim.last_pbm_adjudication_timestamp_approved ) >= ((CONVERT_TIMEZONE('America/New_York', 'UTC', DATEADD(day,-120, DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())) )))) AND (order_claim.last_pbm_adjudication_timestamp_approved ) < ((CONVERT_TIMEZONE('America/New_York', 'UTC', DATEADD(day,120, DATEADD(day,-120, DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())) ) ))))))) AND (dim_user.is_internal = false and dim_user.is_phantom = false AND order_claim.is_fraud = FALSE)
-- GROUP BY 1,2,DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/New_York', order_claim.last_pbm_adjudication_timestamp_approved )),4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;
-- GRANT SELECT ON pricing_dev.mac_computation_utilization_info TO "public";
-- select count(*) from pricing_dev.mac_computation_utilization_info;

SELECT
	gpi,orange_book_code,repackager_indicator,obsolete_date,multi_source_code
	FROM
	dwh.dim_ndc_current_price_data
WHERE
	gpi=82300010000630
	
;
-- 	orange_book_code IN('AA', 'AB', 'AN', 'AO', 'AP', 'AT', 'ZA', 'ZC')
-- 	AND repackager_indicator = '0'
-- 	AND (obsolete_date >= CURRENT_DATE - 730 OR obsolete_date IS NULL)
-- 	AND multi_source_code = 'Y'
-- 	AND NOT lower(gpi) like  '%e%'
-- GROUP BY
-- 	1

select * from api_scraper_external.competitor_pricing where gcn=18126 and date > '2020-01-01' 

