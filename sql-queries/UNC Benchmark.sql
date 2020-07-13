with r180_retail_ssf as (
      select
          tc.id as transactional_claim_id
          ,tc.header_transaction_type as claim_status
          ,ms.multi_source_code
          ,m.gcn
          ,abs(tc.claim_quantity) as claim_quantity
          ,abs(tc.pricing_unc_cost) as pricing_unc_cost
          ,abs(tc.pricing_unc_cost)::float/abs(tc.claim_quantity)::float as unc_unit_cost
          ,row_number() over (partition by tc.claim_pharmacy_npi, tc.claim_prescription_number, tc.header_date_of_service order by tc.pbm_adjudication_timestamp desc, tc.header_pbm_claim_id desc, tc.id desc) as claim_action_sequence
      	  ,tc.header_date_of_service::date as date_of_service
      from transactional.transactional_claim tc
      left join transactional.med_package mp on case when length(tc.claim_ndc::varchar) <> 11 then LPAD(tc.claim_ndc::varchar, 11, '0') else tc.claim_ndc::varchar end =
                                                case when length(mp.ndc::varchar) <> 11 then LPAD(mp.ndc::varchar, 11, '0') else mp.ndc::varchar end
      left join transactional.med m on mp.medid=m.medid
      left join medispan.mf2ndc ms on case when length(tc.claim_ndc::varchar) <> 11 then LPAD(tc.claim_ndc::varchar, 11, '0') else tc.claim_ndc::varchar end =
                                      case when length(ms.ndc_upc_hri::varchar) <> 11 then LPAD(ms.ndc_upc_hri::varchar, 11, '0') ELSE ms.ndc_upc_hri::varchar end
      where tc.claim_pharmacy_npi not in (1023184660,1811906720) and tc.header_date_of_service::date >= '2020-01-01' and tc.header_date_of_service::date < current_date 
      and abs(tc.pricing_unc_cost)::float > 0.01 and abs(tc.claim_quantity)::float > 0
    )


, r180_generic_unc_unit_1 as (
	select 
		gcn
		,unc_unit_cost
		,calendar."date"::date as target_date
		,date_of_service
		,row_number() over (partition by gcn,target_date order by unc_unit_cost desc) as unc_unit_rank
		,COUNT(*) OVER(partition by gcn,target_date) AS row_count
	from r180_retail_ssf
		cross join static_data.calendar 
	where 
		claim_action_sequence = 1 and multi_source_code = 'Y' and gcn is not null
		and target_date > date_of_service
		and target_date < date_of_service + INTERVAL '90 day'
		and target_date < current_date
		and target_date >= '2020-03-01'
		and unc_unit_rank = (row_count+1)/2::int
)

, r180_generic_unc_unit_2 as (
select 
	*
FROM 
	r180_generic_unc_unit_1
-- WHERE 
-- 	unc_unit_rank = (row_count+1)/2::int
order BY
	1,2
)

select * from r180_generic_unc_unit_2;


