-- Strategic Drugs (TBD)l, 
-- top 200 for us, #
-- top 200 for industry, #  
-- walmart list drugs, 
-- lists of drugs whose prices we’ve changed recently.

drop table if exists mktg_dev.sdey_gcn_list_wmt_ranked;
create table mktg_dev.sdey_gcn_list_wmt_ranked as  
SELECT
	f_gcn.gcn,
	f_gcn.gcn_seqno,
	f_gcn.gcn_symphony_2017_rank,
	f_gcn.gcn_symphony_2017_fills,
	case when wmt_4_dollar_list_gcn.gcn is not null and wmt_4_dollar_list_gcn.price1 = 4 then true else false end as wmt_4_dollar_list,
	case when wmt_4_dollar_list_gcn.gcn is not null and wmt_4_dollar_list_gcn.price1 = 9 then true else false end as wmt_9_dollar_list,
	case when wmt_4_dollar_list_gcn.gcn is not null then true else false end as wmt_list,
	sum(1) as r90_fills,
	dense_rank() OVER (ORDER BY sum(1) DESC) as fill_rank
FROM
	transactional.available_med am
LEFT OUTER JOIN 
	(select * from dwh.fact_order_item WHERE fill_sequence IS NOT NULL AND is_fraud = FALSE 
		AND last_pbm_adjudication_timestamp_approved::timestamp::date + INTERVAL '90 day' >= CURRENT_DATE) AS foi
ON
	am.medid = foi.med_id
	AND am.gcn = foi.gcn 
	AND am.gcn_seqno = foi.gcn_seqno
LEFT OUTER JOIN 
	dwh.dim_user AS du 
ON 
	foi.account_id = du.account_id
	AND du.is_internal = FALSE
	AND du.is_phantom = FALSE
LEFT OUTER JOIN 
	dwh.dim_gcn_seqno_hierarchy f_gcn 
ON 
	foi.last_claim_gcn_seqno_approved = f_gcn.gcn_seqno
LEFT OUTER JOIN 
	git_data_import.wmt_4_dollar_list_gcn
ON
	f_gcn.gcn=wmt_4_dollar_list_gcn.gcn 
	AND wmt_4_dollar_list_gcn.effective_date::date = '2018-11-28'
GROUP BY
	1,2,3,4,5,6,7
;

GRANT SELECT ON mktg_dev.sdey_gcn_list_wmt_ranked TO "public";

-- select count(distinct(gcn)) from transactional.available_med;
select count(*),count(distinct(gcn)),count(distinct(gcn_seqno)) from mktg_dev.sdey_gcn_list_wmt_ranked;

