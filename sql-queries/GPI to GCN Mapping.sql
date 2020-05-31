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
select * from map_gcn_gpi where gpi=79700030003015;



with ndc_gpi as (
	select 
		distinct ndc.ndc_upc_hri as ndc, n.generic_product_identifier as gpi
	from 
		medispan.mf2ndc ndc
		INNER JOIN medispan.mf2name n ON ndc.drug_descriptor_id = n.drug_descriptor_id
)
	select * from ndc_gpi;