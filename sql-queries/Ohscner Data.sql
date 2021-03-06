SELECT
	og.gcn AS "gcn",
	dm.gcn_seqno AS "gcn seqno",
	hicl_seqno AS "hicl seqno",
	hicl_desc AS "hicl desc",
	brand_med_name AS "brand med name",
	gtc_desc AS "gtc desc",
	stc_desc AS "stc desc",
	ctc_desc AS "ctc desc",
	dm.strength AS "strength",
	form AS "form",
	maint_flg AS "maint flg",
	dea_flg AS "dea flg",
	opioid_flg AS "opioid flg",
	available_med_flg AS "available med flg",
	tem_available_med_flg AS "tem available med flg",
	default_gcn_flag AS "default gcn flag",
	edlp_priced_flg AS "edlp priced flg",
	bsd_priced_flg AS "bsd priced flg",
	hd_priced_flg AS "hd priced flg",
	grx_tracked_flg AS "grx tracked flg",
	blink_gcn_rank AS "blink gcn rank",
	blink_hicl_gcn_rank AS "blink hicl gcn rank",
	blink_hicl_rank AS "blink hicl rank",
	blink_r30_gcn_rank AS "blink r30 gcn rank",
	blink_r30_hicl_rank AS "blink r30 hicl rank",
	symphony_2017_scripts AS "symphony 2017 scripts",
	symphony_2017_pct_of_hicl_scripts AS "symphony 2017 pct of hicl scripts",
	blink_pct_of_hicl_scripts AS "blink pct of hicl scripts",
	dm.bh01_mac_price AS "bh01 mac price",
	dm.bh02_mac_price AS "bh02 mac price",
	dm.bh03_mac_price AS "bh03 mac price",
	dm.wmt_mac_price AS "wmt mac price",
	dm.hd_syr_mac_price AS "hd syr mac price",
	dm.bh01_dispensing_fee AS "bh01 dispensing fee",
	dm.bh02_dispensing_fee AS "bh02 dispensing fee",
	dm.bh03_dispensing_fee AS "bh03 dispensing fee",
	dm.wmt_dispensing_fee AS "wmt dispensing fee",
	hd_syr_dispensing_fee AS "hd syr dispensing fee",
	gpi_gcn_mac_list_conflict_flg AS "gpi gcn mac list conflict flg",
	fixed_stripe_charge_cost AS "fixed stripe charge cost",
	variable_stripe_charge_cost AS "variable stripe charge cost",
	pac_unit AS "pac unit",
	pac_low_unit AS "pac low unit",
	pac_high_unit AS "pac high unit",
	pac_retail_unit AS "pac retail unit",
	r90_wmt_unc_unit_cost AS "r90 wmt unc unit cost",
	r90_pblx_unc_unit_cost AS "r90 pblx unc unit cost",
	r90_beh_unc_unit_cost AS "r90 beh unc unit cost",
	awp_unit_cost AS "awp unit cost",
	ger_wmt_mac AS "ger wmt mac",
	wmt_script_pct AS "wmt script pct",
	edlp_script_pct AS "edlp script pct",
	bsd_script_pct AS "bsd script pct",
	hd_script_pct AS "hd script pct",
	wmt_nfp_script_pct AS "wmt nfp script pct",
	edlp_nfp_script_pct AS "edlp nfp script pct",
	bsd_nfp_script_pct AS "bsd nfp script pct",
	hd_nfp_script_pct AS "hd nfp script pct",
	dm.edlp_fixed_price AS "edlp fixed price",
	dm.edlp_unit_price AS "edlp unit price",
	dm.bsd_fixed_price AS "bsd fixed price",
	dm.bsd_unit_price AS "bsd unit price",
	dm.hd_fixed_price AS "hd fixed price",
	dm.hd_unit_price AS "hd unit price",
	ltd_filling_patients AS "ltd filling patients",
	ltd_scripts AS "ltd scripts",
	ltd_qty AS "ltd qty",
	ltd_nfp AS "ltd nfp",
	ltd_nfp_scripts AS "ltd nfp scripts",
	ltd_nfp_qty AS "ltd nfp qty",
	r30_scripts AS "r30 scripts",
	r30_qty AS "r30 qty",
	r30_nfp_scripts AS "r30 nfp scripts",
	r30_nfp_qty AS "r30 nfp qty",
	r30_wmt_scripts AS "r30 wmt scripts",
	r30_wmt_qty AS "r30 wmt qty",
	r30_wmt_nfp_scripts AS "r30 wmt nfp scripts",
	r30_wmt_nfp_qty AS "r30 wmt nfp qty",
	r30_edlp_scripts AS "r30 edlp scripts",
	r30_edlp_qty AS "r30 edlp qty",
	r30_edlp_nfp_scripts AS "r30 edlp nfp scripts",
	r30_edlp_nfp_qty AS "r30 edlp nfp qty",
	r30_bsd_scripts AS "r30 bsd scripts",
	r30_bsd_qty AS "r30 bsd qty",
	r30_bsd_nfp_scripts AS "r30 bsd nfp scripts",
	r30_bsd_nfp_qty AS "r30 bsd nfp qty",
	r30_hd_scripts AS "r30 hd scripts",
	r30_hd_qty AS "r30 hd qty",
	r30_hd_nfp_scripts AS "r30 hd nfp scripts",
	r30_hd_nfp_qty AS "r30 hd nfp qty",
	r30_bh01_edlp_scripts AS "r30 bh01 edlp scripts",
	r30_bh01_edlp_qty AS "r30 bh01 edlp qty",
	r30_bh01_edlp_nfp_scripts AS "r30 bh01 edlp nfp scripts",
	r30_bh01_edlp_nfp_qty AS "r30 bh01 edlp nfp qty",
	r30_bh02_edlp_scripts AS "r30 bh02 edlp scripts",
	r30_bh02_edlp_qty AS "r30 bh02 edlp qty",
	r30_bh02_edlp_nfp_scripts AS "r30 bh02 edlp nfp scripts",
	r30_bh02_edlp_nfp_qty AS "r30 bh02 edlp nfp qty",
	r30_bh03_edlp_scripts AS "r30 bh03 edlp scripts",
	r30_bh03_edlp_qty AS "r30 bh03 edlp qty",
	r30_bh03_edlp_nfp_scripts AS "r30 bh03 edlp nfp scripts",
	r30_bh03_edlp_nfp_qty AS "r30 bh03 edlp nfp qty",
	r30_bh01_bsd_scripts AS "r30 bh01 bsd scripts",
	r30_bh01_bsd_qty AS "r30 bh01 bsd qty",
	r30_bh01_bsd_nfp_scripts AS "r30 bh01 bsd nfp scripts",
	r30_bh01_bsd_nfp_qty AS "r30 bh01 bsd nfp qty",
	r30_bh02_bsd_scripts AS "r30 bh02 bsd scripts",
	r30_bh02_bsd_qty AS "r30 bh02 bsd qty",
	r30_bh02_bsd_nfp_scripts AS "r30 bh02 bsd nfp scripts",
	r30_bh02_bsd_nfp_qty AS "r30 bh02 bsd nfp qty",
	r30_bh03_bsd_scripts AS "r30 bh03 bsd scripts",
	r30_bh03_bsd_qty AS "r30 bh03 bsd qty",
	r30_bh03_bsd_nfp_scripts AS "r30 bh03 bsd nfp scripts",
	r30_bh03_bsd_nfp_qty AS "r30 bh03 bsd nfp qty",
	coalesce(dm.default_quantity,dq.quantity) AS "default quantity",
	blink_edlp_price AS "blink edlp price",
	blink_bsd_price AS "blink bsd price",
	blink_hd_price AS "blink hd price",
	edlp_vs_bsd_gap AS "edlp vs bsd gap",
	edlp_vs_hd_gap AS "edlp vs hd gap",
	bsd_vs_hd_gap AS "bsd vs hd gap",
	min_grx AS "min grx",
	min_retail_grx AS "min retail grx",
	min_bh_retail_index_grx AS "min bh retail index grx",
	min_grx_northeast AS "min grx northeast",
	min_grx_south AS "min grx south",
	min_grx_midwest AS "min grx midwest",
	min_grx_west AS "min grx west",
	min_hwh_grx AS "min hwh grx",
	min_cvs_grx AS "min cvs grx",
	min_wag_grx AS "min wag grx",
	min_wmt_grx AS "min wmt grx",
	min_rad_grx AS "min rad grx",
	min_kr_grx AS "min kr grx",
	min_sfwy_grx AS "min sfwy grx",
	min_pblx_grx AS "min pblx grx",
	min_bksh_grx AS "min bksh grx",
	min_geagle_grx AS "min geagle grx",
	min_heb_grx AS "min heb grx",
	lowest_tracked_price_grx_pharmacy AS "lowest tracked price grx pharmacy",
	lowest_tracked_grx_price AS "lowest tracked grx price",
	lowest_tracked_grx_pharmacy_type AS "lowest tracked grx pharmacy type",
	wmt_2018_07_27_flg AS "wmt 2018 07 27 flg",
	wmt_2018_07_27_qty1 AS "wmt 2018 07 27 qty1",
	wmt_2018_07_27_price1 AS "wmt 2018 07 27 price1",
	wmt_2018_07_27_qty2 AS "wmt 2018 07 27 qty2",
	wmt_2018_07_27_price2 AS "wmt 2018 07 27 price2",
	wmt_2018_11_28_flg AS "wmt 2018 11 28 flg",
	wmt_2018_11_28_qty1 AS "wmt 2018 11 28 qty1",
	wmt_2018_11_28_price1 AS "wmt 2018 11 28 price1",
	wmt_2018_11_28_qty2 AS "wmt 2018 11 28 qty2",
	wmt_2018_11_28_price2 AS "wmt 2018 11 28 price2",
	wmt_retail_list_comp_price AS "wmt retail list comp price",
	pblx_2018_10_12_flg AS "pblx 2018 10 12 flg",
	pblx_2018_10_12_qty1 AS "pblx 2018 10 12 qty1",
	pblx_2018_10_12_price1 AS "pblx 2018 10 12 price1",
	pblx_2018_10_12_qty2 AS "pblx 2018 10 12 qty2",
	pblx_2018_10_12_price2 AS "pblx 2018 10 12 price2",
	pblx_est_unc_price AS "pblx est unc price",
	beh_est_unc_price AS "beh est unc price",
	edlp_grx_price_leader AS "edlp grx price leader",
	edlp_min_retail_grx_price_leader AS "edlp min retail grx price leader",
	edlp_min_bh_retail_index_grx_price_leader AS "edlp min bh retail index grx price leader",
	edlp_min_wmt_grx_price_leader AS "edlp min wmt grx price leader",
	edlp_vs_min_grx_gap AS "edlp vs min grx gap",
	edlp_vs_min_bh_retail_index_grx_gap AS "edlp vs min bh retail index grx gap",
	edlp_vs_min_wmt_grx_gap AS "edlp vs min wmt grx gap",
	bsd_vs_min_grx_gap AS "bsd vs min grx gap",
	bsd_vs_min_bh_retail_index_grx_gap AS "bsd vs min bh retail index grx gap",
	bsd_vs_min_wmt_grx_gap AS "bsd vs min wmt grx gap",
	hd_vs_min_grx_gap AS "hd vs min grx gap",
	hd_vs_min_bh_retail_index_grx_gap AS "hd vs min bh retail index grx gap",
	hd_vs_min_wmt_grx_gap AS "hd vs min wmt grx gap",
	hd_vs_hwh_grx_gap AS "hd vs hwh grx gap",
	projected_gcn_12_mos_30ds_normalized_scripts_from_nfp_scripts AS "projected gcn 12 mos 30ds normalized scripts from nfp scripts",
	r30_gcn_viewed_product_sessions AS "r30 gcn viewed product sessions",
	r30_gcn_purchased_product_sessions AS "r30 gcn purchased product sessions",
	r30_gcn_filled_product_sessions AS "r30 gcn filled product sessions",
	r30_gcn_purchased_cvr AS "r30 gcn purchased cvr",
	r30_gcn_filled_cvr AS "r30 gcn filled cvr",
	r30_gcn_purchase_to_fill_rate AS "r30 gcn purchase to fill rate",
	top_30ds_quantity AS "top 30ds quantity",
	blink_edlp_price_30ds AS "blink edlp price 30ds",
	blink_bsd_price_30ds AS "blink bsd price 30ds",
	blink_hd_price_30ds AS "blink hd price 30ds",
	top_90ds_quantity AS "top 90ds quantity",
	blink_edlp_price_90ds AS "blink edlp price 90ds",
	blink_bsd_price_90ds AS "blink bsd price 90ds",
	blink_hd_price_90ds AS "blink hd price 90ds",
	last_30ds_qty_scrape_date AS "last 30ds qty scrape date",
	last_30ds_qty AS "last 30ds qty",
	min_hwh_grx_30ds AS "min hwh grx 30ds",
	min_wmt_grx_30ds AS "min wmt grx 30ds",
	min_kr_grx_30ds AS "min kr grx 30ds",
	min_sfwy_grx_30ds AS "min sfwy grx 30ds",
	min_pblx_grx_30ds AS "min pblx grx 30ds",
	min_bksh_grx_30ds AS "min bksh grx 30ds",
	min_geagle_grx_30ds AS "min geagle grx 30ds",
	min_heb_grx_30ds AS "min heb grx 30ds",
	last_90ds_qty_scrape_date AS "last 90ds qty scrape date",
	last_90ds_qty AS "last 90ds qty",
	min_hwh_grx_90ds AS "min hwh grx 90ds",
	min_wmt_grx_90ds AS "min wmt grx 90ds",
	min_kr_grx_90ds AS "min kr grx 90ds",
	min_sfwy_grx_90ds AS "min sfwy grx 90ds",
	min_pblx_grx_90ds AS "min pblx grx 90ds",
	min_bksh_grx_90ds AS "min bksh grx 90ds",
	min_geagle_grx_90ds AS "min geagle grx 90ds",
	min_heb_grx_90ds AS "min heb grx 90ds",
	ltd_30_day_scripts AS "ltd 30 day scripts",
	ltd_90_day_scripts AS "ltd 90 day scripts",
	ltd_30_day_scripts_pct AS "ltd 30 day scripts pct",
	ltd_90_day_scripts_pct AS "ltd 90 day scripts pct",
	r30_30_day_scripts AS "r30 30 day scripts",
	r30_90_day_scripts AS "r30 90 day scripts",
	r30_30_day_script_pct AS "r30 30 day script pct",
	r30_90_day_script_pct AS "r30 90 day script pct",
	r30_30_day_nfp_scripts AS "r30 30 day nfp scripts",
	r30_90_day_nfp_scripts AS "r30 90 day nfp scripts",
	r30_30_day_nfp_script_pct AS "r30 30 day nfp script pct",
	r30_90_day_nfp_script_pct AS "r30 90 day nfp script pct",
	r30_hd_30_day_scripts AS "r30 hd 30 day scripts",
	r30_hd_90_day_scripts AS "r30 hd 90 day scripts",
	r30_hd_30_day_script_pct AS "r30 hd 30 day script pct",
	r30_hd_90_day_script_pct AS "r30 hd 90 day script pct",
	r30_hd_30_day_nfp_scripts AS "r30 hd 30 day nfp scripts",
	r30_hd_90_day_nfp_scripts AS "r30 hd 90 day nfp scripts",
	r30_hd_30_day_nfp_script_pct AS "r30 hd 30 day nfp script pct",
	r30_hd_90_day_nfp_script_pct AS "r30 hd 90 day nfp script pct",
	r90_fills AS "r90 fills",
	r90_quantities AS "r90 quantities",
	r90_revenue AS "r90 revenue",
	r90_cogs AS "r90 cogs",
	gcn_symphony_2017_rank AS "gcn symphony 2017 rank",
	gcn_symphony_2017_fills AS "gcn symphony 2017 fills",
	wmt_4_dollar_list AS "wmt $4 list",
	wmt_9_dollar_list AS "wmt $9 list",
	wmt_list AS "wmt list",
	fill_rank AS "fill rank",
	wtd_10th_percentile_benchmark AS "wtd 10th %ile benchmark",
	wtd_10th_percentile_phamrmacy AS "wtd 10th %ile pharmacy",
	universal_price_benchmark AS "universal price benchmark",
	major_retailer_price_benchmark AS "major retailer price benchmark",
	walmart_price_benchmark AS "walmart price benchmark"
FROM
	mktg_dev.oshner_gcn og
	LEFT JOIN mktg_dev.sdey_generic_price_portfolio_datamart dm ON og.gcn = dm.gcn
	LEFT JOIN mktg_dev.sdey_pricing_wbr_competition pc ON pc.gcn = og.gcn
	LEFT JOIN mktg_dev.sdey_gcn_default_quantity_mapping dq ON dq.gcn = og.gcn