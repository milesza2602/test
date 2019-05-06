--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_ACCENTURE_017E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_ACCENTURE_017E" 
AS
  --**************************************************************************************************
  --  Date:        November 2017
  --  Author:      A. Ugolini
  --  Purpose:     Stored Procedures being called for the Inventury Forecast data Extract.
  --
  --  Maintenance:
  --
  --**************************************************************************************************
  
--- Build dynamic sql where clause

BEGIN

EXECUTE IMMEDIATE 'truncate table w7122944.inventory_forecast drop storage';

--- Stock -1 
insert into w7122944.inventory_forecast  
select /*+ full(stk) parallel (stk,4) */   
stk.sk1_location_no,
stk.sk1_item_no,
--
item.group_name,
item.group_no,
item.department_name,
item.department_no,
item.class_name,
item.class_no,
item.subclass_name,
item.subclass_no,
item.sk1_style_colour_no,
item.item_no,
item.item_desc,
item.size_id,
cal.fin_year_no,
cal.fin_week_no,
--
stk.this_week_start_date,
nvl(stk.soh_qty,0)			soh_qty,
nvl(stk.inbound_incl_cust_ord_qty,0)	inbound_qty,
0 reg_sales_qty,
0 prom_sales_qty,
0 clear_sales_qty,
0 ch_alloc_qty,
0 avail_ch_num_avail_days,
0 avail_ch_num_catlg_days
from 
   dwh_performance.rtl_loc_item_wk_rms_stock  stk,
   dwh_performance.dim_item                   item,
   dwh_performance.dim_calendar_wk            cal
where stk.this_week_start_date  = cal.this_week_start_date
  and stk.sk1_item_no           = item.sk1_item_no
  and stk.this_week_start_date between '27/Jun/16' and '25/Jun/17'
  and item.class_no     in (2468,2478,1036,1037,2689);
commit;

---- Regular Sales 2      
insert into w7122944.inventory_forecast
select /*+ full(sls) parallel (sls,4) */
sls.sk1_location_no,
sls.sk1_item_no,
--
item.group_name,
item.group_no,
item.department_name,
item.department_no,
item.class_name,
item.class_no,
item.subclass_name,
item.subclass_no,
item.sk1_style_colour_no,
item.item_no,
item.item_desc,
item.size_id,
cal.fin_year_no,
cal.fin_week_no,
--
sls.this_week_start_date,
0 soh_qty,
0 inbound_qty,
nvl(sls.reg_sales_qty,0) reg_sales_qty,
0 prom_sales_qty,
0 clear_sales_qty,
0 ch_alloc_qty,
0 avail_ch_num_avail_days,
0 avail_ch_num_catlg_days
from 
  dwh_performance.rtl_loc_item_wk_rms_dense  sls,
  dwh_performance.dim_item                   item,
  dwh_performance.dim_calendar_wk            cal
where sls.this_week_start_date  = cal.this_week_start_date
  and sls.sk1_item_no           =  item.sk1_item_no
  and sls.this_week_start_date between '27/Jun/16' and '25/Jun/17'
  and item.class_no     in (2468,2478,1036,1037,2689);
commit; 


---- Promo + Clearance Sales + Allocation Qty - 3 
insert into w7122944.inventory_forecast
select /*+ full(sls) parallel (sls,4) */
sls.sk1_location_no,
sls.sk1_item_no,
--
item.group_name,
item.group_no,
item.department_name,
item.department_no,
item.class_name,
item.class_no,
item.subclass_name,
item.subclass_no,
item.sk1_style_colour_no,
item.item_no,
item.item_desc,
item.size_id,
cal.fin_year_no,
cal.fin_week_no,
--
sls.this_week_start_date,
0 soh_qty,
0 inbound_qty,
0 reg_sales_qty,
nvl(sls.prom_sales_qty,0)	prom_sales_qty,
nvl(sls.clear_sales_qty,0)	clear_sales_qty,
nvl(sls.ch_alloc_qty,0)		ch_alloc_qty,
0 avail_ch_num_avail_days,
0 avail_ch_num_catlg_days
from 
  dwh_performance.rtl_loc_item_wk_rms_sparse sls,
  dwh_performance.dim_item                   item,
  dwh_performance.dim_calendar_wk            cal
where sls.this_week_start_date  = cal.this_week_start_date
  and sls.sk1_item_no           =  item.sk1_item_no
  and sls.this_week_start_date between '27/Jun/16' and '25/Jun/17'
  and item.class_no     in (2468,2478,1036,1037,2689);
commit;  

---- Availability - 4
insert into w7122944.inventory_forecast
select /*+ full(avl) parallel (avl,4) */ 
avl.sk1_location_no,
avl.sk1_item_no,
--
item.group_name,
item.group_no,
item.department_name,
item.department_no,
item.class_name,
item.class_no,
item.subclass_name,
item.subclass_no,
item.sk1_style_colour_no,
item.item_no,
item.item_desc,
item.size_id,
cal.fin_year_no,
cal.fin_week_no,
--
avl.this_week_start_date,
0 soh_qty,
0 inbound_qty,
0 reg_sales_qty,
0 prom_sales_qty,
0 clear_sales_qty,
0 ch_alloc_qty,
nvl(avl.avail_ch_num_avail_days,0)	avail_ch_num_avail_days,
nvl(avl.avail_ch_num_catlg_days,0)	avail_ch_num_catlg_days
from 
  dwh_performance.rtl_loc_item_wk_ast_catlg  avl,
  dwh_performance.dim_item                   item,
  dwh_performance.dim_calendar_wk            cal
where avl.this_week_start_date  = cal.this_week_start_date
  and avl.sk1_item_no           =  item.sk1_item_no
  and avl.this_week_start_date between '27/Jun/16' and '25/Jun/17'
  and item.class_no     in (2468,2478,1036,1037,2689); 
commit;
 
--- Summarise the result sets of the four queries 

EXECUTE IMMEDIATE 'truncate table w7122944.inventory_forecast_results drop storage'; 

insert into w7122944.inventory_forecast_results
SELECT /*+ full(fcs) parallel (fcs,4) */     
loc.location_no		      		        store_no,
loc.location_name		      		      store_name,
fcs.item_no,
fcs.item_desc,
fcs.group_no,
fcs.group_name,
fcs.department_no,
fcs.department_name,
fcs.class_no,
fcs.class_name,
fcs.subclass_no,
fcs.subclass_name,
sc.style_no,
sc.item_level1_desc	      		     style_desc,
sc.style_colour_no,
sc.style_colour_desc,
sc.diff_type_prim_size_diff_code,
fin_year_no,
fin_week_no,
this_week_start_date,
sum(soh_qty)                  		soh_qty,
sum(inbound_qty)              		inbound_qty,
sum(reg_sales_qty)            		reg_sales_qty,
sum(prom_sales_qty)           		prom_sales_qty,
sum(clear_sales_qty)          		clear_sales_qty,
sum(ch_alloc_qty)             		ch_alloc_qty,
sum(avail_ch_num_avail_days)  		avail_ch_num_avail_days,
sum(avail_ch_num_catlg_days)  		avail_ch_num_catlg_days
from  w7122944.inventory_forecast 	       fcs,
      dwh_performance.dim_location 	       loc,
      dwh_performance.dim_ast_lev1_diff1   sc
where fcs.sk1_location_no 	= loc.sk1_location_no
  and fcs.sk1_style_colour_no	= sc.sk1_style_colour_no
group by 
	loc.location_no,
	loc.location_name,
  fcs.item_no,
  fcs.item_desc,
	fcs.group_no,
	fcs.group_name,
	fcs.department_no,
	fcs.department_name,
	fcs.class_no,
	fcs.class_name,
  fcs.subclass_no,
  fcs.subclass_name,
	sc.style_no,
	sc.item_level1_desc,
	sc.style_colour_no,
	sc.style_colour_desc,
	sc.diff_type_prim_size_diff_code,
	fin_year_no,
	fin_week_no,
	this_week_start_date
;
commit;

END WH_PRF_EXT_ACCENTURE_017E;
