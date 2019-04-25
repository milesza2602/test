--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_010U_FOODX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_010U_FOODX" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Special History Extract for foods - year 2013 and 2014
--  22 Dec 2014 - Wendy Lyttle
--**************************************************************************************************
--  Date:        Sept 2010
--  Author:      Wendy Lyttle
--  Purpose:     BRIDGETHORN EXTRACT
--               Merge extracted data in temp tables for :
--               last 4 weeks (this_week_start_date between today - 4 weeks and today)
--               stores only (loc_type = 'S')
--               foods only (business_unit_no = 50)
--               any area except 'NON-CUST CORPORATE' (area_no <> 9978)
--  Tables:      Input  - temp_rtl_area_item_wk_dense
--                        temp_rtl_area_item_wk_sparse
--                        temp_rtl_area_item_wk_stock
--                        temp_rtl_area_item_wk_price
--                        temp_rtl_area_item_wk_catalog
--               Output - rtl_loc_item_wk_bridgethorn
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 oct 2010 - added NO_MERGE statement and gather stats
--  9 dec 2010 - qc4190 - change to reflect all foods departments
--                        instead of certain foods departments (department_no in(12 ,15 ,16 ,
--                        22 ,23 ,32 ,34 ,37 ,40 ,41 ,42 ,43 ,44 ,45 ,53 ,59 ,66 ,73 ,87 ,
--                        88 ,93 ,95 ,97 ,99 )
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fin_day_no number:=0;
g_rec_out            rtl_loc_item_wk_rms_sparse%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date         date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_010U_FOOD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'BRIDGETHORN MERGE INTO rtl_loc_item_wk_bridgethorn';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'ROLLUP OF rtl_loc_item_wk_bridgethorn EX WEEK LEVEL STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   
     l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    SELECT MIN(THIS_WEEK_START_DATE), MAX(THIS_WEEK_END_DATE) 
    INTO g_start_date,      g_end_date
    FROM DIM_CALENDAR 
    WHERE FIN_YEAR_NO = 2018;


    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate ('truncate table dwh_datafix.rtl18_loc_item_wk_bridgethorn');
    commit;
    l_text := 'TRUNCATED table dwh_datafix.rtl18_loc_item_wk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
--    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
--                                   'rtl14_loc_item_wk_bridgethorn', DEGREE => 8);
--    commit;
--    l_text := 'GATHER STATS on dwh_datafix.rtl14_loc_item_wk_bridgethorn';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    INSERT /*+ APPEND */ INTO dwh_datafix.rtl18_loc_item_wk_bridgethorn
     select
                 st_store_type
                ,(case when pack_item_no is null then item_no else pack_item_no end) ITEM_NO
                ,this_week_start_date
--                ,dl.area_no
                ,sum(nvl(sales_qty,0) )
                ,sum(nvl(sales,0) )
                ,sum(nvl(sales_cost,0) )
                ,sum(nvl(boh_qty,0) )
                ,sum(nvl(boh_selling,0) )
                ,max(item_STATUS)
                ,MAX(nvl(promotion_count,0))
--                ,sum(nvl(promotion_count,0) )
                ,sum(nvl(waste_cost,0) )
                ,max(nvl(reg_rsp_excl_vat,0))
--                ,sum((case when pack_item_no is null then 0 else nvl(this_wk_catalog_ind,0) end)  )
                ,sum(nvl(this_wk_catalog_ind,0)  )
                ,sum(nvl(stores_with_stock,0) )
                ,sum(nvl(stores_with_sales,0) )
                ,sum(nvl(units_per_week,0))
    from
      ( select    (case when raistk.st_store_type = 'WW'
                      THEN 'corporate stores'
                      else 'franchise stores'
                      end) st_store_type
                ,raistk.item_no
                ,fpid.pack_item_no
                ,raistk.this_week_start_date
--                ,dl.area_no
                ,0 sales_qty
                ,0 sales
                ,0 sales_cost
                ,0 boh_qty
                ,0 boh_selling
                ,(case product_status_short_desc
                when 'A' then 'A - Development'
                when 'D' then 'D - Pending Elimination'
                when 'N' then 'N - Pending Introduction'
                when 'O' then 'O - Open to Order'
                when 'P' then 'P - Delisted'
                when 'U' then 'U - Out of Season'
                when 'X' then 'X - Eliminated'
                when 'Z' then 'Z - Temporarily Unavailable'
                when null then 'X - no status'
                else to_char(product_status_1_code)||' - Unknown'
                end ) item_STATUS
                ,0 promotion_count
                ,0 waste_cost
                ,0 reg_rsp_excl_vat
                ,sum(nvl(raistk.this_wk_catalog_ind,0)) this_wk_catalog_ind
                ,0 stores_with_stock
                ,0 stores_with_sales
                ,0 units_per_week
     from     dwh_datafix.t18_rtl_area_item_wk_catalog raistk  ,
              dwh_performance.dim_product_status dps,
              dwh_foundation.fnd_pack_item_detail fpid
     where raistk.item_no = fpid.item_no(+)
       and dps.product_status_code = to_char(product_status_1_code)
group by (case when raistk.st_store_type = 'WW' THEN 'corporate stores' else 'franchise stores' end),
     raistk.item_no,fpid.pack_item_no,  raistk.this_week_start_date,
     (case product_status_short_desc
                when 'A' then 'A - Development'
                when 'D' then 'D - Pending Elimination'
                when 'N' then 'N - Pending Introduction'
                when 'O' then 'O - Open to Order'
                when 'P' then 'P - Delisted'
                when 'U' then 'U - Out of Season'
                when 'X' then 'X - Eliminated'
                when 'Z' then 'Z - Temporarily Unavailable'
                when null then 'X - no status'
                else to_char(product_status_1_code)||' - Unknown'
                end )
     union all
      select   (case when raistk.st_store_type = 'WW'
                      THEN 'corporate stores'
                      else 'franchise stores'
                      end) st_store_type
                ,raistk.item_no ITEM_NO
                ,fpid.pack_item_no PACK_ITEM_NO
                ,raistk.this_week_start_date THIS_WEEK_START_DATE
--                ,dl.area_no
                ,0 sales_qty
                ,0 sales
                ,0 sales_cost
                ,sum((nvl(boh_qty,0)) / (nvl(pack_item_qty,1)))    boh_qty
                ,sum(nvl(boh_selling,0)) boh_selling
                ,null  item_STATUS
                ,0 promotion_count
                ,0 waste_cost
                ,0 reg_rsp_excl_vat
                ,0 this_wk_catalog_ind
                ,sum(nvl(stores_with_stock,0)) stores_ith_stock
                ,0 stores_with_sales
                ,0 units_per_week
     from     dwh_datafix.t18_rtl_area_item_wk_stock raistk,
              dwh_foundation.fnd_pack_item_detail fpid
     where raistk.item_no = fpid.item_no(+)
      group by (case when raistk.st_store_type = 'WW' THEN 'corporate stores' else 'franchise stores' end),
     raistk.item_no,fpid.pack_item_no,  raistk.this_week_start_date
     union all
            select   (case when raistk.st_store_type = 'WW'
                      THEN 'corporate stores'
                      else 'franchise stores'
                      end) st_store_type
                ,raistk.item_no
                ,fpid.pack_item_no
                ,raistk.this_week_start_date
--                ,dl.area_no
                ,0 sales_qty
                ,0 sales
                ,0 sales_cost
                ,0 boh_qty
                ,0 boh_selling
                ,null item_STATUS
                ,MAX(nvl(promotion_count,0))
                ,sum(nvl(waste_cost,0))
                ,0 reg_rsp_excl_vat
                ,0 this_wk_catalog_ind
                ,0 stores_with_stock
                ,0 stores_with_sales
                ,0 units_per_week
     from     dwh_datafix.t18_rtl_area_item_wk_sparse raistk,
              dwh_foundation.fnd_pack_item_detail fpid
     where raistk.item_no = fpid.item_no(+)
     group by (case when raistk.st_store_type = 'WW' THEN 'corporate stores' else 'franchise stores' end),
     raistk.item_no,fpid.pack_item_no,  raistk.this_week_start_date
  
  -- rate of sale measure
  UNION ALL   
     
              select   (case when roswk.st_store_type = 'WW'
                      THEN 'corporate stores'
                      else 'franchise stores'
                      end) st_store_type
                ,roswk.item_no
                ,pack_item_no
                ,roswk.this_week_start_date
--                ,dl.area_no
                ,0 sales_qty
                ,0 sales
                ,0 sales_cost
                ,0 boh_qty
                ,0 boh_selling
                ,null item_STATUS
                ,0 promotion_count
                ,0 waste_cost
                ,0 reg_rsp_excl_vat
                ,0 this_wk_catalog_ind
                ,0 stores_with_stock
                ,0 stores_with_sales
                , sum(nvl(units_per_week,0))
     from     dwh_datafix.t18_RATE_OF_SALE_WK roswk,
              dwh_foundation.fnd_pack_item_detail fpid
     where roswk.item_no = fpid.item_no(+)
     group by (case when roswk.st_store_type = 'WW' THEN 'corporate stores' else 'franchise stores' end),
     roswk.item_no,fpid.pack_item_no,  roswk.this_week_start_date
     
     
     
     union all
     select    st_store_type
                ,item_no
                ,pack_item_no
                ,this_week_start_date
--                ,dl.area_no
                ,0 sales_qty
                ,0 sales
                ,0 sales_cost
                ,0 boh_qty
                ,0 boh_selling
                ,null item_STATUS
                ,0 promotion_count
                ,0 waste_cost
                ,(nvl(reg_rsp_excl_vat_avg,0) / nvl(LOC_CNT,0)) reg_rsp_excl_vat_avg
                ,0 this_wk_catalog_ind
                ,0 stores_with_stock
                ,0 stores_with_sales
                ,0 units_per_week
     from     (select    (case when raistk.st_store_type = 'WW'
                      THEN 'corporate stores'
                      else 'franchise stores'
                      end) st_store_type
                ,raistk.item_no
                ,fpid.pack_item_no
                ,raistk.this_week_start_date
                ,SUM(nvl(reg_rsp_excl_vat_avg,0)) reg_rsp_excl_vat_avg
                ,SUM(NVL(LOC_CNT,0)) LOC_CNT
      from     dwh_datafix.t18_rtl_area_item_wk_price raistk,
              dwh_foundation.fnd_pack_item_detail fpid
     where raistk.item_no = fpid.item_no(+)
     group by (case when raistk.st_store_type = 'WW' THEN 'corporate stores' else 'franchise stores' end),
     raistk.item_no,fpid.pack_item_no,  raistk.this_week_start_date )
     group by ST_STORE_TYPE,
    item_no,pack_item_no,  this_week_start_date, (nvl(reg_rsp_excl_vat_avg,0) / nvl(LOC_CNT,0))
   union all
            select    (case when raistk.st_store_type = 'WW'
                      THEN 'corporate stores'
                      else 'franchise stores'
                      end) st_store_type
                ,raistk.item_no
                ,fpid.pack_item_no
                ,raistk.this_week_start_date
--                ,dl.area_no
                ,sum((nvl(sales_qty,0))/ (nvl(pack_item_qty,1)))
                ,sum(nvl(sales,0))
                ,sum(nvl(sales_cost,0))
                ,0 boh_qty
                ,0 boh_selling
                ,null item_STATUS
                ,0 promotion_count
                ,0 waste_cost
                ,0 reg_rsp_excl_vat
                ,0 this_wk_catalog_ind
                ,0 stores_with_stock
                ,sum(nvl(stores_with_sales,0)   )
                ,0 units_per_week
     from     dwh_datafix.t18_rtl_area_item_wk_dense raistk,
              dwh_foundation.fnd_pack_item_detail fpid
     where raistk.item_no = fpid.item_no(+)
     group by (case when raistk.st_store_type = 'WW' THEN 'corporate stores' else 'franchise stores' end),
     raistk.item_no,fpid.pack_item_no,  raistk.this_week_start_date
       )
     group by st_store_type, (case when pack_item_no is null then item_no else pack_item_no end), this_week_start_date
;
                g_recs_read  :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

commit;
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   'rtl18_loc_item_wk_bridgethorn', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_datafix.rtl18_loc_item_wk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    p_success := true;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;



END WH_PRF_BRTH_010U_FOODX;
