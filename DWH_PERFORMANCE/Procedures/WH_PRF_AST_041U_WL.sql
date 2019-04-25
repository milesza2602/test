--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_041U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_041U_WL" 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
---- FOR PRODLINK DATAFIX 2 AUG 2017 - POSTDATES = 3,10 JULY 2017
--**************************************************************************************************
--  Date:        aUGUST 2017
--  Author:      Wendy Lyttle
--**************************************************************************************************
--  Date:        Jan 2013
--  Author:      Wendy Lyttle
--  Purpose:     Create the daily CHBD item catalog table with sales in the performance layer
--               with input ex RP table from performance layer.
--
--               Cloned from WH_PRF_RP_001C
--
--  Runtime instructions :
--               Due to the fact that data is sent 1 day ahead of time and that we do not have the
--               stock and sales values at that point,
--               the PERFORMANCE layer is run first in batch before the FOUNDATION layer.
--               In this procedure WH_PRF_AST_041U, 
--                       we select the data based upon the LAST_UPDATED_DATE= batch_DATE. 
--                       This is due to the fact that sales data can be late
--               Eg. batch_date                = '5 March 2013'
--                   Data sent from srce       = '6 March 2013'
--                   Stock_data for this batch = '5 March 2013'
--                   Therefore, PRD will load with '5 March 2013'
--                         and FND will load with '6 March 2013';
--               In the previous procedure WH_PRF_AST_040U, 
--                       we select the data based upon the POST_DATE= batch_DATE.
--*************************************************************************************************
--  Tables:      Input  - fnd_rtl_loc_item_dy_rms_sale
--               Output - RTL_loc_item_dy_ast_catlg
--  Packages:    constants, dwh_log, dwh_valid
--*************************************************************************************************
--  Maintenance:
--  W LYTTLE 15 JUNE 2016 -- TEMP FILTER FOR EXCLUDING CHAIN_NO = 40 ADDED
--                           procedure back = wh_prf_ast_041u_bck150616
--                           chg44990
--  W LYTTLE 28 JUNE 2016 -- TEMP FILTER FOR EXCLUDING CHAIN_NO = 40 removed
--                           chg??
---
---
--  W LYTTLE 20 OCTOBER 2016 -- ADD COLUMNS FOR PRODUCT-LINKING

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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_today_fin_day_no   number        :=  0;
g_soh_qty            number        :=  0;
g_soh_selling        number        :=  0;
g_fin_day_no         number        :=  0;
g_uda_value_no       number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_year_no        number        :=  0;
g_rec_out            RTL_loc_item_dy_ast_catlg%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_week_start_date date        := sysdate;
g_this_week_end_date date          := sysdate;
g_next_week_start_date date        := sysdate;
g_min_post_date      date;
g_default_date       date          := dwh_constants.sk_to_date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_041U_WL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE AST DAILY CHBD FAST ITEM CATALOG FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- Merge data - insert or update
--**************************************************************************************************
procedure merge_ins_upd as
begin
 
   merge  /*+ APPEND parallel (MERCAT,4) */ into dwh_performance.RTL_loc_item_dy_AST_catlg  MERCAT
   using (
             with
                   rms_sales as
                                (select /*+ materialize parallel (FND_RTL_LOC_ITEM_DY_RMS_SALE,8)  full (FND_RTL_LOC_ITEM_DY_RMS_SALE)*/
                                        item_no,
                                        fnd.location_no,
                                        post_date,
                                        reg_sales_qty,
                                        reg_sales,
                                        prom_sales_qty,
                                        prom_sales
                                 FROM   FND_RTL_LOC_ITEM_DY_RMS_SALE fnd
                          --    USE FOR REPROCESSING -- WHERE  POST_DATE = G_DATE
                          WHERE  POST_DATE = G_DATE
                            --  WHERE  fnd.last_updated_date = G_DATE
                                 ),
                   CATUDA AS (
                               select /*+ materialize parallel (RTL_LOC_ITEM_DY_AST_CATLG,8)  full (RTL_LOC_ITEM_DY_AST_CATLG)    
                                          FULL(DI) FULL(DL) */
                                      cat.sk1_item_no,
                                      cat.sk1_location_no,
                                      cat.sk1_avail_uda_value_no,
                                      cat.post_date,
                                      nvl(sal.reg_sales_qty,0)    reg_sales_qty_CATLG,
                                      nvl(sal.reg_sales,0)        reg_sales_CATLG,
                                      nvl(sal.prom_sales_qty,0)    prom_sales_qty_CATLG,
                                      nvl(sal.prom_sales,0)        prom_sales_CATLG,
                                      nvl(sal.reg_sales_qty,0) + nvl(sal.prom_sales_qty,0)     prom_reg_sales_qty_catlg,
                                      nvl(sal.reg_sales,0) + nvl(sal.prom_sales,0)              prom_reg_sales_catlg
                               from   rtl_loc_item_dy_ast_catlg cat
                               join   dim_item di on
                                      cat.sk1_item_no                   = di.sk1_item_no
                               join   dim_location dl on
                                      cat.sk1_location_no               = dl.sk1_location_no
                               join   rms_sales sal on
                                      sal.item_no                       = di.item_no and
                                      sal.location_no                   = dl.location_no and
                                      sal.post_date                     = cat.post_date)
      SELECT *
      FROM CATUDA
      
        ) MEREXT
             on    ( MERCAT.sk1_location_no            = MEREXT.sk1_location_no  and
                     MERCAT.sk1_item_no                = MEREXT.sk1_item_no      and
                     MERCAT.post_date                  = MEREXT.post_date        and
                     MERCAT.sk1_avail_uda_value_no     = MEREXT.sk1_avail_uda_value_no	
                    )
             when matched then 
             update set
                      reg_sales_catlg            = MEREXT.reg_sales_catlg,
                      reg_sales_qty_catlg        = MEREXT.reg_sales_qty_catlg,
                      prom_sales_qty_catlg       = MEREXT.prom_sales_qty_catlg,
                      prom_sales_catlg           = MEREXT.prom_sales_catlg,
                      prom_reg_sales_qty_catlg   = MEREXT.prom_reg_sales_qty_catlg,
                      prom_reg_sales_catlg       = MEREXT.prom_reg_sales_catlg,
                      last_updated_date          = G_DATE

                   ;
                       
                g_recs_updated := g_recs_updated +  sql%rowcount;       
          
                commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end merge_ins_upd;
  

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

    l_text := 'LOAD OF RTL_loc_item_dy_AST_catlg EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

    dwh_lookup.dim_control(g_date);
    
         g_date := '10 JULY 2017';
         
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
 
--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************
    execute immediate 'alter session enable parallel dml';

    l_text := 'merge started ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select this_week_start_date, this_week_end_date, fin_week_no, fin_year_no
    into g_this_week_start_date, g_this_week_end_date, g_fin_week_no, g_fin_year_no
    from dim_calendar
    where calendar_date = g_date;

            merge_ins_upd;

 
           l_text := 'Merged RECS =  '||g_recs_UPDATED||' g_date:='||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;



--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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

END WH_PRF_AST_041U_WL;
