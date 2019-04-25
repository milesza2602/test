--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_861U_ADHOC_I
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_861U_ADHOC_I" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create Dimension Display Group data with input ex Intactix Item Display
--  Tables:      Input  - DWH_PERFORMANCE.TEMP_FOODS_TAB_DENSE_BUILD
--               Output - DWH_PERFORMANCE.MART_FDS_LI_WK_TAB_REBUILD
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_reset         integer       :=  0;
g_stg_count          integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_rec_out            DWH_PERFORMANCE.MART_FDS_LI_WK_TAB_REBUILD%rowtype;
g_rec_in             DWH_PERFORMANCE.TEMP_FOODS_TAB_DENSE_BUILD%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_max_date           date   := trunc(sysdate);
g_cnt                number := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_861U_ADHOC_I';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DISPLAY GROUP DATA EX INTACTIX ITEM DISPLAY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

  merge /*+ parallel (fli,4) */ into DWH_PERFORMANCE.MART_FDS_LI_WK_TAB_REBUILD fli using (
     select /*+ PARALLEL(a,4)  */
            FIN_YEAR_NO,
                  FIN_WEEK_NO,
                  ITEM_NO,
                  LOCATION_NO,
--                  this_week_start_date,
--                  sales,
                  sales_qty,
                  sales_cost,
                  sales_margin
/*                  prom_sales,
                  prom_sales_qty,
                  prom_sales_margin,
                  waste_cost,
                  waste_qty,
                  like_4_like_ind,
                  reg_rsp,
                  prom_rsp,
                  ruling_rsp */
      from DWH_DATAFIX.MART_FDS_LI_WK_AJ_REBUILD a 
--      from  DWH_DATAFIX.MART_FDS_LI_WK_AJ_REBUILD a
      where FIN_YEAR_NO = 2018
       and  FIN_WEEK_NO between 15 and 17
       
  ) mer_mart
  
  on (fli.FIN_YEAR_NO      = mer_mart.FIN_YEAR_NO and
      fli.FIN_WEEK_NO      = mer_mart.FIN_WEEK_NO and
      fli.ITEM_NO          = mer_mart.ITEM_NO and
      fli.LOCATION_NO      = mer_mart.LOCATION_NO 
     )

when matched then
  update 
--       set SALES         = mer_mart.SALES,
--           this_week_start_date = mer_mart.this_week_start_date,
       set SALES_QTY     = mer_mart.SALES_QTY,
           SALES_COST    = mer_mart.SALES_COST,
           SALES_MARGIN  = mer_mart.SALES_MARGIN
/*           prom_sales    = mer_mart.prom_sales,
           prom_sales_qty     = mer_mart.prom_sales_qty,
           prom_sales_margin  = mer_mart.prom_sales_margin,
           waste_cost         = mer_mart.waste_cost,
           waste_qty        = mer_mart.waste_qty,
           like_4_like_ind  = mer_mart.like_4_like_ind,
           reg_rsp          = mer_mart.reg_rsp,
           prom_rsp         = mer_mart.prom_rsp,
           ruling_rsp       = mer_mart.ruling_rsp */
     
;
  
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

  commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end do_merge;
  
--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF DWH_PERFORMANCE.MART_FDS_LI_WK_TAB_REBUILD EX INTACTIX STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
  --**************************************************************************************************
  -- De Duplication of the staging table to avoid Bulk insert failures
  --************************************************************************************************** 
      
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    select max(last_updated_date) into g_max_date from DWH_PERFORMANCE.TEMP_FOODS_TAB_DENSE_BUILD;

    do_merge;
   
    l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
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
end wh_prf_corp_861u_adhoc_i;
