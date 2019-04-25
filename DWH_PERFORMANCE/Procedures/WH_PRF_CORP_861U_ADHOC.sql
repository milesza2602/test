--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_861U_ADHOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_861U_ADHOC" (p_forall_limit in integer,p_success out boolean) as

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
g_rec_out            DWH_PERFORMANCE.mart_fds_loc_item_wk_tableau%rowtype;
g_rec_in             DWH_PERFORMANCE.TEMP_FOODS_TAB_DENSE_BUILD%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_max_date           date   := trunc(sysdate);
g_start_date         date ;
g_end_date           date ;
g_loop_date          date ;
g_cnt                number := 0;
g_fin_week_no        number        :=  0;
g_fin_year_no        number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_861U_ADHOC';
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

  g_loop_date := '27 mar 17';

  FOR g_sub IN 0..12
  LOOP
    g_recs_read := 0;
    SELECT
      this_week_start_date,
      this_week_end_date,
      fin_year_no,
      fin_week_no
    INTO
      g_start_date,
      g_end_date,
      g_fin_year_no,
      g_fin_week_no
    FROM dim_calendar
    WHERE calendar_date = g_loop_date + (g_sub * 7); 

  l_text       := '-------------------------------------------------------------';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--  l_text       := 'Rollup range is:- '||g_start_date||'  To '||g_end_date;
  l_text       := 'Year/week range is:- '||g_fin_year_no||' '||g_fin_week_no;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--  merge /*+ parallel (fli,4) */ into dwh_datafix.aj_mart_fds_loc_item_wk_tab fli using 
  merge /*+ parallel (fli,4) */ into dwh_performance.MART_FDS_LOC_ITEM_WK_TABLEAU fli using 
 
  (
  with SELITM as (select SK1_ITEM_NO, item_no
                  from DIM_ITEM where BUSINESS_UNIT_NO = 50),
        SELLOC as (select LOCATION_NO, SK1_LOCATION_NO
                   from dim_location where area_no = 9951)
--        SELCAL as (select distinct THIS_WEEK_START_DATE, FIN_YEAR_NO, FIN_QUARTER_NO, FIN_MONTH_NO, FIN_WEEK_NO, (FIN_YEAR_NO*100+FIN_WEEK_NO) FIN_YRWK_NO
--                   from dim_calendar 
--                   where CALENDAR_DATE between '11 sep 17' and '18 sep 17')
                   
    select /*+ parallel(SRC,8) */ src.fin_year_no, src.fin_week_no, si.item_no, sl.location_no
--           , sum(nvl(fd_num_cust_avail_adj,0))  fd_num_cust_avail_adj
           , sum(nvl(fd_num_cust_catlg_adj,0))  fd_num_cust_catlg_adj -- new
           , SUM(NVL(fd_cust_avail,0))          fd_cust_avail -- new 
    from  RTL_LOC_ITEM_WK_CATALOG SRC, SELITM SI, SELLOC SL
    where src.sk1_item_no = si.sk1_item_no 
    and   src.sk1_location_no = sl.sk1_location_no 
    and   src.fin_year_no = g_fin_year_no 
    and   src.fin_week_no = g_fin_week_no    
    and   (fd_num_catlg_days > 0 or fd_num_catlg_days_adj > 0 or fd_num_cust_catlg_adj > 0)
    group by SRC.fin_year_no, SRC.fin_week_no, si.item_no, sl.location_no
       
  ) mer_mart
  
  on (fli.FIN_YEAR_NO      = mer_mart.FIN_YEAR_NO and
      fli.FIN_WEEK_NO      = mer_mart.FIN_WEEK_NO and
      fli.ITEM_NO          = mer_mart.ITEM_NO and
      fli.LOCATION_NO      = mer_mart.LOCATION_NO 
     )

when matched then
  update 
       set fd_num_cust_catlg_adj = mer_mart.fd_num_cust_catlg_adj,
           fd_cust_avail         = mer_mart.fd_cust_avail
           
    ;
  
  
   g_recs_read      :=  g_recs_read + sql%rowcount;
--   g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;
   g_recs_updated   :=  g_recs_updated + sql%rowcount;
   
   l_text := 'RECORDS PROCESSED :- '||g_recs_read||' '||g_fin_year_no||' '||g_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  COMMIT;

 end loop;

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

    l_text := 'LOAD OF DWH_PERFORMANCE.mart_fds_loc_item_wk_tableau STARTED AT '||
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
   
    do_merge;
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
end wh_prf_corp_861u_adhoc;
