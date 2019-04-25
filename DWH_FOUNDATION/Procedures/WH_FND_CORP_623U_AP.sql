--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_623U_AP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_623U_AP" (p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
--  Date:        may 2015
--  Author:      W Lyttle
--  Purpose:     Update fnd_rtl_allocation_aps with derived first_dc_no for supply_chain_code = 'WH'
--  Tables:      Input  - temp_alloc_first_dc_no
--               Output - fnd_rtl_allocation_ap
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_date               date;
g_start_date         date;
g_rec_out            fnd_alloc_tracker_alloc%rowtype;

  g_date_min_120    DATE;
  g_date_min_90    DATE;
  g_date_min_91    DATE;
  g_date_min_60    DATE;
  g_date_min_61    DATE;
  g_date_min_30    DATE;
  g_date_min_31    DATE;
  g_START_WK_DATE    DATE;
  g_END_WK_DATE    DATE;
  g_TEST_DATE    DATE;
  G_COUNT_WEEKS    NUMBER := 0;
   G_alloc_CALC NUMBER := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_623U_AP';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'create temp table with correct first_dc_no';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
       IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
        g_forall_limit  := p_forall_limit;
      END IF;
      
      p_success := false;
      
      l_text    := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
    
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      EXECUTE immediate 'alter session enable parallel dml';

      
      --**************************************************************************************************
      -- Look up batch date from dim_control
      --**************************************************************************************************
      Dwh_Lookup.Dim_Control(G_Date);
      --g_date := g_date -1;    --XXX
--- testing may 2015
 --    g_date := '24 may 2015';
--- testing may 2015     
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      --||
      --' '||p_from_loc_no||' '||p_to_loc_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


 for g_sub in 0..19 loop
    select  distinct this_week_start_date, this_week_end_date
    into    g_start_wk_date, g_end_wk_date
    from   dim_calendar
    where  calendar_date = g_date - (g_sub * 7);     
--*********************************************************************************************   
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
--
--       **** determine period
--       going to do from monday or current_day-120 to sunday current_week
--
      g_recs_read    := 0;
      g_recs_UPDATED := 0;
      G_alloc_CALC := 0;
      
      for v_cur in (
                WITH selall AS
                            ( SELECT /*+ PARALLEL(FND,4) */ DISTINCT alloc_no
                            FROM fnd_rtl_allocation_ap FND
                            WHERE po_no IS NULL
                            AND release_date BETWEEN g_start_wk_date AND g_END_wk_date
                            ) ,
                    seltmp AS
                            (SELECT
                              /*+ full(tmp) */
                              alloc_no,
                              MIN(tmp.first_dc_no) FIRST_DC_NO
                            FROM dwh_foundation.temp_alloc_first_dc_no tmp
                            GROUP BY alloc_no
                            )
                   SELECT DISTINCT A.alloc_no,
                            B.first_dc_no
                          FROM selall a,
                            seltmp b
                          WHERE a.alloc_no = b.alloc_no 
                         )
                          loop


-- run update  
     update dwh_foundation.fnd_rtl_allocation_ap fnd
             set fnd.first_dc_no = v_cur.first_dc_no
             where fnd.alloc_no = v_cur.alloc_no
    ;

                 
      g_recs_read    := g_recs_read  + SQL%ROWCOUNT;
      g_recs_UPDATED := g_recs_UPDATED + SQL%ROWCOUNT;
      
      
         COMMIT;
     end loop;
     
     l_text := 'period loaded : '||g_start_wk_date||' TO '||g_END_wk_date||' - RECS='||g_recs_read;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
      
-- run update  


end loop;

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



END WH_FND_CORP_623U_AP;
