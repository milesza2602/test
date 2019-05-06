--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_842U_DJ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_842U_DJ" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
    )
AS
  --**************************************************************************************************
  --  Date:        May 2015
  --  Author:      W Lyttle
  --  Purpose:     Load DJ(David Jones) Allocation data to 'allocation tracker receipts at first warehouse table'
  --               for supply_chain_code = 'XD'.
  --               from the 4 temp tables split into 4 periods;
  --               NB> the last 120-days sub-partitions are truncated via a procedure before this one runs.
  --               For CHBD only.
  --  Tables:      Input  - temp_alloc_tracker_frst_rcpt_XD_DJ1,temp_alloc_tracker_frst_rcpt_XD_DJ2,temp_alloc_tracker_frst_rcpt_XD_DJ3,temp_alloc_tracker_frst_rcpt_XD_DJ4
  --               Output - fnd_alloc_tracker_frst_rcpt_DJ
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --               BK - adjust stats collection on very large partitioned tables for 12c upgrade (old way is very slow)           Ref: 23Jan2019
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_date DATE;
  g_start_date DATE;
  g_rec_out fnd_alloc_tracker_alloc%rowtype;

  g_tot_ins NUMBER  := 0;
  g_count_weeks NUMBER  := 0;

  g_min_fin_year_no NUMBER  := 0;
  g_min_fin_month_no NUMBER  := 0;
  g_min_fin_week_no NUMBER  := 0;

  g_max_fin_year_no NUMBER  := 0;
  g_max_fin_month_no NUMBER  := 0;
  g_max_fin_week_no NUMBER  := 0;

    g_begin_subpart VARCHAR2(200);
  g_end_subpart VARCHAR2(200);
  G_COUNT_DAYS  NUMBER;
  G_PART_DT  VARCHAR2(6);
  G_SUBPART_NAME  VARCHAR2(200);
  G_SQL_TRUNC_PARTITION VARCHAR2(200);
  G_SUB_DATE DATE;
  G_START_WK_DATE DATE;
   g_end_wk_date date;
 -- p_from_loc_no integer := 0;
 -- p_to_loc_no integer := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_842U_DJ';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS Alloc Data TO ALLOC TRACKER receipts at first warehouse TABLE';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  

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
      -- TESTING
      --G_DATE := '19 MAY 2015';
      -- TESTING
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--
--       **** determine period
--       going to do from monday or current_day-120 to sunday current_week
--
      select distinct this_week_start_date, fin_year_no, fin_month_no, fin_week_no into g_start_wk_date, g_min_fin_year_no, g_min_fin_month_no, g_min_fin_week_no
      from dim_calendar 
 --     where calendar_date = g_date - 120;
      where calendar_date = '7 may 2015';
      -- for full refresh
      
      select distinct this_week_end_date, fin_year_no, fin_month_no, fin_week_no into g_end_wk_date, g_max_fin_year_no, g_max_fin_month_no, g_max_fin_week_no
      from dim_calendar 
      where calendar_date = g_date ;  

      select count(distinct this_week_start_date), COUNT(CALENDAR_DATE) into g_count_weeks, G_COUNT_DAYS
      FROM DIM_CALENDAR 
      where calendar_date  between g_start_wk_date and g_end_wk_date;

       l_text := 'DATA LOADED FOR PERIOD '||g_start_wk_date||' TO '||g_end_wk_date||' **no_of_weeks='||g_count_weeks;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

       l_text := 'FULL REFRESH ** truncate table dwh_foundation.fnd_alloc_tracker_frst_rcpt_dj';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
        execute immediate ('truncate table dwh_foundation.fnd_alloc_tracker_frst_rcpt_dj');   
--**************************************************************************************************
-- build partition_truncate_sql
  -- TFND_ATAND_150515 
--**************************************************************************************************

      G_SUB_DATE := g_end_wk_date + 2;
      g_count_days := g_count_days - 1;
      
      for g_sub in 0..G_COUNT_DAYS loop
      
                      G_SUB_DATE := G_SUB_DATE - 1  ;
                      
                      select TO_CHAR(CALENDAR_DATE,'DDMMYY') INTO G_PART_DT
                      from dim_calendar 
                      where calendar_date  = G_SUB_DATE;
                      
                        G_SUBPART_NAME := 'FND_FATAND_'||G_PART_DT;
                        
                        g_sql_trunc_partition := 'alter table dwh_foundation.FND_ALLOC_TRACKER_FRST_RCPT_DJ truncate SUBPARTITION '||G_SUBPART_NAME;

                        
                        EXECUTE IMMEDIATE g_sql_trunc_partition;
                        commit; 
       
                    if g_sub = 0 then
                           g_begin_subpart := G_SUBPART_NAME;
                        else
                          g_end_subpart := G_SUBPART_NAME;
                        end if;
      END LOOP;

     l_text := 'truncate SUBPARTITION  - from '||g_begin_subpart||' to '||g_end_subpart;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := 'Running GATHER_TABLE_STATS ON FND_ALLOC_TRACKER_FRST_RCPT_DJ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
--                                   'FND_ALLOC_TRACKER_FRST_RCPT_DJ', DEGREE => 8);  
                                   
    -- below is the new code for stats collection in 12c - Ref: 23Jan2019
     DWH_FOUNDATION.GENERIC_GATHER_TABLE_STATS(l_procedure_name,'FND_ALLOC_TRACKER_FRST_RCPT_DJ');
     g_tot_ins := 0; 

--*********************************************************************************************   
-- full insert
--********************************************************************************************* 
    INSERT /*+ APPEND */
      INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ
      SELECT
        /*+ full(a)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_XD_DJ1 a
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_XD_DJ1 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(b)  */
                *
              FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_XD_DJ2 b
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_XD_DJ2 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(c)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_XD_DJ3 c
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_XD_DJ3 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(d)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_XD_DJ4 d
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_XD_DJ4 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(e)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_WH_DJ1 e
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_WH_DJ1 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(f)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_WH_DJ2 f
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_WH_DJ2 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(g)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_WH_DJ3 g
            ;
           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_WH_DJ3 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
    INSERT /*+ APPEND parallel(z,4) */
    INTO dwh_foundation.fnd_alloc_tracker_frst_rcpt_DJ z 
      SELECT
        /*+ full(h)  */
                *
            FROM dwh_foundation.TEMP_A_TRACK_FRST_RCPT_WH_DJ4 h
            ;

           g_recs_read     := 0;
           g_recs_inserted := 0;
           g_recs_read     :=SQL%ROWCOUNT;
           g_recs_inserted :=SQL%ROWCOUNT;
            COMMIT;
     g_tot_ins := g_tot_ins +   g_recs_inserted;  
     l_text := 'TEMP_A_TRACK_FRST_RCPT_WH_DJ4 inserted='||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--------------------------------------------------------------------------------------     
     l_text := 'Running GATHER_TABLE_STATS ON FND_ALLOC_TRACKER_FRST_RCPT_DJ';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
--                                       'FND_ALLOC_TRACKER_FRST_RCPT_DJ', DEGREE => 8);

    -- below is the new code for stats collection in 12c - Ref: 23Jan2019
     DWH_FOUNDATION.GENERIC_GATHER_TABLE_STATS(l_procedure_name,'FND_ALLOC_TRACKER_FRST_RCPT_DJ');

    g_recs_read := g_tot_ins;
    g_recs_inserted := g_tot_ins;     
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
END WH_FND_CORP_842U_dj;
