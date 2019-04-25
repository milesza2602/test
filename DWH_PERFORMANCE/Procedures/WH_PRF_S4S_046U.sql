--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_046U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_046U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load Noshow-absconsion information for Scheduling for Staff(S4S)
--               Step 2 write to RTL_table
--               
--  Tables:      Input    - RTL_EMP_LOC_STATUS_DY 
--               Output   - DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY  
--                     then    DWH_PERFORMANCE.MART_ABSCONSION 
--  Packages:    dwh_constants, dwh_log, dwh_valid
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            MART_ABSCONSION%rowtype;
g_found              boolean;
g_date               date;
G_DATE_MIN_21             date;
G_START_DATE  date;
G_END_DATE  date;
G_noshow_start_DATE  date;
G_noshow_END_DATE  date;

G_PREV_no_show_ind number;
               G_START_RANK   number        :=  0;
               G_END_RANK   number        :=  0;

G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_constr_end_date  date;
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;
G_start_rnk number;
G_end_rnk number;
g_prev_SK1_LOCATION_NO	NUMBER(9,0);
g_prev_SK1_EMPLOYEE_ID	NUMBER(9,0);
G_NEW_SET_IND NUMBER;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_046U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE MART_ABSCONSION data  EX FOUNDATION';
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
    l_text := 'LOAD OF MART_ABSCONSION  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

-- hardcoding batch_date for testing
--   
--g_date := '9 nov 2014';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table


  G_START_DATE := G_DATE - 20;
  G_END_DATE := G_DATE;
   
   l_text := '21-DAYS PERIOD START= '||G_START_DATE||'-'||G_END_DATE;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_NOSHOW_LOC_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_NOSHOW_LOC_EMP_DY', DEGREE => 8);



--**************************************************************************************************
--
--    Load of DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY
--
--**************************************************************************************************
  l_text := '-------------------------------- ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'TRUNCATE TABLE DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY  ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY  ');   

    g_recs_read  := 0;
    g_recs_inserted  := 0;
 G_PREV_sk1_EMPLOYEE_ID :=NULL;
   
   for v_cur in
   (WITH selbat as 
                 (SELECT
                  /*+ full(flr) parallel(flr,6) */
                  max(flr.sys_load_date) sys_load_date,
                  sys_source_batch_id
                FROM DWH_PERFORMANCE.RTL_NOSHOW_LOC_EMP_DY   flr
               WHERE  flr.sys_source_batch_id = (select max(c.sys_source_batch_id) from DWH_PERFORMANCE.RTL_NOSHOW_LOC_EMP_DY c)
               group by sys_source_batch_id
                )
           , selemp as 
                 (SELECT
                  /*+ full(flr) parallel(flr,6) */
                  DISTINCT FLR.SK1_LOCATION_NO ,
                            FLR.SK1_employee_ID ,
                            dc.calendar_date,
                            sb.sys_load_date,
                  sb.sys_source_batch_id
                FROM dwh_performance.dim_calendar dc,
                     DWH_PERFORMANCE.RTL_NOSHOW_LOC_EMP_DY   flr, selbat sb
               WHERE  flr.sys_load_date = sb.sys_load_date
                and flr.sys_source_batch_id = sb.sys_source_batch_id
                and dc.calendar_date BETWEEN g_start_date AND g_end_date 
            --   and  sk1_employee_id in( 1070471, 1008133, 1003914,1009157,1008098)
                ORDER BY FLR.SK1_LOCATION_NO ,
                  FLR.SK1_employee_ID,
                  dc.calendar_date, sb.sys_load_date,
                  sb.sys_source_batch_id)
                , 
        selext AS
              (
              SELECT
                /*+ full(flr) parallel(flr,6) */
                DISTINCT  dc.calendar_date ,
                          FLR.BUSINESS_DATE ,
                          FLR.NO_SHOW_IND ,
                          DC.sK1_LOCATION_NO ,
                          dc.sk1_employee_ID
              FROM selemp dc
              JOIN DWH_PERFORMANCE.RTL_NOSHOW_LOC_EMP_DY flr
                    ON flr.business_date = dc.calendar_date
                    AND flr.SK1_employee_id  = dc.SK1_employee_id
                    AND flr.SK1_LOCATION_NO  = dc.SK1_LOCATION_NO
                    and flr.sys_load_date = dc.sys_load_date
                    and flr.sys_source_batch_id = dc.sys_source_batch_id
  ORDER BY dc.SK1_LOCATION_NO ,
                        dc.SK1_employee_ID,
                        dc.calendar_date
                  )
    SELECT 
              SK1_LOCATION_NO ,
              no_show_ind,
              sk1_employee_ID,
              calendar_date ,
              rnk
      FROM
              (SELECT BUSINESS_DATE ,
                      SK1_LOCATION_NO ,
                      no_show_ind,
                      sk1_employee_ID,
                      calendar_date,
                      DENSE_RANK () OVER ( PARTITION BY sk1_LOCATION_NO , sk1_employee_ID ORDER BY calendar_date) rnk
                FROM SELEXT
                             ) srk
      
              
      ORDER BY   sk1_employee_ID,
                 SK1_LOCATION_NO ,
                 calendar_date 
                 
)
loop

      g_recs_read              := g_recs_read + 1;

  /*  l_text :=  G_PREV_sk1_EMPLOYEE_ID||'-'||V_CUR.SK1_EMPLOYEE_ID
              ||'-'||G_PREV_sk1_LOCATION_NO||'-'||V_CUR.SK1_LOCATION_NO
              ||'-'||G_PREV_no_show_ind||'-'||V_CUR.no_show_ind
              ||'-'||v_cur.calendar_date
              ||'-'||g_noshow_start_date||'-'||g_noshow_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);      
    */      
-----------------------------------------------------------------------------
       IF (G_PREV_sk1_EMPLOYEE_ID IS NULL )
             THEN
              G_PREV_sk1_EMPLOYEE_ID := V_CUR.SK1_EMPLOYEE_ID;
              G_PREV_sk1_LOCATION_NO := V_CUR.SK1_LOCATION_NO;
              G_PREV_no_show_ind      := V_CUR.no_show_ind;
              g_noshow_start_date := v_cur.calendar_date;
              g_noshow_end_date := v_cur.calendar_date;
              G_START_RANK := V_CUR.RNK;
               G_END_RANK := V_CUR.RNK;
-----------------------------------------------------------------------------
      end if;
      if G_PREV_sk1_EMPLOYEE_ID <> V_CUR.SK1_EMPLOYEE_ID
      or G_PREV_no_show_ind  <> V_CUR.no_show_ind
      then  
            INSERT
                                  /*+ append */
                                INTO DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY VALUES
                                  (
                                    G_PREV_sk1_LOCATION_NO,
                                    G_PREV_sk1_EMPLOYEE_ID,
                                    g_noshow_start_date,
                                    g_noshow_end_date,
                                    G_DATE,
                                    G_PREV_no_show_ind, 
                                    G_END_RANK-G_START_RANK + 1
                                  );
                                g_recs_inserted := g_recs_inserted + 1;
                                COMMIT;
             G_prev_sk1_EMPLOYEE_ID  := V_CUR.SK1_EMPLOYEE_ID;
              G_prev_sk1_LOCATION_NO  := V_CUR.SK1_LOCATION_NO;
              G_prev_no_show_ind       := V_CUR.no_show_ind;
              g_noshow_start_date := v_cur.calendar_date;
              g_noshow_end_date := v_cur.calendar_date;
                G_START_RANK := V_CUR.RNK;
               G_END_RANK := V_CUR.RNK;
      else
         g_noshow_end_date  := v_cur.calendar_date;
             G_END_RANK := V_CUR.RNK;
      end if;
      
   
  end loop;
  IF G_PREV_no_show_ind = 1
  THEN 
    insert /*+ append */ into DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY  
                      VALUES(     G_PREV_sk1_LOCATION_NO,
                                  G_PREV_sk1_EMPLOYEE_ID,
                                  g_noshow_start_date,
                                  g_noshow_end_date,
                                  G_DATE,
                                  G_PREV_no_show_ind,
                                  G_END_RANK - G_START_RANK + 1);
                  g_recs_inserted            := g_recs_inserted + 1;
                  COMMIT;
                  END IF;
  
--**************************************************************************************************
--
--    Gather stats
--
--**************************************************************************************************
  l_text := 'TEMP_S4S_NOSHOW_LOC_EMP_DY recs inserted='||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_NOSHOW_LOC_EMP_DY  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_NOSHOW_LOC_EMP_DY', DEGREE => 8);


--**************************************************************************************************
--
--    Load of DWH_PERFORMANCE.MART_ABSCONSION
--
--**************************************************************************************************
    g_recs_read  := 0;
    g_recs_inserted  := 0;
    
  l_text := '-------------------------------- ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'TRUNCATE TABLE DWH_PERFORMANCE.MART_ABSCONSION ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_PERFORMANCE.MART_ABSCONSION  ');   

      INSERT /*+ APPEND */ INTO DWH_PERFORMANCE.MART_ABSCONSION 
      SELECT SK1_LOCATION_NO
            ,SK1_EMPLOYEE_ID
            ,MISSED_SHIFT_START_DATE
            ,MISSED_SHIFT_END_DATE
            , NO_SHOW_COUNT
            ,G_DATE LAST_UPDATED_DATE
            
      FROM DWH_PERFORMANCE.TEMP_S4S_NOSHOW_LOC_EMP_DY
      where    MISSED_SHIFT_START_DATE <> MISSED_SHIFT_END_DATE
      and no_show_ind = 1
               ;

   g_recs_read:=SQL%ROWCOUNT;
   g_recs_inserted:=SQL%ROWCOUNT;
 
   commit;
    
  l_text := 'Running GATHER_TABLE_STATS ON MART_ABSCONSION  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'MART_ABSCONSION', DEGREE => 8);
                                   
  l_text := 'MART_ABSCONSION recs inserted='||g_recs_inserted;
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



END WH_PRF_S4S_046U;
