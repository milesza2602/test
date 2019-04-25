--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_248U_2013L4L
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_248U_2013L4L" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- LIKE-4-LIKE rerun of data from beginning fin_year=2013
  -- WENDY LYTTLE SEPTEMBER 2013
  -- testing location_no = 3043 - fin_year_no = 2013 - 0 to 1
  -- testing location_no = 230 - fin_year_no = 2012 - 1 to 0
  -- testing location_no = 103 - fin_year_no = 2013 - 1
  -- testing location_no = 105 - fin_year_no = 2013 - 0 to 1
  --**************************************************************************************************
  --  Date:        July 2009
  --  Author:      Wendy Lyttle
  --  Purpose:     Load like4like ind table in the foundation layer
  --               with input ex staging table from an Excel SS ex finance.
  --  Tables:      Input  - dwh_datafix.TMP_2013_L4L
  --               Output - dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  08 July 2009 - defect 2017 - Add field LIKE_FOR_LIKE_ADJ_IND to tables
  --                               dwh_datafix.tmp_fnd_rtl_loc_dy_l4l and RTL_LOC_DY
  --  14 August 2009 - defect 2252 - Ensure that check for valid location_no is
  --                                 done in FND and not PRF for Like4Like
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := 10000;
  g_recs_read     INTEGER := 0;
  g_recs_updated1  INTEGER := 0;
  g_recs_updated2  INTEGER := 0;
    g_recs_updated3  INTEGER := 0;
      g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_inserted2 INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_recs_zeroised INTEGER := 0;
  g_count         NUMBER  := 0;
  g_hospital      CHAR(1) := 'N';
  g_hospital_text stg_excel_like_4_like_hsp.sys_process_msg%type;
  g_found BOOLEAN;
  g_date DATE          := TRUNC(sysdate);
  g_fin_year_no    NUMBER := 0;
  g_fin_week_no    NUMBER := 0;
  g_fin_day_no     NUMBER := 0;
  g_ly_fin_year_no NUMBER := 0;
  g_ly_fin_week_no NUMBER := 0;
  g_week1 NUMBER := 0;
  g_week2 NUMBER := 0;
  g_ly_calendar_date DATE;
  v_like_for_like_ind     NUMBER(1);
  v_like_for_like_adj_ind NUMBER(1);
  g_sub                   NUMBER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_248U_2013L4L';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD LIKE FOR LIKE TRANSACTION EX FINANCE SPREADSHEET';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For input bulk collect --

  --**************************************************************************************************
  --                           M A I N     P R O C E S S
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF dwh_datafix.tmp_fnd_rtl_loc_dy_l4l EX POS STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- SETUP TABLES FOR PROCESSING
  --**************************************************************************************************
  EXECUTE immediate ('truncate table dwh_datafix.tmp_fnd_rtl_loc_dy_l4l');
  COMMIT;
  l_text := 'TRUNCATED table dwh_datafix.tmp_fnd_rtl_loc_dy_l4l';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  INSERT INTO dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
  SELECT *
  FROM fnd_rtl_loc_dy_like_4_like
  WHERE 
  --location_no IN( 3043,230,103,105)
--  --and 
  CALENDAR_DATE < '25 june 2012';
  
  g_recs_read       := SQL%ROWCOUNT;
  COMMIT;
  
  l_text := 'Load table dwh_datafix.tmp_fnd_rtl_loc_dy_l4l - recs='||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  g_recs_read := 0;
    --**************************************************************************************************
    -- Look up batch date from dim_control
    --*************************************************************************************************
  g_date := '11 june 2012';
  
  FOR G_Sub IN 0..13
  LOOP


    G_DATE := G_DATE + 7;
    
    SELECT FIN_WEEK_NO INTO G_WEEK1
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = G_DATE + 7;
    SELECT FIN_WEEK_NO INTO G_WEEK2
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = G_DATE + 14;


    --
    -- Following values are used in processing
    --
    SELECT ly_calendar_date,
      fin_year_no,
      fin_week_no,
      fin_day_no,
      ly_fin_year_no,
      ly_fin_week_no
    INTO g_ly_calendar_date,
      g_fin_year_no,
      g_fin_week_no,
      g_fin_day_no,
      g_ly_fin_year_no,
      g_ly_fin_week_no
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE    = g_date;
 
 
 l_text := ' g_date='|| g_date||
' g_ly_calendar_date='|| g_ly_calendar_date||
' g_fin_year_no='||g_fin_year_no||
' g_fin_week_no='||g_fin_week_no||
' g_fin_day_no='||g_fin_day_no||
' g_ly_fin_year_no='||g_ly_fin_year_no||
' g_ly_fin_week_no='||g_ly_fin_week_no;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    
    IF g_ly_calendar_date IS NULL THEN
      SELECT MIN(ly_calendar_date) INTO g_ly_calendar_date FROM DIM_CALENDAR;
              l_text := 'BATCH DATE and LY BATCH DATE not found:- '||g_date||' - '||g_ly_calendar_date||' wks='||g_week1||','||g_week2;
    ELSE
              l_text := 'BATCH DATE and LY BATCH DATE:- '||g_date||' - '||g_ly_calendar_date||' wks='||g_week1||','||g_week2;
    END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    



    -------------------------------------------------------------------------------
    --  DESIGN :
    --
    --> At the start of each Fin Year (Fin Week 1 Fin Day 1)
    --              the values in the L4L Adj column
    --              for matching Fin Week and Fin Day of the "Last Year",
    --              for the entire ("Last Year") year must be set to zero ("0").
    -------------------------------------------------------------------------------
    IF g_fin_week_no = 1 AND g_fin_day_no = 1 THEN
      UPDATE dwh_datafix.tmp_fnd_rtl_loc_dy_l4l rld
      SET rld.like_for_like_adj_ind = 0,
        last_updated_date           = '31 dec 2030'
      WHERE rld.calendar_date      IN
        (SELECT dc2.calendar_date
        FROM dim_calendar dc2
        WHERE dc2.ly_fin_year_no = g_ly_fin_year_no
        );
        
      g_recs_zeroised := SQL%ROWCOUNT;
      l_text          := 'LY fin_year - '||g_ly_fin_year_no||' zeroised adj_ind : RECORDS UPDATED = '||g_recs_zeroised;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      COMMIT;
 --     
 --        
 --     
 --     
 --     
    END IF;
    --**************************************************************************************************
    -- Look up batch date from dim_control
    --*************************************************************************************************
    FOR V_CUR IN
    (SELECT /*+ parallel (a,4) */ a.LOCATION_NO
      --,a.LOCATION_NAME
      ,
      a.fin_year_no c_fin_year_no ,
      a.fin_week_no c_fin_week_no ,
      c.fin_day_no c_fin_day_no ,
      c.calendar_date c_calendar_date ,
      c.ly_calendar_date c_ly_calendar_date ,
      c.ly_fin_year_no c_ly_fin_year_no ,
      c.ly_fin_week_no c_ly_fin_week_no ,
      a.LIKE_FOR_LIKE_IND c_like_for_like_ind 
 --     C.ly_calendar_date g_ly_calendar_date,
 --     C.fin_year_no g_fin_year_no,
 --     C.fin_week_no g_fin_week_no,
 --     C.fin_day_no g_fin_day_no,
 --     C.ly_fin_year_no g_ly_fin_year_no,
 --     C.ly_fin_week_no g_ly_fin_week_no
    FROM
      --dwh_foundation.STG_EXCEL_LIKE_4_LIKE_arc a,
      dwh_datafix.TMP_2013_L4L a,
      dim_calendar c
    WHERE c.fin_year_no = a.fin_year_no
    AND c.fin_week_no   = a.fin_week_no
--    AND c.fin_year_no   = 2013
    and C.FIN_WEEK_NO in(g_week1, g_week2)
 --   AND location_no IN (3043)
    --   AND location_no IN (3043,230,103,105)
    ORDER BY a.LOCATION_NO ,
      a.FIN_YEAR_NO ,
      a.FIN_WEEK_NO ,
      c.FIN_DAY_NO
    )
    LOOP
-------------------------------------------------------------------------------
--  DESIGN :
--
--> If loading data for the current Fin Year
--       then populate both columns
--       and the L4L Adj column
--          for the same Fin Week, Fin Day "Last Year"
-------------------------------------------------------------------------------

--  l_text          := 'vc_fin_year_no='||v_cur.c_fin_year_no 
--  ||' vc_in_week_no='||v_cur.c_fin_week_no 
--  ||' vc_fin_day_no='||v_cur.c_fin_day_no 
--  ||' vc_calendar_date='||v_cur.c_calendar_date 
--  ||' vc_ly_calendar_date='||v_cur.c_ly_calendar_date 
--  ||' vc_ly_fin_year_no='||v_cur.c_ly_fin_year_no 
--  ||' vc_ly_fin_week_no='||v_cur.c_ly_fin_week_no 
--  ||' vc_like_for_like_ind='||v_cur.c_like_for_like_ind;
--        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
 --    IF v_cur.c_fin_week_no  = 1 THEN 
 -- insert into  dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
 --              values(v_cur.LOCATION_NO
 --                    ,v_cur.c_CALENDAR_DATE
 --                    ,v_cur.c_like_for_like_ind
 --                    , ''
 --                    ,g_date
 --                    ,v_cur.c_like_for_like_ind);
 --commit;
 --        g_recs_inserted := g_recs_inserted + 1;
 --     end if;
  
        
           if v_cur.c_fin_year_no = g_fin_year_no
           then
                v_like_for_like_ind     := v_cur.c_like_for_like_ind;
                v_like_for_like_adj_ind := v_cur.c_like_for_like_ind;
--                      l_text          := 'upd1-'||v_cur.c_like_for_like_ind;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                begin
                     update dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
                        set like_for_like_adj_ind = v_cur.c_like_for_like_ind,
                            last_updated_date = '31 dec 2030'
                      where calendar_date = v_cur.c_ly_calendar_date
                        and location_no = v_cur.location_no;
                     commit;
                    g_recs_updated1 := g_recs_updated1 + 1;
                exception
                   when no_data_found
                       then
                         l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
                         dwh_log.record_error(l_module_name,sqlcode,l_message);
                end;
                
           else
-------------------------------------------------------------------------------
--  DESIGN :
--
--> If loading data for a previous Fin Year
--     update only the L4L column
--            for that date
--      and also the L4L Adj column
--             for the same Fin Week, Fin Day "Last Year"
--> For historical records, updates only no inserts for L4L Adj process
-------------------------------------------------------------------------------
           if v_cur.c_fin_year_no < g_fin_year_no then
                v_like_for_like_ind := v_cur.c_like_for_like_ind;
--                      l_text          := 'upd2-'||v_cur.c_like_for_like_ind;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                begin
                   update dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
                      set like_for_like_adj_ind = v_cur.c_like_for_like_ind,
                          last_updated_date = '31 dec 2030'
                   where calendar_date = v_cur.c_ly_calendar_date
                     and location_no = v_cur.location_no;
                   commit;
                   g_recs_updated2 := g_recs_updated2 + 1;
           exception
                    when no_data_found
                        then
                            l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
                            dwh_log.record_error(l_module_name,sqlcode,l_message);
                end;
           end if;
           end if;
--**************************************************************************************************
-- Check to see if item is present on table
--      and update/insert accordingly Fin Week, Fin Day "This Year"
--**************************************************************************************************
    g_count := null;

    select count(1)
      into   g_count
     from    dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
    where  location_no      = v_cur.location_no
      and  calendar_date    = v_cur.c_calendar_date;

   if g_count = 1 then
--      l_text          := 'upd3-'||v_cur.c_like_for_like_ind;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       update  dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
       set    like_for_like_ind            = v_like_for_like_ind,
              like_for_like_adj_ind        = case
                                             when v_cur.c_fin_year_no < g_fin_year_no
                                                 then
                                                  v_cur.c_like_for_like_ind
                                             else
                                                  like_for_like_adj_ind
                                             end,
              last_updated_date          = '31 dec 2030'
       where  location_no                = v_cur.location_no
         and  calendar_date              = v_cur.c_calendar_date;
         commit;
        g_recs_updated3  := g_recs_updated3 + 1;
    else
      if v_cur.c_fin_year_no = g_fin_year_no then
--          l_text          := 'ins-'||v_cur.c_like_for_like_ind;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          insert into  dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
               values(v_cur.LOCATION_NO
                     ,v_cur.c_CALENDAR_DATE
                     ,v_cur.c_like_for_like_ind
                     , ''
                     ,g_date
                     ,v_cur.c_like_for_like_ind);
         commit;
         g_recs_inserted := g_recs_inserted + 1;
      end if;
    end if;

--    dbms_output.put_line(g_recs_read||' '||g_recs_inserted||' '||g_recs_updated||' '||g_recs_hospital);
 end loop;
       l_text          := 'RECORDS ins='||g_recs_inserted||'upd='||g_recs_updated1||'-'||g_recs_updated2||'-'||g_recs_updated3||' wks='||g_week1||','||g_week2;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  END LOOP;
  
  insert into dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
  select z.* from dwh_foundation.fnd_rtl_loc_dy_like_4_like z,
  (select LOCATION_NO, CALENDAR_DATE from dwh_foundation.fnd_rtl_loc_dy_like_4_like 
  minus
  select LOCATION_NO, CALENDAR_DATE from dwh_datafix.tmp_fnd_rtl_loc_dy_l4l) y
  where y.location_no = z.location_no
  and y.calendar_date = z.calendar_date;
   g_recs_inserted2 := SQL%ROWCOUNT;
      l_text          := 'RECORDS inserted 2 = '||g_recs_inserted2;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  commit;
  
  g_recs_updated := g_recs_updated1 + g_recs_updated2 + g_recs_updated3;
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
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
END WH_FND_CORP_248U_2013L4L;
