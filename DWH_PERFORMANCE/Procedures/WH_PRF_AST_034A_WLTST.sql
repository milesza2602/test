--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034A_WLTST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034A_WLTST" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
    AS
    --
     --**************************************************************************************************

      --**************************************************************************************************
      --  date:        april 2013
      --  author:      wendy lyttle
      --  purpose:
      --  tables:      input  - rtl_sc_trading
      --               output - temp_rtl_ast_sc_cont_wk_034a
      --  packages:    constants, dwh_log, dwh_valid
      --
      --  maintenance:
      --  May 2013 - wendy - new version of wh_prf_ast_034u 
      --                   - write to temp_rtl_ast_sc_cont_wk_034a (old version rtl_sc_continuity)
      --  21 June 2013 - release to prd
      --  naming conventions
      --  g_  -  global variable
      --  l_  -  log table variable
      --  a_  -  array variable
      --  v_  -  local variable as found in packages
      --  p_  -  parameter
      --  c_  -  prefix to cursor
      --**************************************************************************************************
      g_forall_limit              INTEGER := dwh_constants.vc_forall_limit;
      g_recs_read                 INTEGER := 0;
      g_recs_inserted             INTEGER := 0;
      g_date                      DATE;
      g_this_week_start_date      DATE;
      g_this_week_start_date_42   DATE;
      g_curr_season_start_date    DATE;
      g_curr_season_end_date    DATE;
      g_curr_season_wk_start_date DATE;
      g_date_minus_84             DATE;
      g_fin_year_no               NUMBER;
      g_fin_half_no               NUMBER;
      g_fin_year_no_NEW               NUMBER;
      g_fin_half_no_NEW               NUMBER;
      g_NEW_season_start_date    DATE;
      g_NEW_season_end_date    DATE;
            g_new_season_wk_start_date DATE;
      l_message sys_dwh_errlog.log_text%type;
      l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_AST_034A_WLTST';
      l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_roll;
      l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_roll;
      l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
      l_text sys_dwh_log.log_text%type ;
      l_description sys_dwh_log_summary.log_description%type   := 'LOAD temp_rtl_ast_sc_cont_wk_034a';
      l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
      --**************************************************************************************************
      -- main process
      --**************************************************************************************************
    BEGIN
      IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
        g_forall_limit  := p_forall_limit;
      END IF;
      p_success := false;
      l_text    := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      L_TEXT := 'LOAD OF temp_rtl_ast_sc_cont_wk_034a STARTED AT '||TO_CHAR(SYSDATE,('DD MON YYYY HH24:MI:SS'));
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      DWH_LOG.INSERT_LOG_SUMMARY(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_DESCRIPTION, L_PROCESS_TYPE,DWH_CONSTANTS.VC_LOG_STARTED,'','','','','');  
      
   --   EXECUTE immediate 'ALTER SESSION ENABLE PARALLEL DML';

      --**************************************************************************************************
      --
      -- s t e p 1 : setup dates required for processing
      --
      --**************************************************************************************************
      dwh_lookup.dim_control(g_date);
--      g_date := g_date + 1;
--      l_text := 'BATCH DATE BEING PROCESSED - '||G_DATE;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  

       G_DATE := '30 OCT 2016';
      g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date||' THRU 30 OCT 2016';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


      
      g_date_minus_84 := g_date - (7 * 12);
      
      SELECT this_week_start_date,        this_week_start_date - 42
      INTO g_this_week_start_date,        g_this_week_start_date_42
          FROM dim_calendar
          WHERE calendar_date = g_date;
      
      SELECT fin_half_no,        fin_year_no
      INTO g_fin_half_no,        g_fin_year_no
          FROM dim_calendar
          WHERE calendar_date = g_this_week_start_date_42; 
      
      SELECT DISTINCT MIN(season_start_date),        MIN(this_week_start_date), min(FIN_HALF_end_date)
      INTO g_curr_season_start_date,                 g_curr_season_wk_start_date, g_curr_season_end_date
          FROM dim_calendar
          WHERE fin_half_no = g_fin_half_no
          AND Fin_Year_No   = G_Fin_Year_No;
 
 
       SELECT fin_half_no,        fin_year_no
      INTO g_fin_half_no_NEW,        g_fin_year_no_NEW
          FROM dim_calendar
          WHERE calendar_date = g_this_week_start_date; 
      
      SELECT DISTINCT MIN(season_start_date),        MIN(this_week_start_date), min(FIN_HALF_end_date)
      INTO g_NEW_season_start_date,                 g_NEW_season_wk_start_date, g_NEW_season_end_date
          FROM dim_calendar
          WHERE fin_half_no = g_fin_half_no_NEW
          AND Fin_Year_No   = G_Fin_Year_No_NEW;
 
 
      
      l_text := 'G_This_Week_Start_Date = '||g_this_week_start_date||' G_This_Week_Start_Date_42 = '||g_this_week_start_date_42;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'G_Date_Minus_84 = '||g_date_minus_84;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'G_Fin_Half_No = '||g_fin_half_no||' G_Fin_Year_No = '||g_fin_year_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'G_CURR_Season_Start_Date = '||g_CURR_season_start_date||' G_CURR_Season_End_Date = '||g_CURR_season_end_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := ' G_CURR_Season_Wk_Start_Date = '||g_CURR_season_wk_start_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'G_Fin_Half_No_NEW = '||g_fin_half_no_NEW||' G_Fin_Year_No_NEW = '||g_fin_year_no_NEW;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'G_NEW_Season_Start_Date = '||g_NEW_season_start_date||' G_NEW_Season_End_Date = '||g_NEW_season_end_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := ' G_NEW_Season_Wk_Start_Date = '||g_NEW_season_wk_start_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      --**************************************************************************************************
      --
      -- s t e p 2 : write to temp_rtl_sc_continuity_wk
      --             all the dates per sk1_tyle_colour_no/continuitty_ind
      --
      --           : default dates to '01-01-3030' when null
      --
      --**************************************************************************************************




      execute immediate 'Truncate Table dwh_performance.Wtemp_rtl_ast_sc_cont_wk_034a';
     l_text := 'Truncate Table dwh_performance.Wtemp_rtl_ast_sc_cont_wk_034a';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            
      insert 
      into dwh_performance.Wtemp_rtl_ast_sc_cont_wk_034a a
   with selext as
      (select 
               sk1_style_colour_no,
              continuity_ind,
              season_first_trade_date,  
              cont_prev_week_date,
              cont_start_week_date,
              cont_end_week_date,
              fash_start_week_date,
               fash_end_week_date,
               last_week_updated
              from 
              (select 
                                                                                                                              sk1_style_colour_no,
                                                                                                                              continuity_ind,
                                                                                                                              season_first_trade_date,  
              (case when continuity_ind = 1 then c1.this_week_start_date + 35 
                    else c1.this_week_start_date 
                    end)                                                                                                      cont_prev_week_date,
              (case when continuity_ind=1 then (case when c1.this_week_start_date < g_curr_season_wk_start_date
                                                          then g_curr_season_wk_start_date
                                                     else c1.this_week_start_date end)                   
                    else null 
                    end)                                                                                                      cont_start_week_date,
              (case when continuity_ind=1 then c1.this_week_start_date + 42                        
                    else null
                    end)                                                                                                      cont_end_week_date,
              min(case when c1.calendar_date < season_first_trade_date
                       then null
                       else c1.this_week_start_date end)  
                       over (partition by sk1_style_colour_no)                                                                fash_start_week_date,
      --                 else c1.this_week_start_date end) over (partition by sk1_style_colour_no)                              fash_start_week_date,
              few.this_week_start_date                                                                                        fash_end_week_date,
              g_date                                                                                                          last_week_updated
      from DWH_PERFORMANCE.Wrtl_sc_trading t
            left outer join dim_calendar c1
            on c1.calendar_date between (case when continuity_ind=0 then t.season_first_trade_date
                                              else g_date_minus_84
                                              end)
                                and g_date
            left outer join dim_calendar few         
            on few.calendar_date = (case when c1.calendar_date <= season_first_trade_date then season_first_trade_date
                                         when c1.calendar_date > season_first_trade_date + 35 then season_first_trade_date + 35
                                         else c1.calendar_date
                                    end) + 7
          
      where t.season_start_date = g_curr_season_start_date
        and t.continuity_ind is not null                      --FIX ADDED 29 SEPTEMBER 2014 - REMOVE !!
    --  AND   sk1_style_colour_no in (5502589,16420002)
      
        --  and t.sk1_style_colour_no in ( 723286,726634,727553,777292,17592175,17592176,17592198)
        --       and t.sk1_style_colour_no in ( 17592175))
        )
        WHERE cont_prev_week_date < g_curr_season_END_date
      group by 
                 sk1_style_colour_no,
              continuity_ind,
              season_first_trade_date,  
              cont_prev_week_date,
              cont_start_week_date,
              cont_end_week_date,
              fash_start_week_date,
               fash_end_week_date,
               last_week_updated                                                     
  
  UNION
  select 
               sk1_style_colour_no,
              continuity_ind,
              season_first_trade_date,  
              cont_prev_week_date,
              cont_start_week_date,
              cont_end_week_date,
              fash_start_week_date,
               fash_end_week_date,
               last_week_updated
              from 
              (select 
                                                                                                                              sk1_style_colour_no,
                                                                                                                              continuity_ind,
                                                                                                                              season_first_trade_date,  
              (case when continuity_ind = 1 then c1.this_week_start_date + 35 
                    else c1.this_week_start_date 
                    end)                                                                                                      cont_prev_week_date,
              (case when continuity_ind=1 then (case when c1.this_week_start_date < g_NEW_season_wk_start_date
                                                          then g_NEW_season_wk_start_date
                                                     else c1.this_week_start_date end)                   
                    else null 
                    end)                                                                                                      cont_start_week_date,
              (case when continuity_ind=1 then c1.this_week_start_date + 42                        
                    else null
                    end)                                                                                                      cont_end_week_date,
              min(case when c1.calendar_date < season_first_trade_date
                       then null
                       else c1.this_week_start_date end)  
                       over (partition by sk1_style_colour_no)                                                                fash_start_week_date,
      --                 else c1.this_week_start_date end) over (partition by sk1_style_colour_no)                              fash_start_week_date,
              few.this_week_start_date                                                                                        fash_end_week_date,
              g_date                                                                                                          last_week_updated
      from DWH_PERFORMANCE.Wrtl_sc_trading t
            left outer join dim_calendar c1
            on c1.calendar_date between (case when continuity_ind=0 then t.season_first_trade_date
                                              else g_date_minus_84
                                              end)
                                and g_date
            left outer join dim_calendar few         
            on few.calendar_date = (case when c1.calendar_date <= season_first_trade_date then season_first_trade_date
                                         when c1.calendar_date > season_first_trade_date + 35 then season_first_trade_date + 35
                                         else c1.calendar_date
                                    end) + 7
      where t.season_start_date = g_NEW_season_start_date
      and t.continuity_ind is not null                      --FIX ADDED 29 SEPTEMBER 2014 - REMOVE !!
 --     AND   sk1_style_colour_no in (5502589,16420002)
        --  and t.sk1_style_colour_no in ( 723286,726634,727553,777292,17592175,17592176,17592198)
        --       and t.sk1_style_colour_no in ( 17592175))
        )
               WHERE cont_prev_week_date >= g_NEW_season_START_date
      group by 
                 sk1_style_colour_no,
              continuity_ind,
              season_first_trade_date,  
              cont_prev_week_date,
              cont_start_week_date,
              cont_end_week_date,
              fash_start_week_date,
               fash_end_week_date,
               last_week_updated                   
        
      )
    select 
      sk1_style_colour_no,
      continuity_ind,
      season_first_trade_date,
      nvl(cont_prev_week_date,to_date('01-01-3030','DD-MM-YYYY')) cont_prev_week_date,
      nvl(cont_start_week_date,to_date('01-01-3030','DD-MM-YYYY')) cont_start_week_date,
      nvl(cont_end_week_date,to_date('01-01-3030','DD-MM-YYYY')) cont_end_week_date,
      nvl(fash_start_week_date,to_date('01-01-3030','DD-MM-YYYY')) fash_start_week_date,
      nvl(fash_end_week_date,to_date('01-01-3030','DD-MM-YYYY')) fash_end_week_date ,
      g_date
    from selext se
    where cont_prev_week_date >= g_this_week_start_date_42
    and cont_prev_week_date    < g_this_week_start_date ;
        
    g_recs_inserted           := sql%rowcount;
    
    commit;
    
    l_text := 'Recs Inserted Into WTEMP_RTL_SC_CONTINUITY_WK_034A = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --**************************************************************************************************
    -- write final log data
    --**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('DD MON YYYY HH24:MI:SS'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
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


END WH_PRF_AST_034A_WLTST;
