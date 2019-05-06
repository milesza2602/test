--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034A_WLFXTST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034A_WLFXTST" (
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
G_RUN_DATE DATE;
        g_6WK_BCK_START_date        DATE;
        g_CURR_HALF_WK_START_DATE   DATE;
        g_CURR_SEASON_END_DATE      DATE;
        g_curr_season_fin_week_no   NUMBER  :=0;
        g_curr_season_start_date    DATE;
        g_curr_season_wk_start_date DATE;
        g_date                      DATE;
        g_date_minus_84             DATE;
        g_end_fin_week_no           NUMBER  :=0;
        g_fin_half_no               NUMBER  :=0;
        g_fin_half_no_NEW           NUMBER  :=0;
        g_fin_year_no               NUMBER  :=0;
        g_fin_year_no_NEW           NUMBER  :=0;
        g_LOOP_DATE                 DATE;
        g_new_half_wk_start_date    DATE;
        g_new_season_end_date       DATE;
        g_new_season_fin_week_no    NUMBER  :=0;
        g_NEW_season_start_date     DATE;
        g_NEW_SEASON_WK_START_DATE  DATE;
        G_START_DATE                DATE;
        G_START_YEAR                NUMBER  :=0;
        G_START_WEEK                NUMBER  :=0;
        g_start_fin_week_date       DATE;
        g_start_fin_week_no         NUMBER  :=0;
        g_this_week_start_date      DATE;
        g_this_week_END_date        DATE;
        g_fin_week_code             varchar2(7);
        g_this_week_start_date_42   DATE;
        g_start_month        integer       :=  0;

      l_message sys_dwh_errlog.log_text%type;
      l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_AST_034A_WLFXTST';
      l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_roll;
      l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_roll;
      l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
      l_text sys_dwh_log.log_text%type ;
      l_description sys_dwh_log_summary.log_description%type   := 'LOAD temp_rtl_ast_sc_cont_wk_034a';
      l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
--
--   PART A
--
--**************************************************************************************************
procedure PART_A as

begin
      --**************************************************************************************************
      --
      -- s t e p 1 : setup dates required for processing
      --
      --**************************************************************************************************

   

    g_recs_inserted := 0;

     
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





            
      insert 
      into dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1 a
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
      from rtl_sc_trading t
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
--  FOR RERUN NOV 2016
         AND season_first_trade_date < G_DATE
--  FOR RERUN NOV 2016

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
      from rtl_sc_trading t
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
--  FOR RERUN NOV 2016
         AND season_first_trade_date < G_DATE
--  FOR RERUN NOV 2016
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
    
    l_text := 'Recs Inserted Into trtl_ast_sccontwk_034a_wlFIX1 = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   exception
      when dwh_errors.e_insert_error then
       l_message := 'PART_A'||dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'PART_A'||dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end part_A;
 
 --**************************************************************************************************
--
--   PART B
--
--**************************************************************************************************
procedure PART_B as

begin

 
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


  



insert /*+ append */ into  dwh_datafix.trtl_ast_sc_dates_wlFIX1
                   select
                            sk1_style_colour_no 
                            ,continuity_ind 
                            ,season_first_trade_date 
                            ,dc1.fin_year_no                                                                           cont_prev_week_year_no
                            ,dc1.fin_week_no                                                                           cont_prev_week_week_no
                            ,cont_prev_week_date 
                            ,cont_start_week_date 
                            ,cont_end_week_date 
                            ,fash_start_week_date 
                            ,fash_end_week_date
                            ,case when continuity_ind=1 then cont_start_week_date else fash_start_week_date end 
                            derive_start_date 
                            ,case when continuity_ind=0 then fash_end_week_date else cont_end_week_date    end         derive_end_date
                          from dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1 a,
                                dim_calendar dc1
                          where cont_prev_week_date = dc1.calendar_date(+)
                          group by sk1_style_colour_no 
                            ,continuity_ind 
                            ,season_first_trade_date 
                            ,dc1.fin_year_no 
                            ,dc1.fin_week_no 
                            ,cont_prev_week_date 
                            ,cont_start_week_date 
                            ,cont_end_week_date 
                            ,fash_start_week_date 
                            ,fash_end_week_date
                            ,case when continuity_ind=1 then cont_start_week_date else fash_start_week_date end 
                            ,case when continuity_ind=0 then fash_end_week_date else cont_end_week_date    end
;
COMMIT;

--**************************************************************************************************
--  
-- S T E P 3 : create a table with the 6 week measures, 
--             which is to be used in the main select statement below
--    
--             CONTINUITY ITEMS - ie. continuity_ind = 1
--          
--**************************************************************************************************



    insert /*+ append */ into  dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1
              with selcal as
                          (select distinct   this_week_start_date                            this_week_start_date
                                            ,this_week_start_date-7                          last_week_start_date
                                            ,this_week_end_date                              this_week_end_date
                                            ,this_week_end_date-7                            last_week_end_date
                                            ,fin_year_no                                     fin_year_no
                                            ,fin_week_no                                     fin_week_no
                          from dwh_performance.dim_calendar cw
                          ),
                   selext as
                          (select
                            /*+  parallel (a,8) */
                                   a.sk1_chain_no                                                                                                   sk1_chain_no
                                  ,a.sk1_grade_no                                                                                                   sk1_grade_no
                                  ,a.sk1_style_colour_no                                                                                            sk1_style_colour_no
                                  ,cont_prev_week_year_no                                                                                           fin_year_no
                                  ,cont_prev_week_week_no                                                                                           fin_week_no
                                  ,ic.cont_prev_week_date                                                                                           this_week_start_date
                                  ,ic.continuity_ind                                                                                                continuity_ind
                                  ,sum(case when dc2.this_week_start_date < ic.cont_end_week_date then nvl(a.sales_qty,0) else 0  end)              sales_qty_6wk_act
                                  ,sum(case when dc2.this_week_start_date < cont_end_week_date then nvl(a.sales,0) else 0 end)                      sales_6wk_act
                                  ,sum(case when dc2.this_week_start_date = cont_prev_week_date then nvl(a.target_stock_selling,0) else 0 end)      target_stock_selling_6wk_act
                                  ,sum(case when dc2.this_week_start_date < ic.fash_end_week_date then nvl(a.sales_qty,0) else 0 end)               sales_qty_6wk_act_f
                                  ,sum(case when dc2.this_week_start_date < ic.fash_end_week_date then nvl(a.sales,0) else 0 end)                   sales_6wk_act_f
                                  ,sum(case when dc2.this_week_start_date = sc.last_week_start_date then nvl(a.target_stock_selling,0) else 0 end)  target_stock_selling_6wk_act_f
              from dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act a
                                 join dwh_performance.dim_calendar  dc2
                                            on dc2.fin_year_no  = a.fin_year_no
                                            and dc2.fin_week_no = a.fin_week_no
                                            and dc2.fin_day_no = 1
                                 join dwh_datafix.trtl_ast_sc_dates_wlFIX1 ic
                                            on a.sk1_style_colour_no = ic.sk1_style_colour_no
                                            and dc2.this_week_start_date >= derive_start_date
                                            and  dc2.this_week_start_date <= ic.cont_end_week_date
                                 join selcal sc
                                            on sc.this_week_start_date       = ic.derive_end_date
                           where 
                                a.sk1_plan_type_no     = 63
                                and ic.cont_prev_week_date   between G_CURR_SEASON_START_DATE AND G_CURR_SEASON_END_DATE
--                     and a.sk1_chain_no = 243 and   a.sk1_style_colour_no in (5502589,16420002)
--and a.sk1_grade_no = 6
                          --      and dc2.fin_half_end_date 
                           group by 
                                  a.sk1_chain_no
                                  ,a.sk1_grade_no
                                  ,a.sk1_style_colour_no
                                  ,cont_prev_week_year_no 
                                  ,cont_prev_week_week_no
                                  ,ic.cont_prev_week_date 
                                  ,ic.continuity_ind
 union
                                  select
                            /*+  parallel (a,8) */
                                   a.sk1_chain_no                                                                                                   sk1_chain_no
                                  ,a.sk1_grade_no                                                                                                   sk1_grade_no
                                  ,a.sk1_style_colour_no                                                                                            sk1_style_colour_no
                                  ,cont_prev_week_year_no                                                                                           fin_year_no
                                  ,cont_prev_week_week_no                                                                                           fin_week_no
                                  ,ic.cont_prev_week_date                                                                                           this_week_start_date
                                  ,ic.continuity_ind                                                                                                continuity_ind
                                  ,sum(case when dc2.this_week_start_date < ic.cont_end_week_date then nvl(a.sales_qty,0) else 0  end)              sales_qty_6wk_act
                                  ,sum(case when dc2.this_week_start_date < cont_end_week_date then nvl(a.sales,0) else 0 end)                      sales_6wk_act
                                  ,sum(case when dc2.this_week_start_date = cont_prev_week_date then nvl(a.target_stock_selling,0) else 0 end)      target_stock_selling_6wk_act
                                  ,sum(case when dc2.this_week_start_date < ic.fash_end_week_date then nvl(a.sales_qty,0) else 0 end)               sales_qty_6wk_act_f
                                  ,sum(case when dc2.this_week_start_date < ic.fash_end_week_date then nvl(a.sales,0) else 0 end)                   sales_6wk_act_f
                                  ,sum(case when dc2.this_week_start_date = sc.last_week_start_date then nvl(a.target_stock_selling,0) else 0 end)  target_stock_selling_6wk_act_f
              from dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act a
                                 join dwh_performance.dim_calendar  dc2
                                            on dc2.fin_year_no  = a.fin_year_no
                                            and dc2.fin_week_no = a.fin_week_no
                                            and dc2.fin_day_no = 1
                                 join temp_rtl_ast_sc_dates ic
                                            on a.sk1_style_colour_no = ic.sk1_style_colour_no
                                            and dc2.this_week_start_date >= derive_start_date
                                            and  dc2.this_week_start_date <= ic.cont_end_week_date
                                 join selcal sc
                                            on sc.this_week_start_date       = ic.derive_end_date
                           where 
                                a.sk1_plan_type_no     = 63
                                and ic.cont_prev_week_date   between G_NEW_SEASON_START_DATE AND G_NEW_SEASON_END_DATE
 --                    and a.sk1_chain_no = 243 and   a.sk1_style_colour_no in (5502589,16420002)
--and a.sk1_grade_no = 6
                          --      and dc2.fin_half_end_date 
                           group by 
                                  a.sk1_chain_no
                                  ,a.sk1_grade_no
                                  ,a.sk1_style_colour_no
                                  ,cont_prev_week_year_no 
                                  ,cont_prev_week_week_no
                                  ,ic.cont_prev_week_date 
                                  ,ic.continuity_ind
                                        ) 
                        
          select /*+ parallel (a,8) */ distinct
                sk1_chain_no
               ,sk1_grade_no
               ,sk1_style_colour_no
               ,se.fin_year_no
               ,se.fin_week_no
               ,se.this_week_start_date                                                         this_week_start_date
               ,case when continuity_ind = 0 then sales_qty_6wk_act_f
                    else sales_qty_6wk_act end                                                  sales_qty_6wk_act
               ,case when continuity_ind = 0 then sales_6wk_act_f
                    else sales_6wk_act end                                                      sales_6wk_act
               ,case when continuity_ind = 0 then target_stock_selling_6wk_act_f
                    else target_stock_selling_6wk_act end                                       target_stock_selling_6wk_act
          from selext se
          ;
         
    g_recs_inserted := SQL%ROWCOUNT;
    
    Commit;
    
    L_TEXT := 'Recs inserted into trtl_ast_sc_6wk_034b_wlFIX1 = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
      when dwh_errors.e_insert_error then
       l_message := 'PART_B'||dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'PART_B'||dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end part_B;

--**************************************************************************************************
--
--   PART C
--
--**************************************************************************************************

procedure PART_C as

begin   

    g_recs_inserted := 0;

    g_date_minus_84 := g_date - (7 * 12);

    SELECT THIS_WEEK_START_DATE,    THIS_WEEK_START_DATE - 42,     FIN_WEEK_NO
    into   g_this_week_start_date,  g_6wk_bck_start_date,          g_end_fin_week_no
    from   dim_calendar
    where  calendar_date = g_date;

    select fin_half_no,       fin_year_no
    into   g_fin_half_no,     g_fin_year_no
    from   DIM_CALENDAR
    where  calendar_date = g_6wk_bck_start_date;

    select distinct min(SEASON_START_DATE),     min(FIN_WEEK_NO),             min(this_week_start_date), min(fin_half_end_date)
    into  g_curr_season_start_date,             g_curr_season_fin_week_no,    g_curr_half_wk_start_date, g_curr_season_end_date
    from  dim_calendar
    where fin_half_no = g_fin_half_no
    and   fin_year_no = g_fin_year_no;

--
-- This part caters for the situatrion at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    select fin_half_no,       fin_year_no
    into   g_fin_half_no_new,     g_fin_year_no_new
    from   DIM_CALENDAR
    where  calendar_date = g_date;

    select distinct min(SEASON_START_DATE),     min(FIN_WEEK_NO),             min(this_week_start_date), min(fin_half_end_date)
    into  g_new_season_start_date,             g_new_season_fin_week_no,    g_new_half_wk_start_date, g_new_season_end_date
    from  dim_calendar
    where fin_half_no = g_fin_half_no_new
    and   fin_year_no = g_fin_year_no_new;


    SELECT THIS_WEEK_START_DATE,         FIN_WEEK_NO
     into  g_start_fin_week_date,        g_start_fin_week_no
    from   DIM_CALENDAR
    where  CALENDAR_DATE = case when g_6wk_bck_start_date < G_CURR_SEASON_START_DATE
                                          then G_CURR_SEASON_START_DATE
                                else
                                          g_6wk_bck_start_date
                                end;


    L_TEXT := 'g_this_week_start_date = '||G_THIS_WEEK_START_DATE||' g_6wk_bck_start_date = '||g_6wk_bck_start_date;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_end_fin_week_no = '||G_END_FIN_WEEK_NO||' g_fin_half_no = '||g_fin_half_no||' g_fin_year_no = '||g_fin_year_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_curr_season_start_date = '||g_curr_season_start_date||' g_curr_season_fin_week_no = '||g_curr_season_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   L_TEXT := 'g_curr_half_wk_start_date = '||g_curr_half_wk_start_date||' g_start_fin_week_date = '||G_START_FIN_WEEK_DATE;
       DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_start_fin_week_no = '||G_START_FIN_WEEK_NO;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_new_season_start_date = '||g_new_season_start_date||' g_new_season_fin_week_no = '||g_new_season_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--**************************************************************************************************
--
-- S T E P 2 : create MART table
--
--**************************************************************************************************


    insert /*+append */ into  dwh_datafix.mart_ch_ast_chngrdscwk_wlFIX1 m
      with
    --------------------------------------------------------------------------------
    -- last completed week
    --------------------------------------------------------------------------------
      cal as (select distinct fin_year_no, fin_week_no, fin_week_code, this_week_start_date, this_week_end_date
              from   dim_calendar
              where  this_week_start_date >= g_start_fin_week_date
              and    this_week_start_date < g_this_week_start_date) ,

    --------------------------------------------------------------------------------
    -- actual and pre-act -- comment AJ must not go 6 wks back past start of new season
    --------------------------------------------------------------------------------
      act as (select /*+parallel (a,4) */
                     a.sk1_chain_no,
                     a.sk1_grade_no,
                     a.sk1_style_colour_no,
                     a.fin_year_no,
                     a.fin_week_no,
                     cal.this_week_start_date,
                     max(case when a.sk1_plan_type_no = 63 then nvl(a.cost_price,0) else 0 end)     cost_price_act, -- AJ 18/1/13
                     max(case when a.sk1_plan_type_no = 63 then nvl(a.selling_price,0) else 0 end)  selling_price,  -- AJ 18/1/13
                     sum(case when a.sk1_plan_type_no = 63 then nvl(a.sales_qty,0) else 0 end)      sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 64 then nvl(a.sales_qty,0) else 0 end)      sales_planned_qty_pre_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(target_stock_qty,0) else 0 end) target_stock_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(target_stock_selling,0) else 0 end) target_stock_selling_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(a.sales,0) else 0 end)          sales_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(a.aps_sales_qty,0) else 0 end)  aps_sales_qty,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(store_count,0) else 0 end)      store_count_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(prom_sales_selling,0) else 0 end) prom_sales_selling_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(prom_sales_qty,0) else 0 end)   prom_sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(clear_sales_qty,0) else 0 end)  clear_sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(clear_sales_selling,0) else 0 end) clear_sales_selling_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(reg_sales_qty,0) else 0 end)    reg_sales_qty_act,
                     sum(case when a.sk1_plan_type_no = 63 then nvl(reg_sales_selling,0) else 0 end) reg_sales_selling_act
              from dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act a
              join
                  cal
                  on a.fin_year_no = cal.fin_year_no
                 and a.fin_week_no = cal.fin_week_no
              where  a.sk1_plan_type_no in (63, 64)
  --     and   sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)

              group by a.sk1_chain_no,
                       a.sk1_grade_no,
                       a.sk1_style_colour_no,
                       a.fin_year_no,
                       a.fin_week_no,
                       cal.this_week_start_date),
    --------------------------------------------------------------------------------
    -- chain intake margin -- comment include CHAIN_INTAKE_SELLING
    --------------------------------------------------------------------------------
     mgn as (
              select /*+ parallel (a,4) */
                     a.sk1_chain_no,
                     999                                                                           sk1_grade_no,
                     a.sk1_style_colour_no,
                     a.fin_year_no,
                     a.fin_week_no,
                     cal.this_week_start_date,
                     (sum(nvl(a.chain_intake_selling,0))- sum(nvl(a.chain_intake_cost,0)))          chain_intake_margin_act,
                     sum(nvl(a.chain_intake_selling,0))                                             chain_intake_selling_act
              from   rtl_chain_sc_wk_ast_pln a
              join   cal
                  on a.fin_year_no      = cal.fin_year_no
                 and a.fin_week_no      = cal.fin_week_no
              where  a.sk1_plan_type_no = 63
    --            and   sk1_style_colour_no in (5502589,16420002)
   --                and sk1_chain_no = 243
              group by a.sk1_chain_no,
                       999,
                       a.sk1_style_colour_no,
                       a.fin_year_no,
                       a.fin_week_no,
                       cal.this_week_start_date),
    --------------------------------------------------------------------------------
    -- 6week measures
    --------------------------------------------------------------------------------
     sixwk as (
              select /*+parallel(w,4)*/
                     sk1_chain_no,
                     sk1_grade_no,
                     sk1_style_colour_no,
                     to_number(fin_year_no) fin_year_no,
                     to_number(fin_week_no) fin_week_no,
                     this_week_start_date,
                     sales_qty_6wk_act,
                     sales_6wk_act,
                     target_stock_selling_6wk_act
              from dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1  w
   --           where sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)
          ),
    --------------------------------------------------------------------------------
    --- season geo to date
    --
-- The UNION caters for the situation at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    --------------------------------------------------------------------------------
 selgdt as
              (
              select distinct dc1.fin_week_no ,
                                dc1.fin_year_no,
                                dc1.this_week_start_date,
                                dc1.fin_half_end_date
              from dim_calendar dc1
              where dc1.this_week_start_date >=
            g_curr_season_start_date
         --   '24/DEC/12'
              and dc1.this_week_start_date <   g_this_week_start_date
        --      '8/jul/13'
              )
              ,
      selgeowk as
               (
                select /*+parallel(geowk,4)*/
                          distinct
                          sk1_chain_no,
                          sk1_geography_no,
                                    sk1_grade_no,
                                    sk1_style_colour_no,
                                    geowk.fin_year_no,
                                    geowk.fin_week_no,
                                    SK1_PLAN_TYPE_NO ,
                                    this_week_start_date,
                                    fin_half_end_date,
                                    sales_qty,
                                    sales,
                                    store_intake_selling,
                                    selling_price,
                                    sales_cost,
                                    target_stock_qty
                from dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act geowk, dim_calendar dcgeowk
                where sk1_plan_type_no    = 63
                  and dcgeowk.this_week_start_date >=   g_curr_season_start_date
            --'24/DEC/12'
                  and dcgeowk.this_week_start_date   < g_this_week_start_date
             -- '08/JUL/13'
                  and geowk.fin_year_no   = dcgeowk.fin_year_no
                  and geowk.fin_week_no   = dcgeowk.fin_week_no

--                  and geowk.fin_year_no   = g_fin_year_no -- start of season year
--                 and geowk.fin_week_no   >= g_curr_season_fin_week_no   -- start of season week
--                  and geowk.fin_week_no   < g_end_fin_week_no
--                  and geowk.fin_year_no   = dcgeowk.fin_year_no
--                  and geowk.fin_week_no   = dcgeowk.fin_week_no
                       --    and sk1_style_colour_no in(727553,727553,5502589,16420002)
     --                           and sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)
                ),
      ssng as
                (select sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no                                                   fin_year_no ,
                        gdt.fin_week_no                                                   fin_week_no ,
                        gdt.this_week_start_date                                          this_week_start_date,
                        sum(nvl(sales_qty,0))                                             sales_qty_std_act,
                        sum(nvl(sales,0))                                                 sales_std_act,
                        sum(nvl(store_intake_selling,0))                                  store_intake_selling_std_act,
                        max(case when selling_price = 0 then null else selling_price end) selling_price_std,
                        sum(nvl(sales_cost,0))                                            sales_cost_std_act,
                        sum(nvl(target_stock_qty,0))                                      target_stock_qty_std_act
                from  selgdt gdt ,
                      selgeowk gwk
                where gdt.this_week_start_date >= gwk.this_week_start_date
                and gdt.fin_half_end_date = gwk.fin_half_end_date
                and gdt.this_week_start_date between  g_curr_season_start_date and g_curr_season_end_date
                group by sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no ,
                        gdt.fin_week_no,
                        gdt.this_week_start_date
                union
                select sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no                                                   fin_year_no ,
                        gdt.fin_week_no                                                   fin_week_no ,
                        gdt.this_week_start_date                                          this_week_start_date,
                        sum(nvl(sales_qty,0))                                             sales_qty_std_act,
                        sum(nvl(sales,0))                                                 sales_std_act,
                        sum(nvl(store_intake_selling,0))                                  store_intake_selling_std_act,
                        max(case when selling_price = 0 then null else selling_price end) selling_price_std,
                        sum(nvl(sales_cost,0))                                            sales_cost_std_act,
                        sum(nvl(target_stock_qty,0))                                      target_stock_qty_std_act
                from  selgdt gdt ,
                      selgeowk gwk
                where gdt.this_week_start_date >= gwk.this_week_start_date
                and gdt.fin_half_end_date = gwk.fin_half_end_date
                and gdt.this_week_start_date between  g_new_season_start_date and g_new_season_end_date
                group by sk1_chain_no,
                        sk1_grade_no,
                        sk1_style_colour_no,
                        gdt.fin_year_no ,
                        gdt.fin_week_no,
                        gdt.this_week_start_date
                 ) ,
     ssngval as
                (            select sg.sk1_chain_no                                                  sk1_chain_no,
                        sg.sk1_grade_no                                                  sk1_grade_no ,
                        sg.sk1_style_colour_no                                           sk1_style_colour_no ,
                        sg.fin_year_no                                                   fin_year_no,
                        sg.fin_week_no                                                   fin_week_no ,
                        sg.this_week_start_date                                          this_week_start_date,
                        sg.sales_qty_std_act                                             sales_qty_std_act  ,
                        sg.sales_std_act                                                 sales_std_act,
                        sg.store_intake_selling_std_act                                  store_intake_selling_std_act,
                        sg.selling_price_std                                             selling_price_std ,
                        sg.sales_cost_std_act                                            sales_cost_std_act ,
                        sg.target_stock_qty_std_act                                      target_stock_qty_std_act
                from  ssng sg
                where sg.this_week_start_date >= g_6wk_bck_start_date
              --  '3/june/13'
    --           and sk1_chain_no = 243 and  sk1_grade_no = 6 and sk1_style_colour_no in (5502589,16420002)
                )
                ,
    --------------------------------------------------------------------------------
    ---                    season sc to date
-- The UNION caters for the situation at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    --------------------------------------------------------------------------------
     selsdt as
              (select distinct dc2.fin_week_no ,
                                dc2.fin_year_no,
                                dc2.this_week_start_date,
                                dc2.fin_half_end_date
              from dim_calendar dc2
                            where dc2.this_week_start_date >=
            g_curr_season_start_date
         --   '24/DEC/12'
              and dc2.this_week_start_date <   g_this_week_start_date
   --           where dc2.fin_half_no = g_fin_half_no
   --           and dc2.fin_year_no   = g_fin_year_no
   --           and dc2.this_week_start_date < g_this_week_start_date
              ),
      selwkpln as
                (select /*+parallel(wkpln,4)*/
                  distinct
                                   sk1_chain_no,
                           sk1_style_colour_no,
                           wkpln.fin_year_no,
                           wkpln.fin_week_no,
                           this_week_start_date,
                           fin_half_end_date,
                           SK1_PLAN_TYPE_NO,
                        --   chain_intake_selling,
                       --    chain_intake_cost,
                           chain_intake_selling chain_intake_selling_std_act,
                           nvl(chain_intake_selling,0) - nvl(chain_intake_cost,0)        CHAIN_INTAKE_MARGIN_STD_ACT
                from rtl_chain_sc_wk_ast_pln wkpln,
                     dim_calendar dcwkpln
                where sk1_plan_type_no   = 63
                  and dcwkpln.this_week_start_date >=  g_curr_season_start_date  -- start of season year
                  and dcwkpln.this_week_start_date   < G_THIS_WEEK_START_DATE
                  and wkpln.fin_year_no   = dcwkpln.fin_year_no
                  and wkpln.fin_week_no   = dcwkpln.fin_week_no
--                and wkpln.fin_year_no            = g_fin_year_no -- start of season year
--                and wkpln.fin_week_no           >= g_curr_season_fin_week_no   -- start of season week
--                and wkpln.fin_week_no            < g_end_fin_week_no
--                and wkpln.fin_year_no = dcwkpln.fin_year_no
--                and wkpln.fin_week_no = dcwkpln.fin_week_no
   --                     and sk1_chain_no = 243 and   sk1_style_colour_no in (5502589,16420002)

                ),
      ssns as
                (select sk1_chain_no,
                        999                                               sk1_grade_no,
                        sk1_style_colour_no,
                        dt.fin_year_no                                    fin_year_no ,
                        dt.fin_week_no                                    fin_week_no,
                        dt.this_week_start_date,
                        sum(nvl(chain_intake_selling_std_act,0))          chain_intake_selling_std_act,
                        sum(nvl(CHAIN_INTAKE_MARGIN_STD_ACT ,0))          CHAIN_INTAKE_MARGIN_STD_ACT
                from selsdt dt ,
                     selwkpln wkp
                where dt.this_week_start_date >= wkp.this_week_start_date
                and dt.fin_half_end_date = wkp.fin_half_end_date
                and dt.this_week_start_date between  g_curr_season_start_date and g_curr_season_end_date
                group by  sk1_chain_no,
                          sk1_style_colour_no,
                          dt.fin_year_no ,
                          dt.fin_week_no, dt.this_week_start_date
                          union
                  select sk1_chain_no,
                        999                                               sk1_grade_no,
                        sk1_style_colour_no,
                        dt.fin_year_no                                    fin_year_no ,
                        dt.fin_week_no                                    fin_week_no,
                        dt.this_week_start_date,
                        sum(nvl(chain_intake_selling_std_act,0))          chain_intake_selling_std_act,
                        sum(nvl(CHAIN_INTAKE_MARGIN_STD_ACT ,0))          CHAIN_INTAKE_MARGIN_STD_ACT
                from selsdt dt ,
                     selwkpln wkp
                where dt.this_week_start_date >= wkp.this_week_start_date
                and dt.fin_half_end_date = wkp.fin_half_end_date
                and dt.this_week_start_date between  g_new_season_start_date and g_new_season_end_date
                group by  sk1_chain_no,
                          sk1_style_colour_no,
                          dt.fin_year_no ,
                          dt.fin_week_no, dt.this_week_start_date
             )
            ,
       ssnsval as
                (select sn.sk1_chain_no                                   sk1_chain_no,
                        sn.sk1_grade_no                                   sk1_grade_no,
                        sn.sk1_style_colour_no                            sk1_style_colour_no        ,
                        sn.fin_year_no                                    fin_year_no ,
                        sn.fin_week_no                                    fin_week_no,
                        sn.this_week_start_date                           this_week_start_date,
                        sn.chain_intake_selling_std_act                   chain_intake_selling_std_act,
                        sn.CHAIN_INTAKE_MARGIN_STD_ACT                    CHAIN_INTAKE_MARGIN_STD_ACT
                from ssns sn
                where sn.this_week_start_date >= g_6wk_bck_start_date
             )
            ,
    --------------------------------------------------------------------------------
    ---                    Season to date Availability and Catalog days
 -- The UNION caters for the situation at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    --------------------------------------------------------------------------------
     selloc as
               (select location_no, sk1_location_no, SK1_CHAIN_NO
                from dim_location
                where area_no not in (9965, 8800, 9978, 9979, 9953)
                AND CHAIN_NO = 10),
     selsdt as
              (select distinct dc3.fin_week_no ,
                                dc3.fin_year_no,
                                dc3.this_week_start_date,
                                dc3.fin_half_end_date
              from dim_calendar dc3
             where dc3.this_week_start_date >=
            g_curr_season_start_date
         --   '24/DEC/12'
              and dc3.this_week_start_date <   g_this_week_start_date
         --     where dc2.fin_half_no = g_fin_half_no
         --     and dc2.fin_year_no   = g_fin_year_no
         --     and dc2.this_week_start_date < g_this_week_start_date
              ),
      seldays as
                (select /*+parallel(wkpln,2)*/
                distinct
                  sk1_chain_no,
                  wkcat.SK1_LOCATION_NO,
                           sk1_style_colour_no,
                           wkcat.fin_year_no,
                           wkcat.fin_week_no,
                           SK1_AVAIL_UDA_VALUE_NO,
                           dcwkcat.this_week_start_date,
                           dcwkcat.fin_half_end_date,
                           ch_num_avail_days                                                     NUM_AVAIL_DAYS_STD_ACT   ,
                           ch_num_catlg_days                                                     NUM_CATLG_DAYS_STD_ACT
                from RTL_LOC_SC_WK_ast_CATLG wkcat,
                     dim_calendar dcwkcat,
                     SELLOC dl
                where dcwkcat.this_week_start_date >=  g_curr_season_start_date  -- start of season year
                  and dcwkcat.this_week_start_date   < G_THIS_WEEK_START_DATE
                  and wkcat.fin_year_no   = dcwkcat.fin_year_no
                  and wkcat.fin_week_no   = dcwkcat.fin_week_no
--                  wkcat.fin_year_no            = g_fin_year_no -- start of season year
--                and wkcat.fin_week_no           >= g_curr_season_fin_week_no   -- start of season week
--                and wkcat.fin_week_no            < g_end_fin_week_no
--                and wkcat.fin_year_no = dcwkcat.fin_year_no
--                and wkcat.fin_week_no = dcwkcat.fin_week_no
               and wkcat.sk1_location_no = dl.sk1_location_no
   --         and sk1_chain_no = 243 and   sk1_style_colour_no in (5502589,16420002)
                ),
      ssna as
                (select sk1_chain_no,
                        999                                               sk1_grade_no,
                        sk1_style_colour_no,
                        dt.fin_year_no                                    fin_year_no ,
                        dt.fin_week_no                                    fin_week_no,
                        dt.this_week_start_date,
                        sum(nvl(NUM_AVAIL_DAYS_STD_ACT,0))                NUM_AVAIL_DAYS_STD_ACT,
                        sum(nvl(NUM_CATLG_DAYS_STD_ACT ,0))               NUM_CATLG_DAYS_STD_ACT
                from selsdt dt ,
                     seldays wkc
                where dt.this_week_start_date >= wkc.this_week_start_date
                and dt.fin_half_end_date = wkc.fin_half_end_date
                and dt.this_week_start_date between  g_curr_season_start_date and g_curr_season_end_date
                group by  sk1_chain_no,
                          sk1_style_colour_no,
                          dt.fin_year_no ,
                          dt.fin_week_no, dt.this_week_start_date
                union
                select sk1_chain_no,
                        999                                               sk1_grade_no,
                        sk1_style_colour_no,
                        dt.fin_year_no                                    fin_year_no ,
                        dt.fin_week_no                                    fin_week_no,
                        dt.this_week_start_date,
                        sum(nvl(NUM_AVAIL_DAYS_STD_ACT,0))                NUM_AVAIL_DAYS_STD_ACT,
                        sum(nvl(NUM_CATLG_DAYS_STD_ACT ,0))               NUM_CATLG_DAYS_STD_ACT
                from selsdt dt ,
                     seldays wkc
                where dt.this_week_start_date >= wkc.this_week_start_date
                and dt.fin_half_end_date = wkc.fin_half_end_date
                and dt.this_week_start_date between  g_new_season_start_date and g_new_season_end_date
                group by  sk1_chain_no,
                          sk1_style_colour_no,
                          dt.fin_year_no ,
                          dt.fin_week_no, dt.this_week_start_date
             )
            ,
       ssnaval as
                (select sa.sk1_chain_no                                   sk1_chain_no,
                        sa.sk1_grade_no                                   sk1_grade_no,
                        sa.sk1_style_colour_no                            sk1_style_colour_no        ,
                        sa.fin_year_no                                    fin_year_no ,
                        sa.fin_week_no                                    fin_week_no,
                        sa.this_week_start_date                           this_week_start_date,
                        sa.NUM_AVAIL_DAYS_STD_ACT                         NUM_AVAIL_DAYS_STD_ACT,
                        sa.NUM_CATLG_DAYS_STD_ACT                         NUM_CATLG_DAYS_STD_ACT
                from ssna sa
                where sa.this_week_start_date >= g_6wk_bck_start_date
             )

     ---************************************************************************************---
     --***************
    ---         MAIN JOIN OF ALL DATA EXTRACTS ABOVE
     --***************
     ---************************************************************************************---
     select
               nvl(ac.sk1_chain_no,nvl(mg.sk1_chain_no, nvl(ai.sk1_chain_no, nvl(ss.sk1_chain_no, nvl(sg.sk1_chain_no, av.sk1_chain_no)))))                   sk1_chain_no,
               nvl(ac.sk1_grade_no,nvl(mg.sk1_grade_no, nvl(ai.sk1_grade_no, nvl(ss.sk1_grade_no,  nvl(sg.sk1_grade_no, av.sk1_grade_no)))))                                         sk1_grade_no,
               nvl(ac.sk1_style_colour_no,nvl(mg.sk1_style_colour_no, nvl(ai.sk1_style_colour_no, nvl(ss.sk1_style_colour_no,  nvl(sg.sk1_style_colour_no,av.sk1_style_colour_no)))))      sk1_style_colour_no,
               nvl(ac.fin_year_no,nvl(mg.fin_year_no, nvl(ai.fin_year_no, nvl(ss.fin_year_no,  nvl(sg.fin_year_no,av.fin_year_no)))))                                              fin_year_no,
               nvl(ac.fin_week_no,nvl(mg.fin_week_no, nvl(ai.fin_week_no, nvl(ss.fin_week_no,  nvl(sg.fin_week_no,av.fin_week_no)))))                                              fin_week_no,
               nvl(ac.this_week_start_date,nvl(mg.this_week_start_date, nvl(ai.this_week_start_date, nvl(ss.this_week_start_date,  nvl(sg.this_week_start_date, av.this_week_start_date))))) this_week_start_date,
               round((max(selling_price) / 1.14),2)                                               reg_rsp_excl_vat,
               round(sum(NVL(sales_act,0)),2)                                                     sales_act,
               round(max(cost_price_act),2)                                                       cost_price_act,
               round(max(selling_price),2)                                                        selling_rsp_act,
               round(sum(nvl(chain_intake_margin_act,0)) ,2)                                      chain_intake_margin_act,
               round(sum(NVL(sales_qty_act,0)),2)                                                 sales_qty_act,
               round(sum(NVL(sales_planned_qty_pre_act,0)),2)                                     sales_planned_qty_pre_act,
               round(sum(NVL(target_stock_qty_act,0)),2)                                          target_stock_qty_act,
               round(sum(NVL(aps_sales_qty,0)) / greatest(sum(NVL(store_count_act,0)),1),2)       aps_sales_qty_act,
               round(sum(NVL(sales_std_act,0)),2)                                                 sales_std_act,
               round(sum(NVL(sales_std_act,0)) - sum(NVL(sales_cost_std_act,0)),2)                sales_margin_std_act,
               round(sum(NVL(store_intake_selling_std_act,0)),2)                                  store_intake_selling_std_act,
               round(sum(NVL(sales_qty_std_act,0)),2)                                             sales_qty_std_act,
               round(sum(NVL(target_stock_qty_act,0)),2)                                          target_stock_qty_std_act,
               round(sum(NVL(sales_qty_6wk_act,0)),2)                                             sales_qty_6wk_act,
               round(sum(NVL(sales_6wk_act,0)),2)                                                 sales_6wk_act,
               round(sum(NVL(target_stock_selling_6wk_act,0)),2)                                  target_stock_selling_6wk_act,
               sum(NVL(store_count_act,0))                                                        Store_count_act,
               round(sum(NVL(prom_sales_qty_act,0)),2)                                            prom_sales_qty_act,
               round(sum(NVL(prom_sales_selling_act,0)),2)                                        prom_sales_selling_act,
               round(sum(NVL(clear_sales_qty_act,0)),2)                                           clear_sales_qty_act,
               round(sum(NVL(clear_sales_selling_act,0)),2)                                       clear_sales_selling_act,
               round(sum(NVL(target_stock_selling_act,0)),2)                                      target_stock_selling_act,
               round(sum(NVL(reg_sales_qty_act,0)),2)                                             reg_sales_qty_act,
               round(sum(NVL(reg_sales_selling_act,0)),2)                                         reg_sales_selling_act,
               g_date                                                                    last_updated_date,
               round(sum(NUM_AVAIL_DAYS_STD_ACT),2)                                               NUM_AVAIL_DAYS_STD_ACT,
               round(sum(NUM_CATLG_DAYS_STD_ACT),2)                                               NUM_CATLG_DAYS_STD_ACT,
               round(sum(NVL(chain_intake_selling_act,0)),2)                                      chain_intake_selling_act,
               round(sum(nvl(chain_intake_selling_std_act,0)),2)                                  chain_intake_selling_std_act,
               round(sum(nvl(chain_intake_margin_std_act,0)),2)                                  chain_intake_margin_std_act
     from act ac
     full outer join mgn mg
                 on      mg.sk1_chain_no = ac.sk1_chain_no
                     and mg.sk1_grade_no = ac.sk1_grade_no
                     and mg.sk1_style_colour_no = ac.sk1_style_colour_no
                     and mg.fin_year_no = ac.fin_year_no
                     and mg.fin_week_no = ac.fin_week_no
     full outer join sixwk ai
                 on ai.sk1_chain_no = mg.sk1_chain_no
                     and ai.sk1_grade_no = mg.sk1_grade_no
                     and ai.sk1_style_colour_no = mg.sk1_style_colour_no
                     and ai.fin_year_no = mg.fin_year_no
                     and ai.fin_week_no = mg.fin_week_no
     full outer join ssnsval ss
                on ss.sk1_chain_no = ai.sk1_chain_no
                   and ss.sk1_grade_no = ai.sk1_grade_no
                   and ss.sk1_style_colour_no = ai.sk1_style_colour_no
                   and ss.fin_year_no = ai.fin_year_no
                   and ss.fin_week_no = ai.fin_week_no
     full outer join ssngval sg
                on sg.sk1_chain_no = ss.sk1_chain_no
                   and sg.sk1_grade_no = ss.sk1_grade_no
                   and sg.sk1_style_colour_no = ss.sk1_style_colour_no
                   and sg.fin_year_no = ss.fin_year_no
                   and sg.fin_week_no = ss.fin_week_no
     full outer join ssnaval av
                on av.sk1_chain_no = sg.sk1_chain_no
                   and av.sk1_grade_no = sg.sk1_grade_no
                   and av.sk1_style_colour_no = sg.sk1_style_colour_no
                   and av.fin_year_no = sg.fin_year_no
                   and av.fin_week_no = sg.fin_week_no
      group by
               nvl(ac.sk1_chain_no,nvl(mg.sk1_chain_no, nvl(ai.sk1_chain_no, nvl(ss.sk1_chain_no, nvl(sg.sk1_chain_no, av.sk1_chain_no)))))   ,
               nvl(ac.sk1_grade_no,nvl(mg.sk1_grade_no, nvl(ai.sk1_grade_no, nvl(ss.sk1_grade_no,  nvl(sg.sk1_grade_no, av.sk1_grade_no)))))  ,
               nvl(ac.sk1_style_colour_no,nvl(mg.sk1_style_colour_no, nvl(ai.sk1_style_colour_no, nvl(ss.sk1_style_colour_no,  nvl(sg.sk1_style_colour_no,av.sk1_style_colour_no)))))    ,
               nvl(ac.fin_year_no,nvl(mg.fin_year_no, nvl(ai.fin_year_no, nvl(ss.fin_year_no,  nvl(sg.fin_year_no,av.fin_year_no)))))   ,
               nvl(ac.fin_week_no,nvl(mg.fin_week_no, nvl(ai.fin_week_no, nvl(ss.fin_week_no,  nvl(sg.fin_week_no,av.fin_week_no)))))   ,
               nvl(ac.this_week_start_date,nvl(mg.this_week_start_date, nvl(ai.this_week_start_date, nvl(ss.this_week_start_date,  nvl(sg.this_week_start_date, av.this_week_start_date)))))
               ;

    g_recs_inserted := sql%rowcount;

    commit;

    L_TEXT := 'Recs inserted into dwh_datafix.MART_CH_AST_CHNGRDSCWK_WLFIX1 = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   exception
      when dwh_errors.e_insert_error then
       l_message := 'PART_C'||dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'PART_C'||dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end part_c;

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
      
      EXECUTE immediate 'ALTER SESSION ENABLE PARALLEL DML';

     G_RUN_DATE := '7 AUG 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
    L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_RUN_DATE := '14 AUG 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     G_RUN_DATE := '21 AUG 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
    L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_RUN_DATE := '28 AUG 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     G_RUN_DATE := '4 SEP 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
    L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_RUN_DATE := '11 SEP 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   G_RUN_DATE := '18 SEP 2016';
    l_text := 'Run date = '||g_run_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     l_text := '************************************* A STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
     l_text := 'Truncate Table dwh_datafix.trtl_ast_sccontwk_034a_wlFIX1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_A;
     l_text := '************************************* A COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* B STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_dates_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    L_TEXT := 'truncate table dwh_datafix.trtl_ast_sc_6wk_034b_wlFIX1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_B;
     l_text := '************************************* B COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     l_text := '************************************* C STARTED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   execute immediate 'truncate table DWH_DATAFIX.MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   L_TEXT := 'truncate table MART_CH_AST_CHNGRDSCWK_WLFIX1';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_date := G_RUN_DATE;
    g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     PART_C;
     l_text := '************************************* C COMPLETED****************************************************';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     DBMS_STATS.gather_table_stats ('DWH_DATAFIX','MART_CH_AST_CHNGRDSCWK_WLFIX1', DEGREE => 8);
   l_text := '**********Completed GATHER_TABLE_STATS ON MART_CH_AST_CHNGRDSCWK_WLFIX1********************************';
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


END WH_PRF_AST_034A_WLFXTST;
