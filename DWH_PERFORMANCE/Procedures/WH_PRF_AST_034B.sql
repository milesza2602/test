--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034B
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034B" (P_FORALL_LIMIT in integer,P_SUCCESS OUT BOOLEAN) 
as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Wendy Lyttle
--
--  Purpose:     Recreate a temporary table of 6-week Chain-Grade-Style-colour sales and stock data for C&GM
--
--
--  Tables:      Input  - temp_rtl_ast_sc_cont_wk_034a
--               Output - temp_rtl_ast_sc_6wk_034b
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  May 2013 - wendy - new version of wh_prf_ast_034u 
--                   - write to temp_rtl_ast_sc_6wk_034b (old version rtl_ch_ast_chn_grd_sc_wk_6w)
      --  21 June 2013 - release to prd
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************


g_forall_limit           integer       :=  dwh_constants.vc_forall_limit;
g_recs_read              integer       :=  0;
g_recs_inserted          integer       :=  0;

g_date                      date;
g_this_week_start_date_42   date;
g_curr_season_start_date    date;
g_curr_season_wk_start_date date;
g_date_minus_84             date;
g_6WK_BCK_START_date        date;
g_CURR_HALF_WK_START_DATE       date;
g_CURR_SEASON_END_DATE       date;
g_NEW_SEASON_WK_START_DATE       date;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_half_no             number;

g_start_fin_week_no       number;
g_end_fin_week_no         number;
g_curr_season_fin_week_no number;
g_start_fin_week_date     date;
g_this_week_start_date    date;

g_fin_half_no_new            number;
g_fin_year_no_NEW          NUMBER;

g_new_season_start_date date;
g_new_season_fin_week_no number;
g_new_half_wk_start_date date;
g_new_season_end_date date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034B';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD temp_rtl_ast_sc_6wk_034b';
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
    
    L_TEXT := 'LOAD OF temp_rtl_ast_sc_6wk_034b STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    DWH_LOG.INSERT_LOG_SUMMARY(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_DESCRIPTION,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
 
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
--  
-- S T E P 1 : setup dates required for processing
--           
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
  -- G_DATE := '8 july 2013';
    g_date := g_date + 1;
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

/*    
    g_date_minus_84 := g_date - (7 * 12);

    select this_week_start_date, this_week_start_date - 42, fin_week_no
    into   g_this_week_start_date, g_this_week_start_date_42, g_end_fin_week_no
    from   dim_calendar
    where  calendar_date = g_date;

    select fin_half_no, fin_year_no
    into   g_fin_half_no, g_fin_year_no
    from   dim_calendar
    where  calendar_date = g_this_week_start_date_42;

    select distinct min(season_start_date), min(fin_week_no), min(this_week_start_date)
    into  g_curr_season_start_date, g_curr_season_fin_week_no, g_curr_season_wk_start_date
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


    select this_week_start_date, fin_week_no
    into   g_start_fin_week_date, g_start_fin_week_no
    from   dim_calendar
    where  calendar_date = case when g_this_week_start_date_42 < g_curr_season_start_date then g_curr_season_start_date else g_this_week_start_date_42 end;
    
 
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
 
*/
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
-- S T E P 2 : create a table with the 6 week measures, 
--             which is to be used in the main select statement below
--    
--             CONTINUITY ITEMS - ie. continuity_ind = 1
--          
--**************************************************************************************************     

    execute immediate 'truncate table dwh_performance.temp_rtl_ast_sc_dates';
    L_TEXT := 'truncate table dwh_performance.temp_rtl_ast_sc_dates';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


insert /*+ append */ into  dwh_performance.temp_rtl_ast_sc_dates
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
                          from dwh_performance.temp_rtl_ast_sc_cont_wk_034a a,
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

    execute immediate 'truncate table dwh_performance.temp_rtl_ast_sc_6wk_034b';
    L_TEXT := 'truncate table dwh_performance.temp_rtl_ast_sc_6wk_034b';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    insert /*+ append */ into  dwh_performance.temp_rtl_ast_sc_6wk_034b 
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
                                 join dwh_performance.temp_rtl_ast_sc_dates ic
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
    
    L_TEXT := 'Recs inserted into temp_rtl_ast_sc_6wk_034b = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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



END WH_PRF_AST_034B;
