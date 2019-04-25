--------------------------------------------------------
--  DDL for Procedure FIXME
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."FIXME" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2012
--  Author:      Jacqui Pember
--  Purpose:
--  Tables:      Input  - rtl_chn_geo_grd_sc_wk_ast_act and rtl_chain_sc_wk_ast_pln
--               Output - mart_ch_ast_chn_grd_sc_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit         integer       :=  dwh_constants.vc_forall_limit;
g_recs_read            integer       :=  0;
g_recs_inserted        integer       :=  0;
g_date                 date;
g_this_week_start_date date;
g_new_this_week_start_date date;
g_season_start_date    date;
g_season_end_date      date;
g_new_season_start_date  date;
g_new_season_6wk_end_date date;
g_curr_season_start_date date;
g_this_week_end_date   date;
g_fin_week_no          dim_calendar.fin_week_no%type;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_week_code        dim_calendar.fin_week_code%type;
g_6wk_fin_week_code    number(6);
g_fash_end_week_code   number(6);
g_prev_fin_week_code   number(6);
g_fash_fin_week_code   number(6);
g_fin_year_no_ly       dim_calendar.fin_year_no%type;
g_fin_week_no_ly       dim_calendar.fin_week_no%type;
g_fin_half_no          dim_calendar.fin_half_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DATAMART - CHN LEVEL';
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
   l_text := 'LOAD OF mart_ch_ast_chn_grd_sc_wk STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   --g_date := to_date('23072012','ddmmyyyy');

-- new 12/01/13
   select FIN_HALF_NO, FIN_YEAR_NO, THIS_WEEK_END_DATE, FIN_YEAR_NO||FIN_WEEK_NO
   into g_fin_half_no, g_fin_year_no, g_this_week_end_date, g_fin_week_code
   from dim_calendar
   where calendar_date = g_date;

   select distinct min(season_start_date), max(fin_half_end_date)
   into g_new_season_start_date, g_season_end_date
   from dim_calendar
   where fin_half_no = g_fin_half_no
   and   fin_year_no = g_fin_year_no;
-- end new 12/01/13
   select this_week_start_date - 43, season_start_date
   into   g_this_week_start_date, g_curr_season_start_date
   from   dim_calendar
   where  calendar_date = g_date;

   --g_this_week_start_date:=to_date('20120618','yyyymmdd');
   select fin_week_no, fin_year_no, season_start_date
   into   g_fin_week_no, g_fin_year_no, g_season_start_date
   from   dim_calendar
   where  calendar_date = g_this_week_start_date + 1;

   select fin_week_no, fin_year_no - 1
   into   g_fin_week_no_ly, g_fin_year_no_ly
   from   dim_calendar
   where  calendar_date = (g_date - 1);
--new AJ 12/01/13
   if g_this_week_start_date < g_new_season_start_date then
      g_new_this_week_start_date := g_new_season_start_date;
   end if;

   g_new_season_6wk_end_date := g_new_season_start_date + 35;  -- setting 6wk from start of season

-- 08/2/13
   select fin_year_no||lpad(fin_week_no,2,0)
   into g_6wk_fin_week_code
   from dim_calendar
   where calendar_date = g_new_season_6wk_end_date;

   select last_wk_fin_year_no||lpad(last_wk_fin_week_no,2,0) -- setting prev wk
   into g_prev_fin_week_code
   from dim_control;

   if g_fin_week_code >= g_6wk_fin_week_code then
      if g_prev_fin_week_code > g_6wk_fin_week_code then
         g_fash_fin_week_code := g_6wk_fin_week_code;
      else
         g_fash_fin_week_code := g_prev_fin_week_code;
      end if;
   else
         g_fash_fin_week_code := g_prev_fin_week_code;
   end if;

   l_text := 'Data extract for week '||g_fin_week_no|| 'of year '||g_fin_year_no;
   l_text := 'LY Data extract for week '||g_fin_week_no_ly|| 'of year '||g_fin_year_no_ly;

   execute immediate 'alter session enable parallel dml';

   -- the following provides the date ranges for the calculation of the accumulated 6 week measures
   -- populate global temp table
   execute immediate 'truncate table dwh_performance.rtl_sc_continuity_wk';

   insert /*+parallel(a,2)*/ into rtl_sc_continuity_wk
          (sk1_style_colour_no,
           continuity_ind,
           season_first_trade_date,
           cont_prev_week_code,
           cont_start_week_code,
           cont_end_week_code,
           fash_start_week_code,
           fash_end_week_code,
           last_updated_date)
   select /*+parallel(t,4) */ distinct
           sk1_style_colour_no,
           continuity_ind,
           season_first_trade_date,
           case when continuity_ind = 1 then
                c3.fin_year_no||lpad(c3.fin_week_no,2,'0')
           else c1.fin_year_no||lpad(c1.fin_week_no,2,'0')
           end  cont_prev_week_code,

           case when continuity_ind=1 then
                     -- set first week to start of season if it's less
                     case when c1.fin_year_no||lpad(c1.fin_week_no,2,'0') < ssw.fin_year_no||lpad(ssw.fin_week_no,2,'0') then--season start week code
                          ssw.fin_year_no||lpad(ssw.fin_week_no,2,'0')
                     else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') end
                else '999999' end cont_start_week_code,

           case when continuity_ind=1 then
                     c2.fin_year_no||lpad(c2.fin_week_no,2,'0')
                else '999999' end cont_end_week_code,

           min(case when c1.calendar_date < season_first_trade_date then '999999' else c1.fin_year_no||lpad(c1.fin_week_no,2,'0') end)
               over (partition by sk1_style_colour_no) fash_start_week_code,

           few.fin_year_no||lpad(few.fin_week_no,2,'0') fash_end_week_code,
           g_date

   from    rtl_sc_trading t
     join  dim_calendar c1

       on  c1.calendar_date between
             case when continuity_ind=0 then t.season_first_trade_date else g_date - (7 * 12) end
             and  g_date

     -- get the rpl end week code (6 weeks from calendar date)
     join  dim_calendar c2
       on  c2.calendar_date = c1.calendar_date + (7*6)

     -- get the previous week's code of the rpl end week code
     join  dim_calendar c3
       on  c3.calendar_date = c2.calendar_date - 7

     -- get the week code for the season start date
     join  dim_calendar ssw
       on  ssw.calendar_date = g_curr_season_start_date

     -- get the fashion end week code
     join  dim_calendar few
       on  few.calendar_date = (case when c1.calendar_date <= season_first_trade_date then season_first_trade_date
                                     when c1.calendar_date > season_first_trade_date + (5*7) then season_first_trade_date + (5*7)
                                     else c1.calendar_date end) + 7

   where   t.season_start_date = g_curr_season_start_date
   and     case when continuity_ind=1 then c1.calendar_date else to_date('19000101','yyyymmdd') end <=  g_date - 42
   order by
           sk1_style_colour_no,
           case when continuity_ind = 1 then
                c3.fin_year_no||lpad(c3.fin_week_no,2,'0')
           else c1.fin_year_no||lpad(c1.fin_week_no,2,'0')
           end;
-- AJ 08/2/13



      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

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

end  fixme;
