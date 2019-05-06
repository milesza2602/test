--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_229U_WLFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_229U_WLFIX" 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- This procedure has been changed to cater for C&GM only (ie. business_unit_no = 50)
-- Wendy - 14 Feb 2013
--***************************************************************************************************
-- This procedure will run weekly on a Saturday.
-- It will take current week as last completed week.
--**************************************************************************************************
--  Date:        February 2013
--  Author:      Wendy lyttle
--  Purpose:     Extract first sale date per item for last 6 weeks - update first_trade_date, no_of_weeks
--  Tables:      Input  -   rtl_loc_item_rms_dense
--               Output -   rtl_item_trading_new
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  qc4835     31 Jan 2013 - change to process all items-
--                             but..
--                             a.) Insert if new
--                             b.) Update season_first_trade_date for clothing and home only for the current season
--             17 oct 2013 -  hint changed from /* +  to /*+
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
--
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_item_trading_new%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_week_start_date date;
g_season_start_date date;
g_season_end_date   date;
g_upd2       integer       :=  0;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_half_no          dim_calendar.fin_half_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_229U_WLFIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT FIRST SALE DATE PER ITEM fd';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_item_trading_new%rowtype index by binary_integer;
type tbl_array_u is table of rtl_item_trading_new%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_item_trading_new CH STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'alter session enable parallel dml';

   select fin_half_no, fin_year_no
   into g_fin_half_no, g_fin_year_no
   from dim_calendar
   where calendar_date = g_date;

   select distinct min(season_start_date), max(fin_half_end_date)
   into g_season_start_date, g_season_end_date
   from dim_calendar
   where fin_half_no = g_fin_half_no
   and   fin_year_no = g_fin_year_no;

   l_text := 'Data extract from '||g_season_start_date|| ' to '||g_season_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- DELETE FROM ITEM_TRADING
--**************************************************************************************************
   g_recs_read := 0;
   g_recs_inserted :=  0;
   DELETE FROM DWH_PERFORMANCE.rtl_item_trading_new a
   WHERE exists (select sk1_item_no from dim_item b
                     where a.sk1_item_no = b.sk1_item_no  and ITEM_NO IN(157636	,42299837	,42299851	,42299868	,3423478491458	,5000394050853	,6001009276438	,
                                  6001009307897	,6005000536102	,6005000675979	,6005000704426	,6005000704440	,6005000964226	,6005000964233	,6005000964264	,
                                  6008000190091	,6009000259313	,6009000341643	,6009000385272	,6009000385357	,6009000445228	,6009000445235	,6009000445242	,
                                  6009000472491	,6009000472583	,6009000472620	,6009000472651	,6009000936382	,6009000936399	,6009000936405	,6009000936412	,
                                  6009000936429	,6009000936436	,6009000936443	,6009000936450	,6009101050178	,6009101050185	,6009101050192	,6009101050208	,
                                  6009101050215	,6009101050222	,6009101050239	,6009101050246	,6009101174645	,6009101174690	,6009101175888	,6009101177585	,
                                  6009101548262	,6009101615261	,6009101626496	,6009101626519	,6009101646791	,6009101646807	,6009101646814	,6009101646821	,
                                  6009101646838	,6009101646845	,6009101646852	,6009101665426	,6009101665433	,6009101665440	,6009101665464	,6009101665471	,
                                  6009101665488	,6009101665495	,6009101665501	,6009101665518	,6009101665525	,6009101665532	,6009101665549	,6009101665556	,
                                  6009101665563	,6009101665570	,6009101665594	,6009101665617	,6009101665624	,6009101665631	,6009101665648	,6009101665655	,
                                  6009101665662	,6009101665679	,6009101665686	,6009101665693	,6009101665709	,6009101665716	,6009101665723	,6009101665730	,
                                  6009101665747	,6009101696574	,6009101736935	,6009101788408	,6009101822645	,6009101836406	,6009101973262	,6009101973279	,
                                  6009101973286	,6009101973293	,6009101973309	,6009101973316	,6009101973323	,6009101993833	,6009101998197	,6009101998203	,
                                  6009101998210	,6009101998227	,6009101998234	,6009101998241	,6009101998258	,6009101998265	,6009101998272	,6009171104511	,
                                  6009171436582	,6009171436599	,6009171499471	,6009171499488	,6009171499495	,6009171499501	,6009171698195	,6009173189684	,
                                  6009173360427	,6009173360700	,9781453006931	,9781631097423));
   g_recs_read := g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

  l_text := 'Deleted:- RECS =  '||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;
  l_text := ' ==================  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- insert fixed recs
--- Note that due to these items trading for the first time in this season, 
--    the season_first_trade_date = first_trade_date ---- only for this fix.
--**************************************************************************************************
   g_recs_read := 0;
   g_recs_inserted :=  0;


   INSERT /*+ APPEND */ INTO rtl_item_trading_new
with selitem as (select item_no, sk1_item_no, sk1_business_unit_no, business_unit_no 
                 from dim_item
                 where  business_unit_no = 50
                           and ITEM_NO IN(157636	,42299837	,42299851	,42299868	,3423478491458	,5000394050853	,6001009276438	,
                                  6001009307897	,6005000536102	,6005000675979	,6005000704426	,6005000704440	,6005000964226	,6005000964233	,6005000964264	,
                                  6008000190091	,6009000259313	,6009000341643	,6009000385272	,6009000385357	,6009000445228	,6009000445235	,6009000445242	,
                                  6009000472491	,6009000472583	,6009000472620	,6009000472651	,6009000936382	,6009000936399	,6009000936405	,6009000936412	,
                                  6009000936429	,6009000936436	,6009000936443	,6009000936450	,6009101050178	,6009101050185	,6009101050192	,6009101050208	,
                                  6009101050215	,6009101050222	,6009101050239	,6009101050246	,6009101174645	,6009101174690	,6009101175888	,6009101177585	,
                                  6009101548262	,6009101615261	,6009101626496	,6009101626519	,6009101646791	,6009101646807	,6009101646814	,6009101646821	,
                                  6009101646838	,6009101646845	,6009101646852	,6009101665426	,6009101665433	,6009101665440	,6009101665464	,6009101665471	,
                                  6009101665488	,6009101665495	,6009101665501	,6009101665518	,6009101665525	,6009101665532	,6009101665549	,6009101665556	,
                                  6009101665563	,6009101665570	,6009101665594	,6009101665617	,6009101665624	,6009101665631	,6009101665648	,6009101665655	,
                                  6009101665662	,6009101665679	,6009101665686	,6009101665693	,6009101665709	,6009101665716	,6009101665723	,6009101665730	,
                                  6009101665747	,6009101696574	,6009101736935	,6009101788408	,6009101822645	,6009101836406	,6009101973262	,6009101973279	,
                                  6009101973286	,6009101973293	,6009101973309	,6009101973316	,6009101973323	,6009101993833	,6009101998197	,6009101998203	,
                                  6009101998210	,6009101998227	,6009101998234	,6009101998241	,6009101998258	,6009101998265	,6009101998272	,6009171104511	,
                                  6009171436582	,6009171436599	,6009171499471	,6009171499488	,6009171499495	,6009171499501	,6009171698195	,6009173189684	,
                                  6009173360427	,6009173360700	,9781453006931	,9781631097423))
 select /*+ PARALLEL(f,4) FULL(f) */
          f.sk1_item_no, di.sk1_business_unit_no, min(post_date) as first_trade_date,  min(post_date) as season_first_trade_date ,0,g_date
     from rtl_loc_item_dy_rms_dense f,
          selitem di
    where f.sk1_item_no = di.sk1_item_no
      and f.post_date between '27 june 2016' and '13 oct 2016'
      and f.sales != 0
      and f.sales is not null
      group by f.sk1_item_no, di.sk1_business_unit_no, di.business_unit_no;
   g_recs_read := g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

  l_text := 'Insert NEW:- RECS =  '||g_recs_inserted||' period=27 june 2016 to 13 oct 2016';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;
  l_text := ' ==================  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Update of no_of_weeks from first_trade_date to current date
-- Is used by FOODs only for the time being but we are still updating for C&GM
--**************************************************************************************************

     select this_week_start_date
     into g_this_week_start_date
     from dim_calendar
     where calendar_date = g_date;
     
     update rtl_item_trading_new rit 
     set no_of_weeks = (
            select (g_this_week_start_date - dc.this_week_start_date) / 7 DIFF7
              from dim_calendar dc
             where dc.calendar_date = rit.first_trade_date
            group by (g_this_week_start_date - dc.this_week_start_date) / 7)
     where sk1_item_no in (select sk1_item_no from dim_item where business_unit_no <> 50);
     g_upd2 := g_upd2 + sql%rowcount;
     commit;
     l_text := 'Update of no_of_weeks = '||g_upd2;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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

END WH_PRF_CORP_229U_WLFIX;
