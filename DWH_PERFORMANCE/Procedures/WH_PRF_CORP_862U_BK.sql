--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_862U_BK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_862U_BK" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2016
--  Author:      A Joshua
--  Purpose:     Load 6 week sales data ex temp tables populated by WH_PRF_CORP_860U (prededessor)
--               Needed as input into TABLEAU mart (successor WH_PRF_CORP_861U)
--  Tables:      Input  - temp_foods_tab_2yrs_dense, temp_foods_tab_2yrs_sparse, mart_fd_zone_loc_item_6wkavg
--               Output - rtl_loc_item_wk_sales_6wkavg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  04 Oct 16 - Use cleanse promotional data from mart mart_fd_zone_loc_item_6wkavg
--  06 Ovt 16 - Combine dense + sparse for 6 week summaries by each week period.  Ref: BK06Oct1016
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
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_chain_corporate    integer       :=  0;
g_chain_franchise    integer       :=  0;

g_maxwk             dim_calendar.fin_week_no%type;
g_maxyr             dim_calendar.fin_year_no%type;
g_loopwk            date;
g_loopyr            dim_calendar.fin_year_no%type;
g_sub               integer :=  0;
g_maxprd            date;

g_cnta              int :=  0;
g_cntb              int :=  0;
g_cntc              int :=  0;
g_cntd              int :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_862U_BK';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WEEKLY SALES AVERAGE DATA';
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
   l_text := 'LOAD OF RTL_LOC_ITEM_WK_SALES_6WKAVG STARTED AT '||
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
   
   select fin_year_no, fin_week_no
   into g_fin_year_no, g_fin_week_no
   from dim_calendar where calendar_date = trunc(sysdate) - 7;

   execute immediate 'alter session enable parallel dml';

-- BK06Oct2016 (start)  
  -- Part 1 (combine Dense and sparse)
  execute immediate 'truncate table dwh_datafix.bk_temp_foods_tab_dense_sparse';
  commit;
  
  select /*+ parallel (a,4) full (a) */ max(FIN_YEAR_NO), max(FIN_WEEK_NO)
  into    g_maxyr, g_maxwk
  from    DWH_PERFORMANCE.temp_foods_tab_2yrs_dense a;
  select  max(THIS_WEEK_START_DATE) into g_maxprd from dim_calendar where FIN_YEAR_NO = g_maxyr and FIN_WEEK_NO = g_maxwk;
       
  -- build combined table key set of last 6 weeks - current week plus 5 weeks pervious weeks (there are 12 weeks in table)...
  insert /*+ parallel (c,4) */ into dwh_datafix.bk_temp_foods_tab_dense_sparse c
  select    THIS_WEEK_START_DATE,	
            THIS_WEEK_END_DATE,
            FIN_YEAR_NO,
            FIN_WEEK_NO,
            SK1_ITEM_NO,
            SK1_LOCATION_NO,
            0,0,0,0
  from (
        select   /*+ parallel (a,4) full(a)  */ 
                  b.THIS_WEEK_START_DATE,	
                  b.THIS_WEEK_END_DATE,
                  a.FIN_YEAR_NO,
                  a.FIN_WEEK_NO,
                  a.SK1_ITEM_NO,
                  a.SK1_LOCATION_NO                 
        from      DWH_PERFORMANCE.temp_foods_tab_2yrs_dense  a
        join      dim_calendar                               b on (a.FIN_YEAR_NO = b.FIN_YEAR_NO and a.FIN_WEEK_NO = b.FIN_WEEK_NO and b.FIN_DAY_NO = 1)
        where     b.THIS_WEEK_START_DATE between g_maxprd-35 and g_maxprd
--    and SK1_LOCATION_NO = 695
        group by  b.THIS_WEEK_START_DATE,	
                  b.THIS_WEEK_END_DATE,
                  a.FIN_YEAR_NO,
                  a.FIN_WEEK_NO,
                  a.SK1_ITEM_NO,
                  a.SK1_LOCATION_NO
        union
        select   /*+ parallel (a,4) full(a)  */ 
                  b.THIS_WEEK_START_DATE,	
                  b.THIS_WEEK_END_DATE,
                  a.FIN_YEAR_NO,
                  a.FIN_WEEK_NO,
                  a.SK1_ITEM_NO,
                  a.SK1_LOCATION_NO                 
        from      DWH_PERFORMANCE.temp_foods_tab_2yrs_sparse a
        join      dim_calendar                               b on (a.FIN_YEAR_NO = b.FIN_YEAR_NO and a.FIN_WEEK_NO = b.FIN_WEEK_NO and b.FIN_DAY_NO = 1)
        where     b.THIS_WEEK_START_DATE between g_maxprd-35 and g_maxprd
--      and SK1_LOCATION_NO = 695
        group by  b.THIS_WEEK_START_DATE,	
                  b.THIS_WEEK_END_DATE,
                  a.FIN_YEAR_NO,
                  a.FIN_WEEK_NO,
                  a.SK1_ITEM_NO,
                  a.SK1_LOCATION_NO
       );
g_cntA := sql%rowcount;
commit;

g_loopwk := g_maxprd;

-- update the combined table with associated measures from Dense and Sparse (6 week previous summarised into each week) ...
for g_sub in 1..6 loop
 
    -- A. Measures from Dense ....
    merge /*+ parallel(tgt,4) */ into dwh_datafix.bk_temp_foods_tab_dense_sparse tgt using
    (
      select /*+ parallel (a,4) full (a) */ 
            a.sk1_item_no, 
            a.sk1_location_no,
            sum(a.sales)                sales_6wk, 
            sum(a.sales_qty)            sales_6wk_qty, 
            sum(a.sales_margin)         sales_6wk_margin
      from  dwh_performance.temp_foods_tab_2yrs_dense a
      join  dim_calendar_wk                              b on (a.FIN_YEAR_NO = b.FIN_YEAR_NO and a.FIN_WEEK_NO = b.FIN_WEEK_NO )
      where b.THIS_WEEK_START_DATE between g_loopwk-35 and g_loopwk
--and SK1_LOCATION_NO = 695
      group by 
            a.sk1_item_no, a.sk1_location_no
    ) wk_d
      on   (wk_d.sk1_location_no     = tgt.sk1_location_no and
            wk_d.sk1_item_no         = tgt.sk1_item_no     and
            tgt.THIS_WEEK_START_DATE = g_loopwk
           )
    when matched then
    update 
    set     tgt.SALES         = wk_d.sales_6wk,
            tgt.SALES_QTY     = wk_d.sales_6wk_qty,
            tgt.SALES_MARGIN  = wk_d.sales_6wk_margin;
            
    g_cntB := g_cntB + sql%rowcount;
    commit;

    -- B. Measures from Sparse ....
    merge /*+ parallel(tgt,4) */ into dwh_datafix.bk_temp_foods_tab_dense_sparse tgt using
    (
      select /*+ parallel (a,4) full (a) */ 
            a.sk1_item_no, 
            a.sk1_location_no,
            sum(a.waste_cost)         waste_cost
      from  DWH_PERFORMANCE.temp_foods_tab_2yrs_sparse a
      join  dim_calendar                               b on (a.FIN_YEAR_NO = b.FIN_YEAR_NO and a.FIN_WEEK_NO = b.FIN_WEEK_NO and b.FIN_DAY_NO = 1)
      where b.THIS_WEEK_START_DATE between g_loopwk-35 and g_loopwk
--and SK1_LOCATION_NO = 695
      group by 
            a.sk1_item_no, a.sk1_location_no
    ) wk_s
      on   (wk_s.sk1_location_no      = tgt.sk1_location_no and
            wk_s.sk1_item_no          = tgt.sk1_item_no     and
            tgt.THIS_WEEK_START_DATE  = g_loopwk
           )
    when matched then
    update 
    set     tgt.WASTE_COST  = wk_s.waste_cost;  
    
    g_cntC := g_cntC + sql%rowcount;
    commit;

    g_loopwk := g_loopwk - 7;
end loop;


-- Part 2
merge /*+ parallel(sls,6) */ into dwh_datafix.bk_loc_item_wk_sales_6wkavg sls using
   (
      with 
        sales_xtr as 
          (select * from dwh_datafix.bk_temp_foods_tab_dense_sparse),
        
        cleanse_xtr as 
          (select /*+ parallel (a,6) full (a) */
                  g_maxyr                            as fin_year_no,
                  g_maxwk                            as fin_week_no,
                  a.sk1_item_no, 
                  a.sk1_location_no,
                  sum(a.sales_6wkavg_excl_promo_qty) as sales_6wkavg_excl_promo_qty,
                  sum(a.sales_6wkavg_excl_promo)     as sales_6wkavg_excl_promo
           from   mart_fd_zone_loc_item_6wkavg  a
--where SK1_LOCATION_NO = 695
           group by 
                  a.sk1_item_no, a.sk1_location_no
           )

        select nvl(r1.sk1_location_no, r2.sk1_location_no)  sk1_location_no,
               nvl(r1.sk1_item_no, r2.sk1_item_no)          sk1_item_no,
               nvl(r1.fin_year_no, r2.fin_year_no)          fin_year_no,
               nvl(r1.fin_week_no, r2.fin_week_no)          fin_week_no,
              
               r1.sales_qty                    as sales_6wk_qty,
               r2.sales_6wkavg_excl_promo_qty  as sales_6wkavg_excl_promo_qty,
               r1.sales                        as sales_6wk,
               r2.sales_6wkavg_excl_promo      as sales_6wkavg_excl_promo,
               r1.sales_margin                 as sales_6wk_margin,
               1,
               r1.waste_cost                   as waste_6wk_cost,
               2,
               3,
               g_date                          as last_updated_date,
               
               -- 6wk averages ...
               round(r1.sales_qty/6, 0)        as sales_6wkavg_qty,               
               round(r1.sales/6, 2)            as sales_6wkavg,               
               round(r1.sales_margin/6, 2)     as sales_6wkavg_margin,               
               round(r1.waste_cost/6, 2)       as waste_6wkavg_cost               
        from       sales_xtr   r1
        full join  cleanse_xtr r2 on  r1.sk1_item_no     = r2.sk1_item_no      and
                                      r1.sk1_location_no = r2.sk1_location_no  and
                                      r1.fin_year_no     = r2.fin_year_no      and
                                      r1.fin_week_no     = r2.fin_week_no        
   ) mer_mart  
     on (sls.sk1_location_no  = mer_mart.sk1_location_no 
     and sls.sk1_item_no      = mer_mart.sk1_item_no
     and sls.fin_year_no      = mer_mart.fin_year_no
     and sls.fin_week_no      = mer_mart.fin_week_no
        )
              
     when matched then
     update 
       set sales_6wk_qty                    = mer_mart.sales_6wk_qty,
           sales_6wkavg_excl_promo_qty      = mer_mart.sales_6wkavg_excl_promo_qty,
           sales_6wk                        = mer_mart.sales_6wk,
           sales_6wkavg_excl_promo          = mer_mart.sales_6wkavg_excl_promo,
           sales_6wk_margin                 = mer_mart.sales_6wk_margin,
           waste_6wk_cost                   = mer_mart.waste_6wk_cost,
           last_updated_date                = mer_mart.last_updated_date,
           sales_6wkavg_qty                 = mer_mart.sales_6wkavg_qty,
           sales_6wkavg                     = mer_mart.sales_6wkavg,           
           sales_6wkavg_margin              = mer_mart.sales_6wkavg_margin,
           waste_6wkavg_cost                = mer_mart.waste_6wkavg_cost           
     when not matched then
     insert 
     values
       (
          mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.fin_year_no,
          mer_mart.fin_week_no,
          mer_mart.sales_6wk_qty,
          mer_mart.sales_6wkavg_excl_promo_qty,
          mer_mart.sales_6wk,
          mer_mart.sales_6wkavg_excl_promo,
          mer_mart.sales_6wk_margin,
          0,
          mer_mart.waste_6wk_cost,
          0,
          0,
          g_date,
          mer_mart.sales_6wkavg_qty,          
          mer_mart.sales_6wkavg,          
          mer_mart.sales_6wkavg_margin,          
          mer_mart.waste_6wkavg_cost          
       );
       
  g_cntD := sql%rowcount;
  commit;
     
--  g_recs_read     := g_recs_read     + sql%rowcount;
--  g_recs_inserted := g_recs_inserted + sql%rowcount;

   commit;
-- BK06Oct2016 (end)

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  'Combined Keys created: '||g_cntA;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  'Dense updates (6 week rollup): '||g_cntB;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  'Sparse updates (6 week rollup): '||g_cntC;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  'Merge with 6wks AVG: '||g_cntD;
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

end wh_prf_corp_862u_bk;
