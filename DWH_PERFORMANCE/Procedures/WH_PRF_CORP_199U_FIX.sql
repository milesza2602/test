--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_199U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_199U_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Jun 2013
--  Author:      Alastair de Wet
--  Purpose:     Create a summary rollup of the Foods catalog
--  Tables:      Input  - rtl_loc_item_dy_catalog
--               Output - mart_fd_area_item_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  27 Mar 2017 - add YTD measure fd_num_cust_avail_adj_ytd : chg-4956
--  27 Mar 2017 - add YTD measures fd_num_cust_avail_adj_ytd : Chg-12260

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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_count              number        :=  0;
g_fin_year_cur       number        :=  0;
g_fin_year_prev      number        :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_199U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE FOODS CATALOG to AREA WITH YTD FIGURES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLLUP OF rtl_loc_item_catalog EX DAY LEVEL STARTED AT '||
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

    select fin_year_no 
    into g_fin_year_cur
    from dim_calendar 
    where calendar_date = g_date; 
    
    select fin_year_no 
    into g_fin_year_prev
    from dim_calendar 
    where calendar_date = g_date - 1; 

    execute immediate 'alter session enable parallel dml';
    delete /*+ parallel (a,8) */ from  mart_fd_area_item_dy a where calendar_date = g_date;
    g_recs_deleted := g_recs_deleted + sql%rowcount;

    commit;

Insert  /*+ APPEND */ into mart_fd_area_item_dy

with locs as (
    select sk1_location_no,sk1_area_no,area_no
    from   dwh_performance.dim_location 
    where  chain_no = 10
    ),
items as (
    select item_no,sk1_item_no
    from   dwh_performance.dim_item 
    where  business_unit_no = 50
    ),
catlg as (
    select /*+parallel (rtl,2)*/
           locs.sk1_area_no,
           rtl.sk1_item_no,
           rtl.calendar_date,
           max(locs.area_no) area_no,
           max(items.item_no) item_no,
           max(commercial_manager_desc_562) commercial_manager_desc_562,
           sum(rtl.fd_num_avail_days_adj) fd_num_avail_days_adj,
           sum(rtl.fd_num_catlg_days_adj) fd_num_catlg_days_adj,
           sum(rtl.fd_num_cust_avail_adj) fd_num_cust_avail_adj, -- chg-4956
           sum(rtl.fd_num_cust_catlg_adj) fd_num_cust_catlg_adj, -- Chg-12260
           sum(rtl.fd_cust_avail)         fd_cust_avail         -- Chg-12260
    from   dwh_performance.rtl_loc_item_dy_catalog  rtl,
           locs,
           items,
           dim_item_uda  du
    where  rtl.calendar_date = g_date
    and    rtl.sk1_location_no = locs.sk1_location_no
    and    rtl.sk1_item_no     = items.sk1_item_no
    and    du.sk1_item_no      = items.sk1_item_no
    group by locs.sk1_area_no, rtl.sk1_item_no, rtl.calendar_date
    ),
mart_filter as (
    select /*+parallel (mart,2)*/  * 
    from   dwh_performance.mart_fd_area_item_dy  mart
    where  mart.calendar_date = g_date - 1
    ),
final_join as (
select nvl(cat.sk1_area_no,mart.sk1_area_no) sk1_area_no,
       nvl(cat.sk1_item_no,mart.sk1_item_no) sk1_item_no,
       g_date calendar_date,
       nvl(cat.area_no,mart.area_no) area_no ,
       nvl(cat.item_no,mart.item_no) item_no , 
--      (nvl(cat.fd_num_avail_days_adj,0) + nvl(mart.fd_num_avail_days_adj_ytd,0)) avail_days_adj_ytd,    --old way of doing
      case when g_fin_year_cur = g_fin_year_prev
           then (nvl(cat.fd_num_avail_days_adj,0) + nvl(mart.fd_num_avail_days_adj_ytd,0))
           else nvl(cat.fd_num_avail_days_adj,0)
           end fd_num_avail_days_adj_ytd, --avail_days_adj_ytd,           
--      (nvl(cat.fd_num_catlg_days_adj,0) + nvl(mart.fd_num_catlg_days_adj_ytd,0)) catlg_days_adj_ytd,    --old way of doing
      case when g_fin_year_cur = g_fin_year_prev
           then (nvl(cat.fd_num_catlg_days_adj,0) + nvl(mart.fd_num_catlg_days_adj_ytd,0))
           else nvl(cat.fd_num_catlg_days_adj,0)
           end fd_num_catlg_days_adj_ytd,--catlg_days_adj_ytd, 
      nvl(cat.commercial_manager_desc_562,mart.commercial_manager_desc_562) commercial_manager_desc_562,    
      g_date last_updated_date,
--      (nvl(cat.fd_num_cust_avail_adj,0) + nvl(mart.fd_num_cust_avail_adj_ytd,0)) fd_num_cust_avail_adj_ytd,  -- chg-4956 --old way of doing
      case when g_fin_year_cur = g_fin_year_prev
           then (nvl(cat.fd_num_cust_avail_adj,0) + nvl(mart.fd_num_cust_avail_adj_ytd,0))
           else nvl(cat.fd_num_cust_avail_adj,0)
           end fd_num_cust_avail_adj_ytd,       
--      (nvl(cat.fd_num_cust_catlg_adj,0) + nvl(mart.fd_num_cust_catlg_adj_ytd,0)) fd_num_cust_catlg_adj_ytd,  -- Chg-12260
      case when g_fin_year_cur = g_fin_year_prev 
           then (nvl(cat.fd_num_cust_catlg_adj,0) + nvl(mart.fd_num_cust_catlg_adj_ytd,0)) 
           else  nvl(cat.fd_num_cust_catlg_adj,0)
           end fd_num_cust_catlg_adj_ytd,
--      (nvl(cat.fd_cust_avail,0) + nvl(mart.fd_cust_avail_ytd,0)) fd_cust_avail_ytd  -- Chg-12260 
      case when g_fin_year_cur = g_fin_year_prev
           then (nvl(cat.fd_cust_avail,0) + nvl(mart.fd_cust_avail_ytd,0))
           else  nvl(cat.fd_cust_avail,0)
           end fd_cust_avail_ytd
from   catlg cat
full outer join mart_filter mart
on     cat.sk1_area_no       = mart.sk1_area_no and
       cat.sk1_item_no       = mart.sk1_item_no ) 

select * from final_join  fj
where (fj.fd_num_avail_days_adj_ytd <> 0 or fj.fd_num_catlg_days_adj_ytd <> 0 or
       fj.fd_num_cust_avail_adj_ytd <> 0 or
       fj.fd_num_cust_catlg_adj_ytd <> 0 or fj.fd_cust_avail_ytd <> 0);

  g_recs_read := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_199u_fix;
