--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_536U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_536U_FIX" (p_forall_limit in integer,p_success out boolean) as
-- ******************************************************************************************
--  Date:        January 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create shelf life facing on fnd_location_item
--               with input ex Intactix fnd_shelf_edge table from foundation layer.
--  Tables:      Input  - fnd_shelf_edge
--               Output - fnd_location_item
--  Packages:    constants, dwh_log, dwh_valid
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_year_no        number        :=  0;
g_rec_out            fnd_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date ;
g_end_date           date ;
g_loop_date          date ;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_536U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD location_item shelf life facing EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Bulk merge
--**************************************************************************************************
procedure local_bulk_merge as
begin

g_loop_date := '27 jun 16';

FOR g_sub IN 0..22
  LOOP
    g_recs_read := 0;
    SELECT
      this_week_start_date,
      this_week_end_date,
      fin_year_no,
      fin_week_no
    INTO
      g_start_date,
      g_end_date,
      g_fin_year_no,
      g_fin_week_no
    FROM dim_calendar
    WHERE calendar_date = g_loop_date + (g_sub * 7); 

  l_text       := '-------------------------------------------------------------';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text       := 'Rollup range is:- '||g_start_date||'  To '||g_end_date;
  l_text       := 'Year/week range is:- '||g_fin_year_no||' '||g_fin_week_no;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 -- G_DATE:='Moo';
   merge /*+ parallel(cat,8) */ into rtl_loc_item_wk_catalog cat using
  (
   select /*+  parallel(lid,8) full (lid) full (dc)  */
          lid.sk1_item_no,
          lid.sk1_location_no,
          dc.fin_year_no,
          dc.fin_week_no,
          sum(nvl(lid.fd_cust_avail,0)) fd_cust_avail,
          sum(nvl(lid.fd_num_cust_catlg_adj,0)) fd_num_cust_catlg_adj,
          sum(nvl(lid.fd_num_cust_avail_adj,0)) fd_num_cust_avail_adj,
          sum(nvl(lid.fd_num_catlg_days_adj,0)) fd_num_catlg_days_adj
   from   rtl_loc_item_dy_catalog lid,
          dim_calendar dc
   where  lid.calendar_date between g_start_date and g_end_date
    and   lid.calendar_date = dc.calendar_date
--    and   lid.sk1_item_no = 23150952   
--      and   lid.sk1_location_no  = 4602  ----- store_no 1005
   group by dc.fin_year_no,  dc.fin_week_no, lid.sk1_item_no, lid.sk1_location_no

  ) mer_mart

   on (cat.sk1_item_no            = mer_mart.sk1_item_no
   and cat.sk1_location_no        = mer_mart.sk1_location_no
   and cat.fin_year_no            = mer_mart.fin_year_no
   and cat.fin_week_no            = mer_mart.fin_week_no
      )

   when matched then
     update
     set fd_cust_avail            = mer_mart.fd_cust_avail,
         fd_num_cust_catlg_adj    = mer_mart.fd_num_cust_catlg_adj,
         fd_num_cust_avail_adj    = mer_mart.fd_num_cust_avail_adj,
         fd_num_catlg_days_adj    = mer_mart.fd_num_catlg_days_adj
     ;

   g_recs_read      :=  g_recs_read + sql%rowcount;
--   g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;
   g_recs_updated   :=  g_recs_updated + sql%rowcount;
   
   l_text := 'RECORDS PROCESSED :- '||g_recs_read||' '||g_fin_year_no||' '||g_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  COMMIT;

 end loop;

   exception
     when dwh_errors.e_insert_error then
       l_message := 'BULK MERGE - INSERT / UPDATE ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

     when others then
       l_message := 'BULK MERG - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_bulk_merge;

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

    l_text := 'Update OF rtl_loc_item_wk_catalog STARTED AT '||
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

    local_bulk_merge;

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
end wh_prf_corp_536u_fix;
