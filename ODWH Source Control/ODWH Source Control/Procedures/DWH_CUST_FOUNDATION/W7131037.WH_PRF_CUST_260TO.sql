-- ****** Object: Procedure W7131037.WH_PRF_CUST_260TO Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_260TO" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Feb 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust_csm_st_wk fact table in the performance layer
--               with input ex basket table from performance layer.
--               THIS JOB RUNS WEEKLY ON SAY A FRIDAY OR TOWARD WEEK END
--  Tables:      Input  - cust_basket
--               Output - cust_csm_st_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            cust_csm_st_wk%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;
g_csm_period         varchar2(10);
g_stmt               varchar2(300);
g_yr_00              number;
g_wk_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_260U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE BASKET to CUST/STORE/WEEK CSM';
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

    l_text := 'ROLLUP OF cust_csm_st_wk EX Basket LEVEL STARTED AT '||
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

--**************************************************************************************************
-- Look up period
--**************************************************************************************************
--select * from dim_csm_period
--into g_csm_period
--where csm_period_active_ind = 'Y'

    g_csm_period := '13WKBK';
    l_text := 'CSM PERIOD SELECTED  '||g_csm_period;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Main loop
--**************************************************************************************************

for g_sub in 125..125 loop
    select fin_day_no, this_week_start_date, this_week_end_date,fin_year_no,fin_week_no
    into   g_fin_day_no, g_start_date, g_end_date,g_yr_00,g_wk_00
    from   dim_calendar
    where  calendar_date = g_date - (g_sub * 7);

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

    g_stmt      := 'Alter table  W7131037.CUST_CSM_ST_WK truncate  subpartition for ('||g_yr_00||','||g_wk_00||') update global indexes';
    l_text      := g_stmt;

    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --   execute immediate g_stmt;


insert /*+ APPEND parallel (csm,4)*/ into cust_csm_st_wk csm
SELECT /*+ FULL(a)  parallel (a,4) */
        a.primary_customer_identifier,
        g_csm_period,
        a.location_no,
        g_yr_00,
        g_wk_00,
        sum(a.item_tran_selling-a.discount_selling) value,
        sum(a.item_tran_qty) items,
        count (distinct a.tran_date) visits,
        max(g_date) g_date
from    cust_basket_item a,
        dim_location dl
where   a.tran_date         between g_start_date and g_end_date
and     a.location_no       = dl.location_no
and     dl.sk1_country_code = 196
and     a.primary_customer_identifier is not null
and     a.primary_customer_identifier <> 0
and     a.tran_type not in('P','N','L')
group by a.primary_customer_identifier,
         g_csm_period,
         a.location_no
order by a.primary_customer_identifier,
         g_csm_period,
         a.location_no;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;
end loop;
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

end wh_prf_cust_260to;
