-- ****** Object: Procedure W7131037.WH_PRF_CUST_550E Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_550E" (p_forall_limit in integer,p_success out boolean) as
--************************************************************************************************
--**** productionising this put on hold until end-user report spec is finalised  ********************
--**************************************************************************************************
--  Date:        May 2015
--  Author:      Naresh Chauhan
--  Purpose:     Create Customer Complaints extract to flat file.
--  Tables:      Input  - CUST_CL_INQUIRY
--               Output - flat file extracts
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_count              number        :=  0;

-- ********** temp mod to get test data
--g_date               date          := trunc(sysdate);
g_date               date          := to_date('11/MAY/2004','DD/MON/YYYY');


g_last_wk_strt_dte   date;
g_last_wk_end_dte    date;

g_xfile              varchar(100);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_550E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT TO FLAT FILE - source: CUST_CL_INQUIRY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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

    l_text := 'EXTRACT  STARTED AT '||to_char(sysdate,('YYYY-MM-DD hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

--**** temp remove for testing
--    dwh_lookup.dim_control(g_date);

    select ct.this_week_start_date-7 as strt_dt, ct.this_week_end_date-7+1 as end_dt
    into   g_last_wk_strt_dte, g_last_wk_end_dte
    from   dim_calendar ct
    where  ct.calendar_date = g_date;

    g_xfile:='Data_Extract-Complaint_Details_'||to_char(g_last_wk_strt_dte,'YYYYMMDD');

    l_text := 'Batch date being processed : '||to_char(g_date,'YYYY-MM-DD')||
              ' ('|| to_char(g_last_wk_strt_dte,'YYYY-MM-DD HH24:MI')||
              ' to '||to_char(g_last_wk_end_dte,'YYYY-MM-DD HH24:MI')||')';

    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_count := dwh_performance.dwh_generic_file_extract(
    'select
      h.region_name as Region,
      a.cl_inq_category_desc as Category,
      b.cl_inq_feedback_desc Tier1,
      c.cl_inq_type_cat_desc as Tier2 ,
      d.cl_inq_sub_cat_desc Tier3,
      e.inq_details as Detail,
      e.interaction_no as interaction_no,
      e.inquiry_no,
      g.cl_user_name as logged_by ,
      h.location_no as store_no,
      h.location_name as store_name ,
      i.department_name as department_Name ,
      f.channel_inbound_desc as Channel,
      e.logged_date as logged_date,
      e.closed_date as closed_date,
      z.cl_user_name as "Owner User",
      e.inquiry_bus_area as Call_Center
      from  dim_cust_cl_inq_cat a,
            dim_cust_cl_feedback b,
            dim_cust_cl_type_cat c,
            dim_cust_cl_Sub_cat d,
            cust_cl_inquiry e,
            dim_cust_cl_chanel_inbound f,
            dim_cust_cl_user g,
            dim_cust_cl_user z,
            dim_location h,
            dim_item i
      where a.cl_inq_category_no = b.cl_inq_category_no
      and a.cl_inq_category_no in (1331,1332,1333)
      and b.cl_inq_feedback_no in
        (1337,1338,1339,1341,1342,1343,1345,1346,
         1347,99991331,99991332,99991333)
      and b.sk1_cl_inq_feedback_no = c.sk1_cl_inq_feedback_no
      and d.sk1_cl_inq_type_cat_no = c.sk1_cl_inq_type_cat_no
      and d.sk1_cl_inq_sub_cat_no = e.sk1_cl_inq_sub_cat_no
      and e.logged_date between '
      ||''''||g_last_wk_strt_dte||''' and '''||g_last_wk_end_dte||''' '||'
      and f.sk1_channel_inbound_no = e.sk1_channel_inbound_no
      and g.sk1_cl_user_no = e.sk1_logged_by_user_no
      and z.sk1_cl_user_no = e.SK1_OWNER_USER_NO
      and h.sk1_location_no = e.sk1_location_no
      and i.sk1_item_no = e.sk1_item_no
      order by logged_date',
    '|','DWH_FILES_OUT', g_xfile

    );

    l_text := g_count|| '  Records extracted to extract file '''||g_xfile||'''';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('YYYY-MM-DD hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text :=  dwh_constants.vc_log_run_completed ||to_char(sysdate,('YYYY-MM-DD hh24:mi:ss'));
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

END "WH_PRF_CUST_550E";
