-- ****** Object: Procedure W7131037.WH_PRF_CUST_348A Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_348A" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Dec 2017
--  Author:      Alastair de Wet
--  Purpose:     UPDATE LOC_ITEM_DAY DENSE EX Staff Sales
--  Tables:      Input  - cust_staff_sales
--               Output - rtl_loc_item_dy_rms_dense
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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

g_count              integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_348A';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE LOC/ITEM/DAY DENSE EXSTAFF SALES AT LOC/ITEM/DAY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'UPDATE DENSE SALES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

--execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = false';

--**************************************************************************************************


             merge /*+ parallel (dns,8)  */ into rtl_loc_item_dy_rms_dense dns
             using (
               select /*+ parallel (csd,8) */
                      TRAN_DATE,
                      DI.SK1_ITEM_NO,
                      DL.SK1_LOCATION_NO,
                      STAFF_SALES,
                      STAFF_DISCOUNT_SELLING,
                      ONLINE_STAFF_SALES,
                      ONLINE_STAFF_DISCOUNT_SELLING
               from   cust_staff_sales csd,
                      dim_item di,
                      dim_location dl
               where  tran_date       > g_date - 31
--               where  tran_date       = '27 NOV 2017'
               and    csd.item_no     = di.item_no
               and    csd.location_no = dl.location_no
                   ) mer_rec
             on    (dns.post_date	      =	mer_rec.tran_date and
                    dns.sk1_location_no =	mer_rec.sk1_location_no and
                    dns.sk1_item_no     = mer_rec.sk1_item_no)
             when matched then
             update set
                    dns.staff_sales                          =	mer_rec.staff_sales ,
                    dns.staff_discount_selling               =	mer_rec.staff_discount_selling ,
                    dns.online_staff_sales                   =	mer_rec.online_staff_sales  ,
                    dns.online_staff_discount_selling        =	mer_rec.online_staff_discount_selling  ,
                    dns.last_updated_date                    =  g_date
             where  nvl(dns.staff_sales,0)                   <>	mer_rec.staff_sales or
                    nvl(dns.staff_discount_selling,0)        <>	mer_rec.staff_discount_selling or
                    nvl(dns.online_staff_sales,0)            <>	mer_rec.online_staff_sales  or
                    nvl(dns.online_staff_discount_selling,0) <>	mer_rec.online_staff_discount_selling
                       ;

              g_recs_updated := g_recs_updated  + sql%rowcount;


    commit;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||'348A'||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
--        execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = true';
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END "WH_PRF_CUST_348A";
