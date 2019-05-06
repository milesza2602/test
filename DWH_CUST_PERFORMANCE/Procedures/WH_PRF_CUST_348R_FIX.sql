--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_348R_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_348R_FIX" (p_forall_limit in integer,p_success out boolean,p_date in date) AS

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

g_start_date         date          ;
g_end_date           date          ;
g_count              integer       :=  0;
g_date               date          := nvl(p_date,trunc(sysdate));
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_348R';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP LID TO LIW STAFF SALES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLLUP CUST_STAFF_SALES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
--    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    select this_week_end_date
    into   g_end_date
    from   dim_calendar
    where  calendar_date = g_date; 

    select this_week_start_date
    into   g_start_date
    from   dim_calendar
    where  calendar_date = g_date - 21;

    l_text := 'ROLL UP RANGE IS:- '||g_start_date||'  '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = false';

--**************************************************************************************************


             merge /*+ parallel (csw,8)  */ into cust_staff_sales_wk csw
             using (
               select /*+ parallel (csd,8) */
                      dc.fin_year_no,
                      dc.fin_week_no,
                      item_no,
                      location_no,
                      max(sk1_item_no) sk1_item_no,
                      max(sk1_location_no) sk1_location_no,
                      max(original_item_no) original_item_no ,
                      sum(staff_sales) staff_sales,
                      sum(staff_discount_selling) staff_discount_selling,
                      sum(online_staff_sales)  online_staff_sales,
                      sum(online_staff_discount_selling) online_staff_discount_selling
               from   cust_staff_sales csd,
                      dim_calendar dc 
               where  tran_date         between g_start_date and g_end_date
               and    csd.tran_date     = dc.calendar_date
               group by dc.fin_year_no,
                        dc.fin_week_no,
                        item_no,
                        location_no 
                   ) mer_rec
             on    (csw.fin_year_no	=	mer_rec.fin_year_no and
                    csw.fin_week_no	=	mer_rec.fin_week_no and
                    csw.location_no =	mer_rec.location_no and
                    csw.item_no     = mer_rec.item_no)
             when matched then 
             update set 
                    csw.staff_sales                          =	mer_rec.staff_sales ,
                    csw.staff_discount_selling               =	mer_rec.staff_discount_selling ,
                    csw.online_staff_sales                   =	mer_rec.online_staff_sales  ,
                    csw.online_staff_discount_selling        =	mer_rec.online_staff_discount_selling  ,
                    csw.last_updated_date                    =  g_date 
             where  nvl(csw.staff_sales,0)                   <>	mer_rec.staff_sales or
                    nvl(csw.staff_discount_selling,0)        <>	mer_rec.staff_discount_selling or
                    nvl(csw.online_staff_sales,0)            <>	mer_rec.online_staff_sales  or
                    nvl(csw.online_staff_discount_selling,0) <>	mer_rec.online_staff_discount_selling  
              when not matched then
              insert
                      (         
                      fin_year_no,
                      fin_week_no,
                      location_no,
                      item_no,
                      sk1_item_no,
                      sk1_location_no,
                      original_item_no,
                      staff_sales,
                      staff_discount_selling,
                      online_staff_sales,
                      online_staff_discount_selling,
                      last_updated_date
                      )
              values
                      ( 
                      mer_rec.fin_year_no,
                      mer_rec.fin_week_no,
                      mer_rec.location_no,
                      mer_rec.item_no,
                      mer_rec.sk1_item_no,
                      mer_rec.sk1_location_no,
                      mer_rec.original_item_no,
                      mer_rec.staff_sales,
                      mer_rec.staff_discount_selling,
                      mer_rec.online_staff_sales,
                      mer_rec.online_staff_discount_selling,
                      g_date
                      )                          

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
    l_text := dwh_cust_constants.vc_log_run_completed||'348R'||sysdate;
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

END WH_PRF_CUST_348R_FIX;
