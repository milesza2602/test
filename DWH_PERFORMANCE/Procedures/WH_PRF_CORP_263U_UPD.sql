--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_263U_UPD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_263U_UPD" 
                        (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        June 2014
--  Author:      Quentin Smit
--  Purpose:     Load Supplier Master Data History mart
--
--  Tables:      Input  - rtl_zone_item_supp_hist
--                        rtl_loc_item_dy_st_ord
--                        rtl_location_item
--                        rtl_zone_item_om
--                        rtl_depot_item_wk
--               Output - MART_SUPPLIER_MASTER_DATA_HIST
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       
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
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_physical_updated   integer       :=  0;

g_date               date          := trunc(sysdate);
g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_last_wk_start_date date;
g_last_wk_end_date   date;
g_calendar_date      date;
g_loop_date          date;
g_start_date         date ;
g_end_date           date ;
g_fin_week_no        number        :=  0;
g_fin_year_no        number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_263U_RECLASS';                              
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE MASTER DATA ON MART';   
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure do_merge_update as
begin
 
 g_loop_date := '4 nov 18';

 FOR g_sub IN 0..132
  LOOP
    g_recs_read := 0;
    SELECT
      calendar_date
    INTO
      g_calendar_date
    FROM dim_calendar
    WHERE calendar_date = g_loop_date - g_sub ;

   l_text       := '-------------------------------------------------------------';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text       := 'Data processed is:- '||g_calendar_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
--   merge /* parallel (rtl,8) append */ into dwh_datafix.mart_supplier_master_data_hist rtl 
   merge /* parallel (rtl,8) */ into dwh_performance.MART_SUPPLIER_MASTER_DATA_HIST rtl 
   using (
         with dim_xtrct as (
          select /*+ full (di) parallel (di,6) */ 
                 sk1_item_no,
                 department_no,
                 subclass_no
          from   dim_item di
          where  business_unit_no = 50 --and sk1_item_no = 21279981
          ),
          
          mart_xtrct as (
          select /*+ full (a) parallel (a,8) full (b) parallel (b,8) */
                 a.SK1_SUPPLIER_NO,
                 a.SK1_ITEM_NO,
                 a.SK1_ZONE_GROUP_ZONE_NO,
                 a.SK1_PRODUCT_STATUS_NO,
                 a.CALENDAR_DATE,
                 a.department_no olddept,
                 a.subclass_no oldsubcl,
                 b.department_no newdept,
                 b.subclass_no newsubcl
          from   dwh_performance.MART_SUPPLIER_MASTER_DATA_HIST a,
                 dim_xtrct b
          where  a.calendar_date = g_calendar_date
            and  a.sk1_item_no = b.sk1_item_no
            and (a.subclass_no <> b.subclass_no
            or   a.department_no <> b.department_no)
           )   
           select /*+ full (a) parallel (a,8) */
                 sk1_supplier_no,
                 sk1_item_no,
                 sk1_zone_group_zone_no,
                 sk1_product_status_no,
                 calendar_date,
                 newdept,
                 newsubcl
           from mart_xtrct a
        ) mer_rec
         
   on    (rtl.sk1_supplier_no	        =	mer_rec.sk1_supplier_no        and
          rtl.sk1_item_no  	          =	mer_rec.sk1_item_no            and
          rtl.sk1_zone_group_zone_no	=	mer_rec.sk1_zone_group_zone_no and    
          rtl.sk1_product_status_no   =	mer_rec.sk1_product_status_no  and
          rtl.calendar_date           =	mer_rec.calendar_date)
            
   when matched then 
   update set  
          rtl.subclass_no             =	mer_rec.newsubcl,
          rtl.department_no           =	mer_rec.newdept
            
          ;   
          
   g_recs_read      :=  g_recs_read + sql%rowcount;
   g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;

   
   l_text := 'RECORDS PROCESSED :- '||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;

  end loop;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
 
end do_merge_update;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

--    dbms_output.put_line('Execute Parallel ');
    execute immediate 'alter session enable parallel dml';
 
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

    dwh_lookup.dim_control(g_date);
    
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
--    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    do_merge_update;
   
    l_text := 'MERGE DONE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
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
end wh_prf_corp_263u_upd;                                                                              -- STORE PROC CHANGE
