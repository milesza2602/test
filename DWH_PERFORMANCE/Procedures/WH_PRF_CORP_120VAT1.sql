--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_120VAT1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_120VAT1" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        AUG 2015
--  Author:      Alastair de Wet
--  Purpose:     FIX VAT
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
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
g_count              integer;
g_lud                date := '10 JUL 2015';

   
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g05_day              integer       := TO_CHAR(current_date,'DD');

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_120VAT';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'FIX VAT ON DENSE EX SHIPMENT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- Update all record flaged as 'N' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin

for g_count in 1..41 
loop

g_lud := g_lud + 1;

    l_text := 'CURRENT DATE BEING FIXED IS:- '||G_LUD||' SO FAR UPDATED  '||G_RECS_UPDATED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
   merge  into rtl_loc_item_dy_rms_dense  dnsw 
   using (
   with lid_list as
   (
   select /*+ parallel (4) */ item_no,to_loc_no,shp.actl_rcpt_date
   from   fnd_rtl_shipment shp,
          dim_location dl
   where  shp.last_updated_date  = g_lud and
          shp.to_loc_no          = dl.location_no and
          dl.loc_type            = 'S' and
          shp.actl_rcpt_date     is not null
          AND (CHAIN_CODE <> 'DJ' or chain_code is null)
   group by item_no,to_loc_no,shp.actl_rcpt_date
   )
   select /*+ parallel (4) */
          sum(nvl(shp.received_qty,0)) as received_qty,
          sum(nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + di.vat_rate_perc))) as received_selling,
          sum(nvl(shp.received_qty,0) * shp.cost_price) as received_cost,
          trunc(shp.actl_rcpt_date) as actl_rcpt_date,
          di.sk1_item_no,
          max(di.sk1_department_no) as sk1_department_no,
          dl.sk1_location_no,
          max(dl.chain_no) as chain_no,
          max(dl.sk1_fd_zone_group_zone_no) as sk1_fd_zone_group_zone_no 

   from   fnd_rtl_shipment shp,
          lid_list ,
          dim_item di,
          dim_location dl
   where  shp.item_no                = lid_list.item_no        and
          shp.to_loc_no              = lid_list.to_loc_no      and
          shp.actl_rcpt_date         = lid_list.actl_rcpt_date   and
          shp.received_qty           <> 0                  and
          shp.received_qty           is not null           and
          lid_list.item_no           = di.item_no          and
          lid_list.to_loc_no         = dl.location_no       
   group by di.sk1_item_no, dl.sk1_location_no,shp.actl_rcpt_date 
            ) mer_rec
   on    (  dnsw.	post_date	            =	mer_rec.actl_rcpt_date	 and
            dnsw.	sk1_location_no	      =	mer_rec.sk1_location_no	and
            dnsw.	sk1_item_no	          =	mer_rec.sk1_item_no	)
   when matched then 
   update set
            actl_store_rcpt_selling         =  mer_rec.received_selling
   where    dnsw.actl_store_rcpt_selling    <> round(mer_rec.received_selling,2);

      g_recs_updated := g_recs_updated +  sql%rowcount;       

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
end flagged_records_update;
  

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
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
    
    g_run_date :=  g_date + 1;

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************


    

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

 
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
       RAISE;

END WH_PRF_CORP_120VAT1;
