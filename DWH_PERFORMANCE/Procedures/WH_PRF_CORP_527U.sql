--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_527U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_527U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Update shrinkage to loc_bu_dy_shrinkage fact table in the performance layer
--               with input ex RMS fnd_loc_item_rms_sparse table from performance layer.
--
--  Tables:      Input  -  fnd_rtl_inventory_adj  
--                      -  rtl_loc_item_dy_rms_sparse(old table ) Replaced with fnd_rtl_inventory_adj (new table)
--               Output - rtl_loc_bu_dy_shrinkage
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
--  09 Dec 2014 - Source table changed by Mapopa Phiri 
-- Note: Reason code 10 was filtered (ReasoCode in (0, 90, 111) and from the fnd_rtl_inventory_adj table, then rolled up and summerised
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.

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
g_truncate_count     INTEGER       :=  0;

g_start_date         date;
g_end_date           date;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_527U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD SHRINKAGE EX SPARSE TO LOC_BU_DY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_lid_sparse is
      with 
intentory as (
select 
      loc.sk1_location_no,
      dc.calendar_date,
      di.item_no,
      LOC.location_no,
      di.sk1_item_no,
      shr.post_date,
      di.sk1_item_no,
      di.sk1_business_unit_no  
From 
dim_calendar dc, dim_item di, dim_location loc, dwh_performance.rtl_loc_bu_dy_shrinkage shr
where 
              loc.sk1_location_no     = shr.sk1_location_no
         AND  di.sk1_business_unit_no = shr.sk1_business_unit_no
         AND  shr.post_date           = dc.calendar_date 
)
select  /*+ full (lid) PARALLEL(lid,4) */         
        loc.sk1_location_no,
        di.sk1_business_unit_no,
        lid.post_date,
        sum(nvl(lid.inv_adj_qty,0)) shrinkage_qty,
        sum(nvl(lid.inv_adj_selling,0)) shrinkage_selling,
        sum(nvl(lid.inv_adj_cost,0)) shrinkage_cost
  FROM  
    dwh_foundation.fnd_rtl_inventory_adj lid, dim_item di, dim_location loc 
    where lid.reason_code in (0,90,111)
    and  lid.post_date BETWEEN g_start_date AND g_end_date
     and  lid.item_no     = di.item_no 
     and  lid.location_no = loc.location_no 
--     and  lid.last_updated_date = g_date 
GROUP BY lid.post_date, loc.sk1_location_no, di.sk1_business_unit_no
;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
BEGIN

   INSERT  /*+ APPEND parallel (rtl_liwrd,4) */ INTO dwh_performance.rtl_loc_bu_dy_shrinkage rtl_liwrd
    select          
        loc.sk1_location_no,
        di.sk1_business_unit_no,
        lid.post_date,0,0,0,
        sum(nvl(lid.inv_adj_qty,0)) shrinkage_qty,
        sum(nvl(lid.inv_adj_selling,0)) shrinkage_selling,
        sum(nvl(lid.inv_adj_cost,0)) shrinkage_cost,
        max(g_date) last_update_date
  FROM  
    dwh_foundation.fnd_rtl_inventory_adj lid, dim_item di, dim_location loc --  GROUP BY lid.post_date,lid.location_no
        where lid.reason_code in (0,90,111)
        and lid.post_date  BETWEEN g_start_date AND g_end_date
        and loc.location_no=lid.location_no 
        and lid.item_no=di.item_no 
--        and lid.last_updated_date = g_date 
        and lid.inv_adj_qty    is not null
        and not exists
              (select /*+ full (shr) PARALLEL(shr,4) */  * from dwh_performance.rtl_loc_bu_dy_shrinkage shr
               where   shr.sk1_location_no       = loc.sk1_location_no       and
                       shr.sk1_business_unit_no  = di.sk1_business_unit_no  and
                       shr.post_date             = lid.post_date )
    group by lid.post_date, loc.sk1_location_no, di.sk1_business_unit_no
;

  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


  commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message); 
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



for upd_rec in c_lid_sparse
   loop
     UPDATE rtl_loc_bu_dy_shrinkage prf
     SET    shrinkage_qty                  = upd_rec.shrinkage_qty,
            shrinkage_selling              = upd_rec.shrinkage_selling,
            shrinkage_cost                 = upd_rec.shrinkage_cost,
            prf.last_updated_date       = g_date
     WHERE  prf.sk1_location_no         = upd_rec.sk1_location_no AND
            prf.sk1_business_unit_no    = upd_rec.sk1_business_unit_no  AND
            prf.post_date               = upd_rec.post_date and
              (
            upd_rec.shrinkage_qty         <> prf.shrinkage_qty OR
            upd_rec.shrinkage_selling     <> prf.shrinkage_selling OR
            upd_rec.shrinkage_cost        <> prf.shrinkage_cost  );

      g_recs_updated := g_recs_updated + 1;
      g_recs_read    := g_recs_read + 1;
   end loop;


      commit;


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

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************
    select this_week_start_date
    into   g_start_date
    FROM   dim_calendar
    WHERE  calendar_date = g_date - 35;

    select this_week_end_date
    into   g_end_date
    FROM   dim_calendar
    where  calendar_date = g_date ;

--    select count(*)
--    into   g_recs_read
--    from   DWH_FOUNDATION.fnd_rtl_inventory_adj
--    where  last_updated_date = g_date;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;


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
   
end WH_PRF_CORP_527U  ;
