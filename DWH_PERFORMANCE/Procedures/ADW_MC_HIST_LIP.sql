--------------------------------------------------------
--  DDL for Procedure ADW_MC_HIST_LIP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."ADW_MC_HIST_LIP" 
                                                             (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        AUG 2018
--  Author:      ADW
--  Purpose:     POPULATE MC MASTER DATA WITH HIST MEASURES
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
g_sub                integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'ADW_MC_HIST_LIP';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'MC DATA POPULATE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor   c_fnd_locations is
select   SK1_location_no  
from     dim_location
where    SK1_location_no > 565   --restart point--
order by SK1_location_no;



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
   l_text := 'STARTED AT '||
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
   
  execute immediate 'alter session enable parallel dml';

   for loc_record in c_fnd_locations
   loop

   
UPDATE  /*+ parallel(xyz,12) */
        RTL_LOCATION_ITEM xyz
SET     REG_RSP_LOCAL = REG_RSP,
        REG_RSP_OPR   = REG_RSP,
        SELLING_RSP_LOCAL = SELLING_RSP,
        SELLING_RSP_OPR   = SELLING_RSP,
        PROM_RSP_LOCAL    = PROM_RSP,
        PROM_RSP_OPR      = PROM_RSP,
        PROM_SELLING_RSP_LOCAL = PROM_SELLING_RSP,
        PROM_SELLING_RSP_OPR   = PROM_SELLING_RSP
WHERE   (
         NVL(REG_RSP_OPR,0)     <> NVL(REG_RSP,0) OR
         NVL(PROM_RSP_OPR,0)    <> NVL(PROM_RSP,0) OR
         NVL(SELLING_RSP_OPR,0) <> NVL(SELLING_RSP,0)
         )

AND     SK1_LOCATION_NO = LOC_RECORD.SK1_LOCATION_NO;
 

      g_recs_read     := g_recs_read     + sql%rowcount;

      l_text := 'UPDATED LOCATION ' ||LOC_RECORD.SK1_LOCATION_NO||'  '||
                to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount||' total '||g_recs_READ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      commit;
   end loop;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end ADW_MC_HIST_LIP;
