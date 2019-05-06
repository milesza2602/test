--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_129U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_129U"                (p_forall_limit in integer,
                  p_success out boolean,
                  p_event_no   in fnd_prom_event.event_no%type, 
                  p_event_desc in fnd_prom_event.theme_desc%type, 
                  p_prom_no    in fnd_prom.prom_no%type,
                  p_prom_name  in fnd_prom.prom_name%type,
                  p_appr_date  in date,
                  p_start_date in date, 
                  p_end_date   in date) as
--**************************************************************************************************
--  Date:        January 2010
--  Author:      M Munnik
--  Purpose:     Manual load of promotion masterdata, which will not be supplied by RMS.
--               NB NB NB Declaration of p_event_desc is that of fnd_prom_event.theme_desc, 
--               because the same input parameter is used for both event_desc and theme_desc.
--               In the definition of fnd_prom_event, theme_desc is shorter than event_desc.
--               
--  Tables:      Input     - input parameters
--               Output    - fnd_prom_event
--                           fnd_prom
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
g_forall_limit                integer       :=  10000;
g_recs_inserted               integer       :=  0;
g_date                        date;

invalid_event_no              exception;
not_enough_par                exception;

l_message                     sys_dwh_errlog.log_text%type;
l_module_name                 sys_dwh_errlog.log_procedure_name%type          := 'WH_FND_CORP_129U';
l_name                        sys_dwh_log.log_name%type                       := dwh_constants.vc_log_name_rtl_md;
l_system_name                 sys_dwh_log.log_system_name%type                := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name                 sys_dwh_log.log_script_name%type                := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name              sys_dwh_log.log_procedure_name%type             := l_module_name;
l_text                        sys_dwh_log.log_text%type ;
l_description                 sys_dwh_log_summary.log_description%type        := 'MANUAL LOAD OF PROM MASTERDATA';
l_process_type                sys_dwh_log_summary.log_process_type%type       := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > 1000 then
      g_forall_limit := p_forall_limit;
   end if;
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'MANUAL LOAD OF PROM MASTERDATA STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if (p_event_no is null) or (p_event_desc is null) or (p_prom_no is null) or (p_prom_name is null) 
   or (p_appr_date is null) or (p_start_date is null) or (p_end_date is null) then
      raise not_enough_par;
   end if;
   
   if substr(p_event_no, 4, 3) <> 'DWH' then
      raise invalid_event_no;
   end if;

-- theme_desc same as event_desc
   insert into fnd_prom_event
              (event_no, event_desc, theme_desc, event_start_date, event_end_date, last_updated_date)
   values     (p_event_no, p_event_desc, p_event_desc, p_start_date, p_end_date, g_date);

   g_recs_inserted := g_recs_inserted + sql%rowcount;

   l_text := 'INSERT TO fnd_prom_event '||to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   insert into fnd_prom
              (prom_no, prom_name, prom_desc, prom_start_date, prom_end_date, currency_code, 
               status_type, event_no, create_date, create_user_id, approval_date, prom_type, last_updated_date)
   values     (p_prom_no, p_prom_name, 'Externally-Initiated '||p_prom_no, p_start_date, p_end_date, 'ZAR', 
               'M', p_event_no, g_date, 'DWH', p_appr_date, 'SK', g_date);

   g_recs_inserted := g_recs_inserted + sql%rowcount;
   
   l_text := 'INSERT TO fnd_prom '||to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,'',g_recs_inserted,'','','');
   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
      when invalid_event_no then
        l_message := 'event_no must consist of 6 characters ending with DWH';
        dwh_log.record_error(l_module_name,sqlcode,l_message);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                   l_process_type,dwh_constants.vc_log_aborted,'','','','','');
        p_success := false;
        raise;
      when not_enough_par then
        l_message := 'Not enough input parameters supplied.';
        dwh_log.record_error(l_module_name,sqlcode,l_message);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                   l_process_type,dwh_constants.vc_log_aborted,'','','','','');
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

end wh_fnd_corp_129u;
