--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_581U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_581U" 
                            (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        22 May 2018
--  Author:      Karna Nallamothu
--  Purpose:     Create Sales Forecast
--               with input ex staging table from  
--
--  Tables:      Input  - STG_FD_TRDGRP_FCST_RATIO_CPY
--               Output - fnd_area_wk_trdgrp_fcst_ratio
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
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_physical_updated   integer       :=  0;
g_rec_out            fnd_area_wk_trdgrp_fcst_ratio%rowtype;
g_date               date          := trunc(sysdate);

g_area_no            STG_FD_TRDGRP_FCST_RATIO_CPY.area_no%type;
g_fin_year_no        STG_FD_TRDGRP_FCST_RATIO_CPY.fin_year_no%type;
g_fin_week_no        STG_FD_TRDGRP_FCST_RATIO_CPY.fin_week_no%type;
g_fin_day_no         STG_FD_TRDGRP_FCST_RATIO_CPY.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_581U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STG_FD_TRDGRP_FCST_RATIO_CPY EX OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor stg_dup is
    select * 
    from stg_fd_trdgrp_fcst_ratio_cpy
    where (area_no,fin_year_no, fin_week_no, fin_day_no)                             
    in (
        select area_no,fin_year_no, fin_week_no, fin_day_no                             
        from stg_fd_trdgrp_fcst_ratio_cpy
        group by area_no,fin_year_no, fin_week_no, fin_day_no                           
        having count(*) > 1
        )
    order by area_no,fin_year_no,fin_week_no,fin_day_no,sys_source_batch_id desc ,sys_source_sequence_no desc ;  

cursor c_STG_FD_TRDGRP_FCST_RATIO is
select /*+ FULL(stg)  parallel (stg,2) */
            stg.area_no,
            stg.fin_year_no,
            stg.fin_week_no,
            stg.fin_day_no,
            stg.create_date,
            stg.forecast_sales,
            stg.forecast_ratio_perc,
            stg.financial_forecast,
            stg.fresh_forecast,
            stg.fresh_forecast_ratio_perc,
            stg.fresh_finance_forecast,
            stg.ll_forecast,
            stg.ll_forecast_ratio_perc,
            stg.ll_finance_forecast
      from  stg_fd_trdgrp_fcst_ratio_cpy stg,
            fnd_area_wk_trdgrp_fcst_ratio fnd
      where stg.area_no          = fnd.area_no      and
            stg.fin_year_no      = fnd.fin_year_no  and
            stg.fin_week_no      = fnd.fin_week_no  and
            stg.fin_day_no       = fnd.fin_day_no   and                         
            stg.sys_process_code = 'N'
      order by
              stg.area_no,stg.fin_year_no,stg.FIN_DAY_NO,stg.sys_source_batch_id,stg.sys_source_sequence_no ;   

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

   g_area_no            := 0;
   g_fin_year_no        := 0;
   g_fin_week_no        := 0;
   g_fin_day_no         := 0;                                                  

for dupp_record in stg_dup
   loop

    if  dupp_record.area_no           = g_area_no and
        dupp_record.fin_year_no       = g_fin_year_no  and
        dupp_record.fin_week_no       = g_fin_week_no and
        dupp_record.fin_day_no        = g_fin_day_no then                       
        update stg_fd_trdgrp_fcst_ratio_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;


    g_area_no            := dupp_record.area_no;
    g_fin_year_no        := dupp_record.fin_year_no;
    g_fin_week_no        := dupp_record.fin_week_no;
    g_fin_day_no         := dupp_record.fin_day_no;                             
   end loop; 

   commit;

   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end remove_duplicates;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

    g_rec_out.last_updated_date  := g_date;

      insert /*+ APPEND parallel(fnd,2)  */ into fnd_area_wk_trdgrp_fcst_ratio fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            cpy.area_no,
            cpy.fin_year_no,
            cpy.fin_week_no,
            cpy.fin_day_no,
            cpy.create_date,
            cpy.forecast_sales,
            cpy.forecast_ratio_perc,
            cpy.financial_forecast,
            cpy.fresh_forecast,
            cpy.fresh_forecast_ratio_perc,
            cpy.fresh_finance_forecast,
            cpy.ll_forecast,
            cpy.ll_forecast_ratio_perc,
            cpy.ll_finance_forecast,
            g_date as last_updated_date         
         from   stg_fd_trdgrp_fcst_ratio_cpy cpy,
             dim_calendar dc,
             fnd_area loc
       where cpy.area_no            = loc.area_no
       and   cpy.fin_year_no        = dc.fin_year_no
       and   cpy.fin_week_no        = dc.fin_week_no
       and   cpy.fin_day_no         = dc.fin_day_no                             
       and   not exists
      (select * from fnd_area_wk_trdgrp_fcst_ratio
       where  area_no   = cpy.area_no and
              fin_year_no       = cpy.fin_year_no and
              fin_week_no       = cpy.fin_week_no and
              fin_day_no        = cpy.fin_day_no)                           
 
       and sys_process_code = 'N'
       GROUP BY
            cpy.area_no,
            cpy.fin_year_no,
            cpy.fin_week_no,
            cpy.fin_day_no,
            cpy.create_date,
            cpy.forecast_sales,
            cpy.forecast_ratio_perc,
            cpy.financial_forecast,
            cpy.fresh_forecast,
            cpy.fresh_forecast_ratio_perc,
            cpy.fresh_finance_forecast,
            cpy.ll_forecast,
            cpy.ll_forecast_ratio_perc,
            cpy.ll_finance_forecast,
            g_date ;                                             
 
      g_recs_inserted := g_recs_inserted + sql%rowcount;

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

for upd_rec in c_stg_fd_trdgrp_fcst_ratio
   loop
     update fnd_area_wk_trdgrp_fcst_ratio fnd
     set    
--            fnd.area_no                   = upd_rec.area_no, 
--            fnd.fin_year_no               = upd_rec.fin_year_no, 
--            fnd.fin_week_no               = upd_rec.fin_week_no, 
            fnd.fin_day_no                = upd_rec.fin_day_no, 
            fnd.create_date               = upd_rec.create_date, 
            fnd.forecast_sales            = upd_rec.forecast_sales, 
            fnd.forecast_ratio_perc       = upd_rec.forecast_ratio_perc, 
            fnd.financial_forecast        = upd_rec.financial_forecast, 
            fnd.fresh_forecast            = upd_rec.fresh_forecast, 
            fnd.fresh_forecast_ratio_perc = upd_rec.fresh_forecast_ratio_perc, 
            fnd.fresh_finance_forecast    = upd_rec.fresh_finance_forecast, 
            fnd.ll_forecast               = upd_rec.ll_forecast, 
            fnd.ll_forecast_ratio_perc    = upd_rec.ll_forecast_ratio_perc, 
            fnd.ll_finance_forecast       = upd_rec.ll_finance_forecast, 
            fnd.last_updated_date         = g_date                     
     where  fnd.area_no     = upd_rec.area_no      and
            fnd.fin_year_no = upd_rec.fin_year_no  and
            fnd.fin_week_no = upd_rec.fin_week_no  and
            fnd.fin_day_no  = upd_rec.fin_day_no;

      g_recs_updated := g_recs_updated + 1;
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
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin

      insert /*+ APPEND parallel(hsp,2) */ into stg_fd_trdgrp_fcst_ratio_hsp hsp 
      select /*+ FULL(cpy)  parallel (cpy,2) */
            cpy.sys_source_batch_id,
            cpy.sys_source_sequence_no,
            sysdate,
            'Y',
            'DWH',
            cpy.sys_middleware_batch_id,
            'VALIDATION FAIL - REFERENCIAL ERROR',
            cpy.source_data_status_code,
            cpy.area_no,
            cpy.fin_year_no,
            cpy.fin_week_no,
            cpy.fin_day_no,
            cpy.create_date,
            cpy.forecast_sales,
            cpy.forecast_ratio_perc,
            cpy.financial_forecast,
            cpy.fresh_forecast,
            cpy.fresh_forecast_ratio_perc,
            cpy.fresh_finance_forecast,
            cpy.ll_forecast,
            cpy.ll_forecast_ratio_perc,
            cpy.ll_finance_forecast       
      from   stg_fd_trdgrp_fcst_ratio_cpy cpy
      where
      (
      not exists
        (select * from dim_calendar dc
         where  cpy.fin_year_no       = dc.fin_year_no
         and cpy.fin_week_no          = dc.fin_week_no
         and cpy.fin_day_no           = dc.fin_day_no ) or                      
      not exists
        (select * from fnd_area loc
         where  cpy.area_no       = loc.area_no )
      )
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---
      and sys_process_code = 'N';

g_recs_hospital := g_recs_hospital + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;



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


    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;

    select count(*)
    into   g_recs_read
    from   stg_fd_trdgrp_fcst_ratio_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_hospital;

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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

END WH_FND_CORP_581U;
