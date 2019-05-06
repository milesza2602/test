--------------------------------------------------------
--  DDL for Procedure WH_PRF_MP_023U_ADHOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MP_023U_ADHOC" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2015
--  Author:      K. Lehabe
--  Purpose:     Load Loc Fin/Forecast from foundation level.
--  Tables:      Input  - fnd_rtl_loc_dept_mth_plan_mp
--               Output - rtl_loc_dept_mth_plan_mp
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
g_recs_inserted      integer       :=  0;
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;


L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MP_023U_ADHOC';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_script_rtl_prf_mp;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE MP MONTLHY LOC FIN/FCST  FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
   l_text := 'LOAD OF RTL_LOC_DEPT_MTH_PLAN_MP STARTED AT '||
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

      insert /*+ APPEND parallel (mp,2) */ into rtl_loc_dept_mth_plan_mp mp
      SELECT /*+ full (fnd) parallel (fnd,2) */
            loc.sk1_location_no,
            dept.sk1_department_no,
             fnd.fin_year_no,
            fnd.fin_month_no,
            pln.sk1_plan_type_no,
            fnd.gof_code,
            fnd.catalogue_ind,
            fnd.sales,
            fnd.tpm,
            fnd.customer_class_code,
            fnd.store_size_cluster,
            fnd.dept_store_size_cluster,
            fnd.pln_trading_mtr,
            fnd.pln_req_merch_mtr,
            fnd.pln_act_merch_mtr,
            fnd.pln_disp_prop_factor,
            fnd.pln_tot_facings,
            g_date as last_updated_date,
            'M'||fnd.fin_year_no||fnd.fin_month_no as fin_month_code
      from   dwh_foundation.fnd_rtl_loc_dept_mth_plan_mp fnd,
             DIM_LOCATION LOC,
             DIM_DEPARTMENT DEPT,
             DIM_PLAN_TYPE PLN
      where  FND.FIN_YEAR_NO = 2015
       and   fnd.fin_month_no = 12
       and  fnd.location_no      = loc.location_no
       and  FND.DEPARTMENT_NO    = DEPT.DEPARTMENT_NO
       and  FND.PLAN_TYPE_NO      = PLN.PLAN_TYPE_NO
       and fnd.last_updated_date = g_date;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

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

end WH_PRF_MP_023U_ADHOC;
