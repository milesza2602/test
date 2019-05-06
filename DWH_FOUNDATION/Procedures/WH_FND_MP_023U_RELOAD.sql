--------------------------------------------------------
--  DDL for Procedure WH_FND_MP_023U_RELOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MP_023U_RELOAD" (p_forall_limit in integer , p_success out boolean) as

--**************************************************************************************************
-- reclass of CGM - May 2017 - RELOAD OF TARGET TABLE
--**************************************************************************************************
--  Date:        June 2015
--  Author:      Kgomotso Lehabe
--  Purpose:     Create daily Location Plan (dept/loc) table in the foundation layer
--               with input ex staging table from MP.
--  Tables:      Input  - stg_mp_chain_dept_mth_plan_cpy
--               Output - fnd_rtl_loc_dept_mth_plan_mp
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  1. CREATE TABLE dwh_foundation.STG_MP_CHNDPTMTHPLAN_CPY170517
--       tablespace FND_MASTER as
--      SELECT * FROM dwh_foundation.STG_MP_CHAIN_dept_MTH_PLAN_cpy
-- 2017	2	816945
-- 2018	12	3921336
-- 2019	12	3921336
--
--  2. TRUNCATE TABLE dwh_foundation.STG_MP_CHAIN_dept_MTH_PLAN_cpy
--
--  3. INSERT /*+ APPEND */ INTO  dwh_foundation.STG_MP_CHAIN_dept_MTH_PLAN_cpy
--        SELECT * FROM dwh_foundation.STG_MP_CHAIN_dept_MTH_PLAN
--      3,787,318 rows inserted
--
--  4. select /*+ FULL(A) */ FIN_YEAR_NO, COUNT(DISTINCT FIN_MONTH_NO), count(*) 
--        from dwh_foundation.STG_MP_CHAIN_dept_MTH_PLAN_cpy A
--     GROUP BY FIN_YEAR_NO
--     ORDER BY FIN_YEAR_NO
--     2016	1	  329332
--     2017	10	3457986
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
g_recs_hsp_read      integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_inserted_hsp  integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_RECS        number        :=  0;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MP_023U_RELOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DAILY LOCATION PLAN EX MP';
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
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
   l_text := 'LOAD OF FND_RTL_LOC_DEPT_MTH_PLAN_MP STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

         execute immediate 'alter session enable parallel dml';
        g_recs := 0;

 --   EXECUTE IMMEDIATE('TRUNCATE TABLE  DWH_FOUNDATION.FND_RTL_LOC_DEPT_MTH_PLAN_MP');
 --      l_text := 'Truncate DWH_FOUNDATION.FND_RTL_LOC_DEPT_MTH_PLAN_MP';
  --     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

        g_recs  := 0;
       select count(*) into g_recs from dwh_foundation.stg_mp_chain_dept_mth_plan_cpy;
       g_recs_read := g_recs;
       l_text := 'recs read = '|| g_recs;
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        g_recs  := 0;

--**************************************************************************************************
-- Bulk load data into foundation
--**************************************************************************************************
       g_recs_inserted := 0;

      insert /*+ APPEND parallel (finfct,4) */ into fnd_rtl_loc_dept_mth_plan_mp finfct

       WITH STG_LOAD AS
          (
          SELECT   /*+ full (stg) parallel (stg, 4) */
              chain_no,
              location_no,
              department_no,
              plan_type_no,
              fin_year_no,
              fin_month_no,
              nvl(goodness_of_fit, 0) goodness_of_fit,
              nvl(catalog_ind, 0) catalog_ind,
              sales,
              tpm,
              nvl(store_cluster , 0) store_cluster,
              nvl(store_size_cluster, 0) store_size_cluster,
              nvl(dept_store_size_cluster, 0) dept_store_size_cluster,
              pln_trading_mtr,
              pln_req_merch_mtr,
              pln_act_merch_mtr,
              pln_disp_prop_factor,
              pln_tot_facings
             from stg_mp_chain_dept_mth_plan_cpy
--             WHERE NOT (fin_year_no = 2017 AND fin_month_no = 3) --DO NOT PROCESS ILLEGAL DATA FIX

          )

      select /*+ full (stg) parallel (stg,4) */
            chn.chain_no,
            loc.location_no,
            dept.department_no,
             pln.plan_type_no,
            stg.fin_year_no,
            stg.fin_month_no,
            nvl(gof.gof_code, -1) gof_code,
            nvl(cat.catalogue_ind, -1) catalogue_ind,
            stg.sales,
            stg.tpm,
            nvl(cls.customer_class_code , -1) customer_class_code,
            nvl(strcl.store_cluster_code, -1) store_cluster_code,
            nvl(deptcl.dept_store_cluster_code, -1) dept_store_cluster_code,
            stg.pln_trading_mtr,
            stg.pln_req_merch_mtr,
            stg.pln_act_merch_mtr,
            stg.pln_disp_prop_factor,
            stg.pln_tot_facings,
            g_date as last_updated_date

      from   STG_LOAD stg,
             fnd_chain chn,
             fnd_location loc,
             fnd_department dept,
             fnd_plan_type pln,
             fnd_goodness_of_fit gof,
             fnd_mp_catalogue cat,
             fnd_cust_class cls,
             fnd_location_cluster strcl,
             fnd_dept_loc_cluster deptcl
      where stg.chain_no                = chn.chain_no (+)
      and   stg.location_no             = loc.location_no(+)
      and   stg.department_no           = dept.department_no(+)
      and   stg.plan_type_no            = pln.plan_type_no(+)
      and   stg.goodness_of_fit         = gof.gof_code (+)
      and   stg.catalog_ind             = cat.catalogue_ind(+)
      and   stg.store_cluster           = cls.customer_class_code (+)
      and   stg.store_size_cluster      = strcl.store_cluster_code (+)
      and   stg.dept_store_size_cluster = deptcl.dept_store_cluster_code (+)
-- load records with valid lookup codes for chain_no, location_no, department_no, and plan_type_no
      and  (case when chn.chain_no       is not null then 1 else 0 end) > 0
      and  (case when loc.location_no    is not null then 1 else 0 end) > 0
      and  (case when dept.department_no  is not null then 1 else 0 end) > 0
      and  (case when pln.plan_type_no   is not null then 1 else 0 end) > 0;

      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

       l_text := 'recs INSERT to FND = '|| g_recs_inserted;
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);


--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
 
 g_recs_inserted_hsp  := 0;


 insert /*+ APPEND parallel (hsp,4) */ into stg_mp_chain_dept_mth_plan_hsp hsp
    select /*+ full (apln) parallel (stg,4) */
           stg.sys_source_batch_id,
           stg.sys_source_sequence_no,
           g_date,
           'Y',
           'DWH',
           stg.sys_middleware_batch_id,
           'INVALID LOOKUP CODE - CHAIN_NO/ DEPARTMENT_NO/ LOCATION_NO/ PLAN_TYPE_NO',
           stg.source_data_status_code,
           stg.chain_no,
           stg.department_no,
           stg.location_no,
           stg.plan_type_no,
           stg.fin_year_no,
           stg.fin_month_no,
           stg.goodness_of_fit,
           stg.catalog_ind,
           stg.sales,
           stg.tpm,
           stg.store_cluster,
           stg.store_size_cluster,
           stg.dept_store_size_cluster,
           stg.pln_trading_mtr,
           stg.pln_req_merch_mtr,
           stg.pln_act_merch_mtr,
           stg.pln_disp_prop_factor,
           stg.pln_tot_facings
    from   stg_mp_chain_dept_mth_plan_cpy stg,
             fnd_chain chn,
             fnd_location loc,
             fnd_department dept,
             fnd_plan_type pln,
             fnd_goodness_of_fit gof,
             fnd_mp_catalogue cat,
             fnd_cust_class cls,
             fnd_location_cluster strcl,
             fnd_dept_loc_cluster deptcl
      where  stg.chain_no             = chn.chain_no(+)
      and    stg.location_no               = loc.location_no(+)
       and   stg.department_no           = dept.department_no(+)
       and   stg.plan_type_no              = pln.plan_type_no(+)
       and   stg.goodness_of_fit          = gof.gof_code (+)
       and   stg.catalog_ind               = cat.catalogue_ind (+)
       and   stg.store_cluster           = cls.customer_class_code (+)
       and   stg.store_size_cluster       = strcl.store_cluster_code (+)
       and   stg.dept_store_size_cluster   = deptcl.dept_store_cluster_code (+)
-- load records with invalid lookup codes for chain_no, location_no, department_no, and plan_type_no
      and  (case when chn.chain_no      is not null then 0 else 1 end) +
          (case when loc.location_no    is not null then 0 else 1 end)  +
          (case when dept.department_no is not null then 0 else 1 end)  +
          (case when pln.plan_type_no   is not null then 0 else 1 end) > 0 ;

       g_recs_inserted_hsp := g_recs_inserted_hsp + sql%rowcount;

           commit;

       l_text := 'recs INSERT to HSP = '|| g_recs_inserted_hsp;
       dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

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

end WH_FND_MP_023U_RELOAD;
