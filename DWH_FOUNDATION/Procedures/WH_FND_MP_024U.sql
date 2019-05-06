--------------------------------------------------------
--  DDL for Procedure WH_FND_MP_024U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MP_024U" (p_forall_limit in integer , p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2015
--  Author:      Kgomotso Lehabe
--  Purpose:     Create  GOF, Cat Ind Dept Subclass Weekly  table in the foundation layer
--               with input ex staging table from MP.
--  Tables:      Input  - stg_mp_loc_subc_wk_cpy
--               Output - fnd_rtl_loc_subc_wk_mp
--  Packages:    dwh_constants, dwh_log, dwh_valid
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_hsp_read      integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_inserted_hsp  integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MP_024U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DAILY LOCATION PLAN EX MP';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Bulk load data into foundation
--**************************************************************************************************
procedure local_bulk_insert as
begin

      execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (subwk,4) */ into fnd_rtl_loc_subc_wk_mp subwk

        WITH STG_LOAD AS
          (
          SELECT   /*+ full (stg) parallel (stg, 4) */
            chain_no,
            department_no,
            class_no,
            subclass_no,
            location_no,
            fin_year_no,
            fin_week_no,
            nvl(dept_store_size_cluster, 0) dept_store_size_cluster,
            nvl(gof_code, 0) gof_code,
            nvl(catalog_ind,0) catalog_ind,
            nvl(grade_cat_code,0) grade_cat_code
          from stg_mp_loc_subc_wk_cpy


          )

      SELECT /*+ full (stg) parallel (stg,4) */
            chn.chain_no,
            dept.department_no,
            cls.class_no,
            subc.subclass_no,
            loc.location_no,
            stg.fin_year_no,
            stg.fin_week_no,
            nvl(deptcl.dept_store_cluster_code, -1) dept_store_size_cluster,
            nvl(gof.gof_code, -1) gof_code,
            nvl(cat.catalogue_ind, -1) catalogue_ind,
            nvl(grd.grade_cat_code, -1) grade_cat_code,
            g_date as last_updated_date

      from   STG_LOAD stg,
             fnd_chain chn,
             fnd_location loc,
             fnd_department dept,
             fnd_class  cls,
             fnd_subclass subc,
             fnd_grade_cat grd,
             fnd_mp_catalogue cat,
             fnd_goodness_of_fit gof,
             fnd_dept_loc_cluster deptcl
      where   stg.chain_no                = chn.chain_no (+)
      and   stg.location_no             = loc.location_no(+)
      and   stg.subclass_no             = subc.subclass_no(+)
      and   stg.class_no                = subc.class_no(+)
      and   stg.department_no           = subc.department_no(+)
      and   stg.class_no                = cls.class_no(+)
      and   stg.department_no           = cls.department_no(+)
      and   stg.department_no           = dept.department_no(+)
      and   stg.grade_cat_code          = grd.grade_cat_code (+)
      and   stg.catalog_ind             = cat.catalogue_ind(+)
      and   stg.gof_code                 = gof.gof_code (+)
      and   stg.dept_store_size_cluster = deptcl.dept_store_cluster_code (+)
-- load records with valid lookup codes for chain_no, location_no, department_no, class_no, subclass_no and plan_type_no
      and  (case when chn.chain_no       is not null then 1 else 0 end) > 0
      and  (case when loc.location_no    is not null then 1 else 0 end) > 0
      and  (case when dept.department_no is not null then 1 else 0 end) > 0
      and  (case when cls.class_no       is not null then 1 else 0 end) > 0
      and  (case when subc.subclass_no   is not null then 1 else 0 end) > 0;



      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


end local_bulk_insert;
--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************

procedure local_write_hospital as
begin

    execute immediate 'alter session enable parallel dml';

   insert /*+ APPEND parallel (hsp,4) */ into stg_mp_loc_subc_wk_hsp hsp
    select /*+ full (apln) parallel (stg,4) */
           stg.sys_source_batch_id,
           stg.sys_source_sequence_no,
           g_date,
           'Y',
           'DWH',
           stg.sys_middleware_batch_id,
           'INVALID LOOKUP CODE - CHAIN_NO/ DEPARTMENT_NO/ LOCATION_NO/ PLAN_TYPE_NO',
           stg.chain_no,
           stg.location_no,
           stg.department_no,
           stg.class_no,
           stg.subclass_no,
           stg.fin_year_no,
           stg.fin_week_no,
           stg.dept_store_size_cluster,
           stg.gof_code,
           stg.catalog_ind,
           stg.grade_cat_code
    from   stg_mp_loc_subc_wk_cpy stg,
             fnd_chain chn,
             fnd_location loc,
             fnd_department dept,
             fnd_class  cls,
             fnd_subclass subc,
             fnd_grade_cat grd,
             fnd_mp_catalogue cat,
             fnd_goodness_of_fit gof,
             fnd_dept_loc_cluster deptcl
      where stg.chain_no                = chn.chain_no (+)
      and   stg.location_no             = loc.location_no(+)
      and   stg.subclass_no             = subc.subclass_no(+)
      and   stg.class_no                = subc.class_no(+)
      and   stg.department_no           = subc.department_no(+)
      and   stg.class_no                = cls.class_no(+)
      and   stg.department_no           = cls.department_no(+)
      and   stg.department_no           = dept.department_no(+)
      and   stg.grade_cat_code               = grd.grade_cat_code (+)
      and   stg.catalog_ind             = cat.catalogue_ind(+)
      and   stg.gof_code               = gof.gof_code (+)
      and   stg.dept_store_size_cluster = deptcl.dept_store_cluster_code (+)
-- load records with invalid lookup codes for chain_no, location_no, department_no, and plan_type_no
      and  (case when chn.chain_no      is not null then 0 else 1 end) +
           (case when loc.location_no    is not null then 0 else 1 end) +
           (case when dept.department_no is not null then 0 else 1 end) +
           (case when cls.class_no       is not null then 0 else 1 end) +
           (case when subc.subclass_no   is not null then 0 else 1 end)  > 0 ;

       g_recs_hsp_read     := g_recs_hsp_read     + sql%rowcount;
       g_recs_inserted_hsp := g_recs_inserted_hsp + sql%rowcount;

           commit;
end local_write_hospital;

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
   l_text := 'LOAD OF FND_RTL_LOC_SUBC_WK_MP STARTED AT '||
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

    local_bulk_insert;
    local_write_hospital;

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

end WH_FND_MP_024U;
