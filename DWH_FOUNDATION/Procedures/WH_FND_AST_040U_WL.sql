--------------------------------------------------------
--  DDL for Procedure WH_FND_AST_040U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_AST_040U_WL" 
(p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
---- FOR PRODLINK DATAFIX 2 AUG 2017 - POSTDATES = 3,10 JULY 2017
--**************************************************************************************************
--  Date:        aUGUST 2017
--  Author:      Wendy Lyttle
--  Purpose:     Create C&H catalog fact table in the foundation layer
--               ex staging table from RP.
--
--  FYI          Eventhough the catalog data from source comes in 1 day ahead of time,
--               all processing is done for post_date = g_date
--               ie. runs one day 'behind' received data
--
--               Cloned from WH_FND_RP_001U
--
--  Tables:      Input  - stg_ast_loc_item_dy_catlg_cpy
--               Output - wfnd_ast_loc_item_dy_catlg
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  oct/nov 2016 =- wendy add product-linking columns default values
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
g_sub                integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_AST_040U_WL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ASSORT DAILY CATALOG DATA';
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
   l_text := 'LOAD OF wfnd_ast_loc_item_dy_catlg STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
 g_date := '10 JULY 2017';
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      execute immediate 'alter session enable parallel dml';


      insert /*+ APPEND parallel (ast,2) */ into dwh_foundation.fnd_ast_loc_item_dy_catlg ast
      select /*+ full (AST) parallel (AST,2) */
              ast.location_no
              ,ast.item_no
              ,ast.post_date 
              ,ast.active_from_date
              ,ast.active_to_date
              ,ast.source_data_status_code
              ,g_date as last_updated_date
              , ast.item_no GROUP_ITEM_NO
              , 0 RECAT_IND
              , 1 PROD_LINK_IND
              , 'CU' PROD_LINK_TYPE              
      from   stg_ast_loc_item_dy_catlg_cpy ast,
             fnd_item fi,
             fnd_location fl,
             dim_calendar dc
      where  ast.item_no = fi.item_no
       and   ast.location_no = fl.location_no
       and   ast.post_date   = dc.calendar_date
       and  (case when ast.item_no     is not null then 1 else 0 end) > 0
       and  (case when ast.location_no is not null then 1 else 0 end) > 0
       and  (case when ast.post_date is not null then 1 else 0 end) > 0
   --    AND  SYS_SOURCE_BATCH_ID = 6223
    --   AND SYS_SOURCE_BATCH_ID = 1 AND SYS_SOURCE_SEQUENCE_NO = 106
       ;

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

end wh_fnd_ast_040u_WL;
