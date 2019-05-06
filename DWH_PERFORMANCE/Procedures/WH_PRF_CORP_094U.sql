--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_094U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_094U" (p_forall_limit in integer,p_success out boolean) as

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Release 8 Oct 2008 determined that the table created by this program is not available or required
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

--**************************************************************************************************
--  Date:        September 2008
--  Author:      Alastair de Wet
--  Purpose:     Update dim_item and item hist dimention table in the performance layer
--               with dim_lev1 and lev1_diff1 sk data ex performance  table.
--  Tables:      Input  - dim_item, lev1 and lev1_diff1,
--               Output - dim_item and dim_item_hist
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_updated_h     integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_count              integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_094U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Update dim_item EX dim_lev1 and lev1_diff1 sk data';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dim_item EX dim_item STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************

    --============================= DIM_ITEM ===================================
    l_text := 'STARTING DIM_ITEM UPDATE ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
    MERGE /*+ PARALLEL(di_mart,4) */ INTO DIM_ITEM  di_mart 
     USING (   
       select /*+ full(di) */ di.item_no,
              dld.sk1_style_colour_no,
              dld.sk1_style_no,
              di.last_updated_date
       from   dim_item di,
              dim_lev1_diff1 dld
       where  di.style_colour_no = dld.style_colour_no and
              di.style_colour_no is not null
          
          ) mer_mart
  
  on (mer_mart.item_no = di_mart.item_no)
          
  when matched then 
    update set  sk1_style_no           = mer_mart.sk1_style_no,
                sk1_style_colour_no    = mer_mart.sk1_style_colour_no,
                last_updated_date      = g_date
    ;      
    
    g_recs_updated := g_recs_updated + SQL%ROWCOUNT;      
    
    COMMIT;
    
    l_text := 'DONE DIM_ITEM UPDATE ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);      
    
    
    --========================= DIM_ITEM_HIST ================================
          
    l_text := 'STARTING DIM_ITEM_HIST UPDATE ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
    MERGE /*+ PARALLEL(di_mart,4) */ INTO DIM_ITEM_HIST  di_mart 
     USING (   
       select /*+ full(di) */ di.item_no,
              dld.sk1_style_colour_no,
              dld.sk1_style_no,
              di.last_updated_date
       from   dim_item di,
              dim_lev1_diff1 dld
       where  di.style_colour_no = dld.style_colour_no and
              di.style_colour_no is not null
          
          ) mer_mart
  
  on (mer_mart.item_no = di_mart.item_no)
          
  when matched then 
    update set  sk1_style_no           = mer_mart.sk1_style_no,
                sk1_style_colour_no    = mer_mart.sk1_style_colour_no,
                last_updated_date      = g_date
    ;      
    
    g_recs_updated_h := g_recs_updated_h + SQL%ROWCOUNT;      
    
    COMMIT;  
    
    l_text := 'DONE DIM_ITEM_HIST UPDATE ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);      
    
    g_recs_read := g_recs_updated_h;
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||'- hist '||g_recs_updated_h;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
end wh_prf_corp_094u;
