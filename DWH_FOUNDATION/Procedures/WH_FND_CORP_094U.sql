--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_094U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_094U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2017
--  Author:      Quentin Smit
--  Purpose:     De-catalogue all catalogued items daily by setting this_wk_catalog_ind = 0 to ensure
--               ODWH and Catman are in sync daily.  In the past Catman occasionally didn't send de-cat
--               records for items which meant they incorrectly remained catalogued indefinately which 
--               skewed business availablity.
--
--  Tables:      Input     - fnd_location_item
--               Output    - fnd_location_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
--g_hospital_text      stg_rms_item_uda_hsp.sys_process_msg%type;
--g_rec_out            fnd_item_uda%rowtype;
--g_rec_in             stg_rms_item_uda_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_094U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'DE-CATALOGUE OF ITEMS IN FND_LOCATION_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DE-CATALOGUE OF ITEMS IN FND_LOCATION_ITEM STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    
--    Code from WH_FND_CORP_082U that did the cleardown when OM was the source.  Can't quite use it as we don't know 
--    at this point whether the data has been copied from staging to staging copy.  Possible to do but don't think
--    it's worth the trouble - just de-catalogue everything that is currently catalogued!
--
--    update /*+ parallel(li, 6) */ FND_LOCATION_ITEM li
--      set    this_wk_catalog_ind = 0,
--             next_wk_catalog_ind = 0,
--             last_updated_date   = g_date
--      where  (this_wk_catalog_ind = 1  or next_wk_catalog_ind = 1) and
--           not exists
--           (
--             select /*+ parallel(stg, 6) */ 1
--             from   stg_catman_location_item_cpy stg
--             where  stg.location_no = li.location_no and
--                    stg.item_no     = li.item_no
--           )
--    --UPDATE /*+ PARALLEL(A,8) */ W6005682.FND_LOCATION_ITEM_TST A   --

    UPDATE /*+ PARALLEL(A,8) */ DWH_FOUNDATION.FND_LOCATION_ITEM A   
       SET THIS_WK_CATALOG_IND = 0, 
           NEXT_WK_CATALOG_IND = 0,
           LAST_UPDATED_DATE = G_DATE
     WHERE THIS_WK_CATALOG_IND = 1;

    g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
    
    COMMIT;
    
    
    
    l_text := 'DE-CATALOGUE OF ITEMS IN FND_LOCATION_ITEM COMPLETED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
       
end wh_fnd_corp_094u;
