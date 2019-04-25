--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_090U_FOOD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_090U_FOOD" 
(p_forall_limit in integer,
p_success out boolean)
as

--**************************************************************************************************
--  Date:        SEPTEMBER 2010
--  Author:      Wendy Lyttle
--  Purpose:     Create Bridgethorn extracts to flat files in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - vw_supp_item_wk_bridgethorn1
--                        vw_loc_item_wk_bridgethorn1
--                        vw_depot_item_wk_bridgethorn1
--                        vw_item_bridgethorn1
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_count              number        :=  0;
g_start_date               date    ;
g_end_date               date    ;
g_fin_day_no number :=0;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_090U_FOOD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT BRIDGETHORN DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT BRIDGETHORN DATA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
        SELECT fin_day_no
    into g_fin_day_no
    FROM dim_calendar
    WHERE calendar_date = g_date ;
    IF g_fin_day_no <> 7 THEN
    SELECT this_week_start_date-28,
      this_week_start_date     -1
    INTO g_start_date,
      g_end_date
    FROM dim_calendar
    WHERE calendar_date = g_date;
    ELSE
      SELECT this_week_start_date-21,
        g_date
     INTO g_start_date,
       g_end_date
      FROM dim_calendar
      WHERE calendar_date = g_date;
    END IF;

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from vw_extr_nielsens','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************
DBMS_OUTPUT.PUT_LINE('BEFORE supp');
    select count(*) into g_count
    from vw_supp_item_wk_bridgethorn1;
    if g_count > 0 then
    g_count := 0;
    g_count := dwh_generic_file_extract('select * from vw_supp_item_wk_bridgethorn1','|','DWH_FILES_OUT','WWSA_po_data.csv');
    end if;
    l_text :=  'Records extracted to brthn_supp_item_wk '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_OUTPUT.PUT_LINE('BEFORE loc');
    select count(*) into g_count
    from vw_loc_item_wk_bridgethorn1;
    if g_count > 0 then
    g_count := 0;
    g_count := dwh_generic_file_extract('select * from vw_loc_item_wk_bridgethorn1','|','DWH_FILES_OUT','WWSA_store_epos_stock.csv');
    end if;
    l_text :=  'Records extracted to brthn_loc_item_wk '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_OUTPUT.PUT_LINE('BEFORE depot');
    select count(*) into g_count
    from vw_depot_item_wk_bridgethorn1;
    if g_count > 0 then
    g_count := 0;
    g_count := dwh_generic_file_extract('select * from vw_depot_item_wk_bridgethorn1','|','DWH_FILES_OUT','WWSA_depot_stock.csv');
    end if;
    l_text :=  'Records extracted to brthn_depot_item_wk '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
DBMS_OUTPUT.PUT_LINE('BEFORE item');
    select count(*) into g_count
    from vw_item_bridgethorn1;
    if g_count > 0 then
    g_count := 0;
    g_count := dwh_generic_file_extract('select * from vw_item_bridgethorn1','|','DWH_FILES_OUT','WWSA_SKU_Master.csv');
    end if;
    l_text :=  'Records extracted to brthn_item '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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


END WH_PRF_BRTH_090U_FOOD;
