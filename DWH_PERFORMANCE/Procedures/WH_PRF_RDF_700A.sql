--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_700A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_700A" 
(p_forall_limit in integer,p_success out boolean) 
as
--**************************************************************************************************
--  Date:        March 2008
--  Author:      Alfonso Joshua
--  Purpose:     Load weekly forecast LEVEL 1(LOCATION LEVEL) table in performance layer
--               with input ex RDF forecast level 1 table from foundation layer.
--  Tables:      Input  - FND_LOC_ITEM_RDF_WKFCST_L1
--               Output - TEMP_LOC_ITM_WK_RDF_SYSFCST_L1
--  Packages:    constants, dwh_log, dwh_valid
----------------------------PREV VERSION------------------------------------------------------------------------
--  Maintenance
--  04 May 2009: TD-1143 - check for data duplication to prevent unique constraint as this program is insert only

--------------------------------NEW VERSION--------------------------------------------------------------------
--  Maintenance:
--  qc4340 - W LYTTLE: RDF Rollup of LEVEL 1(LOCATION LEVEL) data
      --                        - This procedure was copied from WH_PRF_RDF_005A in PRD
      --                        - was from fnd_rtl_loc_item_wk_rdf_wkfcst	temp_loc_item_wk_rdf_sysfcst
      --                          now from FND_RDF_LOC_ITM_WK_L1	        TEMP_LOC_ITM_WK_RDF_SYSFCST_L1
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fnd_count          number        :=  0;
g_today_fin_day_no   number        :=  0;
g_fd_num_catlg_days  number        :=  0;
g_rec_out            TEMP_LOC_ITM_WK_RDF_SYSFCST_L1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_max_week           number        :=  0;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_700A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := '(NEW)LOAD RDF WEEKLY FCST LEVEL 1 FACTS EX FND TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of TEMP_LOC_ITM_WK_RDF_SYSFCST_L1%rowtype index by binary_integer;
type tbl_array_u is table of TEMP_LOC_ITM_WK_RDF_SYSFCST_L1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


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
    l_text := 'LOAD TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 EX FOUNDATION STARTED '||
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

    execute immediate 'truncate table dwh_performance.TEMP_LOC_ITM_WK_RDF_SYSFCST_L1';
    l_text := 'TABLE TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

/*  TD-1143
    This check will determine whether multiple loads are present on the foundation table (fnd_rtl_loc_item_wk_rdf_dyfcst)
    Should there be duplicate item/location combinations then a lookup will be done on the temp table
    prior to data population (temp_loc_item_dy_rdf_sysfcst)
*/
    select count(*)
    into g_fnd_count
    from
      (select count(*), item_no, location_no
       from fnd_rtl_loc_item_wk_rdf_dyfcst
       where last_updated_date = g_date
       group by item_no, location_no
       having count(*) > 1);


   select max(fin_week_no) + 1
    into   g_max_week
    from   dim_calendar
    where  fin_year_no =
           (
           select fin_year_no
           from   dim_calendar
           where  calendar_date = g_date
           );

    l_text := 'No of weeks in the Fin_year + 1 for calculation in the code  - ' ||g_max_week;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

execute immediate 'alter table dwh_performance.TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 nologging';
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
INSERT /*+  APPEND */ INTO DWH_PERFORMANCE.TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 
       select /*+ parallel(fnd,4) */ 
               location_no,
               item_no,
               (case when (fin_week_no + to_number(substr(syscol,4,2))) > g_max_week
                     then fin_week_no + (to_number(substr(syscol,4,2))- g_max_week)
                     else fin_week_no + (to_number(substr(syscol,4,2))-1)
                end) fin_week_no_calc,    --fin_week_no
               fin_week_no,               --fin_week_no_orig
               (case when (fin_week_no + to_number(substr(syscol,4,2))) > g_max_week
                     then fin_year_no + 1
                   else fin_year_no
                end) fin_year_no_calc,    --fin_year_no
               sysfcst,
               g_date,
               appfcst
        from   DWH_FOUNDATION.FND_LOC_ITEM_RDF_WKFCST_L1 fnd
               unpivot include nulls ((sysfcst,appfcst) for syscol in (
                                                            (wk_01_sys_fcst_qty,wk_01_app_fcst_qty),
                                                            (wk_02_sys_fcst_qty,wk_02_app_fcst_qty),
                                                            (wk_03_sys_fcst_qty,wk_03_app_fcst_qty),
                                                            (wk_04_sys_fcst_qty,wk_04_app_fcst_qty),
                                                            (wk_05_sys_fcst_qty,wk_05_app_fcst_qty),
                                                            (wk_06_sys_fcst_qty,wk_06_app_fcst_qty),
                                                            (wk_07_sys_fcst_qty,wk_07_app_fcst_qty),
                                                            (wk_08_sys_fcst_qty,wk_08_app_fcst_qty),
                                                            (wk_09_sys_fcst_qty,wk_09_app_fcst_qty),
                                                            (wk_10_sys_fcst_qty,wk_10_app_fcst_qty),
                                                            (wk_11_sys_fcst_qty,wk_11_app_fcst_qty),
                                                            (wk_12_sys_fcst_qty,wk_12_app_fcst_qty),
                                                            (wk_13_sys_fcst_qty,wk_13_app_fcst_qty)
                                                             ))
        where  last_updated_date = g_date --and
--               to_number(substr(syscol,4,2)) >= fin_week_no and
--               to_number(substr(syscol,4,2)) <= fin_week_no + 12
        order by location_no, item_no, fin_year_no, fin_week_no desc;

            
  g_recs_read := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;
--end loop;
  
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
       raise;

END WH_PRF_RDF_700A;
