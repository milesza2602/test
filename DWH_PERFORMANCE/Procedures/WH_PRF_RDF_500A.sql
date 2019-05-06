--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_500A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_500A" 
(p_forall_limit in integer,p_success out boolean) 
as
--**************************************************************************************************
--  Date:        August 2010
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast LEVEL 1(LOCATION LEVEL)table in performance layer
--               with input ex RDF forecast level 1 table from foundation layer.
--  Tables:      Input  - FND_LOC_ITEM_RDF_DYFCST_L1
--               Output - DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1
--  Packages:    constants, dwh_log, dwh_valid
----------------------------PREV VERSION------------------------------------------------------------------------
--  Maintenance
--  04 May 2009: TD-1143 - check for data duplication to prevent unique constraint as this program is insert only
--
--  16 May 2011: Do stats gathering on temp_loc_item_dy_rdf_sysfcstl1 table before exiting the program.
--
--
--------------------------------NEW VERSION--------------------------------------------------------------------
--  Maintenance:
--  qc4340 - W LYTTLE: RDF Rollup of LEVEL 1(LOCATION LEVEL)
      --                        - This procedure was copied from WH_PRF_RDF_002A in PRD
      --                        - was from fnd_rtl_li_wk_rdf_dyfcst_l1	  AND  temp_loc_item_dy_rdf_sysfcstl1
      --                          now from FND_RTL_LC_ITM_WK_RDF_WKFS_L1	AND  TEMP_LOC_ITM_DY_RDF_SYSFCST_L1

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
g_rec_out            DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_500A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := '(NEW) TEMP LOAD RDF DAILY FCST LEVEL 1 FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1%rowtype index by binary_integer;
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
    l_text := 'LOAD DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1 EX FOUNDATION STARTED '||
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

    execute immediate 'truncate table DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1';
    l_text := 'TABLE DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1 TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

/*  TD-1143
    This check will determine whether multiple loads are present on the foundation table (FND_LOC_ITEM_RDF_DYFCST_L1)
    Should there be duplicate item/location combinations then a lookup will be done on the temp table
    prior to data population (temp_loc_item_dy_rdf_sysfcst)
*/
    select count(*)
    into g_fnd_count
    from
      (select count(*), item_no, location_no
         from FND_LOC_ITEM_RDF_DYFCST_L1
        where last_updated_date = g_date
       group by item_no, location_no
       having count(*) > 1);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

l_text := 'TURN LOGGING OFF ON TEMP_LOC_ITM_DY_RDF_SYSFCST_L1  STARTED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
execute immediate 'alter table DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1 nologging';
l_text := 'TURN LOGGING OFF ON TEMP_LOC_ITM_DY_RDF_SYSFCST_L1  ENDED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
INSERT /*+  APPEND */ INTO DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1 
        select /*+ parallel(fnd,4) */ 
                location_no,
                item_no,
                (post_date + to_number(substr(syscol,4,2)) -1) post_date_orig,
                --syscol,
                sysfcst,
                g_date,
                post_date,
                appfcst,
                0 static_wk_dly_app_fcst_qty
         from   DWH_FOUNDATION.FND_LOC_ITEM_RDF_DYFCST_L1 fnd
             unpivot include nulls ((sysfcst,appfcst) for syscol in (
                                                           (dy_01_sys_fcst_qty,dy_01_app_fcst_qty),
                                                           (dy_02_sys_fcst_qty,dy_02_app_fcst_qty),
                                                           (dy_03_sys_fcst_qty,dy_03_app_fcst_qty),
                                                           (dy_04_sys_fcst_qty,dy_04_app_fcst_qty),
                                                           (dy_05_sys_fcst_qty,dy_05_app_fcst_qty),
                                                           (dy_06_sys_fcst_qty,dy_06_app_fcst_qty),
                                                           (dy_07_sys_fcst_qty,dy_07_app_fcst_qty),
                                                           (dy_08_sys_fcst_qty,dy_08_app_fcst_qty),
                                                           (dy_09_sys_fcst_qty,dy_09_app_fcst_qty),
                                                           (dy_10_sys_fcst_qty,dy_10_app_fcst_qty),
                                                           (dy_11_sys_fcst_qty,dy_11_app_fcst_qty),
                                                           (dy_12_sys_fcst_qty,dy_12_app_fcst_qty),
                                                           (dy_13_sys_fcst_qty,dy_13_app_fcst_qty),
                                                           (dy_14_sys_fcst_qty,dy_14_app_fcst_qty),
                                                           (dy_15_sys_fcst_qty,dy_15_app_fcst_qty),
                                                           (dy_16_sys_fcst_qty,dy_16_app_fcst_qty),
                                                           (dy_17_sys_fcst_qty,dy_17_app_fcst_qty),
                                                           (dy_18_sys_fcst_qty,dy_18_app_fcst_qty),
                                                           (dy_19_sys_fcst_qty,dy_19_app_fcst_qty),
                                                           (dy_20_sys_fcst_qty,dy_20_app_fcst_qty),
                                                           (dy_21_sys_fcst_qty,dy_21_app_fcst_qty)))
         where  last_updated_date             = g_date
         and    to_number(substr(syscol,4,2)) > (select (case when today_fin_day_no  = 7 then 0
                                                                          else today_fin_day_no
                                                                     end) today_fin_day_no
                                                             from dim_control)
         order by location_no, item_no, post_date desc; /* TD-1143 */

            
  g_recs_read := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;
--end loop;

--**********************************************************************************************
-- Do stats on temp_loc_item_dy_rdf_sysfcstl1 table before exiting the program.
--**********************************************************************************************
    l_text := 'LOAD OF UPDATE STATS STARTED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','TEMP_LOC_ITM_DY_RDF_SYSFCST_L1', DEGREE => 32);
    
    l_text := 'LOAD OF UPDATE STATS ENDED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
   l_text := 'TURN LOGGING ON ON TEMP_LOC_ITM_DY_RDF_SYSFCST_L1  STARTED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   execute immediate 'alter table DWH_PERFORMANCE.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1 nologging';
   
   l_text := 'TURN LOGGING ON ON TEMP_LOC_ITM_DY_RDF_SYSFCST_L1  ENDED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
       raise;


END WH_PRF_RDF_500A;
