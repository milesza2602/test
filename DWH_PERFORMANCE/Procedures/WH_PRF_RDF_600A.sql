--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_600A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_600A" 
(p_forall_limit in integer,p_success out boolean) 
as
-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-03-18 
-- Author: Paul Wakefield
-- Altered hints in INSERT
-- **************************************************************************************************

--**************************************************************************************************
--  Date:        March 2008
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast LEVEL 2(DEPARTMENT LEVEL) table in performance layer
--               with input ex RDF forecast level 1 table from foundation layer.
--  Tables:      Input  - FND_LOC_ITEM_RDF_DYFCST_L2
--               Output - TEMP_LOC_ITM_DY_RDF_SYSFCST_L2
--  Packages:    constants, dwh_log, dwh_valid
----------------------------PREV VERSION------------------------------------------------------------------------
--  Maintenance
--  04 May 2009: TD-1143 - check for data duplication to prevent unique constraint as this program is insert only
--  23 Aug 2011:QC4328 - add read to DIM_ITEM and DIM_LOCATION and hence extra fields
--                        to temp_loc_item_dy_rdf_sysfcst
--
--------------------------------NEW VERSION--------------------------------------------------------------------
--  Maintenance:
--  qc4340 - W LYTTLE: RDF Rollup of LEVEL 2(DEPARTMENT LEVEL) data (TAKEN OVER BY Q.SMIT)
      --                        - This procedure was copied from WH_PRF_RDF_001A in PRD
      --                        - was from FND_LOC_ITEM_RDF_DYFCST_L2  AND   temp_loc_item_dy_rdf_sysfcst
      --                          now from FND_LOC_ITEM_RDF_DYFCST_L2	 AND   TEMP_LOC_ITM_DY_RDF_SYSFCST_L2
--                  VAT Change
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
g_rec_out            TEMP_LOC_ITM_DY_RDF_SYSFCST_L2%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_600A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := '(NEW)LOAD RDF DAILY FCST LEVEL 2 FACTS EX TEMP TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of TEMP_LOC_ITM_DY_RDF_SYSFCST_L2%rowtype index by binary_integer;
type tbl_array_u is table of TEMP_LOC_ITM_DY_RDF_SYSFCST_L2%rowtype index by binary_integer;
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
    l_text := 'LOAD TEMP_LOC_ITM_DY_RDF_SYSFCST_L2 EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '29/JAN/18';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_performance.TEMP_LOC_ITM_DY_RDF_SYSFCST_L2';
    l_text := 'TABLE TEMP_LOC_ITM_DY_RDF_SYSFCST_L2 TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

/*  TD-1143
    This check will determine whether multiple loads are present on the foundation table (FND_LOC_ITEM_RDF_DYFCST_L2)
    Should there be duplicate item/location combinations then a lookup will be done on the temp table
    prior to data population (temp_loc_item_dy_rdf_sysfcst)

    select count(*)
    into g_fnd_count
    from
      (select count(*), item_no, location_no
       from FND_LOC_ITEM_RDF_DYFCST_L2
       where last_updated_date = g_date
       group by item_no, location_no
       having count(*) > 1);

/*for g_sub in 0..5 loop
    select fin_day_no, this_week_start_date, this_week_end_date 
    into   g_fin_day_no, g_start_date, g_end_date
    from   dim_calendar 
    where  calendar_date = g_date - (g_sub * 7);
    
--    if g_fin_day_no = 6 then
--       g_start_date := g_start_date - 35;
--    else
--       g_start_date := g_start_date - 7;
--    end if;
    
--    g_start_date := g_start_date - 35;

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

--execute immediate 'alter table dwh_performance.TEMP_LOC_ITM_DY_RDF_SYSFCST_L2 nologging';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
INSERT /*+ APPEND parallel(L2,8) */ INTO dwh_performance.TEMP_LOC_ITM_DY_RDF_SYSFCST_L2 L2
SELECT location_no,
  item_no,
  (post_date + to_number(SUBSTR(syscol,4,2)) -1) post_date_calc,
  sysfcst,
  g_date, 
  post_date,
  appfcst,
  sk1_location_no,
  sk1_item_no,
  standard_uom_code,
  random_mass_ind ,
  vat_rate_perc,
  static_mass,
  wh_fd_zone_group_no,
  wh_fd_zone_no ,
  reg_rsp,
  static_appfcst
FROM
  (SELECT /*+ parallel(zi,8) */ fr.*,
    di.sk1_item_no,
    dl.sk1_location_no,
    standard_uom_code,
    random_mass_ind ,
    --vat_rate_perc,
    NVL(fi_vr.vat_rate_perc, (case when dl.vat_region_no = 1000 then di.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end ) )  vat_rate_perc,    --VAT rate change
    static_mass,
    wh_fd_zone_group_no,
    wh_fd_zone_no ,
    reg_rsp
    
  FROM FND_LOC_ITEM_RDF_DYFCST_L2 fr
  join  dim_item di      on fr.item_no                        = di.item_no
  join  dim_location dl  on fr.location_no                    = dl.location_no
  join  fnd_zone_item zi on dl.wh_fd_zone_group_no            = zi.zone_group_no
                        and dl.wh_fd_zone_no                  = zi.zone_no
                        and fr.item_no                        = zi.item_no
  
  left outer join FND_ITEM_VAT_RATE fi_vr on fr.item_no       = fi_vr.item_no                                     -- VAT rate change
                                         and dl.vat_region_no = fi_vr.vat_region_no                               -- VAT rate change
                                         and fr.post_date between fi_vr.active_from_date and fi_vr.active_to_date -- VAT rate change

  where  fr.last_updated_date                                  = g_date
  
  ) fr
  unpivot include nulls ((sysfcst,appfcst,static_appfcst) FOR syscol IN ( (dy_01_sys_fcst_qty,dy_01_app_fcst_qty,dy_01_STATIC_APP_FCST_QTY),
  (dy_02_sys_fcst_qty,dy_02_app_fcst_qty,dy_02_STATIC_APP_FCST_QTY), (dy_03_sys_fcst_qty,dy_03_app_fcst_qty,dy_03_STATIC_APP_FCST_QTY),
  (dy_04_sys_fcst_qty,dy_04_app_fcst_qty,dy_04_STATIC_APP_FCST_QTY), (dy_05_sys_fcst_qty,dy_05_app_fcst_qty,dy_05_STATIC_APP_FCST_QTY),
  (dy_06_sys_fcst_qty,dy_06_app_fcst_qty,dy_06_STATIC_APP_FCST_QTY), (dy_07_sys_fcst_qty,dy_07_app_fcst_qty,dy_07_STATIC_APP_FCST_QTY),
  (dy_08_sys_fcst_qty,dy_08_app_fcst_qty,dy_08_STATIC_APP_FCST_QTY), (dy_09_sys_fcst_qty,dy_09_app_fcst_qty,dy_09_STATIC_APP_FCST_QTY),
  (dy_10_sys_fcst_qty,dy_10_app_fcst_qty,dy_10_STATIC_APP_FCST_QTY), (dy_11_sys_fcst_qty,dy_11_app_fcst_qty,dy_11_STATIC_APP_FCST_QTY),
  (dy_12_sys_fcst_qty,dy_12_app_fcst_qty,dy_12_STATIC_APP_FCST_QTY), (dy_13_sys_fcst_qty,dy_13_app_fcst_qty,dy_13_STATIC_APP_FCST_QTY),
  (dy_14_sys_fcst_qty,dy_14_app_fcst_qty,dy_14_STATIC_APP_FCST_QTY), (dy_15_sys_fcst_qty,dy_15_app_fcst_qty,dy_15_STATIC_APP_FCST_QTY),
  (dy_16_sys_fcst_qty,dy_16_app_fcst_qty,dy_16_STATIC_APP_FCST_QTY), (dy_17_sys_fcst_qty,dy_17_app_fcst_qty,dy_17_STATIC_APP_FCST_QTY),
  (dy_18_sys_fcst_qty,dy_18_app_fcst_qty,dy_18_STATIC_APP_FCST_QTY), (dy_19_sys_fcst_qty,dy_19_app_fcst_qty,dy_19_STATIC_APP_FCST_QTY),
  (dy_20_sys_fcst_qty,dy_20_app_fcst_qty,dy_20_STATIC_APP_FCST_QTY), (dy_21_sys_fcst_qty,dy_21_app_fcst_qty,dy_21_STATIC_APP_FCST_QTY)))
WHERE to_number(SUBSTR(syscol,4,2))                         >
  (SELECT (    CASE   WHEN today_fin_day_no = 7   THEN 0
                      ELSE today_fin_day_no
               END) today_fin_day_no
   FROM dim_control  )
ORDER BY location_no,
         item_no,
         post_date DESC
  ;
            
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


END WH_PRF_RDF_600A;
