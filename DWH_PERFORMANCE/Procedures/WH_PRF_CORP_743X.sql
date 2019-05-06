--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_743X
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_743X" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLAN PO data at zone item suppl level to JDAFF fact table in the performance layer
--               with input ex foundation layer.
--
--               ***  ONCE-OFF FULL REFRESH OF THE _R TABLE  ***
--
--  Tables:      Input  - fnd_zone_item_supp_ff_po_plan
--               Output - rtl_zone_item_dy_supp_po_pln_r
--  Packages:    constants, dwh_log, dwh_valid
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
--g_cases              rtl_zone_item_dy_supp_po_pln_r.dc_plan_store_cases%type;
g_rec_out            dwh_performance.rtl_zone_item_dy_supp_po_pln_r%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_743X';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_dy_supp_po_pln_r%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_dy_supp_po_pln_r%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_from_date         date          := trunc(sysdate) - 7;


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
    l_text := 'LOAD OF rtl_zone_item_dy_supp_po_pln_r EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    ----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table dwh_performance.rtl_zone_item_dy_supp_po_pln_r');
    l_text := 'Truncate table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------
    
    execute immediate 'alter session enable parallel dml';

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
    l_text := 'FULL DATA REFRESH .. ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO dwh_performance.rtl_zone_item_dy_supp_po_pln_r mart
select   unique z.SK1_ZONE_GROUP_ZONE_NO as sk1_zone_no,
   c.sk1_item_no,
   s.sk1_supplier_no,
   b.sk1_location_no as sk1_to_loc_no,
   a.WEEK_1_DAY_1_CASES,
   a.WEEK_1_DAY_2_CASES,
   a.WEEK_1_DAY_3_CASES,
   a.WEEK_1_DAY_4_CASES,
   a.WEEK_1_DAY_5_CASES,
   a.WEEK_1_DAY_6_CASES,
   a.WEEK_1_DAY_7_CASES,
   a.WEEK_2_DAY_1_CASES,
   a.WEEK_2_DAY_2_CASES,
   a.WEEK_2_DAY_3_CASES,
   a.WEEK_2_DAY_4_CASES,
   a.WEEK_2_DAY_5_CASES,
   a.WEEK_2_DAY_6_CASES,
   a.WEEK_2_DAY_7_CASES,
   a.WEEK_3_DAY_1_CASES,
   a.WEEK_3_DAY_2_CASES,
   a.WEEK_3_DAY_3_CASES,
   a.WEEK_3_DAY_4_CASES,
   a.WEEK_3_DAY_5_CASES,
   a.WEEK_3_DAY_6_CASES,
   a.WEEK_3_DAY_7_CASES,
   P.CALENDAR_DATE,
   null as DC_SUPP_INBOUND_CASES,
   a.LAST_UPDATED_DATE

 from  FND_ZONE_ITEM_SUPP_OM_PO_PLAN a ,
       RTL_ZONE_ITEM_DY_SUPP_PO_PLAN P ,
       dwh_performance.dim_zone z,
       dwh_performance.dim_location b,
       dwh_performance.dim_supplier s ,
       dwh_performance.dim_item c
where a.zone_no         = z.zone_no
  and a.item_no         = c.item_no
  and a.supplier_no     = s.supplier_no
  and a.to_loc_no       = b.location_no
  and c.SK1_ITEM_NO     = P.SK1_ITEM_NO
  and S.SK1_SUPPLIER_NO = P.SK1_SUPPLIER_NO  --;
  and p.calendar_date   = '05/DEC/13';
  
  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;

commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_743x;
