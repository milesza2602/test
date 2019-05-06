--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_604U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_604U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        October 2018
--  Author:      Anthony Ugolini
--  Purpose:     Create INVENTORY ADJUSTMENTS table in the performance layer
--               with input ex foundation table.
--  Tables:      Input  - fnd_rtl_inventory_adj
--               Output - rtl_loc_item_dy_inv_adj
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--   - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_604U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ADJUSTMENT FACT details';
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
    l_text := 'LOAD OF dwh_performance.rtl_loc_item_dy_inv_adj EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--g_date:= '01/MAR/2018';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE /*+ APPEND PARALLEL(tgt,6) */ INTO dwh_performance.rtl_loc_item_dy_inv_adj tgt using 
    (
       select  /*+ full (fnd) PARALLEL(fnd,8) full (itm) */ 
	loc.SK1_LOCATION_NO, 
	itm.SK1_ITEM_NO,
    fnd.TRAN_DATE,
    fnd.INV_ADJ_NO, 
	fnd.POST_DATE,
	fnd.REASON_CODE, 
	fnd.REASON_DESC, 
	fnd.INV_ADJ_TYPE, 
	fnd.INV_ADJ_TYPE_DESC, 
	fnd.LIABILITY_CODE, 
	fnd.LIABILITY_TYPE, 
	fnd.LIABILITY_TYPE_DESC, 
	fnd.REF_ID_1, 
	fnd.REF_ID_2, 
	fnd.INV_ADJ_COST, 
	fnd.INV_ADJ_QTY, 
	fnd.INV_ADJ_SELLING, 
	fnd.EXT_REF_ID, 
	fnd.RECORD_NO, 
	fnd.SOURCE_DATA_STATUS_CODE, 
	fnd.LAST_UPDATED_DATE, 
	fnd.DEBTORS_COMMISSION_PERC
from 
     dwh_foundation.fnd_rtl_inventory_adj fnd,
     dwh_performance.dim_location   loc,
     dwh_performance.dim_item       itm
where fnd.location_no           =   loc.location_no
  and fnd.item_no               =   itm.item_no
  and fnd.last_updated_date 	= g_date
  ) fnd_src
    on    (    fnd_src.sk1_location_no  = tgt.sk1_location_no
           and fnd_src.sk1_item_no      = tgt.sk1_item_no 
           and fnd_src.tran_date        = tgt.tran_date 
           and fnd_src.reason_code      = tgt.reason_code
           and fnd_src.record_no        = tgt.record_no
	)
     when matched then
      update
        set   
        tgt.inv_adj_no		    = fnd_src.inv_adj_no,
        tgt.post_date		    = fnd_src.post_date,
        tgt.reason_desc			= fnd_src.reason_desc,
        tgt.inv_adj_type		= fnd_src.inv_adj_type,
        tgt.inv_adj_type_desc	= fnd_src.inv_adj_type_desc,
--
        tgt.liability_code		= fnd_src.liability_code,
        tgt.liability_type		= fnd_src.liability_type,
        tgt.liability_type_desc	= fnd_src.liability_type_desc,
--
        tgt.ref_id_1			= fnd_src.ref_id_1,
        tgt.ref_id_2			= fnd_src.ref_id_2,
--
        tgt.inv_adj_cost		= fnd_src.inv_adj_cost,
        tgt.inv_adj_qty		    = fnd_src.inv_adj_qty,
        tgt.inv_adj_selling		= fnd_src.inv_adj_selling,
--
        tgt.ext_ref_id		        = fnd_src.ext_ref_id,
        tgt.source_data_status_code = fnd_src.source_data_status_code,
        tgt.debtors_commission_perc = debtors_commission_perc,
        tgt.last_updated_date       = g_date
--
    WHEN NOT MATCHED THEN
      INSERT values    (
         fnd_src.sk1_location_no,
         fnd_src.sk1_item_no, 
         fnd_src.tran_date,
         fnd_src.inv_adj_no,
         fnd_src.post_date,
         fnd_src.reason_code,
         fnd_src.reason_desc,
         fnd_src.inv_adj_type,
         fnd_src.inv_adj_type_desc,
--
         fnd_src.liability_code,
         fnd_src.liability_type,
         fnd_src.liability_type_desc,
--
         fnd_src.ref_id_1,
         fnd_src.ref_id_2,
--
         fnd_src.inv_adj_cost,
         fnd_src.inv_adj_qty,
         fnd_src.inv_adj_selling,
--
         fnd_src.ext_ref_id,
         fnd_src.record_no,
         fnd_src.source_data_status_code,
         g_date,
         fnd_src.debtors_commission_perc
          );

      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
      g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
      g_recs_read :=  g_recs_read + SQL%ROWCOUNT;

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

end WH_PRF_CORP_604U;
