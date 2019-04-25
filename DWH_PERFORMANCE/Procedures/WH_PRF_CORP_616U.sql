--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_616U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_616U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        October 2018
--  Author:      Anthony Ugolini
--  Purpose:     Create INVENTORY RETURN TO VENDOR table in the performance layer
--               with input ex foundation table.
--  Tables:      Input  - fnd_rtl_rtv
--               Output - rtl_loc_item_dy_rtv
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_616U';
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
    l_text := 'LOAD OF dwh_performance.rtl_loc_item_dy_rtv EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    EXECUTE IMMEDIATE 'ALTER SESSION enable PARALLEL DML';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--  g_date := '30/AUG/2018';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    MERGE /*+ APPEND PARALLEL(tgt,6) */ INTO dwh_performance.rtl_loc_item_dy_rtv tgt using 
    (
       select  /*+ full (fnd) PARALLEL(fnd,8) full (itm) full(sup) */ 
	 loc.SK1_lOCATION_NO,
     itm.SK1_ITEM_NO,
     fnd.TRAN_DATE,
     sup.SK1_SUPPLIER_NO, 
	 fnd.RTV_ORDER_NO, 
	 fnd.SEQ_NO, 
	 fnd.POST_DATE, 
	 fnd.RTV_REF_ID, 
	 fnd.LIABILITY_ID, 
	 fnd.RTV_QTY, 
	 fnd.RTV_COST, 
	 fnd.RTV_SELLING, 
	 fnd.RTV_REASON_CODE, 
	 fnd.RTV_REASON_TYPE, 
	 fnd.SOURCE_DATA_STATUS_CODE, 
	 fnd.LAST_UPDATED_DATE, 
	 fnd.RECORD_NO, 
	 fnd.DEBTORS_COMMISSION_PERC
from 
     dwh_foundation.fnd_rtl_rtv     fnd,
     dwh_performance.dim_location   loc,
     dwh_performance.dim_item       itm,
     dwh_performance.dim_supplier   sup
where fnd.location_no           = loc.location_no
  and fnd.item_no               = itm.item_no
  and fnd.supplier_no           = sup.supplier_no
  and fnd.last_updated_date 	= g_date
  ) fnd_src
    on    (    fnd_src.sk1_location_no  = tgt.sk1_location_no
           and fnd_src.sk1_item_no      = tgt.sk1_item_no 
           and fnd_src.tran_date        = tgt.tran_date 
           and fnd_src.sk1_supplier_no  = tgt.sk1_supplier_no 
           and fnd_src.record_no        = tgt.record_no
	)
     when matched then
      update
        set   
        tgt.rtv_order_no	    = fnd_src.rtv_order_no,
        tgt.seq_no  		    = fnd_src.seq_no,
        tgt.post_date			= fnd_src.post_date,
        tgt.rtv_ref_id  		= fnd_src.rtv_ref_id,
        tgt.liability_id    	= fnd_src.liability_id ,
--
        tgt.rtv_qty     		= fnd_src.rtv_qty,
        tgt.rtv_cost    		= fnd_src.rtv_cost,
        tgt.rtv_selling 	    = fnd_src.rtv_selling,
--
        tgt.rtv_reason_code		= fnd_src.rtv_reason_code,
        tgt.rtv_reason_type		= fnd_src.rtv_reason_type,
--
        tgt.source_data_status_code = fnd_src.source_data_status_code,
        tgt.debtors_commission_perc = debtors_commission_perc,
        tgt.last_updated_date       = g_date
--
    WHEN NOT MATCHED THEN
      INSERT values    (
         fnd_src.sk1_location_no,
         fnd_src.sk1_item_no, 
         fnd_src.tran_date,
         fnd_src.sk1_supplier_no,

         fnd_src.rtv_order_no,
         fnd_src.seq_no,
         fnd_src.post_date,
         fnd_src.rtv_ref_id,
         fnd_src.liability_id,
--
         fnd_src.rtv_qty,
         fnd_src.rtv_cost,
         fnd_src.rtv_selling,
--
         fnd_src.rtv_reason_code,
         fnd_src.rtv_reason_type,
--
         fnd_src.source_data_status_code,
         g_date,
         fnd_src.record_no,
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

end WH_PRF_CORP_616U;
