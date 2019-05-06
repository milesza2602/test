--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_410U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_410U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2018
--  Author:      Anthony Ugolini
--  Purpose:     Create LIMA table in the performance layer
--               with input ex foundation table.
--  Tables:      Input  - fnd_po_supp_style_dy_detail
--               Output - rtl_po_supp_style_dy_detail
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dwh_performance.rtl_po_supp_style_dy_detail%rowtype;
g_found              boolean;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_410U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LIMA FACT details';
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
    l_text := 'LOAD OF dwh_performance.rtl_po_supp_style_dy_detail EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Delete and insert controlling main program execution
--**************************************************************************************************
    l_text := 'DELETE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    delete from  dwh_performance.rtl_po_supp_style_dy_detail
     where post_date in 
     (select distinct post_date from dwh_foundation.fnd_po_supp_style_dy_detail
       where last_updated_date = g_date); 

    g_recs_deleted  :=  g_recs_deleted  + SQL%ROWCOUNT;

    l_text := 'DELETE ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;

    l_text := 'INSERT STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

    insert /*+ PARALLEL(tgt,4) */ into rtl_po_supp_style_dy_detail tgt  
    (
       select  /*+ PARALLEL(fnd,4) */ 
        fnd.po_no,
        lev1.sk1_style_no,
        fnd.style_no,
--
        supp.sk1_supplier_no,
        supp.supplier_no,
--
        fnd.Buying_Agent,
        fnd.Payment_Terms,
        fnd.Unit_Price,
        fnd.Promotion,
--
        fnd.PO_Type,
        fnd.PO_Raised,
        fnd.LPOSD,
        fnd.RLPOSD,
        fnd.OISD,
        fnd.RISD,
--
        fnd.Origin,
        fnd.POL,
        fnd.PO_Mode,
        fnd.PO_Qty,
--
        fnd.QC_Achieved,
        fnd.Cargo_Booking_Achieved,
        fnd.Shipped_Mode,
        fnd.Current_ETD,
        fnd.Current_ETA,
        fnd.ETA_Week,
--
        fnd.Carrier,
        fnd.Vessel,
        fnd.POD,
        fnd.Authorised,
        fnd.Cargo_Receipt,
--
        fnd.Gate_In,
        fnd.MBL,
        fnd.HBL_HAWB,
        fnd.Container_MAWB,
        fnd.Container_Size,
        fnd.Loading,
        fnd.Pack_Type,
        fnd.Shipped_Qty,
        fnd.Ctns,
        fnd.Cubes,
        fnd.Weight,
--
        fnd.Woolies_Ref_Achieved,
        fnd.Woolies_Ref,
        fnd.ACS_Ref,
        fnd.Supplier_Pack_to_Finance,
        fnd.Carrier_ANF_Received,
        fnd.Carrier_ANF_ETA,
        fnd.ACS_ANF_Issued,
        fnd.Cleared_date,
        fnd.Entry_Date,
        fnd.Tariff_Code,
--
        fnd.Arrived_Date,
        fnd.DRO_Pack_Available,
        fnd.DRO_Time_Slot,

        fnd.DRO_Late_Reason ,
        fnd.CTO_Passed,
        fnd.Unpack_Depot,
        fnd.Arrival_At_Storage_Depot,
        fnd.Distribution_Centre,
        fnd.Bkg_Date,
        fnd.Bkg_Time,
--
        fnd.Booking_Week,
        fnd.Into_DC_Week,
        fnd.Delivered_Date,
        fnd.CI_Received_Date,
        fnd.OriginShip_Release_Date,
        fnd.Destination_Ship_Release_Date,
        fnd.Planned_Handover_Date,
        fnd.Actual_Handover_Date,
        fnd.Earliest_Ship_Date,
        fnd.Revised_Earliest_Ship_Date,
--
        fnd.Priority,
        fnd.Latest_Cargo_Booking_ETD,
        fnd.Latest_Cargo_Booking_ETA,
        fnd.First_Advice_of_Despatch_CTD,
        fnd.First_Advice_of_Despatch_ETA,
        fnd.Post_date,
        fnd.last_updated_date
  from  dwh_foundation.fnd_po_supp_style_dy_detail 	fnd,
        dwh_performance.dim_lev1    			    lev1,
        dwh_performance.dim_purchase_order          pord,
        dwh_performance.dim_supplier                supp

  where fnd.style_no	       	= lev1.style_no
    and fnd.po_no               = pord.po_no
    and pord.supplier_no        = supp.supplier_no 
    and fnd.last_updated_date 	= g_date
     );

    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
    g_recs_read     :=  g_recs_read     + SQL%ROWCOUNT;

    l_text := 'INSERT ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_deleted,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end WH_PRF_CORP_410U;
