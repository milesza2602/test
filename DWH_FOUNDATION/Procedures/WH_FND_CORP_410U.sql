--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_410U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_410U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2018
--  Author:      Anthony Ugolini
--  Purpose:     Create LIMA table in the foundation layer
--               with input ex staging table from AIT.
--  Tables:      Input  - stg_lima_po_style_dy_det
--               Output - fnd_po_supp_style_dy_detail
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

g_forall_limit        integer       :=  10000;
g_recs_read           integer       :=  0;
g_recs_deleted        integer       :=  0;
g_recs_inserted       integer       :=  0;
g_recs_hospital       integer       :=  0;
g_recs_duplicate      integer       :=  0;
g_error_count         number        :=  0;
g_error_index         number        :=  0;
g_hospital            char(1)       := 'N';
g_hospital_text       dwh_foundation.stg_lima_po_style_dy_det_hsp.sys_process_msg%type;
stg_src               dwh_foundation.stg_lima_po_style_dy_det_cpy%rowtype;
g_found               boolean;
g_valid               boolean;

g_date                date          := trunc(sysdate);

l_message             sys_dwh_errlog.log_text%type;
l_module_name         sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_410U';

l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name      sys_dwh_log.log_procedure_name%type       := l_module_name;

l_text                sys_dwh_log.log_text%type ;
l_description         sys_dwh_log_summary.log_description%type  := 'LOAD THE LIMA details';
l_process_type        sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_Style_No          dwh_foundation.stg_lima_po_style_dy_det_cpy.Style_No%TYPE;
g_PO_No             dwh_foundation.stg_lima_po_style_dy_det_cpy.PO_No%type;
g_container_mawb    dwh_foundation.stg_lima_po_style_dy_det_cpy.container_mawb%type;
g_post_date         dwh_foundation.stg_lima_po_style_dy_det_cpy.post_date%type;

-- cursor for de-duplication ...
cursor  stg_dup is
        select  /*+ full(stg) parallel(stg,4) */ *
        from    dwh_foundation.stg_lima_po_style_dy_det_cpy stg
        where  (Style_No, PO_No, post_date, nvl(container_mawb,' ')) in ( select  Style_No,
                                                    PO_No, post_date,
                                                    nvl(container_mawb,' ') container_mawb
                                            from    dwh_foundation.stg_lima_po_style_dy_det_cpy
                                            group by
                                                    Style_No,
                                                    PO_No,post_date,
                                                    container_mawb
                                            having  count(*) > 1
                                          )
        order by Style_No, PO_No,post_date,container_mawb, sys_source_batch_id desc, sys_source_sequence_no desc;

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

    l_text := 'LOAD OF fnd_lima_po_style_dy_det Started at: '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select count(*)
    into   g_recs_read
    from   dwh_foundation.stg_lima_po_style_dy_det_cpy
    where  sys_process_code = 'N';

    --**************************************************************************************************
    -- De Duplication of the staging table to avoid Bulk insert failures
    --**************************************************************************************************
    l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_PO_No := 0;
    g_Style_No := 0;
    g_container_mawb := 'Z';
    g_post_date := sysdate;

    for dupp_record in stg_dup loop
        if  dupp_record.PO_No 	                    = g_PO_No
        and dupp_record.Style_No                    = g_Style_No
        and nvl(dupp_record.container_mawb,' ')     = nvl(g_container_mawb,' ')
        and dupp_record.post_date                   = g_post_date
     then
         update dwh_foundation.stg_lima_po_style_dy_det_cpy stg
         set    sys_process_code 	= 'D'
         where  sys_source_batch_id    	= dupp_record.sys_source_batch_id
           and  sys_source_sequence_no 	= dupp_record.sys_source_sequence_no;

            g_recs_duplicate := g_recs_duplicate  + 1;
        end if;

        g_PO_No             := dupp_record.PO_No;
        g_Style_No          := dupp_record.Style_No;
        g_container_mawb    := nvl(dupp_record.container_mawb,' ');
        g_post_date         := dupp_record.post_date ;

    end loop;
    commit;

    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --**************************************************************************************************
    -- Delete and insert controlling main program execution
    --**************************************************************************************************
    l_text := 'DELETE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    delete from  dwh_foundation.fnd_po_supp_style_dy_detail
     where post_date in 
        (Select distinct post_date from dwh_foundation.stg_lima_po_style_dy_det_cpy); 
        
    g_recs_deleted :=  g_recs_deleted + SQL%ROWCOUNT;
    
    l_text := 'DELETE ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    
    l_text := 'INSERT STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    insert /*+ parallel (fnd,4) */ into dwh_foundation.fnd_po_supp_style_dy_detail fnd
    (select /*+ parallel (cpy,4) */  
	    cpy.PO_No,
        cpy.Style_No,
        cpy.Supplier_No,
--
        cpy.Buying_Agent,
        cpy.Payment_Terms,
        cpy.Unit_Price,
        cpy.Promotion,
--
        cpy.PO_Type,
        cpy.PO_Raised,
        cpy.LPOSD,
        cpy.RLPOSD,
        cpy.OISD,
        cpy.RISD,
--
        cpy.Origin,
        cpy.POL,
        cpy.PO_Mode,
        cpy.PO_Qty,
--
        cpy.QC_Achieved,
        cpy.Cargo_Booking_Achieved,
        cpy.Shipped_Mode,
        cpy.Current_ETD,
        cpy.Current_ETA,
        cpy.ETA_Week,
--
        cpy.Carrier,
        cpy.Vessel,
        cpy.POD,
        cpy.Authorised,
        cpy.Cargo_Receipt,
--
        cpy.Gate_In,
        cpy.MBL,
        cpy.HBL_HAWB,
        cpy.Container_MAWB,
        cpy.Container_Size,
        cpy.Loading,
        cpy.Pack_Type,
        cpy.Shipped_Qty,
        cpy.Ctns,
        cpy.Cubes,
        cpy.Weight,
--
        cpy.Woolies_Ref_Achieved,
        cpy.Woolies_Ref,
        cpy.ACS_Ref,
        cpy.Supplier_Pack_to_Finance,
        cpy.Carrier_ANF_Received,
        cpy.Carrier_ANF_ETA,
        cpy.ACS_ANF_Issued,
        cpy.Cleared_date,
        cpy.Entry_Date,
        cpy.Tariff_Code,
--
        cpy.Arrived_Date,
        cpy.DRO_Pack_Available,
        cpy.DRO_Time_Slot,
        
        cpy.DRO_Late_Reason ,
        cpy.CTO_Passed,
        cpy.Unpack_Depot,
        cpy.Arrival_At_Storage_Depot,
        cpy.Distribution_Centre,
        cpy.Bkg_Date,
        cpy.Bkg_Time,
--
        cpy.Booking_Week,
        cpy.Into_DC_Week,
        cpy.Delivered_Date,
        cpy.CI_Received_Date,
        cpy.OriginShip_Release_Date,
        cpy.Destination_Ship_Release_Date,
        cpy.Planned_Handover_Date,
        cpy.Actual_Handover_Date,
        cpy.Earliest_Ship_Date,
        cpy.Revised_Earliest_Ship_Date,
--
        cpy.Priority,
        cpy.Latest_Cargo_Booking_ETD,
        cpy.Latest_Cargo_Booking_ETA,
        cpy.First_Advice_of_Despatch_CTD,
        cpy.First_Advice_of_Despatch_ETA,
	    cpy.Post_date,
	    g_date
     from dwh_foundation.stg_lima_po_style_dy_det_cpy cpy
      where  sys_process_code 	= 'N'
          );

    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
      
    l_text := 'INSERT ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    --**************************************************************************************************
    -- Write final log data
    --**************************************************************************************************
      l_text := 'INSERT DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      insert /*+ APPEND parallel (hsp,2) */ into dwh_foundation.stg_lima_po_style_dy_det_hsp hsp
      select /*+ FULL(TMP) */
              TMP.sys_source_batch_id,
              TMP.sys_source_sequence_no,
              sysdate,
             'Y',
             'LIMA',
              TMP.sys_middleware_batch_id,
             'INVALID INDICATOR OR REFERENCIAL ERROR ON PO/STYLE',
        tmp.PO_No,
        tmp.Style_No,
        tmp.Supplier_No,
        tmp.Buying_Agent,
        tmp.Payment_Terms,
        tmp.Unit_Price,
        tmp.Promotion,
--
        tmp.PO_Type,
        tmp.PO_Raised,
        tmp.LPOSD,
        tmp.RLPOSD,
        tmp.OISD,
        tmp.RISD,
--
        tmp.Origin,
        tmp.POL,
        tmp.PO_Mode,
        tmp.PO_Qty,
--
        tmp.QC_Achieved,
        tmp.Cargo_Booking_Achieved,
        tmp.Shipped_Mode,
        tmp.Current_ETD,
        tmp.Current_ETA,
        tmp.ETA_Week,
--
        tmp.Carrier,
        tmp.Vessel,
        tmp.POD,
        tmp.Authorised,
        tmp.Cargo_Receipt,
--
        tmp.Gate_In,
        tmp.MBL,
        tmp.HBL_HAWB,
        tmp.Container_MAWB,
        tmp.Container_Size,
        tmp.Loading,
        tmp.Pack_Type,
        tmp.Shipped_Qty,
        tmp.Ctns,
        tmp.Cubes,
        tmp.Weight,
--
        tmp.Woolies_Ref_Achieved,
        tmp.Woolies_Ref,
        tmp.ACS_Ref,
        tmp.Supplier_Pack_to_Finance,
        tmp.Carrier_ANF_Received,
        tmp.Carrier_ANF_ETA,
        tmp.ACS_ANF_Issued,
        tmp.Cleared_date,
        tmp.Entry_Date,
        tmp.Tariff_Code,
--
        tmp.Arrived_Date,
        tmp.DRO_Pack_Available,
        tmp.DRO_Time_Slot,
        tmp.DRO_Late_Reason ,
        tmp.CTO_Passed,
        tmp.Unpack_Depot,
        tmp.Arrival_At_Storage_Depot,
        tmp.Distribution_Centre,
        tmp.Bkg_Date,
        tmp.Bkg_Time,
--
        tmp.Booking_Week,
        tmp.Into_DC_Week,
        tmp.Delivered_Date,
        tmp.CI_Received_Date,
        tmp.OriginShip_Release_Date,
        tmp.Destination_Ship_Release_Date,
        tmp.Planned_Handover_Date,
        tmp.Actual_Handover_Date,
        tmp.Earliest_Ship_Date,
        tmp.Revised_Earliest_Ship_Date,
--
        tmp.Priority,
        tmp.Latest_Cargo_Booking_ETD,
        tmp.Latest_Cargo_Booking_ETA,
        tmp.First_Advice_of_Despatch_CTD,
        tmp.First_Advice_of_Despatch_ETA,
        tmp.Post_date
      from    dwh_foundation.stg_lima_po_style_dy_det_cpy TMP
      where   sys_process_code = 'N'

----- check Style_No  (Item Master)

         and (not exists (select *
                          from   dim_lev1 lev1
                          where  tmp.style_no = lev1.style_no
                         )
             );

    g_recs_hospital := g_recs_hospital + sql%rowcount;

    commit;

    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_deleted,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;
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

end wh_fnd_corp_410U;
