--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_612U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_612U_MERGE" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Shipment fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_rtl_shipment
--               Output - fnd_rtl_shipment_ap
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  18 May 2010 - Add Debtors commission to output table and calculate - QC 3774
--
--  9 sep 2016  WL -  - change to code done on 7 sep 2016 was based upon UAT version instead of current PRD version
--               - hence this change today
--               - change is adding CHAIN_CODE to stg table.
--               - this will replace the code deriving chain_code only once 
--                 an impact assessment has been done to asses the difference between RMs and BI ODWH
--                 (as  a precaution - should be the same)
--               - reprocessing data from 5th Sep 2016 thru to 9th Sep
--               - g_date := '9 sep 2016';
--                 l_text := '------------>>>>  Batch_date hardcoded for datafix'||g_date;
--                 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_rtl_shipment_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_shipment_ap%rowtype;
g_found              boolean;
g_date               date;
g_abort               number        :=  0;


L_Message            Sys_Dwh_Errlog.Log_Text%Type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_612U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE SHIPMENT FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



---- For output arrays into bulk load forall statements --
--type tbl_array_i is table of fnd_rtl_shipment_ap%rowtype index by binary_integer;
--type tbl_array_u is table of fnd_rtl_shipment_ap%rowtype index by binary_integer;
--a_tbl_insert        tbl_array_i;
--a_tbl_update        tbl_array_u;
--a_empty_set_i       tbl_array_i;
--a_empty_set_u       tbl_array_u;
--
--a_count             integer       := 0;
--a_count_i           integer       := 0;
--a_count_u           integer       := 0;
--
---- For arrays used to update the staging table process_code --
--type staging_array1 is table of stg_rms_rtl_shipment_cpy.sys_source_batch_id%type
--                                                       index by binary_integer;
--type staging_array2 is table of stg_rms_rtl_shipment_cpy.sys_source_sequence_no%type
--                                                       index by binary_integer;
--a_staging1          staging_array1;
--a_staging2          staging_array2;
--a_empty_set_s1      staging_array1;
--a_empty_set_s2      staging_array2;
--
--a_count_stg         integer       := 0;


cursor stg_dedup is 
 select /*+ Parallel(cpy1,12)  Parallel(fi,12)*/ cpy.*
  from stg_rms_rtl_shipment_cpy cpy1
  inner join fnd_item_vat_rate fi 
          on (cpy1.item_no            =  fi.item_no and
              cpy1.vat_region_no      =  fi.vat_region_no and
              cpy1.active_from_date   >  fi.active_from_date)
 where (sys_source_batch_id, sys_source_sequence_no,cpy1.item_no,cpy1.vat_region_no,cpy1.active_from_date) in
           (select /*+ PARALLEL(12) */ sys_source_batch_id, sys_source_sequence_no,item_no,vat_region_no,active_from_date
              from (select /*+ PARALLEL(t,12) */ t.*,
                           rank ()
                              over (partition by item_no,vat_region_no,active_from_date order by sys_source_batch_id desc, sys_source_sequence_no desc)
                              as rank
                      from stg_rms_item_vat_rate_cpy t
                   )
             where rank = 1)
 order by sys_source_batch_id desc, sys_source_sequence_no;


WITH selext1 AS
            (SELECT
              /*+ materialize full(a) parallel(a,4) */
                  SYS_SOURCE_BATCH_ID
                  ,SYS_SOURCE_SEQUENCE_NO
                  ,SYS_LOAD_DATE
                  ,SYS_PROCESS_CODE
                  ,SYS_LOAD_SYSTEM_NAME
                  ,SYS_MIDDLEWARE_BATCH_ID
                  ,SYS_PROCESS_MSG
                  ,SHIPMENT_NO
                  ,SEQ_NO
                  ,ITEM_NO
                  ,SUPPLIER_NO
                  ,PO_NO
                  ,SDN_NO
                  ,ASN_ID
                  ,SHIP_DATE
                  ,RECEIVE_DATE
                  ,SHIPMENT_STATUS_CODE
                  ,INV_MATCH_STATUS_CODE
                  ,INV_MATCH_DATE
                  ,TO_LOC_NO
                  ,FROM_LOC_NO
                  ,COURIER_ID
                  ,EXT_REF_IN_ID
                  ,DIST_NO
                  ,DIST_TYPE
                  ,REF_ITEM_NO
                  ,TSF_ALLOC_NO
                  ,TSF_PO_LINK_NO
                  ,IBT_TYPE
                  ,SIMS_REF_ID
                  ,DU_ID
                  ,SHIPMENT_LINE_STATUS_CODE
                  ,RECEIVED_QTY
                  ,COST_PRICE
                  ,REG_RSP
                  ,ASN_QTY
                  ,ACTUAL_MASS
                  ,CARTON_STATUS_CODE
                  ,CARTON_STATUS_DESC
                  ,SDN_QTY
                  ,PO_NOT_BEFORE_DATE
                  ,CONTRACT_NO
                  ,ACTL_RCPT_DATE
                  ,SUPP_PACK_SIZE
                  ,CANCELLED_QTY
                  ,AUTO_RECEIPT_CODE
                  ,SOURCE_DATA_STATUS_CODE
                  ,FINAL_LOC_NO
            FROM stg_rms_rtl_shipment_cpy a
             ) ,
  SELEXT2 AS
            (SELECT
                    /*+ MATERIALIZE  parallel(se1,4)  parallel(po,4)  parallel(fat,4)  parallel(fad,4) */
                    DISTINCT se1.* ,
                    DLF.CHAIN_NO FROM_CHAIN_NO ,
                    dlf.loc_type from_loc_type,
                    DLT.CHAIN_NO TO_CHAIN_NO ,
                    dlt.loc_type to_loc_type,
                    DLN.CHAIN_NO FINAL_CHAIN_NO ,
                    dln.loc_type final_loc_type,
                    PO.CHAIN_CODE PO_CHAIN_CODE,
                    DLF.SK1_LOCATION_NO FROM_SK1_LOCATION_NO,
                    DLT.SK1_LOCATION_NO TO_SK1_LOCATION_NO,
                    DLN.SK1_LOCATION_NO Final_SK1_LOCATION_NO
            FROM selext1 se1
            LEFT OUTER JOIN DWH_PERFORMANCE.dim_location dlf
               ON dlf.location_no = se1.from_loc_no
            LEFT OUTER JOIN dim_location dlT
                ON dlT.location_no = se1.TO_loc_no
            LEFT OUTER JOIN dim_location dlN
               ON dlN.location_no = se1.FINAL_loc_no
            LEFT OUTER JOIN FND_RTL_PURCHASE_ORDER PO
               ON PO.PO_NO = SE1.PO_NO )
SELECT  /*+  full(se2) parallel(se2,4) full(di) full(rtlt) parallel(rtlt,4) full(rtlf) parallel(rtlf,4) */
            SE2.*,
            CASE
              WHEN se2.PO_CHAIN_CODE = 'DJ'                                 THEN 'DJ' --1
              WHEN se2.FROM_CHAIN_NO = 40                                   THEN 'DJ' --2
              WHEN se2.TO_CHAIN_NO = 40                                     THEN 'DJ' --3
              WHEN se2.FINAL_CHAIN_NO = 40                                  THEN 'DJ' --4
              WHEN se2.FROM_CHAIN_NO <> 40  AND se2.FROM_CHAIN_NO  IS NOT NULL  THEN 'WW' --5
              WHEN se2.TO_CHAIN_NO <> 40    AND se2.TO_CHAIN_NO  IS NOT NULL    THEN 'WW' --6
              WHEN se2.FINAL_CHAIN_NO <> 40 AND se2.FINAL_CHAIN_NO  IS NOT NULL THEN 'WW' --7
              WHEN se2.from_loc_type = 'W'           AND se2.TO_loc_type = 'W' AND se2.FINAL_loc_type = 'W'            THEN 'WW' --8
              WHEN se2.from_loc_type = 'W'           AND se2.TO_loc_type = 'W' AND se2.FINAL_loc_type NOT IN('S','W')  THEN 'WW' --9
              WHEN se2.from_loc_type NOT IN('S','W') AND se2.TO_loc_type = 'W' AND se2.FINAL_loc_type = 'W'            THEN 'WW' --10
              WHEN se2.from_loc_type NOT IN('S','W') AND se2.TO_loc_type = 'W' AND se2.FINAL_loc_type = 'W' AND se2.PO_CHAIN_CODE IS NULL THEN 'WW' --11
              ELSE 
                  NULL
            END 
            CHAIN_CODE,
            rtlt.debtors_commission_perc to_loc_debt_comm_perc,
            rtlf.debtors_commission_perc FROM_loc_debt_comm_perc,
            di.sk1_item_no,
            ds.SK1_SUPPLIER_NO
FROM SELEXT2 SE2
LEFT OUTER JOIN DIM_ITEM DI
            ON DI.ITEM_NO = SE2.ITEM_NO
LEFT OUTER JOIN rtl_loc_dept_dy rTLF
            on rTLf.sk1_department_no = Di.sk1_department_no
            AND RTLf.SK1_LOCATION_NO = SE2.FROM_SK1_LOCATION_NO
            and RTLf.post_date  = SE2.ship_date
LEFT OUTER JOIN rtl_loc_dept_dy rTLT
            on rTLt.sk1_department_no = Di.sk1_department_no
            AND RTLt.SK1_LOCATION_NO = SE2.TO_SK1_LOCATION_NO
            and RTLt.post_date  = SE2.ship_date
left outer join dim_supplier ds
            on ds.supplier_no = se2.supplier_no
   order by sys_source_batch_id, sys_source_sequence_no;   



   g_rec_out.shipment_no                     := g_rec_in.shipment_no;
   g_rec_out.seq_no                          := g_rec_in.seq_no;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.po_no                           := g_rec_in.po_no;
   g_rec_out.sdn_no                          := g_rec_in.sdn_no;
   g_rec_out.asn_id                          := g_rec_in.asn_id;
   g_rec_out.ship_date                       := g_rec_in.ship_date;
   g_rec_out.receive_date                    := g_rec_in.receive_date;
   g_rec_out.shipment_status_code            := g_rec_in.shipment_status_code;
   g_rec_out.inv_match_status_code           := g_rec_in.inv_match_status_code;
   g_rec_out.inv_match_date                  := g_rec_in.inv_match_date;
   g_rec_out.to_loc_no                       := g_rec_in.to_loc_no;
   g_rec_out.from_loc_no                     := g_rec_in.from_loc_no;
   g_rec_out.courier_id                      := g_rec_in.courier_id;
   g_rec_out.ext_ref_in_id                   := g_rec_in.ext_ref_in_id;
   g_rec_out.dist_no                         := g_rec_in.dist_no;
   g_rec_out.dist_type                       := g_rec_in.dist_type;
   g_rec_out.ref_item_no                     := g_rec_in.ref_item_no;
   g_rec_out.tsf_alloc_no                    := g_rec_in.tsf_alloc_no;
   g_rec_out.tsf_po_link_no                  := g_rec_in.tsf_po_link_no;
   g_rec_out.ibt_type                        := g_rec_in.ibt_type;
   g_rec_out.sims_ref_id                     := g_rec_in.sims_ref_id;
   g_rec_out.du_id                           := g_rec_in.du_id;
   g_rec_out.shipment_line_status_code       := g_rec_in.shipment_line_status_code;
   g_rec_out.received_qty                    := g_rec_in.received_qty;
   g_rec_out.cost_price                      := g_rec_in.cost_price;
   g_rec_out.reg_rsp                         := g_rec_in.reg_rsp;
   g_rec_out.asn_qty                         := g_rec_in.asn_qty;
   g_rec_out.actual_mass                     := g_rec_in.actual_mass;
   g_rec_out.carton_status_code              := g_rec_in.carton_status_code;
   g_rec_out.carton_status_desc              := g_rec_in.carton_status_desc;
   g_rec_out.sdn_qty                         := g_rec_in.sdn_qty;
   g_rec_out.po_not_before_date              := g_rec_in.po_not_before_date;
   g_rec_out.contract_no                     := g_rec_in.contract_no;
   g_rec_out.actl_rcpt_date                  := g_rec_in.actl_rcpt_date;
   g_rec_out.supp_pack_size                  := g_rec_in.supp_pack_size;
   g_rec_out.cancelled_qty                   := g_rec_in.cancelled_qty;
   g_rec_out.auto_receipt_code               := g_rec_in.auto_receipt_code;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.final_loc_no                    := nvl(g_rec_in.final_loc_no, g_rec_in.to_loc_no);
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.FROM_loc_debt_comm_perc         := NVL(g_rec_IN.FROM_loc_debt_comm_perc,0);
   g_rec_out.to_loc_debt_comm_perc           := NVL(g_rec_IN.to_loc_debt_comm_perc,0);
   g_rec_out.chain_code                      := g_rec_IN.chain_code;
      
    if g_rec_in.from_loc_no is not null
   and g_rec_in.from_sk1_location_no is null then
         g_hospital      := 'Y';
         g_hospital_text := 'from_loc_no**'||dwh_constants.vc_location_not_found;
         l_text          := 'from_loc_no**'||dwh_constants.vc_location_not_found||g_rec_out.from_loc_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;
   
   
      if g_rec_in.to_loc_no is not null 
   and g_rec_in.to_sk1_location_no is null then
         g_hospital      := 'Y';
         g_hospital_text := 'to_loc_no**'||dwh_constants.vc_location_not_found;
         l_text          := 'to_loc_no**'||dwh_constants.vc_location_not_found||g_rec_out.from_loc_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;

---------------------------------------------------------------------------------------------------
---- In some instances the final_loc_no contains supplier_no
--- following comment from RMS.......
-----             'the final location in this instance is the supplier' 
------            ' This would occur when goods are RTV�d. the final location on' 
------            '  the shipsku record is the supplier and stock is RTV�d via the DC'
-----
---- WE WILL BE DEFAULTING TO WW where the location is not found in dim_location and is not null
----- We will look at this issue at some later stage
---------------------------------------------------------------------------------------------------
/*     if g_rec_in.final_loc_no is not null 
   and g_rec_in.final_sk1_location_no is null then
         g_hospital      := 'Y';
         g_hospital_text := 'final_loc_no**'||dwh_constants.vc_location_not_found;
         l_text          := 'final_loc_no**'||dwh_constants.vc_location_not_found||g_rec_out.from_loc_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;
*/
---------------------- temp code starts --------------------- 6 may 2015
   if g_rec_out.chain_code is null
   and g_rec_in.final_loc_no is not null 
   and g_rec_in.final_sk1_location_no is null then
     l_text := 'final_loc_no* not found but not rejected - default to WW chain_code - '||g_rec_out.from_loc_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
---------------------- temp code ends -------------------------------------------


   if g_rec_in.sk1_item_no is null then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

      if g_rec_out.supplier_no is not null 
   and g_rec_in.sk1_supplier_no is null then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_supplier_not_found;
         l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;

   end if;

  
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_rms_rtl_shipment_hsp values 
(g_rec_in.SYS_SOURCE_BATCH_ID
,g_rec_in.SYS_SOURCE_SEQUENCE_NO
,g_rec_in.SYS_LOAD_DATE
,g_rec_in.SYS_PROCESS_CODE
,g_rec_in.SYS_LOAD_SYSTEM_NAME
,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
,g_rec_in.SYS_PROCESS_MSG
,g_rec_in.SHIPMENT_NO
,g_rec_in.SEQ_NO
,g_rec_in.ITEM_NO
,g_rec_in.SUPPLIER_NO
,g_rec_in.PO_NO
,g_rec_in.SDN_NO
,g_rec_in.ASN_ID
,g_rec_in.SHIP_DATE
,g_rec_in.RECEIVE_DATE
,g_rec_in.SHIPMENT_STATUS_CODE
,g_rec_in.INV_MATCH_STATUS_CODE
,g_rec_in.INV_MATCH_DATE
,g_rec_in.TO_LOC_NO
,g_rec_in.FROM_LOC_NO
,g_rec_in.COURIER_ID
,g_rec_in.EXT_REF_IN_ID
,g_rec_in.DIST_NO
,g_rec_in.DIST_TYPE
,g_rec_in.REF_ITEM_NO
,g_rec_in.TSF_ALLOC_NO
,g_rec_in.TSF_PO_LINK_NO
,g_rec_in.IBT_TYPE
,g_rec_in.SIMS_REF_ID
,g_rec_in.DU_ID
,g_rec_in.SHIPMENT_LINE_STATUS_CODE
,g_rec_in.RECEIVED_QTY
,g_rec_in.COST_PRICE
,g_rec_in.REG_RSP
,g_rec_in.ASN_QTY
,g_rec_in.ACTUAL_MASS
,g_rec_in.CARTON_STATUS_CODE
,g_rec_in.CARTON_STATUS_DESC
,g_rec_in.SDN_QTY
,g_rec_in.PO_NOT_BEFORE_DATE
,g_rec_in.CONTRACT_NO
,g_rec_in.ACTL_RCPT_DATE
,g_rec_in.SUPP_PACK_SIZE
,g_rec_in.CANCELLED_QTY
,g_rec_in.AUTO_RECEIPT_CODE
,g_rec_in.SOURCE_DATA_STATUS_CODE
,g_rec_in.FINAL_LOC_NO
,g_rec_in.chain_code)
;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_hospital;


       update fnd_rtl_shipment_ap
       set    supplier_no                     = a_tbl_update(i).supplier_no,
              po_no                           = a_tbl_update(i).po_no,
              sdn_no                          = a_tbl_update(i).sdn_no,
              asn_id                          = a_tbl_update(i).asn_id,
              ship_date                       = a_tbl_update(i).ship_date,
              receive_date                    = a_tbl_update(i).receive_date,
              shipment_status_code            = a_tbl_update(i).shipment_status_code,
              inv_match_status_code           = a_tbl_update(i).inv_match_status_code,
              inv_match_date                  = a_tbl_update(i).inv_match_date,
              to_loc_no                       = a_tbl_update(i).to_loc_no,
              from_loc_no                     = a_tbl_update(i).from_loc_no,
              courier_id                      = a_tbl_update(i).courier_id,
              ext_ref_in_id                   = a_tbl_update(i).ext_ref_in_id,
              dist_no                         = a_tbl_update(i).dist_no,
              dist_type                       = a_tbl_update(i).dist_type,
              ref_item_no                     = a_tbl_update(i).ref_item_no,
              tsf_alloc_no                    = a_tbl_update(i).tsf_alloc_no,
              tsf_po_link_no                  = a_tbl_update(i).tsf_po_link_no,
              ibt_type                        = a_tbl_update(i).ibt_type,
              sims_ref_id                     = a_tbl_update(i).sims_ref_id,
              du_id                           = a_tbl_update(i).du_id,
              shipment_line_status_code       = a_tbl_update(i).shipment_line_status_code,
              received_qty                    = a_tbl_update(i).received_qty,
              cost_price                      = a_tbl_update(i).cost_price,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              asn_qty                         = a_tbl_update(i).asn_qty,
              actual_mass                     = a_tbl_update(i).actual_mass,
              carton_status_code              = a_tbl_update(i).carton_status_code,
              carton_status_desc              = a_tbl_update(i).carton_status_desc,
              sdn_qty                         = a_tbl_update(i).sdn_qty,
              po_not_before_date              = a_tbl_update(i).po_not_before_date,
              contract_no                     = a_tbl_update(i).contract_no,
              actl_rcpt_date                  = a_tbl_update(i).actl_rcpt_date,
              supp_pack_size                  = a_tbl_update(i).supp_pack_size,
              cancelled_qty                   = a_tbl_update(i).cancelled_qty,
              auto_receipt_code               = a_tbl_update(i).auto_receipt_code,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              to_loc_debt_comm_perc           = a_tbl_update(i).to_loc_debt_comm_perc,
              final_loc_no                    = a_tbl_update(i).final_loc_no,
              from_loc_debt_comm_perc         = a_tbl_update(i).from_loc_debt_comm_perc,
              CHAIN_CODE                      = a_tbl_update(i).chain_code
              
              
       where  shipment_no                     = a_tbl_update(i).shipment_no
       and    seq_no                          = a_tbl_update(i).seq_no
       and    item_no                         = a_tbl_update(i).item_no;

 




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
    l_text := 'LOAD OF fnd_rtl_shipment_ap EX OM STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
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
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_fnd_corp_612u_merge;