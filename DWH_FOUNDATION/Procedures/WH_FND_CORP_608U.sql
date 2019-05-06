--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_608U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_608U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Load Purchase Order fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_rtl_purchase_order_cpy
--               Output - fnd_rtl_purchase_order
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  11 dec 2012 - qc4807 - Adding an additional measure to RMS PO interface STG_RMS_RTL_PURCHASE_ORDER.
--  08 Aug 2014 - qc5028 - Adding 2 additional measures (Import Reporting requirement)
--  Wendy lyttle 26 aug 2016 -- Due to business process not being followed, 
--                               we have to hospitalise any purchase_orders which have
--                             been changed from DJ to NON-DJ and vice versa.
--                           -- This is at po level.
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
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_rtl_purchase_order_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_purchase_order%rowtype;
g_sys_bat            number        := 0;

g_found              boolean;
g_insert_rec         boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_608U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PURCHASE_ORDER FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_purchase_order%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_purchase_order%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_rtl_purchase_order_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_rtl_purchase_order_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_stg_rms_rtl_purchase_order is
   select a.*, b.po_no rej_rec
   from dwh_foundation.stg_rms_rtl_purchase_order_cpy a, dwh_foundation.TEMP_DEL_PURCHASE_ORDER b
   WHERE 
   SYS_PROCESS_CODE = 'N'
   and
   a.po_no = b.po_no(+)
      --              AND (PO_NO >= 3000000 OR PO_NO IS NULL) -- INTERIM MEASURE TO PREVENT OLD PO'S BEING OVERWRITTEN WITH RE-CYLCLED PO'S
   order by sys_source_batch_id,sys_source_sequence_no;

g_rec_in             c_stg_rms_rtl_purchase_order%rowtype;
-- For input bulk collect --
type stg_array is table of c_stg_rms_rtl_purchase_order%rowtype;
a_stg_input      stg_array;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                     := 'N';
   g_rec_out.po_no                                := g_rec_in.po_no;
   g_rec_out.item_no                              := g_rec_in.item_no;
   g_rec_out.location_no                          := g_rec_in.location_no;
   g_rec_out.into_store_date                      := g_rec_in.into_store_date;
   g_rec_out.not_before_date                      := g_rec_in.not_before_date;
   g_rec_out.supplier_no                          := g_rec_in.supplier_no;
   g_rec_out.not_after_date                       := g_rec_in.not_after_date;
   g_rec_out.close_date                           := g_rec_in.close_date;
   g_rec_out.po_status_code                       := g_rec_in.po_status_code;
   g_rec_out.orig_approval_date                   := g_rec_in.orig_approval_date;
   g_rec_out.po_type                              := g_rec_in.po_type;
   g_rec_out.contract_no                          := g_rec_in.contract_no;
   g_rec_out.fd_discipline_type                   := g_rec_in.fd_discipline_type;
   g_rec_out.ref_item_no                          := g_rec_in.ref_item_no;
   g_rec_out.supp_pack_size                       := g_rec_in.supp_pack_size;
   g_rec_out.merch_class_no                       := g_rec_in.merch_class_no;
   g_rec_out.reg_rsp                              := g_rec_in.reg_rsp;
   g_rec_out.original_po_qty                      := g_rec_in.original_po_qty;
   g_rec_out.amended_po_qty                       := g_rec_in.amended_po_qty;
   g_rec_out.grn_qty                              := g_rec_in.grn_qty;
   g_rec_out.cancel_po_qty                        := g_rec_in.cancel_po_qty;
   g_rec_out.cancel_code                          := g_rec_in.cancel_code;
   g_rec_out.cancel_code_desc                     := g_rec_in.cancel_code_desc;
   g_rec_out.cost_price                           := g_rec_in.cost_price;
   g_rec_out.supply_chain_type                    := g_rec_in.supply_chain_type;
   g_rec_out.transfer_po_link_no                  := g_rec_in.transfer_po_link_no;
   g_rec_out.sell_by_date                         := g_rec_in.sell_by_date;
   g_rec_out.min_sell_by_date                     := g_rec_in.min_sell_by_date;
   g_rec_out.max_sell_by_date                     := g_rec_in.max_sell_by_date;
   g_rec_out.case_mass                            := g_rec_in.case_mass;
   g_rec_out.tolerance_down_mass                  := g_rec_in.tolerance_down_mass;
   g_rec_out.tolerance_up_mass                    := g_rec_in.tolerance_up_mass;
   g_rec_out.static_mass                          := g_rec_in.static_mass;
   g_rec_out.otb_eom_date                         := g_rec_in.otb_eom_date;
   g_rec_out.ext_po_alloc_no                      := g_rec_in.ext_po_alloc_no;
   g_rec_out.scale_priority_code                  := g_rec_in.scale_priority_code;
   g_rec_out.random_mass_unit_cost                := g_rec_in.random_mass_unit_cost;
   g_rec_out.comment_desc                         := g_rec_in.comment_desc;
   g_rec_out.distribute_from_location_no          := nvl(g_rec_in.distribute_from_location_no, g_rec_in.location_no);
   g_rec_out.pre_alloc_ind                        := g_rec_in.pre_alloc_ind;
   g_rec_out.last_updated_date                    := g_date;
   g_rec_out.rejected_cases                       := 0;
   g_rec_out.rejected_barcode                     := 0;
   g_rec_out.rejected_qty_per_lug                 := 0;
   g_rec_out.rejected_sell_price                  := 0;
   g_rec_out.rejected_ideal_temp                  := 0;
   g_rec_out.rejected_max_temp_range              := 0;
   g_rec_out.rejected_alt_supp                    := 0;
   g_rec_out.rejected_over_delivery               := 0;
   g_rec_out.rejected_tolerance_mass              := 0;
   g_rec_out.rejected_out_case                    := 0;
   g_rec_out.rejected_num_sell_by_days            := 0;

   if g_rec_in.cancel_code is null then
      g_rec_out.buyer_edited_po_qty               := null;
      g_rec_out.buyer_edited_cancel_po_qty        := null;
      g_rec_out.buyer_edited_cancel_code          := null;
   else
      if g_rec_in.cancel_code = 'B' then
         g_rec_out.buyer_edited_po_qty            := g_rec_out.amended_po_qty;
         g_rec_out.buyer_edited_cancel_po_qty     := g_rec_out.cancel_po_qty;
         g_rec_out.buyer_edited_cancel_code       := g_rec_out.cancel_code;
      end if;
   end if;

-- qc 5028
   g_rec_out.currency_code                        := g_rec_in.currency_code;
   g_rec_out.import_unit_cost                     := g_rec_in.import_unit_cost;
   g_rec_out.chain_code                           := g_rec_in.chain_code;

--   if not  dwh_valid.fnd_calendar(g_rec_out.not_before_date) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_date_not_found;
--     l_text          := dwh_constants.vc_date_not_found||g_rec_out.not_before_date ;
--     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   end if;

   if not dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_supplier_not_found;
     l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if g_rec_in.rej_rec is not null then
     g_hospital      := 'Y';
     g_hospital_text := 'PO chain_code changed';
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

   g_rec_in.sys_load_date          := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_rms_rtl_purchase_order_hsp values 
   (g_rec_in.SYS_SOURCE_BATCH_ID
, g_rec_in.SYS_SOURCE_SEQUENCE_NO
, g_rec_in.SYS_LOAD_DATE
, g_rec_in.SYS_PROCESS_CODE
, g_rec_in.SYS_LOAD_SYSTEM_NAME
, g_rec_in.SYS_MIDDLEWARE_BATCH_ID
, g_rec_in.SYS_PROCESS_MSG
, g_rec_in.PO_NO
, g_rec_in.ITEM_NO
, g_rec_in.LOCATION_NO
, g_rec_in.INTO_STORE_DATE
, g_rec_in.NOT_BEFORE_DATE
, g_rec_in.SUPPLIER_NO
, g_rec_in.NOT_AFTER_DATE
, g_rec_in.CLOSE_DATE
, g_rec_in.PO_STATUS_CODE
, g_rec_in.ORIG_APPROVAL_DATE
, g_rec_in.PO_TYPE
, g_rec_in.CONTRACT_NO
, g_rec_in.FD_DISCIPLINE_TYPE
, g_rec_in.REF_ITEM_NO
, g_rec_in.SUPP_PACK_SIZE
, g_rec_in.MERCH_CLASS_NO
, g_rec_in.REG_RSP
, g_rec_in.ORIGINAL_PO_QTY
, g_rec_in.AMENDED_PO_QTY
, g_rec_in.GRN_QTY
, g_rec_in.CANCEL_PO_QTY
, g_rec_in.CANCEL_CODE
, g_rec_in.CANCEL_CODE_DESC
, g_rec_in.COST_PRICE
, g_rec_in.SUPPLY_CHAIN_TYPE
, g_rec_in.TRANSFER_PO_LINK_NO
, g_rec_in.SELL_BY_DATE
, g_rec_in.MIN_SELL_BY_DATE
, g_rec_in.MAX_SELL_BY_DATE
, g_rec_in.CASE_MASS
, g_rec_in.TOLERANCE_DOWN_MASS
, g_rec_in.TOLERANCE_UP_MASS
, g_rec_in.STATIC_MASS
, g_rec_in.OTB_EOM_DATE
, g_rec_in.EXT_PO_ALLOC_NO
, g_rec_in.SCALE_PRIORITY_CODE
, g_rec_in.RANDOM_MASS_UNIT_COST
, g_rec_in.COMMENT_DESC
, g_rec_in.DISTRIBUTE_FROM_LOCATION_NO
, g_rec_in.SOURCE_DATA_STATUS_CODE
, g_rec_in.PRE_ALLOC_IND
, g_rec_in.CURRENCY_CODE
, g_rec_in.IMPORT_UNIT_COST
, g_rec_in.CHAIN_CODE);
   g_recs_hospital := g_recs_hospital  + sql%rowcount;

  exception
      when dwh_errors.e_insert_error   then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into fnd_rtl_purchase_order values 
       (a_tbl_insert(i).PO_NO
, a_tbl_insert(i).ITEM_NO
, a_tbl_insert(i).LOCATION_NO
, a_tbl_insert(i).INTO_STORE_DATE
, a_tbl_insert(i).NOT_BEFORE_DATE
, a_tbl_insert(i).SUPPLIER_NO
, a_tbl_insert(i).NOT_AFTER_DATE
, a_tbl_insert(i).CLOSE_DATE
, a_tbl_insert(i).PO_STATUS_CODE
, a_tbl_insert(i).ORIG_APPROVAL_DATE
, a_tbl_insert(i).PO_TYPE
, a_tbl_insert(i).CONTRACT_NO
, a_tbl_insert(i).FD_DISCIPLINE_TYPE
, a_tbl_insert(i).REF_ITEM_NO
, a_tbl_insert(i).SUPP_PACK_SIZE
, a_tbl_insert(i).MERCH_CLASS_NO
, a_tbl_insert(i).REG_RSP
, a_tbl_insert(i).ORIGINAL_PO_QTY
, a_tbl_insert(i).AMENDED_PO_QTY
, a_tbl_insert(i).GRN_QTY
, a_tbl_insert(i).CANCEL_PO_QTY
, a_tbl_insert(i).CANCEL_CODE
, a_tbl_insert(i).CANCEL_CODE_DESC
, a_tbl_insert(i).COST_PRICE
, a_tbl_insert(i).SUPPLY_CHAIN_TYPE
, a_tbl_insert(i).TRANSFER_PO_LINK_NO
, a_tbl_insert(i).SELL_BY_DATE
, a_tbl_insert(i).MIN_SELL_BY_DATE
, a_tbl_insert(i).MAX_SELL_BY_DATE
, a_tbl_insert(i).CASE_MASS
, a_tbl_insert(i).TOLERANCE_DOWN_MASS
, a_tbl_insert(i).TOLERANCE_UP_MASS
, a_tbl_insert(i).STATIC_MASS
, a_tbl_insert(i).REJECTED_CASES
, a_tbl_insert(i).ACTL_TEMPERATURE
, a_tbl_insert(i).MASS_AUDIT_IND
, a_tbl_insert(i).REJECT_IND
, a_tbl_insert(i).IN_TIME
, a_tbl_insert(i).OUT_TIME
, a_tbl_insert(i).CALC_TOLERANCE
, a_tbl_insert(i).SUPPLIER_DELIV_NO
, a_tbl_insert(i).OTB_EOM_DATE
, a_tbl_insert(i).EXT_PO_ALLOC_NO
, a_tbl_insert(i).SCALE_PRIORITY_CODE
, a_tbl_insert(i).RANDOM_MASS_UNIT_COST
, a_tbl_insert(i).COMMENT_DESC
, a_tbl_insert(i).DISTRIBUTE_FROM_LOCATION_NO
, a_tbl_insert(i).SOURCE_DATA_STATUS_CODE
, a_tbl_insert(i).LAST_UPDATED_DATE
, a_tbl_insert(i).BUYER_EDITED_PO_QTY
, a_tbl_insert(i).BUYER_EDITED_CANCEL_PO_QTY
, a_tbl_insert(i).BUYER_EDITED_CANCEL_CODE
, a_tbl_insert(i).REJECTED_BARCODE
, a_tbl_insert(i).REJECTED_QTY_PER_LUG
, a_tbl_insert(i).REJECTED_NUM_SELL_BY_DAYS
, a_tbl_insert(i).REJECTED_SELL_PRICE
, a_tbl_insert(i).REJECTED_IDEAL_TEMP
, a_tbl_insert(i).REJECTED_MAX_TEMP_RANGE
, a_tbl_insert(i).REJECTED_ALT_SUPP
, a_tbl_insert(i).REJECTED_OVER_DELIVERY
, a_tbl_insert(i).REJECTED_TOLERANCE_MASS
, a_tbl_insert(i).REJECTED_OUT_CASE
, a_tbl_insert(i).PRE_ALLOC_IND
, a_tbl_insert(i).CURRENCY_CODE
, a_tbl_insert(i).IMPORT_UNIT_COST
, a_tbl_insert(i).CHAIN_CODE);
    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).po_no||
                       ' '||a_tbl_insert(g_error_index).item_no ;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update fnd_rtl_purchase_order
       set    into_store_date                 = a_tbl_update(i).into_store_date,
              not_before_date                 = a_tbl_update(i).not_before_date,
              supplier_no                     = a_tbl_update(i).supplier_no,
              not_after_date                  = a_tbl_update(i).not_after_date,
              close_date                      = a_tbl_update(i).close_date,
              po_status_code                  = a_tbl_update(i).po_status_code,
              orig_approval_date              = a_tbl_update(i).orig_approval_date,
              po_type                         = a_tbl_update(i).po_type,
              contract_no                     = a_tbl_update(i).contract_no,
              fd_discipline_type              = a_tbl_update(i).fd_discipline_type,
              ref_item_no                     = a_tbl_update(i).ref_item_no,
              supp_pack_size                  = a_tbl_update(i).supp_pack_size,
              merch_class_no                  = a_tbl_update(i).merch_class_no,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              original_po_qty                 = a_tbl_update(i).original_po_qty,
              amended_po_qty                  = a_tbl_update(i).amended_po_qty,
              grn_qty                         = a_tbl_update(i).grn_qty,
              cancel_po_qty                   = a_tbl_update(i).cancel_po_qty,
              cancel_code                     = a_tbl_update(i).cancel_code,
              cancel_code_desc                = a_tbl_update(i).cancel_code_desc,
              cost_price                      = a_tbl_update(i).cost_price,
              supply_chain_type               = a_tbl_update(i).supply_chain_type,
              transfer_po_link_no             = a_tbl_update(i).transfer_po_link_no,
              sell_by_date                    = a_tbl_update(i).sell_by_date,
              min_sell_by_date                = a_tbl_update(i).min_sell_by_date,
              max_sell_by_date                = a_tbl_update(i).max_sell_by_date,
              case_mass                       = a_tbl_update(i).case_mass,
              tolerance_down_mass             = a_tbl_update(i).tolerance_down_mass,
              tolerance_up_mass               = a_tbl_update(i).tolerance_up_mass,
              static_mass                     = a_tbl_update(i).static_mass,
              otb_eom_date                    = a_tbl_update(i).otb_eom_date,
              ext_po_alloc_no                 = a_tbl_update(i).ext_po_alloc_no,
              scale_priority_code             = a_tbl_update(i).scale_priority_code,
              random_mass_unit_cost           = a_tbl_update(i).random_mass_unit_cost,
              comment_desc                    = a_tbl_update(i).comment_desc,
              pre_alloc_ind                   = a_tbl_update(i).pre_alloc_ind,
              distribute_from_location_no     = a_tbl_update(i).distribute_from_location_no,
              buyer_edited_po_qty             = decode((nvl(a_tbl_update(i).cancel_code,'B')),
                                                'B',a_tbl_update(i).buyer_edited_po_qty,buyer_edited_po_qty),
              buyer_edited_cancel_po_qty      = decode((nvl(a_tbl_update(i).cancel_code,'B')),
                                                'B',a_tbl_update(i).buyer_edited_cancel_po_qty,buyer_edited_cancel_po_qty),
              buyer_edited_cancel_code        = decode((nvl(a_tbl_update(i).cancel_code,'B')),
                                                'B',a_tbl_update(i).buyer_edited_cancel_code,buyer_edited_cancel_code),
              last_updated_date               = a_tbl_update(i).last_updated_date,
              currency_code                   = a_tbl_update(i).currency_code,
              import_unit_cost                = a_tbl_update(i).import_unit_cost,
              chain_code                      = a_tbl_update(i).chain_code
       where  po_no                           = a_tbl_update(i).po_no and
              item_no                         = a_tbl_update(i).item_no and
              location_no                     = a_tbl_update(i).location_no;

       g_recs_updated    := g_recs_updated  + a_tbl_update.count;

-- !!! EXPLANATION FOR THE DECODE IN THE UPDATE STATEMENT !!!
-- If the input cancel_code is null or B, then the 3 buyer_edited_ columns must be updated,
-- else if the input cancel_code is A (or anything else) the 3 buyer_edited_ must stay as is.

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).po_no||
                       ' '||a_tbl_update(g_error_index).item_no   ;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_rms_rtl_purchase_order_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_staging_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_rtl_purchase_order
   where  po_no            = g_rec_out.po_no  and
          item_no          = g_rec_out.item_no and
          location_no      = g_rec_out.location_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).po_no         = g_rec_out.po_no and
            a_tbl_insert(i).item_no       = g_rec_out.item_no and
            a_tbl_insert(i).location_no   = g_rec_out.location_no then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
      local_bulk_staging_update;
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;
      commit;
   end if;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

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

    l_text := 'LOAD OF FND_RTL_PURCHASE_ORDER EX OM STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
 --   G_DATE := '26 AUG 2016';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    select distinct sys_source_batch_id into g_sys_bat
    from dwh_foundation.stg_rms_rtl_purchase_order_cpy
    ;
    l_text := 'Sys_source_batch_id='||g_sys_bat;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Due to business process not being followed, we have to hospitalise any comntracts which have
-- been changed from DJ to NON-DJ and vice versa.
--**************************************************************************************************
    l_text := 'Truncate TEMP_DEL_PURCHASE_ORDER - Recs Rejected Chain_code change = '||sql%rowcount;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate('truncate table dwh_foundation.TEMP_DEL_PURCHASE_ORDER');
    commit;

    l_text := 'Insert started TEMP_DEL_PURCHASE_ORDER' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    insert /*+ append */ into dwh_foundation.TEMP_DEL_PURCHASE_ORDER
    with selstg as (select distinct po_no, chain_code 
                    from dwh_foundation.stg_rms_rtl_purchase_order_cpy),
         selfnd as (select distinct a.po_no, a.chain_code 
                    from fnd_rtl_purchase_order a, selstg b
                    where a.po_no = b.po_no),
         selchkstg as (select po_no, count(*) 
                       from selstg 
                       group by po_no
                       having count(*) > 1),
         selchkfnd as (select po_no, count(*) 
                       from (select * from selstg union select *from selfnd)
                       group by po_no 
                       having count(*) > 1)
    select po_no, g_date from selchkstg
    union
    select po_no, g_date from selchkfnd;
    l_text := 'TEMP_DEL_PURCHASE_ORDER - Recs Rejected Chain_code change = '||sql%rowcount;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_rtl_purchase_order;
    fetch c_stg_rms_rtl_purchase_order bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_stg_rms_rtl_purchase_order bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_rtl_purchase_order;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;

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

end wh_fnd_corp_608u;
