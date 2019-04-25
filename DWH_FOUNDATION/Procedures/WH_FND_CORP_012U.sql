--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_012U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_012U" (p_forall_limit in integer,p_success out boolean) as
 

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Sean Le Roux
--  Purpose:     Update supplier unit dimension table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      AIT load - stg_rms_supplier
--               Input    - stg_rms_supplier_cpy
--               Output   - fnd_supplier
--  Packages:    dwh_constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  November 2017 -- Additiion of supplier_company_code for multi-currency (Bhavesh Valodia)
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  1000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_supplier_hsp.sys_process_msg%type;
g_rec_out            fnd_supplier%rowtype;
--g_rec_in             stg_rms_supplier_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_012U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE THE SUPPLIER MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
--type stg_array is table of stg_rms_supplier_cpy%rowtype;
--a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_supplier%rowtype index by binary_integer;
type tbl_array_u is table of fnd_supplier%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_supplier_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_supplier_cpy.sys_source_sequence_no%type 
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_rms_supplier is
   select A.*, B.TRADING_ENTITY_CODE
   from stg_rms_supplier_cpy A, FND_TRADING_ENTITY B
   where sys_process_code = 'N'
   AND SUPPLIER_COMPANY_CODE = TRADING_ENTITY_CODE(+)
   order by sys_source_batch_id,sys_source_sequence_no;


g_rec_in             c_stg_rms_supplier%rowtype;
-- For input bulk collect --
type stg_array is table of c_stg_rms_supplier%rowtype;
a_stg_input      stg_array;
-- order by only where sequencing is essential to the correct loading of data
   
--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.supplier_name                   := g_rec_in.supplier_name;
   g_rec_out.vat_region_no                   := g_rec_in.vat_region_no;
   g_rec_out.contact_name                    := g_rec_in.contact_name;
   g_rec_out.contact_email_address           := g_rec_in.contact_email_address;
   g_rec_out.contact_phone                   := g_rec_in.contact_phone;
   g_rec_out.contact_fax                     := g_rec_in.contact_fax;
   g_rec_out.contact_pager                   := g_rec_in.contact_pager;
   g_rec_out.contact_telex                   := g_rec_in.contact_telex;
   g_rec_out.local_import_code               := g_rec_in.local_import_code;
   g_rec_out.currency_code                   := g_rec_in.currency_code;
   g_rec_out.deal_mngmt_code                 := g_rec_in.deal_mngmt_code;
   g_rec_out.fd_supplier_short_code          := g_rec_in.fd_supplier_short_code;
   g_rec_out.fax_priority_code               := g_rec_in.fax_priority_code;
   g_rec_out.fd_fax_plan_ind                 := g_rec_in.fd_fax_plan_ind;
   g_rec_out.fd_fax_plan_pattern_code        := g_rec_in.fd_fax_plan_pattern_code;
   g_rec_out.fd_fax_order_ind                := g_rec_in.fd_fax_order_ind;
   g_rec_out.fd_fax_order_pattern_code       := g_rec_in.fd_fax_order_pattern_code;
   g_rec_out.quality_control_ind             := g_rec_in.quality_control_ind;
   g_rec_out.vendor_control_ind              := g_rec_in.vendor_control_ind;
   g_rec_out.vmi_order_status_code           := g_rec_in.vmi_order_status_code;
   g_rec_out.merch_order_split_code          := g_rec_in.merch_order_split_code;
   g_rec_out.loc_order_split_code            := g_rec_in.loc_order_split_code;
   g_rec_out.num_deliv_win_wh_days           := g_rec_in.num_deliv_win_wh_days;
   g_rec_out.num_deliv_win_flow_days         := g_rec_in.num_deliv_win_flow_days;
   g_rec_out.num_deliv_win_dsd_days          := g_rec_in.num_deliv_win_dsd_days;
   g_rec_out.num_deliv_win_xd_days           := g_rec_in.num_deliv_win_xd_days;
   g_rec_out.freight_terms_id                := g_rec_in.freight_terms_id;
   g_rec_out.freight_charge_ind              := g_rec_in.freight_charge_ind;
   g_rec_out.shipment_method_id              := g_rec_in.shipment_method_id;
   g_rec_out.shipment_pre_mark_ind           := g_rec_in.shipment_pre_mark_ind;
   g_rec_out.delivery_policy_code            := g_rec_in.delivery_policy_code;
   g_rec_out.delivery_strategy_no            := g_rec_in.delivery_strategy_no;
   g_rec_out.supp_dsd_ind                    := g_rec_in.supp_dsd_ind;
   g_rec_out.store_dsd_ind                   := g_rec_in.store_dsd_ind;
   g_rec_out.backorder_ind                   := g_rec_in.backorder_ind;
   g_rec_out.inv_mngmt_level_code            := g_rec_in.inv_mngmt_level_code;
   g_rec_out.ww_wh_stock_ind                 := g_rec_in.ww_wh_stock_ind;
   g_rec_out.rtn_accept_ind                  := g_rec_in.rtn_accept_ind;
   g_rec_out.rtn_auth_no_req_ind             := g_rec_in.rtn_auth_no_req_ind;
   g_rec_out.rtv_wh_no                       := g_rec_in.rtv_wh_no;
   g_rec_out.debit_memo_code                 := g_rec_in.debit_memo_code;
   g_rec_out.debit_memo_auto_aprv_ind        := g_rec_in.debit_memo_auto_aprv_ind;
   g_rec_out.invc_match_ind                  := g_rec_in.invc_match_ind;
   g_rec_out.invc_prepay_ind                 := g_rec_in.invc_prepay_ind;
   g_rec_out.invc_auto_appr_ind              := g_rec_in.invc_auto_appr_ind;
   g_rec_out.invc_receipt_loc_type           := g_rec_in.invc_receipt_loc_type;
   g_rec_out.invc_gross_net_code             := g_rec_in.invc_gross_net_code;
   g_rec_out.edi_po_ind                      := g_rec_in.edi_po_ind;
   g_rec_out.edi_po_change_ind               := g_rec_in.edi_po_change_ind;
   g_rec_out.edi_po_confirm_ind              := g_rec_in.edi_po_confirm_ind;
   g_rec_out.edi_asn_ind                     := g_rec_in.edi_asn_ind;
   g_rec_out.edi_sales_rpt_freq_code         := g_rec_in.edi_sales_rpt_freq_code;
   g_rec_out.edi_supp_availability_ind       := g_rec_in.edi_supp_availability_ind;
   g_rec_out.edi_contract_ind                := g_rec_in.edi_contract_ind;
   g_rec_out.edi_invc_ind                    := g_rec_in.edi_invc_ind;
   g_rec_out.edi_channel_no                  := g_rec_in.edi_channel_no;
   g_rec_out.edi_cost_chg_var_perc           := g_rec_in.edi_cost_chg_var_perc;
   g_rec_out.edi_cost_chg_var_amt            := g_rec_in.edi_cost_chg_var_amt;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.supplier_company_code          := g_rec_in.supplier_company_code;

--    UN-COMMENT BELOW 4 LINES WHEN LIVE DATA IS FLOWING  ---
--   if g_rec_in.TRADING_ENTITY_CODE IS NULL   then g_hospital      := 'Y';
--     g_hospital_text := 'SUPPLIER_COMPANY_CODE  NULL-INVALID';
--     return;
--   end if;        
 
--   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--     return;
--   end if;        
      
   if not dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_supplier_not_found;
     return;
   end if; 

-- Indicator checking to determine if 0(zero) or 1(one)

   if not dwh_valid.indicator_field(g_rec_out.fd_fax_plan_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.fd_fax_order_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.quality_control_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.vendor_control_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.freight_charge_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;
   
    if not dwh_valid.indicator_field(g_rec_out.shipment_pre_mark_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.supp_dsd_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.store_dsd_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

     if not dwh_valid.indicator_field(g_rec_out.backorder_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.ww_wh_stock_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.rtn_accept_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;
   
    if not dwh_valid.indicator_field(g_rec_out.rtn_auth_no_req_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.debit_memo_auto_aprv_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.invc_match_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.invc_prepay_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.invc_auto_appr_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.edi_po_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.edi_po_change_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.edi_po_confirm_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;
   
    if not dwh_valid.indicator_field(g_rec_out.edi_asn_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.edi_supp_availability_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.edi_contract_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

     if not dwh_valid.indicator_field(g_rec_out.edi_invc_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
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
   
   insert into stg_rms_supplier_hsp values ( G_REC_IN.SYS_SOURCE_BATCH_ID
, G_REC_IN.SYS_SOURCE_SEQUENCE_NO
, G_REC_IN.SYS_LOAD_DATE
, G_REC_IN.SYS_PROCESS_CODE
, G_REC_IN.SYS_LOAD_SYSTEM_NAME
, G_REC_IN.SYS_MIDDLEWARE_BATCH_ID
, G_REC_IN.SYS_PROCESS_MSG
, G_REC_IN.SUPPLIER_NO
, G_REC_IN.SUPPLIER_NAME
, G_REC_IN.VAT_REGION_NO
, G_REC_IN.CONTACT_NAME
, G_REC_IN.CONTACT_EMAIL_ADDRESS
, G_REC_IN.CONTACT_PHONE
, G_REC_IN.CONTACT_FAX
, G_REC_IN.CONTACT_PAGER
, G_REC_IN.CONTACT_TELEX
, G_REC_IN.LOCAL_IMPORT_CODE
, G_REC_IN.CURRENCY_CODE
, G_REC_IN.DEAL_MNGMT_CODE
, G_REC_IN.FD_SUPPLIER_SHORT_CODE
, G_REC_IN.FAX_PRIORITY_CODE
, G_REC_IN.FD_FAX_PLAN_IND
, G_REC_IN.FD_FAX_PLAN_PATTERN_CODE
, G_REC_IN.FD_FAX_ORDER_IND
, G_REC_IN.FD_FAX_ORDER_PATTERN_CODE
, G_REC_IN.QUALITY_CONTROL_IND
, G_REC_IN.VENDOR_CONTROL_IND
, G_REC_IN.VMI_ORDER_STATUS_CODE
, G_REC_IN.MERCH_ORDER_SPLIT_CODE
, G_REC_IN.LOC_ORDER_SPLIT_CODE
, G_REC_IN.NUM_DELIV_WIN_WH_DAYS
, G_REC_IN.NUM_DELIV_WIN_FLOW_DAYS
, G_REC_IN.NUM_DELIV_WIN_DSD_DAYS
, G_REC_IN.NUM_DELIV_WIN_XD_DAYS
, G_REC_IN.FREIGHT_TERMS_ID
, G_REC_IN.FREIGHT_CHARGE_IND
, G_REC_IN.SHIPMENT_METHOD_ID
, G_REC_IN.SHIPMENT_PRE_MARK_IND
, G_REC_IN.DELIVERY_POLICY_CODE
, G_REC_IN.DELIVERY_STRATEGY_NO
, G_REC_IN.SUPP_DSD_IND
, G_REC_IN.STORE_DSD_IND
, G_REC_IN.BACKORDER_IND
, G_REC_IN.INV_MNGMT_LEVEL_CODE
, G_REC_IN.WW_WH_STOCK_IND
, G_REC_IN.RTN_ACCEPT_IND
, G_REC_IN.RTN_AUTH_NO_REQ_IND
, G_REC_IN.RTV_WH_NO
, G_REC_IN.DEBIT_MEMO_CODE
, G_REC_IN.DEBIT_MEMO_AUTO_APRV_IND
, G_REC_IN.INVC_MATCH_IND
, G_REC_IN.INVC_PREPAY_IND
, G_REC_IN.INVC_AUTO_APPR_IND
, G_REC_IN.INVC_RECEIPT_LOC_TYPE
, G_REC_IN.INVC_GROSS_NET_CODE
, G_REC_IN.EDI_PO_IND
, G_REC_IN.EDI_PO_CHANGE_IND
, G_REC_IN.EDI_PO_CONFIRM_IND
, G_REC_IN.EDI_ASN_IND
, G_REC_IN.EDI_SALES_RPT_FREQ_CODE
, G_REC_IN.EDI_SUPP_AVAILABILITY_IND
, G_REC_IN.EDI_CONTRACT_IND
, G_REC_IN.EDI_INVC_IND
, G_REC_IN.EDI_CHANNEL_NO
, G_REC_IN.EDI_COST_CHG_VAR_PERC
, G_REC_IN.EDI_COST_CHG_VAR_AMT
, G_REC_IN.SOURCE_DATA_STATUS_CODE
, G_REC_IN.SUPPLIER_COMPANY_CODE
);
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

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into fnd_supplier values a_tbl_insert(i);
       
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
                       ' '||a_tbl_insert(g_error_index).supplier_no;
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
       update fnd_supplier
       set    supplier_name                   = a_tbl_update(i).supplier_name,
              vat_region_no                   = a_tbl_update(i).vat_region_no,
              contact_name                    = a_tbl_update(i).contact_name,
              contact_email_address           = a_tbl_update(i).contact_email_address,
              contact_phone                   = a_tbl_update(i).contact_phone,
              contact_fax                     = a_tbl_update(i).contact_fax,
              contact_pager                   = a_tbl_update(i).contact_pager,
              contact_telex                   = a_tbl_update(i).contact_telex,
              local_import_code               = a_tbl_update(i).local_import_code,
              currency_code                   = a_tbl_update(i).currency_code,
              deal_mngmt_code                 = a_tbl_update(i).deal_mngmt_code,
              fd_supplier_short_code          = a_tbl_update(i).fd_supplier_short_code,
              fax_priority_code               = a_tbl_update(i).fax_priority_code,
              fd_fax_plan_ind                 = a_tbl_update(i).fd_fax_plan_ind,
              fd_fax_plan_pattern_code        = a_tbl_update(i).fd_fax_plan_pattern_code,
              fd_fax_order_ind                = a_tbl_update(i).fd_fax_order_ind,
              fd_fax_order_pattern_code       = a_tbl_update(i).fd_fax_order_pattern_code,
              quality_control_ind             = a_tbl_update(i).quality_control_ind,
              vendor_control_ind              = a_tbl_update(i).vendor_control_ind,
              vmi_order_status_code           = a_tbl_update(i).vmi_order_status_code,
              merch_order_split_code          = a_tbl_update(i).merch_order_split_code,
              loc_order_split_code            = a_tbl_update(i).loc_order_split_code,
              num_deliv_win_wh_days           = a_tbl_update(i).num_deliv_win_wh_days,
              num_deliv_win_flow_days         = a_tbl_update(i).num_deliv_win_flow_days,
              num_deliv_win_dsd_days          = a_tbl_update(i).num_deliv_win_dsd_days,
              num_deliv_win_xd_days           = a_tbl_update(i).num_deliv_win_xd_days,
              freight_terms_id                = a_tbl_update(i).freight_terms_id,
              freight_charge_ind              = a_tbl_update(i).freight_charge_ind,
              shipment_method_id              = a_tbl_update(i).shipment_method_id,
              shipment_pre_mark_ind           = a_tbl_update(i).shipment_pre_mark_ind,
              delivery_policy_code            = a_tbl_update(i).delivery_policy_code,
              delivery_strategy_no            = a_tbl_update(i).delivery_strategy_no,
              supp_dsd_ind                    = a_tbl_update(i).supp_dsd_ind,
              store_dsd_ind                   = a_tbl_update(i).store_dsd_ind,
              backorder_ind                   = a_tbl_update(i).backorder_ind,
              inv_mngmt_level_code            = a_tbl_update(i).inv_mngmt_level_code,
              ww_wh_stock_ind                 = a_tbl_update(i).ww_wh_stock_ind,
              rtn_accept_ind                  = a_tbl_update(i).rtn_accept_ind,
              rtn_auth_no_req_ind             = a_tbl_update(i).rtn_auth_no_req_ind,
              rtv_wh_no                       = a_tbl_update(i).rtv_wh_no,
              debit_memo_code                 = a_tbl_update(i).debit_memo_code,
              debit_memo_auto_aprv_ind        = a_tbl_update(i).debit_memo_auto_aprv_ind,
              invc_match_ind                  = a_tbl_update(i).invc_match_ind,
              invc_prepay_ind                 = a_tbl_update(i).invc_prepay_ind,
              invc_auto_appr_ind              = a_tbl_update(i).invc_auto_appr_ind,
              invc_receipt_loc_type           = a_tbl_update(i).invc_receipt_loc_type,
              invc_gross_net_code             = a_tbl_update(i).invc_gross_net_code,
              edi_po_ind                      = a_tbl_update(i).edi_po_ind,
              edi_po_change_ind               = a_tbl_update(i).edi_po_change_ind,
              edi_po_confirm_ind              = a_tbl_update(i).edi_po_confirm_ind,
              edi_asn_ind                     = a_tbl_update(i).edi_asn_ind,
              edi_sales_rpt_freq_code         = a_tbl_update(i).edi_sales_rpt_freq_code,
              edi_supp_availability_ind       = a_tbl_update(i).edi_supp_availability_ind,
              edi_contract_ind                = a_tbl_update(i).edi_contract_ind,
              edi_invc_ind                    = a_tbl_update(i).edi_invc_ind,
              edi_channel_no                  = a_tbl_update(i).edi_channel_no,
              edi_cost_chg_var_perc           = a_tbl_update(i).edi_cost_chg_var_perc,
              edi_cost_chg_var_amt            = a_tbl_update(i).edi_cost_chg_var_amt,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date,
              supplier_company_code           = a_tbl_update(i).supplier_company_code              
       where  supplier_no                     = a_tbl_update(i).supplier_no  ;
       
       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
               
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
                       ' '||a_tbl_update(g_error_index).supplier_no;
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
       update stg_rms_supplier_cpy      
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
   
-- Data will only update fnd_supplier table
   g_found := TRUE ;

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
--   if a_count > 1000 then
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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;   
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF FND_SUPPLIER EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_supplier;
    fetch c_stg_rms_supplier bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
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
    fetch c_stg_rms_supplier bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_stg_rms_supplier;
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
end wh_fnd_corp_012u;
