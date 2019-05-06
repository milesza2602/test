--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_010U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_010U" (p_forall_limit in integer,p_success out boolean) as 

--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Supplier master data table in the performance layer
--               with added value ex foundation layer Supplier_master and Address table.
--  Tables:      Input  - fnd_supplier, fnd_supplier_address
--               Output - dim_supplier
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  February 2018 -- Bhavesh Valodia -- Addition of DIM_TRADING_ENTITY table
--  February 2018 -- Wendy Lyttle -- FYI --- no change to code but will use STORE_DSD-IND to derive in_country_suppliers

--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_supplier%rowtype;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_010U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_SUPPLIER EX FND_SUPPLIER & ADDRESS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_supplier%rowtype index by binary_integer;
type tbl_array_u is table of dim_supplier%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-----------------------------------------------------------------------------------------------------------
cursor c_fnd_supplier is
   select fs.*,fsa.address_line_1, fsa.address_line_2, fsa.address_line_3,
               fsa.city_name, fsa.province_state_code, fsa.country_code, fsa.postal_code,
               fsa.oracle_vendor_site_no, fsa.module_type
--               , dtr.trading_entity_code
   from   fnd_supplier fs,
          fnd_supplier_address fsa 
--          fnd_trading_entity dtr
   where  fs.supplier_no = fsa.supplier_no(+) and
           ((fsa.address_type = '04'   and fsa.primary_address_ind = 1) or 
            (fsa.address_type is null  and fsa.primary_address_ind is null));
--            and fs.supplier_company_code = dtr.trading_entity_code;


-----------------------------------------------------------------------------------------------------------
-- The input record and array are now cursor rowtype allowing for joins in cursor select to put all fields
-- into the rec in and input array. Much more efficient than row level lookup in address variables section.

g_rec_in             c_fnd_supplier%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_supplier%rowtype;
a_stg_input      stg_array;
-----------------------------------------------------------------------------------------------------------


--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.supplier_name                   := g_rec_in.supplier_name;
   g_rec_out.control_entity_code             := g_rec_in.control_entity_code;
   g_rec_out.supplier_status_code            := g_rec_in.supplier_status_code;
   g_rec_out.last_active_date                := g_rec_in.last_active_date;
   g_rec_out.merchant_type                   := g_rec_in.merchant_type;
   g_rec_out.supplier_type_code              := g_rec_in.supplier_type_code;
   g_rec_out.parent_supplier_no              := g_rec_in.parent_supplier_no;
   g_rec_out.vat_region_no                   := g_rec_in.vat_region_no;
   g_rec_out.vat_no_id                       := g_rec_in.vat_no_id;
   g_rec_out.contact_name                    := g_rec_in.contact_name;
   g_rec_out.contact_email_address           := g_rec_in.contact_email_address;
   g_rec_out.contact_phone                   := g_rec_in.contact_phone;
   g_rec_out.contact_fax                     := g_rec_in.contact_fax;
   g_rec_out.contact_pager                   := g_rec_in.contact_pager;
   g_rec_out.contact_telex                   := g_rec_in.contact_telex;
   g_rec_out.local_import_code               := g_rec_in.local_import_code;
   g_rec_out.currency_code                   := g_rec_in.currency_code;
   g_rec_out.payment_term_code               := g_rec_in.payment_term_code; 
   g_rec_out.settlement_discount_perc        := g_rec_in.settlement_discount_perc;
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
   g_rec_out.prim_ord_address_line_1         := nvl(g_rec_in.address_line_1,' ');
   g_rec_out.prim_ord_address_line_2         := nvl(g_rec_in.address_line_2,' ');
   g_rec_out.prim_ord_address_line_3         := nvl(g_rec_in.address_line_3,' ');
   g_rec_out.prim_ord_city_name              := nvl(g_rec_in.city_name,' ');
   g_rec_out.prim_ord_province_state_code    := nvl(g_rec_in.province_state_code,' ');
   g_rec_out.prim_ord_country_code           := nvl(g_rec_in.country_code,' ');
   g_rec_out.prim_ord_postal_code            := nvl(g_rec_in.postal_code,' ');
   g_rec_out.prim_ord_oracle_vendor_site_no  := nvl(g_rec_in.oracle_vendor_site_no,0);
   g_rec_out.module_type                     := nvl(g_rec_in.module_type,' ');
--   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.trading_entity_code          := g_rec_in.supplier_company_code;
--  g_rec_out.sk1_trading_entity_code  := g_rec_in.trading_entity_code;
  
 
   
   
--------------------------------------------------------- 
-- Added for OLAP purposes                    
---------------------------------------------------------
   g_rec_out.total                      := 'TOTAL';
   g_rec_out.total_desc                 := 'ALL SUPPLIER'; 
   g_rec_out.supplier_long_desc         := g_rec_out.supplier_no||' - '||g_rec_out.supplier_name;   

     
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   
end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into dim_supplier values a_tbl_insert(i);
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
      update dim_supplier
      set    supplier_name                   = a_tbl_update(i).supplier_name,
             control_entity_code             = a_tbl_update(i).control_entity_code,
             supplier_status_code            = a_tbl_update(i).supplier_status_code,
             last_active_date                = a_tbl_update(i).last_active_date,
             merchant_type                   = a_tbl_update(i).merchant_type,
             supplier_type_code              = a_tbl_update(i).supplier_type_code,
             parent_supplier_no              = a_tbl_update(i).parent_supplier_no,
             vat_region_no                   = a_tbl_update(i).vat_region_no,
             vat_no_id                       = a_tbl_update(i).vat_no_id,
             contact_name                    = a_tbl_update(i).contact_name,
             contact_email_address           = a_tbl_update(i).contact_email_address,
             contact_phone                   = a_tbl_update(i).contact_phone,
             contact_fax                     = a_tbl_update(i).contact_fax,
             contact_pager                   = a_tbl_update(i).contact_pager,
             contact_telex                   = a_tbl_update(i).contact_telex,
             local_import_code               = a_tbl_update(i).local_import_code,
             currency_code                   = a_tbl_update(i).currency_code,
             payment_term_code               = a_tbl_update(i).payment_term_code,
             settlement_discount_perc        = a_tbl_update(i).settlement_discount_perc,
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
             prim_ord_address_line_1         = a_tbl_update(i).prim_ord_address_line_1,
             prim_ord_address_line_2         = a_tbl_update(i).prim_ord_address_line_2,
             prim_ord_address_line_3         = a_tbl_update(i).prim_ord_address_line_3,
             prim_ord_city_name              = a_tbl_update(i).prim_ord_city_name,
             prim_ord_province_state_code    = a_tbl_update(i).prim_ord_province_state_code,
             prim_ord_country_code           = a_tbl_update(i).prim_ord_country_code,
             prim_ord_postal_code            = a_tbl_update(i).prim_ord_postal_code,
             prim_ord_oracle_vendor_site_no  = a_tbl_update(i).prim_ord_oracle_vendor_site_no,
             module_type                     = a_tbl_update(i).module_type,
             supplier_long_desc              = a_tbl_update(i).supplier_long_desc,
             total                           = a_tbl_update(i).total,
             total_desc                      = a_tbl_update(i).total_desc,
             last_updated_date               = a_tbl_update(i).last_updated_date,
             trading_entity_code              = a_tbl_update(i).trading_entity_code
--             sk1_trading_entity_code     = a_tbl_update(i).sk1_trading_entity_code
      where  supplier_no                     = a_tbl_update(i).supplier_no  ;

      g_recs_updated := g_recs_updated + a_tbl_update.count;
 
        
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
-- Write valid data out to the supplier master table
--**************************************************************************************************
procedure local_write_output as
 
begin
   g_found := dwh_valid.dim_supplier(g_rec_out.supplier_no);

-- Place record into array for later bulk writing   
   if not g_found then
      g_rec_out.sk1_supplier_no  := supplier_seq.nextval;
--      g_rec_out.sk_from_date  := g_date;
--      g_rec_out.sk_to_date    := dwh_constants.sk_to_date;
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
   
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
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
-- Write Dummy record
--**************************************************************************************************
procedure local_write_dummy as
 
begin

   g_rec_out.supplier_no                     := 0;
   g_rec_out.sk1_supplier_no                 := 0;
   g_rec_out.supplier_name                   :='Dummy supplier';
   g_rec_out.prim_ord_address_line_1         := ' ';
   g_rec_out.prim_ord_address_line_2         := ' ';
   g_rec_out.prim_ord_address_line_3         := ' ';
   g_rec_out.prim_ord_city_name              := ' ';
   g_rec_out.prim_ord_province_state_code    := ' ';
   g_rec_out.prim_ord_country_code           := 'ZA';
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.supplier_long_desc              := g_rec_out.supplier_no||' - '||g_rec_out.supplier_name;   
  g_found := dwh_valid.dim_supplier(0);

   if not g_found then
      insert into dim_supplier values g_rec_out;
      g_recs_inserted := g_recs_inserted + 1; 
   else
      update dim_supplier
      set    supplier_name                  = g_rec_out.supplier_name             ,
             prim_ord_address_line_1        = g_rec_out.prim_ord_address_line_1  ,
             prim_ord_address_line_2        = g_rec_out.prim_ord_address_line_2  ,
             prim_ord_address_line_3        = g_rec_out.prim_ord_address_line_3  ,
             prim_ord_city_name             = g_rec_out.prim_ord_city_name        ,
             prim_ord_province_state_code   = g_rec_out.prim_ord_province_state_code ,
             prim_ord_country_code          = g_rec_out.prim_ord_country_code        ,
             supplier_long_desc             = g_rec_out.supplier_long_desc ,
             last_updated_date              = g_date 
      where  supplier_no                    = g_rec_out.supplier_no ;
      g_recs_updated := g_recs_updated + 1; 
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
      
end local_write_dummy;



--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF DIM_SUPPLIER EX FND_SUPPLIER STARTED AT '||
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
    open c_fnd_supplier;
    fetch c_fnd_supplier bulk collect into a_stg_input limit g_forall_limit;
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
   
         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;
        
      end loop;
    fetch c_fnd_supplier bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_fnd_supplier;
--************************************************************************************************** 
-- At end write out what remains in the arrays
--**************************************************************************************************
  
      local_bulk_insert;
      local_bulk_update;    
      local_write_dummy;       
    

--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
end wh_prf_corp_010u;
