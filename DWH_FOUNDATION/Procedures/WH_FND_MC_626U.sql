--------------------------------------------------------
--  DDL for Procedure WH_FND_MC_626U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MC_626U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Christie Koorts
--  Purpose:     Create the invoice matching fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_mc_invc_matching_cpy
--               Output - fnd_mc_rtl_invoice_matching
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  November 2017 -- Addition of MC fields (Bhavesh Valodia)
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
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_mc_invc_matching_hsp.sys_process_msg%type;
g_rec_out            fnd_mc_rtl_invoice_matching%rowtype;
g_rec_in             stg_rms_mc_invc_matching_cpy%rowtype;
g_found              boolean;
g_insert_rec         boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MC_626U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE INVOICE MATCHING FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_rms_mc_invc_matching_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_mc_rtl_invoice_matching%rowtype index by binary_integer;
type tbl_array_u is table of fnd_mc_rtl_invoice_matching%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_mc_invc_matching_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_mc_invc_matching_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor stg_rms_invoice_matching is
   select *
   from stg_rms_mc_invc_matching_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;
-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';
g_rec_out.DOC_NO                        := g_rec_in.DOC_NO;
g_rec_out.IM_DOC_DETAIL_REASON_CODES_ID := g_rec_in.IM_DOC_DETAIL_REASON_CODES_ID;
g_rec_out.ITEM_NO                       := g_rec_in.ITEM_NO;
g_rec_out.DOC_TYPE                      := g_rec_in.DOC_TYPE;
g_rec_out.DOC_STATUS_CODE               := g_rec_in.DOC_STATUS_CODE;
g_rec_out.ORDER_NO                      := g_rec_in.ORDER_NO;
g_rec_out.LOCATION_NO                   := g_rec_in.LOCATION_NO;
g_rec_out.LOC_TYPE_CODE                 := g_rec_in.LOC_TYPE_CODE;
g_rec_out.DOC_DATE                      := g_rec_in.DOC_DATE;
g_rec_out.DOC_CREATE_DATE               := g_rec_in.DOC_CREATE_DATE;
g_rec_out.SUPPLIER_NO                   := g_rec_in.SUPPLIER_NO;
g_rec_out.SUPPLIER_INV_NO               := g_rec_in.SUPPLIER_INV_NO;
g_rec_out.MATCH_ID                      := g_rec_in.MATCH_ID;
g_rec_out.MATCH_DATE                    := g_rec_in.MATCH_DATE;
g_rec_out.APPROVAL_USER_ID              := g_rec_in.APPROVAL_USER_ID;
g_rec_out.APPROVAL_DATE                 := g_rec_in.APPROVAL_DATE;
g_rec_out.PRE_PAID_IND                  := g_rec_in.PRE_PAID_IND;
g_rec_out.PRE_PAID_USER_ID              := g_rec_in.PRE_PAID_USER_ID;
g_rec_out.POST_DATE                     := g_rec_in.POST_DATE;
g_rec_out.TOTAL_QTY                     := g_rec_in.TOTAL_QTY;
g_rec_out.MANUALLY_PAID_IND             := g_rec_in.MANUALLY_PAID_IND;
g_rec_out.CUSTOM_DOC_REF_ID_1           := g_rec_in.CUSTOM_DOC_REF_ID_1;
g_rec_out.CUSTOM_DOC_REF_ID_2           := g_rec_in.CUSTOM_DOC_REF_ID_2;
g_rec_out.CUSTOM_DOC_REF_ID_3           := g_rec_in.CUSTOM_DOC_REF_ID_3;
g_rec_out.CUSTOM_DOC_REF_ID_4           := g_rec_in.CUSTOM_DOC_REF_ID_4;
g_rec_out.DOC_LAST_UPDATE_USER_ID       := g_rec_in.DOC_LAST_UPDATE_USER_ID;
g_rec_out.DOC_LAST_UPDATE_DATETIME      := g_rec_in.DOC_LAST_UPDATE_DATETIME;
g_rec_out.REF_DOC_NO                    := g_rec_in.REF_DOC_NO;
g_rec_out.REF_AUTH_NO                   := g_rec_in.REF_AUTH_NO;
g_rec_out.PRE_MATCH_COST_IND            := g_rec_in.PRE_MATCH_COST_IND;
g_rec_out.VARIANCE_WITHIN_TOLERANCE     := g_rec_in.VARIANCE_WITHIN_TOLERANCE;
g_rec_out.RESOLUTION_ADJ_TOTAL_QTY      := g_rec_in.RESOLUTION_ADJ_TOTAL_QTY;
g_rec_out.CONSIGNMENT_IND               := g_rec_in.CONSIGNMENT_IND;
g_rec_out.RTV_IND                       := g_rec_in.RTV_IND;
g_rec_out.REASON_ID                     := g_rec_in.REASON_ID;
g_rec_out.DOC_DETAIL_STATUS_CODE        := g_rec_in.DOC_DETAIL_STATUS_CODE;
g_rec_out.MATCHED_COST_IND              := g_rec_in.MATCHED_COST_IND;
g_rec_out.MATCHED_UNITS_IND             := g_rec_in.MATCHED_UNITS_IND;
g_rec_out.ADJUSTED_QTY                  := g_rec_in.ADJUSTED_QTY;
g_rec_out.INVOICE_QTY                   := g_rec_in.INVOICE_QTY;
g_rec_out.RESOLUTION_ADJ_QTY            := g_rec_in.RESOLUTION_ADJ_QTY;
g_rec_out.INV_DETAIL_STATUS_CODE        := g_rec_in.INV_DETAIL_STATUS_CODE;
g_rec_out.DOC_DETAIL_VAT_CODE           := g_rec_in.DOC_DETAIL_VAT_CODE;
g_rec_out.DOC_DETAIL_VAT_RATE_PERC      := g_rec_in.DOC_DETAIL_VAT_RATE_PERC;
g_rec_out.DOC_DTL_COMMNT_CREATE_USER_ID := g_rec_in.DOC_DTL_COMMNT_CREATE_USER_ID;
g_rec_out.SOURCE_DATA_STATUS_CODE       := g_rec_in.SOURCE_DATA_STATUS_CODE;
g_rec_out.ADJUSTED_UNIT_COST_LOCAL      := g_rec_in.ADJUSTED_UNIT_COST_LOCAL;
g_rec_out.ADJUSTED_UNIT_COST_OPR        := g_rec_in.ADJUSTED_UNIT_COST_OPR;
g_rec_out.RESOLUTN_ADJ_UNIT_COST_LOCAL  := g_rec_in.RESOLUTN_ADJ_UNIT_COST_LOCAL;
g_rec_out.RESOLUTN_ADJ_UNIT_COST_OPR    := g_rec_in.RESOLUTN_ADJ_UNIT_COST_OPR;
g_rec_out.RESOLUTN_ADJ_TOTAL_COST_LOCAL := g_rec_in.RESOLUTN_ADJ_TOTAL_COST_LOCAL;
g_rec_out.RESOLUTN_ADJ_TOTAL_COST_OPR   := g_rec_in.RESOLUTN_ADJ_TOTAL_COST_OPR;
g_rec_out.TOTAL_COST_LOCAL              := g_rec_in.TOTAL_COST_LOCAL;
g_rec_out.TOTAL_COST_OPR                := g_rec_in.TOTAL_COST_OPR;
g_rec_out.TOTAL_COST_INCL_VAT_LOCAL     := g_rec_in.TOTAL_COST_INCL_VAT_LOCAL;
g_rec_out.TOTAL_COST_INCL_VAT_OPR       := g_rec_in.TOTAL_COST_INCL_VAT_OPR;
g_rec_out.UNIT_COST_LOCAL               := g_rec_in.UNIT_COST_LOCAL;
g_rec_out.UNIT_COST_OPR                 := g_rec_in.UNIT_COST_OPR;
g_rec_out.LAST_UPDATED_DATE             := g_date;


   if not dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_supplier_not_found;
     l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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

   insert into stg_rms_mc_invc_matching_hsp values g_rec_in;
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
       insert into fnd_mc_rtl_invoice_matching values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).doc_no||
                       ' '||a_tbl_insert(g_error_index).im_doc_detail_reason_codes_id||
                       ' '||a_tbl_insert(g_error_index).item_no;
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
UPDATE fnd_mc_rtl_invoice_matching
SET 
  DOC_TYPE                        = a_tbl_update(i).DOC_TYPE,
  DOC_STATUS_CODE                 = a_tbl_update(i).DOC_STATUS_CODE,
  ORDER_NO                        = a_tbl_update(i).ORDER_NO,
  LOCATION_NO                     = a_tbl_update(i).LOCATION_NO,
  LOC_TYPE_CODE                   = a_tbl_update(i).LOC_TYPE_CODE,
  DOC_DATE                        = a_tbl_update(i).DOC_DATE,
  DOC_CREATE_DATE                 = a_tbl_update(i).DOC_CREATE_DATE,
  SUPPLIER_NO                     = a_tbl_update(i).SUPPLIER_NO,
  SUPPLIER_INV_NO                 = a_tbl_update(i).SUPPLIER_INV_NO,
  MATCH_ID                        = a_tbl_update(i).MATCH_ID,
  MATCH_DATE                      = a_tbl_update(i).MATCH_DATE,
  APPROVAL_USER_ID                = a_tbl_update(i).APPROVAL_USER_ID,
  APPROVAL_DATE                   = a_tbl_update(i).APPROVAL_DATE,
  PRE_PAID_IND                    = a_tbl_update(i).PRE_PAID_IND,
  PRE_PAID_USER_ID                = a_tbl_update(i).PRE_PAID_USER_ID,
  POST_DATE                       = a_tbl_update(i).POST_DATE,
  TOTAL_QTY                       = a_tbl_update(i).TOTAL_QTY,
  MANUALLY_PAID_IND               = a_tbl_update(i).MANUALLY_PAID_IND,
  CUSTOM_DOC_REF_ID_1             = a_tbl_update(i).CUSTOM_DOC_REF_ID_1,
  CUSTOM_DOC_REF_ID_2             = a_tbl_update(i).CUSTOM_DOC_REF_ID_2,
  CUSTOM_DOC_REF_ID_3             = a_tbl_update(i).CUSTOM_DOC_REF_ID_3,
  CUSTOM_DOC_REF_ID_4             = a_tbl_update(i).CUSTOM_DOC_REF_ID_4,
  DOC_LAST_UPDATE_USER_ID         = a_tbl_update(i).DOC_LAST_UPDATE_USER_ID,
  DOC_LAST_UPDATE_DATETIME        = a_tbl_update(i).DOC_LAST_UPDATE_DATETIME,
  REF_DOC_NO                      = a_tbl_update(i).REF_DOC_NO,
  REF_AUTH_NO                     = a_tbl_update(i).REF_AUTH_NO,
  PRE_MATCH_COST_IND              = a_tbl_update(i).PRE_MATCH_COST_IND,
  VARIANCE_WITHIN_TOLERANCE       = a_tbl_update(i).VARIANCE_WITHIN_TOLERANCE,
  RESOLUTION_ADJ_TOTAL_QTY        = a_tbl_update(i).RESOLUTION_ADJ_TOTAL_QTY,
  CONSIGNMENT_IND                 = a_tbl_update(i).CONSIGNMENT_IND,
  RTV_IND                         = a_tbl_update(i).RTV_IND,
  REASON_ID                       = a_tbl_update(i).REASON_ID,
  DOC_DETAIL_STATUS_CODE          = a_tbl_update(i).DOC_DETAIL_STATUS_CODE,
  MATCHED_COST_IND                = a_tbl_update(i).MATCHED_COST_IND,
  MATCHED_UNITS_IND               = a_tbl_update(i).MATCHED_UNITS_IND,
  ADJUSTED_QTY                    = a_tbl_update(i).ADJUSTED_QTY,
  INVOICE_QTY                     = a_tbl_update(i).INVOICE_QTY,
  RESOLUTION_ADJ_QTY              = a_tbl_update(i).RESOLUTION_ADJ_QTY,
  INV_DETAIL_STATUS_CODE          = a_tbl_update(i).INV_DETAIL_STATUS_CODE,
  DOC_DETAIL_VAT_CODE             = a_tbl_update(i).DOC_DETAIL_VAT_CODE,
  DOC_DETAIL_VAT_RATE_PERC        = a_tbl_update(i).DOC_DETAIL_VAT_RATE_PERC,
  DOC_DTL_COMMNT_CREATE_USER_ID   = a_tbl_update(i).DOC_DTL_COMMNT_CREATE_USER_ID,
  SOURCE_DATA_STATUS_CODE         = a_tbl_update(i).SOURCE_DATA_STATUS_CODE,
  ADJUSTED_UNIT_COST_LOCAL        = a_tbl_update(i).ADJUSTED_UNIT_COST_LOCAL,
  ADJUSTED_UNIT_COST_OPR          = a_tbl_update(i).ADJUSTED_UNIT_COST_OPR,
  RESOLUTN_ADJ_UNIT_COST_LOCAL    = a_tbl_update(i).RESOLUTN_ADJ_UNIT_COST_LOCAL,
  RESOLUTN_ADJ_UNIT_COST_OPR      = a_tbl_update(i).RESOLUTN_ADJ_UNIT_COST_OPR,
  RESOLUTN_ADJ_TOTAL_COST_LOCAL   = a_tbl_update(i).RESOLUTN_ADJ_TOTAL_COST_LOCAL,
  RESOLUTN_ADJ_TOTAL_COST_OPR     = a_tbl_update(i).RESOLUTN_ADJ_TOTAL_COST_OPR,
  TOTAL_COST_LOCAL                = a_tbl_update(i).TOTAL_COST_LOCAL,
  TOTAL_COST_OPR                  = a_tbl_update(i).TOTAL_COST_OPR,
  TOTAL_COST_INCL_VAT_LOCAL       = a_tbl_update(i).TOTAL_COST_INCL_VAT_LOCAL,
  TOTAL_COST_INCL_VAT_OPR         = a_tbl_update(i).TOTAL_COST_INCL_VAT_OPR,
  UNIT_COST_LOCAL                 = a_tbl_update(i).UNIT_COST_LOCAL,
  UNIT_COST_OPR                   = a_tbl_update(i).UNIT_COST_OPR,
  LAST_UPDATED_DATE               = a_tbl_update(i).LAST_UPDATED_DATE
WHERE doc_no                      = a_tbl_update(i).doc_no
AND im_doc_detail_reason_codes_id = a_tbl_update(i).im_doc_detail_reason_codes_id
       and    item_no = a_tbl_update(i).item_no;

       g_recs_updated := g_recs_updated  + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).doc_no||
                       ' '||a_tbl_update(g_error_index).im_doc_detail_reason_codes_id||
                       ' '||a_tbl_update(g_error_index).item_no;
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
       update stg_rms_mc_invc_matching_cpy
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
   into g_count
   from fnd_mc_rtl_invoice_matching
   where  doc_no = g_rec_out.doc_no
   and    im_doc_detail_reason_codes_id = g_rec_out.im_doc_detail_reason_codes_id
   and    item_no = g_rec_out.item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if (a_tbl_insert(i).doc_no = g_rec_out.doc_no and
             a_tbl_insert(i).im_doc_detail_reason_codes_id = g_rec_out.im_doc_detail_reason_codes_id and
             a_tbl_insert(i).item_no = g_rec_out.item_no) then
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

    l_text := 'LOAD OF FND_RTL_INVOICE_MATCHING EX POS STARTED AT '||
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
    open stg_rms_invoice_matching;
    fetch stg_rms_invoice_matching bulk collect into a_stg_input limit g_forall_limit;
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
    fetch stg_rms_invoice_matching bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close stg_rms_invoice_matching;
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
end wh_fnd_MC_626u;
