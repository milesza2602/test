--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_606U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_606U" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Contract fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_rtl_contract_cpy
--               Output - fnd_rtl_contract
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  QC 1908  -  14 July 2009 - Contract Delete data fix - link to qc1848
--  qc 2358 - 6 sept 2009 - PRC - RSP for Approved Contract
-- qc2375 - 8 sept 2009 - REG_RSP_EXCL_VAT is baing calculated incorrectly
--                        on FND_RTL_CONTRACT
--  Wendy lyttle 26 aug 2016 -- Due to business process not being followed, 
--                               we have to hospitalise any comntracts which have
--                             been changed from DJ to NON-DJ and vice versa.
--                           -- This is at contract level.
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
g_vat_rate_perc      dim_item.vat_rate_perc%type;
g_base_rsp_excl_vat  dim_item.base_rsp_excl_vat%type;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_rtl_contract_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_contract%rowtype;
g_found              boolean;
g_insert_rec         boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_606U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CONTRACT FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_contract%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_contract%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_rtl_contract_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_rtl_contract_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_stg_rms_rtl_contract is
   select a.*, b.contract_no rej_rec
   from stg_rms_rtl_contract_cpy a, dwh_foundation.TEMP_DEL_CONTRACT b
   where 
   sys_process_code = 'N'
   and 
   a.contract_no = b.contract_no(+)
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

g_rec_in             c_stg_rms_rtl_contract%rowtype;
-- For input bulk collect --
type stg_array is table of c_stg_rms_rtl_contract%rowtype;
a_stg_input      stg_array;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';

   g_rec_out.last_updated_date               := g_date;
   g_rec_out.contract_no                     := g_rec_in.contract_no;
   g_rec_out.seq_no                          := g_rec_in.seq_no;
   g_rec_out.approval_date                   := g_rec_in.approval_date;
   g_rec_out.ready_date                      := g_rec_in.ready_date;
   g_rec_out.contract_type                   := g_rec_in.contract_type;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.contract_status_code            := g_rec_in.contract_status_code;
   g_rec_out.create_date                     := g_rec_in.create_date;
   g_rec_out.create_id                       := g_rec_in.create_id;
   g_rec_out.cancel_date                     := g_rec_in.cancel_date;
   g_rec_out.complete_date                   := g_rec_in.complete_date;
   g_rec_out.start_date                      := g_rec_in.start_date;
   g_rec_out.contract_end_date               := g_rec_in.contract_end_date;
   g_rec_out.orderable_ind                   := g_rec_in.orderable_ind;
   g_rec_out.production_ind                  := g_rec_in.production_ind;
   g_rec_out.comment_desc                    := g_rec_in.comment_desc;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.ref_item_no                     := g_rec_in.ref_item_no;
   g_rec_out.contract_qty                    := g_rec_in.contract_qty;
   g_rec_out.po_qty                          := g_rec_in.po_qty;
   g_rec_out.received_qty                    := g_rec_in.received_qty;
   g_rec_out.cost_price                      := g_rec_in.cost_price;
   G_REC_OUT.DISPLAY_SEQ                     := G_REC_IN.DISPLAY_SEQ;
   g_rec_out.chain_code                      := g_rec_in.chain_code;



--   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--     return;
--   end if;
   if not dwh_valid.indicator_field(g_rec_out.orderable_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.production_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
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

 --  begin
 --        select reg_rsp
 --        into   g_rec_out.reg_rsp_excl_vat
 --        from   fnd_zone_item
 --        where  zone_group_no       = 2 and
 --               zone_no             = 1 and
 --               item_no             = g_rec_out.item_no   ;
 --        exception
 --        when no_data_found then
 --          g_rec_out.reg_rsp_excl_vat   := 0;
 --  end;
 /*  begin
         select vat_rate_perc, base_rsp_excl_vat
         into   g_vat_rate_perc, g_base_rsp_excl_vat
         from   dim_item
         where  item_no             = g_rec_out.item_no   ;
         exception
         when no_data_found then
           g_vat_rate_perc   := 14;
           g_base_rsp_excl_vat := 0;
   end;
*/
  -- NEW CODE TO DERIVE VAT RATE PERC & BASE RSP FROM THE CORRECT FOUNDATION TABLES
  -- THIS MEANS SCHEUDLING CHANGES NEEDED TO BE MADE TO ENSURE THIS PROGRMA RUNS AFTER :
  --    FND_ITEM_VAT_RATE : WH_FND_CORP_150U  ==> NOT ACTUALLY NEEDED AS THE VAT RATE PERC ISN'T EVEN USED
  --    FND_ZONE_ITEM     : WH_FND_CORP_106U
  -- ADDED 18 SEPTEMBER 2015
  --==============================================================================
 /*   begin
      select vat_rate_perc    --,vat_code
        into g_vat_rate_perc  --,g_rec_out.vat_code
        from fnd_item_vat_rate a
       where item_no          = g_rec_out.item_no 
         and vat_region_no    = 1000   
         and active_from_date = (select max(active_from_date) 
                                     from fnd_item_vat_rate b 
                                    where active_from_date <= g_date 
                                      and a.item_no = b.item_no 
                                      and b.vat_region_no    = 1000) 
         and active_from_date <= g_date;
  
        exception
           when no_data_found then
             g_vat_rate_perc := 14;
  
    end;
*/ 
    begin
      select max(reg_rsp)
        into g_base_rsp_excl_vat
        from fnd_zone_item
       where item_no          = g_rec_out.item_no and
             base_retail_ind  = 1 ;

      exception
         when no_data_found then
           g_base_rsp_excl_vat      := 0;
   end;

-- END OF NEW CODE
-- ===============================================================================

--      g_rec_out.reg_rsp_excl_vat :=   round(g_base_rsp_excl_vat * 100 / (100 + g_vat_rate_perc),2);
      g_rec_out.reg_rsp_excl_vat :=   g_base_rsp_excl_vat ;
      g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;


   if g_rec_in.rej_rec is not null then
     g_hospital      := 'Y';
     g_hospital_text := 'Contract chain_code changed';
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

   insert into stg_rms_rtl_contract_hsp  values (g_rec_in.SYS_SOURCE_BATCH_ID
, g_rec_in.SYS_SOURCE_SEQUENCE_NO
, g_rec_in.SYS_LOAD_DATE
, g_rec_in.SYS_PROCESS_CODE
, g_rec_in.SYS_LOAD_SYSTEM_NAME
, g_rec_in.SYS_MIDDLEWARE_BATCH_ID
, g_rec_in.SYS_PROCESS_MSG
, g_rec_in.CONTRACT_NO
, g_rec_in.SEQ_NO
, g_rec_in.APPROVAL_DATE
, g_rec_in.READY_DATE
, g_rec_in.CONTRACT_TYPE
, g_rec_in.SUPPLIER_NO
, g_rec_in.CONTRACT_STATUS_CODE
, g_rec_in.CREATE_DATE
, g_rec_in.CREATE_ID
, g_rec_in.CANCEL_DATE
, g_rec_in.COMPLETE_DATE
, g_rec_in.START_DATE
, g_rec_in.CONTRACT_END_DATE
, g_rec_in.ORDERABLE_IND
, g_rec_in.PRODUCTION_IND
, g_rec_in.COMMENT_DESC
, g_rec_in.ITEM_NO
, g_rec_in.REF_ITEM_NO
, g_rec_in.CONTRACT_QTY
, g_rec_in.PO_QTY
, g_rec_in.RECEIVED_QTY
, g_rec_in.COST_PRICE
, g_rec_in.DISPLAY_SEQ
, g_rec_in.SOURCE_DATA_STATUS_CODE
, g_rec_in.CHAIN_CODE);
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
       insert into fnd_rtl_contract values (a_tbl_insert(i).CONTRACT_NO
, a_tbl_insert(i).SEQ_NO
, a_tbl_insert(i).APPROVAL_DATE
, a_tbl_insert(i).READY_DATE
, a_tbl_insert(i).CONTRACT_TYPE
, a_tbl_insert(i).SUPPLIER_NO
, a_tbl_insert(i).CONTRACT_STATUS_CODE
, a_tbl_insert(i).CREATE_DATE
, a_tbl_insert(i).CREATE_ID
, a_tbl_insert(i).CANCEL_DATE
, a_tbl_insert(i).COMPLETE_DATE
, a_tbl_insert(i).START_DATE
, a_tbl_insert(i).CONTRACT_END_DATE
, a_tbl_insert(i).ORDERABLE_IND
, a_tbl_insert(i).PRODUCTION_IND
, a_tbl_insert(i).COMMENT_DESC
, a_tbl_insert(i).ITEM_NO
, a_tbl_insert(i).REF_ITEM_NO
, a_tbl_insert(i).CONTRACT_QTY
, a_tbl_insert(i).PO_QTY
, a_tbl_insert(i).RECEIVED_QTY
, a_tbl_insert(i).COST_PRICE
, a_tbl_insert(i).DISPLAY_SEQ
, a_tbl_insert(i).SOURCE_DATA_STATUS_CODE
, a_tbl_insert(i).LAST_UPDATED_DATE
, a_tbl_insert(i).REG_RSP_EXCL_VAT
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
                       ' '||a_tbl_insert(g_error_index).contract_no||
                       ' '||a_tbl_insert(g_error_index).seq_no;

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
       update fnd_rtl_contract
       set    approval_date                   = a_tbl_update(i).approval_date,
              ready_date                      = a_tbl_update(i).ready_date,
              contract_type                   = a_tbl_update(i).contract_type,
              supplier_no                     = a_tbl_update(i).supplier_no,
              contract_status_code            = a_tbl_update(i).contract_status_code,
              create_date                     = a_tbl_update(i).create_date,
              create_id                       = a_tbl_update(i).create_id,
              cancel_date                     = a_tbl_update(i).cancel_date,
              complete_date                   = a_tbl_update(i).complete_date,
              start_date                      = a_tbl_update(i).start_date,
              contract_end_date               = a_tbl_update(i).contract_end_date,
              orderable_ind                   = a_tbl_update(i).orderable_ind,
              production_ind                  = a_tbl_update(i).production_ind,
              comment_desc                    = a_tbl_update(i).comment_desc,
              item_no                         = a_tbl_update(i).item_no,
              ref_item_no                     = a_tbl_update(i).ref_item_no,
              contract_qty                    = a_tbl_update(i).contract_qty,
              po_qty                          = a_tbl_update(i).po_qty,
              received_qty                    = a_tbl_update(i).received_qty,
              cost_price                      = a_tbl_update(i).cost_price,
              display_seq                     = a_tbl_update(i).display_seq,
              reg_rsp_excl_vat                = a_tbl_update(i).reg_rsp_excl_vat ,
              chain_code                      = a_tbl_update(i).chain_code ,
              source_data_status_code         = 'U',
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  contract_no                     = a_tbl_update(i).contract_no  and
              seq_no                          = a_tbl_update(i).seq_no;


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
                       ' '||a_tbl_update(g_error_index).contract_no||
                       ' '||a_tbl_update(g_error_index).seq_no;

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
       update stg_rms_rtl_contract_cpy
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
   from   fnd_rtl_contract
   where  contract_no    = g_rec_out.contract_no  and
          seq_no          = g_rec_out.seq_no ;


   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).contract_no  = g_rec_out.contract_no and
            a_tbl_insert(i).seq_no       = g_rec_out.seq_no then
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

    l_text := 'LOAD OF FND_RTL_CONTRACT EX OM STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
  --  G_DATE := '26 AUG 2016';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Mark all Contracts about to be processed  on FND_RTL_CONTRACTS to 'D'(item-level).
-- This processing is being done here, but the danger of it being done here is that
--  a contract(and it's items) landing in Hospital will be marked to 'D'
--**************************************************************************************************
    l_text := 'source_data_status_code set to D before processing - started at '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    Update fnd_rtl_contract
        set source_data_status_code = 'D'
    where contract_no
        in (select distinct contract_no from stg_rms_rtl_contract_cpy
                                        where sys_process_code = 'N');
    l_text := 'source_data_status_code set to D before processing - recs='||sql%rowcount;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;

--**************************************************************************************************
-- Due to business process not being followed, we have to hospitalise any comntracts which have
-- been changed from DJ to NON-DJ and vice versa.
--**************************************************************************************************
    l_text := 'Truncate TEMP_DEL_CONTRACT - Recs Rejected Chain_code change = '||sql%rowcount;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate('truncate table dwh_foundation.temp_del_contract');
    commit;

    l_text := 'Insert started TEMP_DEL_CONTRACT' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    insert /*+ append */ into dwh_foundation.temp_del_contract
    with selstg as (select distinct contract_no, chain_code 
                    from stg_rms_rtl_contract_cpy),
         selfnd as (select distinct a.contract_no, a.chain_code 
                    from fnd_rtl_contract a, selstg b
                    where a.contract_no = b.contract_no),
         selchkstg as (select contract_no, count(*) 
                       from selstg 
                       group by contract_no
                       having count(*) > 1),
         selchkfnd as (select contract_no, count(*) 
                       from (select * from selstg union select *from selfnd)
                       group by contract_no 
                       having count(*) > 1)
    select contract_no, g_date from selchkstg
    union
    select contract_no, g_date from selchkfnd;
    l_text := 'TEMP_DEL_CONTRACT - Recs Rejected Chain_code change = '||sql%rowcount;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_rtl_contract;
    fetch c_stg_rms_rtl_contract bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_rms_rtl_contract bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_rtl_contract;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;

    l_text := 'update all reg_rsp_excl_vat for contract_status_code = A - started at :'||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    update fnd_rtl_contract a
    set reg_rsp_excl_vat = (select b.base_rsp_excl_vat
         from   dim_item b
         where  b.item_no             = a.item_no )
         where a.item_no in(select c.item_no from dim_item c)
         and a.contract_status_code = 'A';
    l_text := 'update all reg_rsp_excl_vat for contract_status_code = A - recs='||sql%rowcount;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
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
end WH_FND_CORP_606U;
