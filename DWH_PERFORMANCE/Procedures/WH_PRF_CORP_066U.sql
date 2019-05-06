--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_066U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_066U" (p_forall_limit in integer,p_success out boolean) as

--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Release 8 Oct 2008 determined that the table created by this program is not available or required
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

--**************************************************************************************************
--  Date:        Nov 2008
--  Author:      Alastair de Wet
--  Purpose:     Create contract dimention table in the performance layer
--               with fnd_rtl_contract data ex foundation table.
--  Tables:      Input  - fnd_rtl_contract,
--               Output - dim_contract
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  23 April 2009 - defect 1361 - Change total_descr from plural to singular
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
g_count              integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_contract%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_066U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE dim_contract EX fnd_rtl_contract';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_contract%rowtype index by binary_integer;
type tbl_array_u is table of dim_contract%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_rtl_contract is
   select distinct contract_no,
   APPROVAL_DATE,
   CONTRACT_TYPE,
   nvl(cnt.SUPPLIER_NO,0) as supplier_no,
   ds.sk1_supplier_no,
   CONTRACT_STATUS_CODE,
   CREATE_DATE,
   CREATE_ID,
   CANCEL_DATE,
   COMPLETE_DATE,
   START_DATE,
   CONTRACT_END_DATE,
   ORDERABLE_IND,
   PRODUCTION_IND,
   COMMENT_DESC
   from  fnd_rtl_contract cnt,
         dim_supplier ds
   where nvl(cnt.supplier_no,0) = ds.supplier_no
    and (chain_code <> 'DJ' or chain_code is null);



g_rec_in             c_fnd_rtl_contract%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_contract%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.contract_no                     := g_rec_in.contract_no;
   g_rec_out.approval_date                   := g_rec_in.approval_date;
   g_rec_out.contract_type                   := g_rec_in.contract_type;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
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

   g_rec_out.last_updated_date               := g_date;


---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------

   g_rec_out.total                  := 'TOTAL';
   g_rec_out.total_desc             := 'ALL CONTRACT';

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
      insert into dim_contract values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).contract_no;
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
      update dim_contract
      set    approval_date                   = a_tbl_update(i).approval_date,
             contract_type                   = a_tbl_update(i).contract_type,
             supplier_no                     = a_tbl_update(i).supplier_no,
             sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no,
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
             total                           = a_tbl_update(i).total,
             total_desc                      = a_tbl_update(i).total_desc,
             last_updated_date               = a_tbl_update(i).last_updated_date
      where  contract_no                     = a_tbl_update(i).contract_no;

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
                       ' '||a_tbl_update(g_error_index).contract_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   dim_contract
   where  contract_no  = g_rec_out.contract_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).contract_no = g_rec_out.contract_no then
            g_found := TRUE;
         end if;
      end loop;
   end if;
-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_contract_no   := merch_hierachy_seq.nextval;
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
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_dummy as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly

   g_rec_out.contract_no                     := 0;
   g_rec_out.supplier_no                     := 0;
   g_rec_out.orderable_ind                   := 0;
   g_rec_out.production_ind                  := 0;
   g_rec_out.comment_desc                    := '';
   g_rec_out.sk1_contract_no                 := 0;
   g_rec_out.sk1_supplier_no                 := 0;

   select count(1)
   into   g_count
   from   dim_contract
   where  contract_no  = 0;

   if g_count = 1 then
      g_found := TRUE;
   end if;


   if not g_found then
      insert into dim_contract values g_rec_out;
      g_recs_inserted := g_recs_inserted + 1;
   else
      update dim_contract
      set    approval_date                   = g_rec_out.approval_date,
             contract_type                   = g_rec_out.contract_type,
             supplier_no                     = g_rec_out.supplier_no,
             sk1_supplier_no                 = g_rec_out.sk1_supplier_no,
             contract_status_code            = g_rec_out.contract_status_code,
             create_date                     = g_rec_out.create_date,
             create_id                       = g_rec_out.create_id,
             cancel_date                     = g_rec_out.cancel_date,
             complete_date                   = g_rec_out.complete_date,
             start_date                      = g_rec_out.start_date,
             contract_end_date               = g_rec_out.contract_end_date,
             orderable_ind                   = g_rec_out.orderable_ind,
             production_ind                  = g_rec_out.production_ind,
             comment_desc                    = g_rec_out.comment_desc,
             total                           = g_rec_out.total,
             total_desc                      = g_rec_out.total_desc,
             last_updated_date               = g_rec_out.last_updated_date
      where  contract_no                     = g_rec_out.contract_no;
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

    l_text := 'LOAD OF dim_contract EX fnd_rtl_contract STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
  --      G_DATE := '26 AUG 2016';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_rtl_contract;
    fetch c_fnd_rtl_contract bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_contract bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_contract;
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
end wh_prf_corp_066u;
