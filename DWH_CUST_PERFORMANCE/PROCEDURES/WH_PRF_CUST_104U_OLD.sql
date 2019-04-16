--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_104U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_104U_OLD" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        AUGUST 2011
--  Author:      Alastair de Wet
--  Purpose:     Create cust_basket_item performance fact table in the performance layer
--               with added value ex foundation layer fnd_cust_basket_item.
--  Tables:      Input  - fnd_cust_basket_item
--               Output - cust_basket_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_forall_limit       integer       :=  10000;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            cust_basket_item%rowtype;

g_found              boolean;
g_count              integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_104U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE cust_basket_item EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of cust_basket_item%rowtype index by binary_integer;
type tbl_array_u is table of cust_basket_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_cust_basket_item is
   select fcb.*,
          dl.sk1_location_no,
          di.sk1_item_no
   from  fnd_cust_basket_item fcb,
         dim_location    dl,
         dim_item di
   where fcb.last_updated_date = g_date and
         fcb.location_no       = dl.location_no and
         fcb.item_no           = di.item_no;


g_rec_in             c_fnd_cust_basket_item%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_cust_basket_item%rowtype;
a_stg_input      stg_array;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_location_no           := g_rec_in.sk1_location_no;
   g_rec_out.till_no                   := g_rec_in.till_no;
   g_rec_out.tran_no                   := g_rec_in.tran_no;
   g_rec_out.tran_date                 := g_rec_in.tran_date;
   g_rec_out.tran_time                 := g_rec_in.tran_time;
   g_rec_out.item_seq_no               := g_rec_in.item_seq_no;
   g_rec_out.sk1_item_no               := g_rec_in.sk1_item_no;
   g_rec_out.tran_type                 := g_rec_in.tran_type;
   g_rec_out.item_tran_selling         := g_rec_in.item_tran_selling;
   g_rec_out.vat_rate_perc             := g_rec_in.vat_rate_perc;
   g_rec_out.item_tran_qty             := g_rec_in.item_tran_qty;
   g_rec_out.discount_selling          := g_rec_in.discount_selling;
   g_rec_out.ITEM_INPUT_CODE           := g_rec_in.ITEM_INPUT_CODE ;
   g_rec_out.WASTE_DISCOUNT_SELLING    := g_rec_in.WASTE_DISCOUNT_SELLING ;
   g_rec_out.RETURN_REASON_CODE        := g_rec_in.RETURN_REASON_CODE ;
   g_rec_out.ITEM_TYPE_CODE            := g_rec_in.ITEM_TYPE_CODE ;
   g_rec_out.ITEM_REF_CODE             := g_rec_in.ITEM_REF_CODE ;
   g_rec_out.SERIAL_NO                 := g_rec_in.SERIAL_NO ;
   g_rec_out.GIFT_CARD_TYPE            := g_rec_in.GIFT_CARD_TYPE ;
   g_rec_out.source_data_status_code   := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date         := g_date;


---------------------------------------------------------
-- Value add
---------------------------------------------------------

/*

   dwh_lookup.primary_account_no(g_rec_out.loyalty_ww_swipe_no,g_rec_out.sk1_primary_account_no);

   dwh_lookup.c2_customer_no(g_rec_out.loyalty_ww_swipe_no,g_rec_out.c2_customer_no);

*/

   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into cust_basket_item values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no ||
                       ' '||a_tbl_insert(g_error_index).till_no ||
                       ' '||a_tbl_insert(g_error_index).tran_no ||
                       ' '||a_tbl_insert(g_error_index).tran_date ;
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
      update  cust_basket_item
       set    tran_time                  = a_tbl_update(i).tran_time,
              tran_type                  = a_tbl_update(i).tran_type,
              item_tran_selling          = a_tbl_update(i).item_tran_selling,
              vat_rate_perc              = a_tbl_update(i).vat_rate_perc,
              item_tran_qty              = a_tbl_update(i).item_tran_qty,
              discount_selling           = a_tbl_update(i).discount_selling,
              source_data_status_code    = a_tbl_update(i).source_data_status_code,
              ITEM_INPUT_CODE            = a_tbl_update(i).ITEM_INPUT_CODE,
              WASTE_DISCOUNT_SELLING     = a_tbl_update(i).WASTE_DISCOUNT_SELLING,
              RETURN_REASON_CODE         = a_tbl_update(i).RETURN_REASON_CODE,
              ITEM_TYPE_CODE             = a_tbl_update(i).ITEM_TYPE_CODE,
              ITEM_REF_CODE              = a_tbl_update(i).ITEM_REF_CODE,
              SERIAL_NO                  = a_tbl_update(i).SERIAL_NO,
              GIFT_CARD_TYPE             = a_tbl_update(i).GIFT_CARD_TYPE,
              sk1_primary_account_no     = a_tbl_update(i).sk1_primary_account_no,
              c2_customer_no             = a_tbl_update(i).c2_customer_no,
              last_updated_date          = a_tbl_update(i).last_updated_date
       where  sk1_location_no            = a_tbl_update(i).sk1_location_no
         and  till_no                    = a_tbl_update(i).till_no
         and  tran_no                    = a_tbl_update(i).tran_no
         and  tran_date                  = a_tbl_update(i).tran_date
         and  item_seq_no                = a_tbl_update(i).item_seq_no
         and  sk1_item_no                = a_tbl_update(i).sk1_item_no;
      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_location_no ||
                       ' '||a_tbl_update(g_error_index).till_no ||
                       ' '||a_tbl_update(g_error_index).tran_no ||
                       ' '||a_tbl_update(g_error_index).tran_date ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

  g_found := false;
  -- check to see if item is present on table and update/insert accordingly
  select count(1)
  into   g_count
  from   cust_basket_item
  where  sk1_location_no            = g_rec_out.sk1_location_no
    and  till_no                    = g_rec_out.till_no
    and  tran_no                    = g_rec_out.tran_no
    and  tran_date                  = g_rec_out.tran_date
    and  item_seq_no                = g_rec_out.item_seq_no
    and  sk1_item_no                = g_rec_out.sk1_item_no;

  if g_count             = 1 then
    g_found              := true;
  end if;
  -- place data into and array for later writing to table in bulk
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

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF cust_basket_item EX fnd_cust_basket_item STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_cust_basket_item;
    fetch c_fnd_cust_basket_item bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_cust_basket_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_cust_basket_item;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_CUST_104U_OLD;
