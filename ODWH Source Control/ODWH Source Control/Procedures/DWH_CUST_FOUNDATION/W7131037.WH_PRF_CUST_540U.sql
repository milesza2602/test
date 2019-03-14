-- ****** Object: Procedure W7131037.WH_PRF_CUST_540U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_540U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        September 2010
--  Author:      Alastair de Wet
--  Purpose:     Create cl_inquiry roll up fact table in the performance layer
--               with cl_inquiry table ex performance layer.
--  Tables:      Input  - cust_cl_inquiry
--               Output - cust_cl_loc_item_dy_inq
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
g_count              number        :=  0;
g_error_index        number        :=  0;
g_rec_out            cust_cl_loc_item_dy_inq%rowtype;
g_dummy              number        :=  0;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_540U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE cust_cl_loc_item_dy_inq EX PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of cust_cl_loc_item_dy_inq%rowtype index by binary_integer;
type tbl_array_u is table of cust_cl_loc_item_dy_inq%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_cust_cl_inquiry is
   select   prf_inq.sk1_location_no,
            prf_inq.sk1_item_no,
            prf_inq.logged_date,
            prf_inq.sk1_supplier_no,
            prf_inq.sk1_cl_inq_sub_cat_no,
            prf_inq.sk1_channel_inbound_no,
            prf_inq.sk1_owner_user_no,
            prf_inq.sk1_logged_by_user_no,
            prf_inq.sk1_cl_status_no,
            prf_inq.sk1_bus_area_no,
            count(inquiry_no) as no_of_inquiries ,
            sum(taste_wild_ind) taste_wild_qty,
            sum(taste_off_ind) taste_off_qty,
            sum(taste_rancid_ind) taste_rancid_qty,
            sum(taste_salty_ind) taste_salty_qty,
            sum(taste_sour_ind) taste_sour_qty,
            sum(taste_tasteless_ind) taste_tasteless_qty,
            sum(taste_chemical_ind) taste_chemical_qty,
            sum(taste_tough_ind) taste_tough_qty,
            sum(taste_dry_ind) taste_dry_qty,
            sum(smell_chemical_ind) smell_chemical_qty,
            sum(smell_off_ind) smell_off_qty,
            sum(smell_bad_rotten_ind) smell_bad_rotten_qty,
            sum(feel_hard_ind) feel_hard_qty,
            sum(feel_soft_ind) feel_soft_qty,
            sum(feel_dry_ind) feel_dry_qty,
            sum(feel_mushy_ind) feel_mushy_qty,
            sum(look_fatty_ind) look_fatty_qty,
            sum(look_discoloured_ind) look_discoloured_qty,
            sum(look_separated_ind) look_separated_qty,
            sum(look_burnt_ind) look_burnt_qty,
            sum(look_pale_ind) look_pale_qty,
            sum(look_underbaked_raw_ind) look_underbaked_raw_qty,
            sum(look_dry_ind) look_dry_qty,
            sum(look_over_ripe_ind) look_over_ripe_qty,
            sum(look_under_ripe_ind) look_under_ripe_qty,
            sum(packaging_not_sealed_ind) packaging_not_sealed_qty,
            sum(packaging_leaks_ind) packaging_leaks_qty,
            sum(packaging_misleading_ind) packaging_misleading_qty,
            sum(packaging_blewup_microwave_ind ) packaging_blewup_microwave_qty,
            sum(packaging_lack_of_info_ind) packaging_lack_of_info_qty,
            sum(packaging_incorrect_info_ind) packaging_incorrect_info_qty,
            sum(packaging_wrong_product_ind) packaging_wrong_product_qty,
            sum(value_poor_value_ind) value_poor_value_qty,
            sum(value_incorrect_price_ind) value_incorrect_price_qty,
            sum(value_incorrect_promotion_ind) value_incorrect_promotion_qty,
            sum(value_too_expensive_ind) AS value_too_expensive_qty
   from     W7131037.CUST_CL_INQUIRY prf_inq
--   where    prf_inq.last_updated_date  >=  g_date-7
   group by prf_inq.sk1_location_no,
            prf_inq.sk1_item_no,
            prf_inq.logged_date,
            prf_inq.sk1_supplier_no,
            prf_inq.sk1_cl_inq_sub_cat_no,
            prf_inq.sk1_channel_inbound_no,
            prf_inq.sk1_owner_user_no,
            prf_inq.sk1_logged_by_user_no,
            prf_inq.sk1_cl_status_no,
            prf_inq.sk1_bus_area_no ;

g_rec_in             c_cust_cl_inquiry%rowtype;
-- For input bulk collect --
type stg_array is table of c_cust_cl_inquiry%rowtype;
a_stg_input      stg_array;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.logged_date                     := g_rec_in.logged_date;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
   g_rec_out.sk1_cl_inq_sub_cat_no           := g_rec_in.sk1_cl_inq_sub_cat_no;
   g_rec_out.sk1_channel_inbound_no          := g_rec_in.sk1_channel_inbound_no;
   g_rec_out.sk1_owner_user_no               := g_rec_in.sk1_owner_user_no;
   g_rec_out.sk1_logged_by_user_no           := g_rec_in.sk1_logged_by_user_no;
   g_rec_out.sk1_cl_status_no                := g_rec_in.sk1_cl_status_no;
   g_rec_out.sk1_bus_area_no                 := g_rec_in.sk1_bus_area_no;
   g_rec_out.taste_wild_qty                  := g_rec_in.taste_wild_qty;
   g_rec_out.taste_off_qty                   := g_rec_in.taste_off_qty;
   g_rec_out.taste_rancid_qty                := g_rec_in.taste_rancid_qty;
   g_rec_out.taste_salty_qty                 := g_rec_in.taste_salty_qty;
   g_rec_out.taste_sour_qty                  := g_rec_in.taste_sour_qty;
   g_rec_out.taste_tasteless_qty             := g_rec_in.taste_tasteless_qty;
   g_rec_out.taste_chemical_qty              := g_rec_in.taste_chemical_qty;
   g_rec_out.taste_tough_qty                 := g_rec_in.taste_tough_qty;
   g_rec_out.taste_dry_qty                   := g_rec_in.taste_dry_qty;
   g_rec_out.smell_chemical_qty              := g_rec_in.smell_chemical_qty;
   g_rec_out.smell_off_qty                   := g_rec_in.smell_off_qty;
   g_rec_out.smell_bad_rotten_qty            := g_rec_in.smell_bad_rotten_qty;
   g_rec_out.feel_hard_qty                   := g_rec_in.feel_hard_qty;
   g_rec_out.feel_soft_qty                   := g_rec_in.feel_soft_qty;
   g_rec_out.feel_dry_qty                    := g_rec_in.feel_dry_qty;
   g_rec_out.feel_mushy_qty                  := g_rec_in.feel_mushy_qty;
   g_rec_out.look_fatty_qty                  := g_rec_in.look_fatty_qty;
   g_rec_out.look_discoloured_qty            := g_rec_in.look_discoloured_qty;
   g_rec_out.look_separated_qty              := g_rec_in.look_separated_qty;
   g_rec_out.look_burnt_qty                  := g_rec_in.look_burnt_qty;
   g_rec_out.look_pale_qty                   := g_rec_in.look_pale_qty;
   g_rec_out.look_underbaked_raw_qty         := g_rec_in.look_underbaked_raw_qty;
   g_rec_out.look_dry_qty                    := g_rec_in.look_dry_qty;
   g_rec_out.look_over_ripe_qty              := g_rec_in.look_over_ripe_qty;
   g_rec_out.look_under_ripe_qty             := g_rec_in.look_under_ripe_qty;
   g_rec_out.packaging_not_sealed_qty        := g_rec_in.packaging_not_sealed_qty;
   g_rec_out.packaging_leaks_qty             := g_rec_in.packaging_leaks_qty;
   g_rec_out.packaging_misleading_qty        := g_rec_in.packaging_misleading_qty;
   g_rec_out.packaging_blewup_microwave_qty  := g_rec_in.packaging_blewup_microwave_qty;
   g_rec_out.packaging_lack_of_info_qty      := g_rec_in.packaging_lack_of_info_qty;
   g_rec_out.packaging_incorrect_info_qty    := g_rec_in.packaging_incorrect_info_qty;
   g_rec_out.packaging_wrong_product_qty     := g_rec_in.packaging_wrong_product_qty;
   g_rec_out.value_poor_value_qty            := g_rec_in.value_poor_value_qty;
   g_rec_out.value_incorrect_price_qty       := g_rec_in.value_incorrect_price_qty;
   g_rec_out.value_incorrect_promotion_qty   := g_rec_in.value_incorrect_promotion_qty;
   g_rec_out.value_too_expensive_qty         := g_rec_in.value_too_expensive_qty;

   g_rec_out.no_of_inquiries                 := g_rec_in.no_of_inquiries;
   g_rec_out.last_updated_date               := g_date;



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
      insert into cust_cl_loc_item_dy_inq values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no;
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
      update cust_cl_loc_item_dy_inq
       set  no_of_inquiries                 = a_tbl_update(i).no_of_inquiries,
            taste_wild_qty                  = a_tbl_update(i).taste_wild_qty,
            taste_off_qty                   = a_tbl_update(i).taste_off_qty,
            taste_rancid_qty                = a_tbl_update(i).taste_rancid_qty,
            taste_salty_qty                 = a_tbl_update(i).taste_salty_qty,
            taste_sour_qty                  = a_tbl_update(i).taste_sour_qty,
            taste_tasteless_qty             = a_tbl_update(i).taste_tasteless_qty,
            taste_chemical_qty              = a_tbl_update(i).taste_chemical_qty,
            taste_tough_qty                 = a_tbl_update(i).taste_tough_qty,
            taste_dry_qty                   = a_tbl_update(i).taste_dry_qty,
            smell_chemical_qty              = a_tbl_update(i).smell_chemical_qty,
            smell_off_qty                   = a_tbl_update(i).smell_off_qty,
            smell_bad_rotten_qty            = a_tbl_update(i).smell_bad_rotten_qty,
            feel_hard_qty                   = a_tbl_update(i).feel_hard_qty,
            feel_soft_qty                   = a_tbl_update(i).feel_soft_qty,
            feel_dry_qty                    = a_tbl_update(i).feel_dry_qty,
            feel_mushy_qty                  = a_tbl_update(i).feel_mushy_qty,
            look_fatty_qty                  = a_tbl_update(i).look_fatty_qty,
            look_discoloured_qty            = a_tbl_update(i).look_discoloured_qty,
            look_separated_qty              = a_tbl_update(i).look_separated_qty,
            look_burnt_qty                  = a_tbl_update(i).look_burnt_qty,
            look_pale_qty                   = a_tbl_update(i).look_pale_qty,
            look_underbaked_raw_qty         = a_tbl_update(i).look_underbaked_raw_qty,
            look_dry_qty                    = a_tbl_update(i).look_dry_qty,
            look_over_ripe_qty              = a_tbl_update(i).look_over_ripe_qty,
            look_under_ripe_qty             = a_tbl_update(i).look_under_ripe_qty,
            packaging_not_sealed_qty        = a_tbl_update(i).packaging_not_sealed_qty,
            packaging_leaks_qty             = a_tbl_update(i).packaging_leaks_qty,
            packaging_misleading_qty        = a_tbl_update(i).packaging_misleading_qty,
            packaging_blewup_microwave_qty  = a_tbl_update(i).packaging_blewup_microwave_qty,
            packaging_lack_of_info_qty      = a_tbl_update(i).packaging_lack_of_info_qty,
            packaging_incorrect_info_qty    = a_tbl_update(i).packaging_incorrect_info_qty,
            packaging_wrong_product_qty     = a_tbl_update(i).packaging_wrong_product_qty,
            value_poor_value_qty            = a_tbl_update(i).value_poor_value_qty,
            value_incorrect_price_qty       = a_tbl_update(i).value_incorrect_price_qty,
            value_incorrect_promotion_qty   = a_tbl_update(i).value_incorrect_promotion_qty,
            value_too_expensive_qty         = a_tbl_update(i).value_too_expensive_qty,
            last_updated_date               = a_tbl_update(i).last_updated_date

       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no and
              logged_date                     = a_tbl_update(i).logged_date and
              sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no and
              sk1_cl_inq_sub_cat_no           = a_tbl_update(i).sk1_cl_inq_sub_cat_no and
              sk1_channel_inbound_no          = a_tbl_update(i).sk1_channel_inbound_no and
              sk1_owner_user_no               = a_tbl_update(i).sk1_owner_user_no and
              sk1_logged_by_user_no           = a_tbl_update(i).sk1_logged_by_user_no and
              sk1_cl_status_no                = a_tbl_update(i).sk1_cl_status_no and
              sk1_bus_area_no                 = a_tbl_update(i).sk1_bus_area_no ;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no ;
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

   select count(1)
   into   g_count
   from   cust_cl_loc_item_dy_inq
   where  g_rec_out.sk1_location_no                 = sk1_location_no and
          g_rec_out.sk1_item_no                     = sk1_item_no and
          g_rec_out.logged_date                     = logged_date and
          g_rec_out.sk1_supplier_no                 = sk1_supplier_no and
          g_rec_out.sk1_cl_inq_sub_cat_no           = sk1_cl_inq_sub_cat_no and
          g_rec_out.sk1_channel_inbound_no          = sk1_channel_inbound_no and
          g_rec_out.sk1_owner_user_no               = sk1_owner_user_no and
          g_rec_out.sk1_logged_by_user_no           = sk1_logged_by_user_no and
          g_rec_out.sk1_cl_status_no                = sk1_cl_status_no and
          g_rec_out.sk1_bus_area_no                 = sk1_bus_area_no;

   if g_count = 1 then
     g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
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

    l_text := 'LOAD OF cust_cl_loc_item_dy_inq EX cust_cl_inquiry STARTED AT '||
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
    l_text := 'Truncate table - cust_cl_loc_item_dy_inq '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'truncate table W7131037.cust_cl_loc_item_dy_inq';

   l_text := 'Truncate table - cust_cl_loc_item_dy_inq completed '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_cust_cl_inquiry;
    fetch c_cust_cl_inquiry bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_cust_cl_inquiry bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_cust_cl_inquiry;
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

END "WH_PRF_CUST_540U";
