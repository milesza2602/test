-- ****** Object: Procedure W7131037.WH_PRF_CUST_530U_BCK16102015 Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_530U_BCK16102015" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        September 2010
--  Author:      Alastair de Wet
--  Purpose:     Create cl_inquiry fact table in the performance layer
--               with added value ex foundation layer cl_type_cat table.
--  Tables:      Input  - fnd_cust_cl_inquiry
--               Output - cust_cl_inquiry
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
g_rec_out            cust_cl_inquiry%rowtype;
g_dummy              number        :=  0;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_530U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE cust_cl_inquiry EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of cust_cl_inquiry%rowtype index by binary_integer;
type tbl_array_u is table of cust_cl_inquiry%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_cust_cl_inquiry is
   select fnd_inq.*,
          dbus.sk1_bus_area_no,
          dci.sk1_channel_inbound_no,
          ds.sk1_cl_status_no,
          du.sk1_cl_user_no    sk1_owner_user ,
          du1.sk1_cl_user_no   sk1_logged_by_user
   from   fnd_cust_cl_inquiry fnd_inq,
          dim_cust_cl_bus_area dbus,
          dim_cust_cl_chanel_inbound dci,
          dim_cust_cl_status ds,
          dim_cust_cl_user du,
          dim_cust_cl_user du1
   where  fnd_inq.last_updated_date  = g_date and
          fnd_inq.bus_area_no        = dbus.bus_area_no and
          fnd_inq.channel_inbound_no = dci.channel_inbound_no and
          fnd_inq.owner_user_no      = du.cl_user_no and
          fnd_inq.logged_by_user_no  = du1.cl_user_no and
          fnd_inq.cl_status_no       = ds.cl_status_no
          ;

g_rec_in             c_fnd_cust_cl_inquiry%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_cust_cl_inquiry%rowtype;
a_stg_input      stg_array;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.inquiry_no                      := g_rec_in.inquiry_no ;
--   g_rec_out.inq_type_no                     := g_rec_in.inq_type_no;
   g_rec_out.sk1_owner_user_no               := g_rec_in.sk1_owner_user;
   g_rec_out.sk1_logged_by_user_no           := g_rec_in.sk1_logged_by_user;

   ------------------------------------------------------------------------------------
   update dim_cust_cl_user
   set logged_by_user_ind = 1
   where sk1_cl_user_no = g_rec_out.sk1_logged_by_user_no;

   update dim_cust_cl_user
   set owner_user_ind = 1
   where sk1_cl_user_no = g_rec_out.sk1_owner_user_no;

   ------------------------------------------------------------------------------------
   g_rec_out.sk1_channel_inbound_no          := g_rec_in.sk1_channel_inbound_no;
   g_rec_out.sk1_cl_status_no                := g_rec_in.sk1_cl_status_no;
   g_rec_out.interaction_no                  := g_rec_in.interaction_no;
   g_rec_out.sk1_bus_area_no                 := g_rec_in.sk1_bus_area_no;
   g_rec_out.priority                        := g_rec_in.priority;
   g_rec_out.status_central                  := g_rec_in.status_central;
   g_rec_out.inquiry_bus_area                := g_rec_in.inquiry_bus_area;
   g_rec_out.owner_grp                       := g_rec_in.owner_grp;
   g_rec_out.transfer_flag                   := g_rec_in.transfer_flag;
   g_rec_out.transfer_user                   := g_rec_in.transfer_user;
   g_rec_out.transfer_grp                    := g_rec_in.transfer_grp;
   g_rec_out.escal_ind                       := g_rec_in.escal_ind;
   g_rec_out.escal_level                     := g_rec_in.escal_level;
   g_rec_out.receipt_ackn_required           := g_rec_in.receipt_ackn_required;
   g_rec_out.cust_feedback_required          := g_rec_in.cust_feedback_required;
   g_rec_out.inq_details                     := g_rec_in.inq_details;
   g_rec_out.logged_date                     := g_rec_in.logged_date;
   g_rec_out.classified_date                 := g_rec_in.classified_date;
   g_rec_out.receipt_ackn_date               := g_rec_in.receipt_ackn_date;
   g_rec_out.last_prg_upd_date               := g_rec_in.last_prg_upd_date;
   g_rec_out.cust_resolved_date              := g_rec_in.cust_resolved_date;
   g_rec_out.transfer_date                   := g_rec_in.transfer_date;
   g_rec_out.transfer_acc_date               := g_rec_in.transfer_acc_date;
   g_rec_out.special_resolved_date           := g_rec_in.special_resolved_date;
   g_rec_out.special_ext_period              := g_rec_in.special_ext_period;
   g_rec_out.closed_date                     := g_rec_in.closed_date;
   g_rec_out.qa_rqd_ind                      := g_rec_in.qa_rqd_ind;
   g_rec_out.qa_compl_date                   := g_rec_in.qa_compl_date;
   g_rec_out.txt_style_no                    := g_rec_in.txt_style_no;
   g_rec_out.product_desc                    := g_rec_in.product_desc;
   g_rec_out.txt_size                        := g_rec_in.txt_size;
   g_rec_out.txt_col_fragr                   := g_rec_in.txt_col_fragr;
   g_rec_out.purchase_date                   := g_rec_in.purchase_date;
   g_rec_out.item_qty                        := g_rec_in.item_qty;
   g_rec_out.selling_price                   := g_rec_in.selling_price;
   g_rec_out.store_refund_given              := g_rec_in.store_refund_given;
   g_rec_out.refund_value                    := g_rec_in.refund_value;
   g_rec_out.complementary_item_given        := g_rec_in.complementary_item_given;
   g_rec_out.complementary_item_value        := g_rec_in.complementary_item_value;
   g_rec_out.txt_debit_memo                  := g_rec_in.txt_debit_memo;
   g_rec_out.foods_sell_by_date              := g_rec_in.foods_sell_by_date;
   g_rec_out.foods_prod_batch                := g_rec_in.foods_prod_batch;
   g_rec_out.foods_mass_size                 := g_rec_in.foods_mass_size;
   g_rec_out.fo_avail                        := g_rec_in.fo_avail ;
   g_rec_out.fo_present                      := g_rec_in.fo_present;
   g_rec_out.fo_received_date_store          := g_rec_in.fo_received_date_store;
   g_rec_out.fo_received_date_cl             := g_rec_in.fo_received_date_cl;
   g_rec_out.fo_received_date_tech           := g_rec_in.fo_received_date_tech;
   g_rec_out.svc_staff_member                := g_rec_in.svc_staff_member;
   g_rec_out.svc_cia_store_no                := g_rec_in.svc_cia_store_no;
   g_rec_out.cia_incident_date               := g_rec_in.cia_incident_date;
   g_rec_out.cia_cust_asstd                  := g_rec_in.cia_cust_asstd;
   g_rec_out.cia_claims_impl                 := g_rec_in.cia_claims_impl;
   g_rec_out.cia_asst_given                  := g_rec_in.cia_asst_given;
   g_rec_out.cia_store_resolved              := g_rec_in.cia_store_resolved;
   g_rec_out.cia_asst_mgmt                   := g_rec_in.cia_asst_mgmt;
   g_rec_out.resol_type_lev1                 := g_rec_in.resol_type_lev1;
   g_rec_out.resol_type_lev2                 := g_rec_in.resol_type_lev2;
   g_rec_out.specl_resol_user                := g_rec_in.specl_resol_user;
   g_rec_out.cust_resol_user                 := g_rec_in.cust_resol_user;
   g_rec_out.justified_status                := g_rec_in.justified_status;
   g_rec_out.penalty_area                    := g_rec_in.penalty_area;
   g_rec_out.penalty_value                   := g_rec_in.penalty_value;
   g_rec_out.no_corresp_sent                 := g_rec_in.no_corresp_sent;
   g_rec_out.spec_resolved_ind               := g_rec_in.spec_resolved_ind;
   g_rec_out.spec_resolution                 := g_rec_in.spec_resolution;
   g_rec_out.cust_resolved_ind               := g_rec_in.cust_resolved_ind;
   g_rec_out.cust_resolution                 := g_rec_in.cust_resolution;
   g_rec_out.break_cold_chain                := g_rec_in.break_cold_chain;
   g_rec_out.quality_complaint_reason        := g_rec_in.quality_complaint_reason;
   g_rec_out.taste_wild_ind                  := g_rec_in.taste_wild_ind;
   g_rec_out.taste_off_ind                   := g_rec_in.taste_off_ind;
   g_rec_out.taste_rancid_ind                := g_rec_in.taste_rancid_ind;
   g_rec_out.taste_salty_ind                 := g_rec_in.taste_salty_ind;
   g_rec_out.taste_sour_ind                  := g_rec_in.taste_sour_ind;
   g_rec_out.taste_tasteless_ind             := g_rec_in.taste_tasteless_ind;
   g_rec_out.taste_chemical_ind              := g_rec_in.taste_chemical_ind;
   g_rec_out.taste_tough_ind                 := g_rec_in.taste_tough_ind;
   g_rec_out.taste_dry_ind                   := g_rec_in.taste_dry_ind;
   g_rec_out.smell_chemical_ind              := g_rec_in.smell_chemical_ind;
   g_rec_out.smell_off_ind                   := g_rec_in.smell_off_ind;
   g_rec_out.smell_bad_rotten_ind            := g_rec_in.smell_bad_rotten_ind;
   g_rec_out.feel_hard_ind                   := g_rec_in.feel_hard_ind;
   g_rec_out.feel_soft_ind                   := g_rec_in.feel_soft_ind;
   g_rec_out.feel_dry_ind                    := g_rec_in.feel_dry_ind;
   g_rec_out.feel_mushy_ind                  := g_rec_in.feel_mushy_ind;
   g_rec_out.look_fatty_ind                  := g_rec_in.look_fatty_ind;
   g_rec_out.look_discoloured_ind            := g_rec_in.look_discoloured_ind;
   g_rec_out.look_separated_ind              := g_rec_in.look_separated_ind;
   g_rec_out.look_burnt_ind                  := g_rec_in.look_burnt_ind;
   g_rec_out.look_pale_ind                   := g_rec_in.look_pale_ind;
   g_rec_out.look_underbaked_raw_ind         := g_rec_in.look_underbaked_raw_ind;
   g_rec_out.look_dry_ind                    := g_rec_in.look_dry_ind;
   g_rec_out.look_over_ripe_ind              := g_rec_in.look_over_ripe_ind;
   g_rec_out.look_under_ripe_ind             := g_rec_in.look_under_ripe_ind;
   g_rec_out.packaging_not_sealed_ind        := g_rec_in.packaging_not_sealed_ind;
   g_rec_out.packaging_leaks_ind             := g_rec_in.packaging_leaks_ind;
   g_rec_out.packaging_misleading_ind        := g_rec_in.packaging_misleading_ind;
   g_rec_out.packaging_blewup_microwave_ind  := g_rec_in.packaging_blewup_microwave_ind;
   g_rec_out.packaging_lack_of_info_ind      := g_rec_in.packaging_lack_of_info_ind;
   g_rec_out.packaging_incorrect_info_ind    := g_rec_in.packaging_incorrect_info_ind;
   g_rec_out.packaging_wrong_product_ind     := g_rec_in.packaging_wrong_product_ind;
   g_rec_out.value_poor_value_ind            := g_rec_in.value_poor_value_ind;
   g_rec_out.value_incorrect_price_ind       := g_rec_in.value_incorrect_price_ind;
   g_rec_out.value_incorrect_promotion_ind   := g_rec_in.value_incorrect_promotion_ind;
   g_rec_out.value_too_expensive_ind         := g_rec_in.value_too_expensive_ind;

   g_rec_out.last_updated_date               := g_date;

---------------------------------------------------------
-- default to a dummy level 3 number if input not at level 3
---------------------------------------------------------
   if g_rec_in.inq_type_level_ind = 2 then

    begin
     select cl_inq_feedback_no
       into   g_rec_in.inq_type_no
       from   dim_cust_cl_type_cat
     where  g_rec_in.inq_type_no = cl_inq_type_cat_no;

     exception
               when no_data_found then
                  g_rec_in.inq_type_no          := null;
    end;

    begin

     select cl_inq_category_no
       into   g_rec_in.inq_type_no
       from   dim_cust_cl_feedback
     where  g_rec_in.inq_type_no = cl_inq_feedback_no;
--      g_rec_in.inq_type_no := g_rec_in.inq_type_no + 99990000;

     exception
               when no_data_found then
                  g_rec_in.inq_type_no          := null;

      g_rec_in.inq_type_no := nvl(g_rec_in.inq_type_no,0) + 99990000;
     end;
   end if;

   if g_rec_in.inq_type_level_ind = 1 then

      begin
      select CL_INQ_CATEGORY_NO
      into   g_rec_in.inq_type_no
      from   dim_cust_cl_feedback
      where  g_rec_in.inq_type_no = cl_inq_feedback_no;
--      g_rec_in.inq_type_no := g_rec_in.inq_type_no + 99990000;

      exception
               when no_data_found then
                  g_rec_in.inq_type_no          := null;
      g_rec_in.inq_type_no := nvl(g_rec_in.inq_type_no,0) + 99990000;
      end;
   end if;

   if g_rec_in.inq_type_level_ind = 0 then
      g_rec_in.inq_type_no := nvl(g_rec_in.inq_type_no,0) + 99990000;
   end if;

   select sk1_cl_inq_sub_cat_no
   into   g_rec_out.sk1_cl_inq_sub_cat_no
   from   dim_cust_cl_sub_cat
   where  cl_inq_sub_cat_no = g_rec_in.inq_type_no;

---------------------------------------------------------------------------------
   if not  dwh_valid.fnd_supplier(g_rec_in.supplier_no) then
        g_rec_in.supplier_no := 0;
        g_rec_out.sk1_supplier_no := 0;
   end if;

   select sk1_supplier_no
   into   g_rec_out.sk1_supplier_no
   from   dim_supplier
   where  supplier_no = g_rec_in.supplier_no;
----------------------------------------------------------------------------------
   if  g_rec_in.location_no = 0 then
       if g_rec_in.svc_cia_store_no <> 0 and
          g_rec_in.svc_cia_store_no is not null then
          g_rec_in.location_no := g_rec_in.svc_cia_store_no;
       end if;
   end if;

----------------------------------------------------------------------------------
   if not  dwh_valid.fnd_location(g_rec_in.location_no) then
        g_rec_in.location_no := 101;
        g_rec_out.sk1_location_no := 411;
   end if;
   dwh_lookup.dim_location_sk1(g_rec_in.location_no,g_rec_out.sk1_location_no);
----------------------------------------------------------------------------------
   if not  dwh_valid.fnd_item(g_rec_in.item_no) then
        g_rec_in.item_no      := 999999999999992100;
   end if;
   dwh_lookup.dim_item_sk1(g_rec_in.item_no,g_rec_out.sk1_item_no);
-----------------------------------------------------------------------------------
   if not  dwh_valid.fnd_department(g_rec_in.department_no) then
        g_rec_in.department_no := 2100;
        g_rec_out.sk1_department_no := 5375231;
   end if;

   select sk1_department_no
   into   g_rec_out.sk1_department_no
   from   dim_department
   where  department_no = g_rec_in.department_no;
-----------------------------------------------------------------------------------

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
      insert into cust_cl_inquiry values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).inquiry_no;
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
      update cust_cl_inquiry
      set   sk1_cl_inq_sub_cat_no           = a_tbl_update(i).sk1_cl_inq_sub_cat_no,
            sk1_owner_user_no               = a_tbl_update(i).sk1_owner_user_no,
            sk1_logged_by_user_no           = a_tbl_update(i).sk1_logged_by_user_no,
            sk1_channel_inbound_no          = a_tbl_update(i).sk1_channel_inbound_no,
            sk1_cl_status_no                = a_tbl_update(i).sk1_cl_status_no,
            interaction_no                  = a_tbl_update(i).interaction_no,
            sk1_bus_area_no                 = a_tbl_update(i).sk1_bus_area_no,
            priority                        = a_tbl_update(i).priority,
            status_central                  = a_tbl_update(i).status_central,
            inquiry_bus_area                = a_tbl_update(i).inquiry_bus_area,
            owner_grp                       = a_tbl_update(i).owner_grp,
            transfer_flag                   = a_tbl_update(i).transfer_flag,
            transfer_user                   = a_tbl_update(i).transfer_user,
            transfer_grp                    = a_tbl_update(i).transfer_grp,
            escal_ind                       = a_tbl_update(i).escal_ind,
            escal_level                     = a_tbl_update(i).escal_level,
            receipt_ackn_required           = a_tbl_update(i).receipt_ackn_required,
            cust_feedback_required          = a_tbl_update(i).cust_feedback_required,
            inq_details                     = a_tbl_update(i).inq_details,
            logged_date                     = a_tbl_update(i).logged_date,
            classified_date                 = a_tbl_update(i).classified_date,
            receipt_ackn_date               = a_tbl_update(i).receipt_ackn_date,
            last_prg_upd_date               = a_tbl_update(i).last_prg_upd_date,
            cust_resolved_date              = a_tbl_update(i).cust_resolved_date,
            transfer_date                   = a_tbl_update(i).transfer_date,
            transfer_acc_date               = a_tbl_update(i).transfer_acc_date,
            special_resolved_date           = a_tbl_update(i).special_resolved_date,
            special_ext_period              = a_tbl_update(i).special_ext_period,
            closed_date                     = a_tbl_update(i).closed_date,
            qa_rqd_ind                      = a_tbl_update(i).qa_rqd_ind,
            qa_compl_date                   = a_tbl_update(i).qa_compl_date,
            sk1_item_no                     = a_tbl_update(i).sk1_item_no,
            txt_style_no                    = a_tbl_update(i).txt_style_no,
            product_desc                    = a_tbl_update(i).product_desc,
            txt_size                        = a_tbl_update(i).txt_size,
            txt_col_fragr                   = a_tbl_update(i).txt_col_fragr,
            sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no,
            sk1_department_no               = a_tbl_update(i).sk1_department_no,
            purchase_date                   = a_tbl_update(i).purchase_date,
            sk1_location_no                 = a_tbl_update(i).sk1_location_no,
            item_qty                        = a_tbl_update(i).item_qty,
            selling_price                   = a_tbl_update(i).selling_price,
            store_refund_given              = a_tbl_update(i).store_refund_given,
            refund_value                    = a_tbl_update(i).refund_value,
            complementary_item_given        = a_tbl_update(i).complementary_item_given,
            complementary_item_value        = a_tbl_update(i).complementary_item_value,
            txt_debit_memo                  = a_tbl_update(i).txt_debit_memo,
            foods_sell_by_date              = a_tbl_update(i).foods_sell_by_date,
            foods_prod_batch                = a_tbl_update(i).foods_prod_batch,
            foods_mass_size                 = a_tbl_update(i).foods_mass_size,
            fo_avail                        = a_tbl_update(i).fo_avail,
            fo_present                      = a_tbl_update(i).fo_present,
            fo_received_date_store          = a_tbl_update(i).fo_received_date_store,
            fo_received_date_cl             = a_tbl_update(i).fo_received_date_cl,
            fo_received_date_tech           = a_tbl_update(i).fo_received_date_tech,
            svc_staff_member                = a_tbl_update(i).svc_staff_member,
            svc_cia_store_no                = a_tbl_update(i).svc_cia_store_no,
            cia_incident_date               = a_tbl_update(i).cia_incident_date,
            cia_cust_asstd                  = a_tbl_update(i).cia_cust_asstd,
            cia_claims_impl                 = a_tbl_update(i).cia_claims_impl,
            cia_asst_given                  = a_tbl_update(i).cia_asst_given,
            cia_store_resolved              = a_tbl_update(i).cia_store_resolved,
            cia_asst_mgmt                   = a_tbl_update(i).cia_asst_mgmt,
            resol_type_lev1                 = a_tbl_update(i).resol_type_lev1,
            resol_type_lev2                 = a_tbl_update(i).resol_type_lev2,
            specl_resol_user                = a_tbl_update(i).specl_resol_user,
            cust_resol_user                 = a_tbl_update(i).cust_resol_user,
            justified_status                = a_tbl_update(i).justified_status,
            penalty_area                    = a_tbl_update(i).penalty_area,
            penalty_value                   = a_tbl_update(i).penalty_value,
            no_corresp_sent                 = a_tbl_update(i).no_corresp_sent,
            spec_resolved_ind               = a_tbl_update(i).spec_resolved_ind,
            spec_resolution                 = a_tbl_update(i).spec_resolution,
            cust_resolved_ind               = a_tbl_update(i).cust_resolved_ind,
            cust_resolution                 = a_tbl_update(i).cust_resolution,
            break_cold_chain                = a_tbl_update(i).break_cold_chain,
            quality_complaint_reason        = a_tbl_update(i).quality_complaint_reason,
            taste_wild_ind                  = a_tbl_update(i).taste_wild_ind,
            taste_off_ind                   = a_tbl_update(i).taste_off_ind,
            taste_rancid_ind                = a_tbl_update(i).taste_rancid_ind,
            taste_salty_ind                 = a_tbl_update(i).taste_salty_ind,
            taste_sour_ind                  = a_tbl_update(i).taste_sour_ind,
            taste_tasteless_ind             = a_tbl_update(i).taste_tasteless_ind,
            taste_chemical_ind              = a_tbl_update(i).taste_chemical_ind,
            taste_tough_ind                 = a_tbl_update(i).taste_tough_ind,
            taste_dry_ind                   = a_tbl_update(i).taste_dry_ind,
            smell_chemical_ind              = a_tbl_update(i).smell_chemical_ind,
            smell_off_ind                   = a_tbl_update(i).smell_off_ind,
            smell_bad_rotten_ind            = a_tbl_update(i).smell_bad_rotten_ind,
            feel_hard_ind                   = a_tbl_update(i).feel_hard_ind,
            feel_soft_ind                   = a_tbl_update(i).feel_soft_ind,
            feel_dry_ind                    = a_tbl_update(i).feel_dry_ind,
            feel_mushy_ind                  = a_tbl_update(i).feel_mushy_ind,
            look_fatty_ind                  = a_tbl_update(i).look_fatty_ind,
            look_discoloured_ind            = a_tbl_update(i).look_discoloured_ind,
            look_separated_ind              = a_tbl_update(i).look_separated_ind,
            look_burnt_ind                  = a_tbl_update(i).look_burnt_ind,
            look_pale_ind                   = a_tbl_update(i).look_pale_ind,
            look_underbaked_raw_ind         = a_tbl_update(i).look_underbaked_raw_ind,
            look_dry_ind                    = a_tbl_update(i).look_dry_ind,
            look_over_ripe_ind              = a_tbl_update(i).look_over_ripe_ind,
            look_under_ripe_ind             = a_tbl_update(i).look_under_ripe_ind,
            packaging_not_sealed_ind        = a_tbl_update(i).packaging_not_sealed_ind,
            packaging_leaks_ind             = a_tbl_update(i).packaging_leaks_ind,
            packaging_misleading_ind        = a_tbl_update(i).packaging_misleading_ind,
            packaging_blewup_microwave_ind  = a_tbl_update(i).packaging_blewup_microwave_ind,
            packaging_lack_of_info_ind      = a_tbl_update(i).packaging_lack_of_info_ind,
            packaging_incorrect_info_ind    = a_tbl_update(i).packaging_incorrect_info_ind,
            packaging_wrong_product_ind     = a_tbl_update(i).packaging_wrong_product_ind,
            value_poor_value_ind            = a_tbl_update(i).value_poor_value_ind,
            value_incorrect_price_ind       = a_tbl_update(i).value_incorrect_price_ind,
            value_incorrect_promotion_ind   = a_tbl_update(i).value_incorrect_promotion_ind,
            value_too_expensive_ind         = a_tbl_update(i).value_too_expensive_ind,

            last_updated_date               = a_tbl_update(i).last_updated_date

       where             inquiry_no         = a_tbl_update(i).inquiry_no  ;

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
                       ' '||a_tbl_update(g_error_index).inquiry_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_cust_valid.cust_cl_inquiry(g_rec_out.inquiry_no);

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

    l_text := 'LOAD OF cust_cl_inquiry EX fnd_cust_cl_inquiry STARTED AT '||
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
 /*BYPASSING UNTIL SUBCAT ISSUE SORTED
    open c_fnd_cust_cl_inquiry;
    fetch c_fnd_cust_cl_inquiry bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_cust_cl_inquiry bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_cust_cl_inquiry;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;

*/

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

end WH_PRF_CUST_530U_bck16102015;
