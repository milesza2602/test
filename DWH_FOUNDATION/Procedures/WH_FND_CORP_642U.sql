--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_642U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_642U" 
                                                    (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        August 2008
--  Author:      Sean Le Roux
--  Purpose:     Load deal_detail information in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_deal_detail_cpy
--               Output - fnd_deal_detail
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  November 2017 - Addition of Multi-Currency fields (Bhavesh Valodia)
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_deal_detail_hsp.sys_process_msg%type;
g_rec_out            fnd_deal_detail%rowtype;
g_rec_in             stg_rms_deal_detail_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_642U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FND_deal_detail DATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_rms_deal_detail_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_deal_detail%rowtype index by binary_integer;
type tbl_array_u is table of fnd_deal_detail%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_deal_detail_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_deal_detail_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_rms_deal_detail is
   select *
   from stg_rms_deal_detail_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as

v_count              number        :=  0;

begin

   g_hospital                                := 'N';

   g_rec_out.deal_no                         := g_rec_in.deal_no;
   g_rec_out.deal_detail_no                  := g_rec_in.deal_detail_no;
   g_rec_out.deal_comp_type                  := g_rec_in.deal_comp_type;
   g_rec_out.application_order_no            := g_rec_in.application_order_no;
   g_rec_out.billing_type                    := g_rec_in.billing_type;
   g_rec_out.bill_back_period_code           := g_rec_in.bill_back_period_code;
   g_rec_out.collect_start_date              := g_rec_in.collect_start_date;
   g_rec_out.collect_end_date                := g_rec_in.collect_end_date;
   g_rec_out.deal_applied_timing_code        := g_rec_in.deal_applied_timing_code;
   g_rec_out.cost_applied_code               := g_rec_in.cost_applied_code;
   g_rec_out.price_cost_applied_ind          := g_rec_in.price_cost_applied_ind;
   g_rec_out.deal_class_code                 := g_rec_in.deal_class_code;
   g_rec_out.threshold_limit_type            := g_rec_in.threshold_limit_type;
   g_rec_out.threshold_limit_uom_code        := g_rec_in.threshold_limit_uom_code;
   g_rec_out.threshold_value_type            := g_rec_in.threshold_value_type;
   g_rec_out.qty_thresh_buy_item_no          := g_rec_in.qty_thresh_buy_item_no;
   g_rec_out.qty_thresh_get_type             := g_rec_in.qty_thresh_get_type;
   g_rec_out.qty_thresh_get_value            := g_rec_in.qty_thresh_get_value;
   g_rec_out.qty_thresh_buy_qty              := g_rec_in.qty_thresh_buy_qty;
   g_rec_out.qty_thresh_recur_ind            := g_rec_in.qty_thresh_recur_ind;
   g_rec_out.qty_thresh_buy_target_value     := g_rec_in.qty_thresh_buy_target_value;
   g_rec_out.qty_thresh_buy_avg_loc_value    := g_rec_in.qty_thresh_buy_avg_loc_value;
   g_rec_out.qty_thresh_get_item_no          := g_rec_in.qty_thresh_get_item_no;
   g_rec_out.qty_thresh_get_qty              := g_rec_in.qty_thresh_get_qty;
   g_rec_out.qty_thresh_free_item_cost_pric  := g_rec_in.qty_thresh_free_item_cost_pric;
   g_rec_out.tran_level_discount_ind         := g_rec_in.tran_level_discount_ind;
   g_rec_out.rebate_ind                      := g_rec_in.rebate_ind;
   g_rec_out.rebate_active_date              := g_rec_in.rebate_active_date;
   g_rec_out.rebate_calc_type                := g_rec_in.rebate_calc_type;
   g_rec_out.growth_rebate_ind               := g_rec_in.growth_rebate_ind;
   g_rec_out.hist_comp_start_date            := g_rec_in.hist_comp_start_date;
   g_rec_out.hist_comp_end_date              := g_rec_in.hist_comp_end_date;
   g_rec_out.current_comp_start_date         := g_rec_in.current_comp_start_date;
   g_rec_out.current_comp_end_date           := g_rec_in.current_comp_end_date;
   g_rec_out.rebate_purch_sales_ind          := g_rec_in.rebate_purch_sales_ind;
   g_rec_out.deal_detail_comment             := g_rec_in.deal_detail_comment;
   g_rec_out.create_datetime                 := g_rec_in.create_datetime;
   g_rec_out.last_update_user_id             := g_rec_in.last_update_user_id;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_update_datetime            := g_rec_in.last_update_datetime;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.rtv_ind                         := g_rec_in.rtv_ind;
   g_rec_out.rtv_reason_code                 := g_rec_in.rtv_reason_code;
   g_rec_out.qty_thr_buy_avg_loc_val_local     := g_rec_in.qty_thr_buy_avg_loc_val_local;
   g_rec_out.qty_thr_buy_avg_loc_val_opr       := g_rec_in.qty_thr_buy_avg_loc_val_opr; 
   g_rec_out.qty_thr_buy_targ_val_local        := g_rec_in.qty_thr_buy_targ_val_local; 
   g_rec_out.qty_thr_buy_targ_val_opr          := g_rec_in.qty_thr_buy_targ_val_opr;
   g_rec_out.qty_thr_f_item_cost_pric_local    := g_rec_in.qty_thr_f_item_cost_pric_local;
   g_rec_out.qty_thr_f_item_cost_pric_opr      := g_rec_in.qty_thr_f_item_cost_pric_opr;
   g_rec_out.qty_thr_get_val_local             := g_rec_in.qty_thr_get_val_local;
   g_rec_out.qty_thr_get_val_opr               := g_rec_in.qty_thr_get_val_opr;



   if not dwh_valid.fnd_deal(g_rec_out.deal_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_deal_not_found;
     l_text          := dwh_constants.vc_deal_not_found||g_rec_out.deal_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

--   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--     return;
--   end if;

   select count(1)
   into v_count
   from fnd_deal_comp_type
   where deal_comp_type = g_rec_in.deal_comp_type;

   if v_count = 0 then
     g_hospital      := 'Y';
     g_hospital_text := 'INVALID DEAL COMP TYPE - FND_DEAL_COMP_TYPE DOES NOT CONTAIN INPUT VALUES';
     l_text          := 'INVALID DEAL COMP TYPE - FND_DEAL_COMP_TYPE DOES NOT CONTAIN INPUT VALUES'||g_rec_out.deal_comp_type;
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

   insert into stg_rms_deal_detail_hsp values g_rec_in;
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
       insert into fnd_deal_detail values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).deal_no||
                      ' '||a_tbl_insert(g_error_index).deal_detail_no ;
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
       update fnd_deal_detail
       set    deal_comp_type                  = a_tbl_update(i).deal_comp_type,
              application_order_no            = a_tbl_update(i).application_order_no,
              billing_type                    = a_tbl_update(i).billing_type,
              bill_back_period_code           = a_tbl_update(i).bill_back_period_code,
              collect_start_date              = a_tbl_update(i).collect_start_date,
              collect_end_date                = a_tbl_update(i).collect_end_date,
              deal_applied_timing_code        = a_tbl_update(i).deal_applied_timing_code,
              cost_applied_code               = a_tbl_update(i).cost_applied_code,
              price_cost_applied_ind          = a_tbl_update(i).price_cost_applied_ind,
              deal_class_code                 = a_tbl_update(i).deal_class_code,
              threshold_limit_type            = a_tbl_update(i).threshold_limit_type,
              threshold_limit_uom_code        = a_tbl_update(i).threshold_limit_uom_code,
              threshold_value_type            = a_tbl_update(i).threshold_value_type,
              qty_thresh_buy_item_no          = a_tbl_update(i).qty_thresh_buy_item_no,
              qty_thresh_get_type             = a_tbl_update(i).qty_thresh_get_type,
              qty_thresh_get_value            = a_tbl_update(i).qty_thresh_get_value,
              qty_thresh_buy_qty              = a_tbl_update(i).qty_thresh_buy_qty,
              qty_thresh_recur_ind            = a_tbl_update(i).qty_thresh_recur_ind,
              qty_thresh_buy_target_value     = a_tbl_update(i).qty_thresh_buy_target_value,
              qty_thresh_buy_avg_loc_value    = a_tbl_update(i).qty_thresh_buy_avg_loc_value,
              qty_thresh_get_item_no          = a_tbl_update(i).qty_thresh_get_item_no,
              qty_thresh_get_qty              = a_tbl_update(i).qty_thresh_get_qty,
              qty_thresh_free_item_cost_pric  = a_tbl_update(i).qty_thresh_free_item_cost_pric,
              tran_level_discount_ind         = a_tbl_update(i).tran_level_discount_ind,
              rebate_ind                      = a_tbl_update(i).rebate_ind,
              rebate_active_date              = a_tbl_update(i).rebate_active_date,
              rebate_calc_type                = a_tbl_update(i).rebate_calc_type,
              growth_rebate_ind               = a_tbl_update(i).growth_rebate_ind,
              hist_comp_start_date            = a_tbl_update(i).hist_comp_start_date,
              hist_comp_end_date              = a_tbl_update(i).hist_comp_end_date,
              current_comp_start_date         = a_tbl_update(i).current_comp_start_date,
              current_comp_end_date           = a_tbl_update(i).current_comp_end_date,
              rebate_purch_sales_ind          = a_tbl_update(i).rebate_purch_sales_ind,
              deal_detail_comment             = a_tbl_update(i).deal_detail_comment,
              create_datetime                 = a_tbl_update(i).create_datetime,
              last_update_user_id             = a_tbl_update(i).last_update_user_id,
              last_update_datetime            = a_tbl_update(i).last_update_datetime,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              rtv_ind                         = a_tbl_update(i).rtv_ind,
              rtv_reason_code                 = a_tbl_update(i).rtv_reason_code,
              last_updated_date               = a_tbl_update(i).last_updated_date,
             qty_thr_buy_avg_loc_val_local    = a_tbl_update(i).qty_thr_buy_avg_loc_val_local,
             qty_thr_buy_avg_loc_val_opr      = a_tbl_update(i).qty_thr_buy_avg_loc_val_opr,
             qty_thr_buy_targ_val_local       = a_tbl_update(i).qty_thr_buy_targ_val_local, 
             qty_thr_buy_targ_val_opr         = a_tbl_update(i).qty_thr_buy_targ_val_opr,
             qty_thr_f_item_cost_pric_local   = a_tbl_update(i).qty_thr_f_item_cost_pric_local,
             qty_thr_f_item_cost_pric_opr     = a_tbl_update(i).qty_thr_f_item_cost_pric_opr,
             qty_thr_get_val_local            = a_tbl_update(i).qty_thr_get_val_local,
             qty_thr_get_val_opr              = a_tbl_update(i).qty_thr_get_val_opr
       where  deal_no                         = a_tbl_update(i).deal_no
         and  deal_detail_no                  = a_tbl_update(i).deal_detail_no;

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
                       ' '||a_tbl_update(g_error_index).deal_no||
                       ' '||a_tbl_update(g_error_index).deal_detail_no;
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
       update stg_rms_deal_detail_cpy
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

v_count integer := 0;

begin

  g_found := false;

   select count(1)
   into   v_count
   from   fnd_deal_detail
   where  deal_no        = g_rec_out.deal_no  and
          deal_detail_no = g_rec_out.deal_detail_no;

   if v_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).deal_no = g_rec_out.deal_no
          and a_tbl_insert(i).deal_detail_no = g_rec_out.deal_detail_no then
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

    l_text := 'LOAD OF FND_deal_detail EX RMS STARTED AT '||
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
    open c_stg_rms_deal_detail;
    fetch c_stg_rms_deal_detail bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_rms_deal_detail bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_deal_detail;
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
end wh_fnd_corp_642u;
