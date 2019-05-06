--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_100U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_100U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Mar 2018
--  Author:      Alastair de Wet
--  Purpose:     Load RMS Food Stock fact table in performance layer
--               with input ex RMS Stock table from foundation layer for Africa only.
--  Tables:      Input  - fnd_mc_loc_item_dy_rms_stk_fd
--               Output - rtl_mc_loc_item_dy_rms_stock
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_mc_loc_item_dy_rms_stock%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                     rtl_loc_item_dy_rms_price.wac%type             := 0;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_100U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD RMS FOOD AFRICA STOCK FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_mc_loc_item_dy_rms_stock%rowtype index by binary_integer;
type tbl_array_u is table of rtl_mc_loc_item_dy_rms_stock%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_dy_rms_stk is
   select fnd_lid.*,
          di.sk1_department_no,di.sk1_item_no,
          dl.chain_no,dl.sk1_location_no,
          decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray) num_units_per_tray,
          nvl(fnd_li.this_wk_catalog_ind,0) this_wk_catalog_ind,
          nvl(fnd_li.next_wk_catalog_ind,0) next_wk_catalog_ind,
          dih.sk2_item_no,
          dlh.sk2_location_no
   from   fnd_mc_loc_item_dy_rms_stk_fd fnd_lid,
          dim_item di,
          dim_location dl,
          fnd_location_item fnd_li,
          dim_item_hist dih,
          dim_location_hist dlh
   where  fnd_lid.last_updated_date  = g_date and
          fnd_lid.item_no            = di.item_no and
          fnd_lid.location_no        = dl.location_no and
          fnd_lid.item_no            = dih.item_no and
          dih.sk2_active_to_date     = dwh_constants.sk_to_date and
          fnd_lid.location_no        = dlh.location_no and
          dlh.sk2_active_to_date     = dwh_constants.sk_to_date and
          fnd_lid.item_no            = fnd_li.item_no(+) and
          fnd_lid.location_no        = fnd_li.location_no(+);

g_rec_in                   c_fnd_rtl_loc_item_dy_rms_stk%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_rms_stk%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.com_flag_ind                    := g_rec_in.com_flag_ind ;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.soh_qty                         := g_rec_in.soh_qty;
   g_rec_out.soh_selling                     := g_rec_in.soh_selling_opr;
   g_rec_out.soh_cost                        := g_rec_in.soh_cost_opr;
   g_rec_out.sit_qty                         := g_rec_in.sit_qty;
   g_rec_out.sit_selling                     := g_rec_in.sit_selling_opr;
   g_rec_out.sit_cost                        := g_rec_in.sit_cost_opr;
   g_rec_out.non_sellable_qty                := g_rec_in.non_sellable_qty;
   g_rec_out.inbound_excl_cust_ord_qty       := g_rec_in.inbound_excl_cust_ord_qty;
   g_rec_out.inbound_excl_cust_ord_selling   := g_rec_in.inbnd_excl_cust_ord_sell_opr;
   g_rec_out.inbound_excl_cust_ord_cost      := g_rec_in.inbnd_excl_cust_ord_cost_opr;
   g_rec_out.inbound_incl_cust_ord_qty       := g_rec_in.inbound_incl_cust_ord_qty;
   g_rec_out.inbound_incl_cust_ord_selling   := g_rec_in.inbnd_incl_cust_ord_sell_opr;
   g_rec_out.inbound_incl_cust_ord_cost      := g_rec_in.inbnd_incl_cust_ord_cost_opr;
   g_rec_out.boh_qty                         := g_rec_in.boh_qty;
   g_rec_out.boh_selling                     := g_rec_in.boh_selling_opr;
   g_rec_out.boh_cost                        := g_rec_in.boh_cost_opr;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
--MULTI CURRENCY --   
    g_rec_out.inbnd_excl_cust_ord_cost_local  := g_rec_in.inbnd_excl_cust_ord_cost_local;
    g_rec_out.inbnd_excl_cust_ord_sell_local  := g_rec_in.inbnd_excl_cust_ord_sell_local;
    g_rec_out.inbnd_incl_cust_ord_cost_local  := g_rec_in.inbnd_incl_cust_ord_cost_local;
    g_rec_out.inbnd_incl_cust_ord_sell_local  := g_rec_in.inbnd_incl_cust_ord_sell_local;
    g_rec_out.sit_cost_local                  := g_rec_in.sit_cost_local;
    g_rec_out.sit_selling_local               := g_rec_in.sit_selling_local;
    g_rec_out.soh_cost_local                  := g_rec_in.soh_cost_local;
    g_rec_out.soh_selling_local               := g_rec_in.soh_selling_local;
    g_rec_out.boh_selling_local               := g_rec_in.boh_selling_local;
    g_rec_out.boh_cost_local                  := g_rec_in.boh_cost_local;

   g_rec_out.sit_fr_cost                     := '';
   g_rec_out.soh_fr_cost                     := '';
   g_rec_out.franchise_soh_margin            := '';
   g_rec_out.boh_fr_cost                     := '';
   g_rec_out.reg_soh_fr_cost                 := '';
--MC --   
   g_rec_out.sit_fr_cost_local                      := '';
   g_rec_out.soh_fr_cost_local                      := '';
   g_rec_out.franchise_soh_margin_local             := '';
   g_rec_out.boh_fr_cost_local                      := '';
   g_rec_out.reg_soh_fr_cost_local                  := '';

-- Value add and calculated fields added to performance layer
-- Case quantities can not contain fractions, the case quantity has to be an integer value (ie. 976.0).
   g_rec_out.sit_cases           := round((nvl(g_rec_out.sit_qty,0)/g_rec_in.num_units_per_tray),0);
   g_rec_out.soh_cases           := round((nvl(g_rec_out.soh_qty,0)/g_rec_in.num_units_per_tray),0);
   g_rec_out.boh_cases           := round((nvl(g_rec_out.boh_qty,0)/g_rec_in.num_units_per_tray),0);

   g_rec_out.sit_margin          := nvl(g_rec_out.sit_selling,0) - nvl(g_rec_out.sit_cost,0);
   g_rec_out.soh_margin          := nvl(g_rec_out.soh_selling,0) - nvl(g_rec_out.soh_cost,0);
   g_rec_out.reg_soh_qty         := nvl(g_rec_out.soh_qty,0) - nvl(g_rec_out.clear_soh_qty,0);
   g_rec_out.reg_soh_selling     := nvl(g_rec_out.soh_selling,0) - nvl(g_rec_out.clear_soh_selling,0);
   g_rec_out.reg_soh_cost        := nvl(g_rec_out.soh_cost,0) - nvl(g_rec_out.clear_soh_cost,0);

   if g_rec_in.chain_no = 20 then
      begin
         select debtors_commission_perc
         into   g_debtors_commission_perc
         from   rtl_loc_dept_dy
         where  sk1_location_no       = g_rec_out.sk1_location_no and
                sk1_department_no     = g_rec_in.sk1_department_no and
                post_date             = g_rec_out.post_date;
         exception
            when no_data_found then
              g_debtors_commission_perc := 0;
      end;
      if g_debtors_commission_perc is null then
         g_debtors_commission_perc := 0;
      end if;
      g_rec_out.sit_fr_cost                  := nvl(g_rec_out.sit_cost,0) + round((nvl(g_rec_out.sit_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.soh_fr_cost                  := nvl(g_rec_out.soh_cost,0) + round((nvl(g_rec_out.soh_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_soh_margin         := nvl(g_rec_out.soh_selling,0) - nvl(g_rec_out.soh_cost,0);
      g_rec_out.boh_fr_cost                  := nvl(g_rec_out.boh_cost,0) + round((nvl(g_rec_out.boh_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.reg_soh_fr_cost              := nvl(g_rec_out.soh_fr_cost,0) - nvl(g_rec_out.clear_soh_fr_cost,0);
   end if;
   
-- MULTI CURRENCY --   
-- Value add and calculated fields added to performance layer

   g_rec_out.sit_margin_local          := nvl(g_rec_out.sit_selling_local,0) - nvl(g_rec_out.sit_cost_local,0);
   g_rec_out.soh_margin_local          := nvl(g_rec_out.soh_selling_local,0) - nvl(g_rec_out.soh_cost_local,0);
   g_rec_out.reg_soh_selling_local     := nvl(g_rec_out.soh_selling_local,0) - nvl(g_rec_out.clear_soh_selling_local,0);
   g_rec_out.reg_soh_cost_local        := nvl(g_rec_out.soh_cost_local,0)    - nvl(g_rec_out.clear_soh_cost_local,0);

   if g_rec_in.chain_no = 20 then
      g_rec_out.sit_fr_cost_local            := nvl(g_rec_out.sit_cost_local,0) + round((nvl(g_rec_out.sit_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.soh_fr_cost_local            := nvl(g_rec_out.soh_cost_local,0) + round((nvl(g_rec_out.soh_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_soh_margin_local   := nvl(g_rec_out.soh_selling_local,0) - nvl(g_rec_out.soh_cost_local,0);
      g_rec_out.boh_fr_cost_local            := nvl(g_rec_out.boh_cost_local,0) + round((nvl(g_rec_out.boh_cost_local,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.reg_soh_fr_cost_local        := nvl(g_rec_out.soh_fr_cost_local,0) - nvl(g_rec_out.clear_soh_fr_cost_local,0);
   end if;


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_mc_loc_item_dy_rms_stock values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update rtl_mc_loc_item_dy_rms_stock
       set    com_flag_ind                    = a_tbl_update(i).com_flag_ind,
              sit_qty                         = a_tbl_update(i).sit_qty,
              sit_cases                       = a_tbl_update(i).sit_cases,
              sit_selling                     = a_tbl_update(i).sit_selling,
              sit_cost                        = a_tbl_update(i).sit_cost,
              sit_fr_cost                     = a_tbl_update(i).sit_fr_cost,
              sit_margin                      = a_tbl_update(i).sit_margin,
              non_sellable_qty                = a_tbl_update(i).non_sellable_qty,
              soh_qty                         = a_tbl_update(i).soh_qty,
              soh_cases                       = a_tbl_update(i).soh_cases,
              soh_selling                     = a_tbl_update(i).soh_selling,
              soh_cost                        = a_tbl_update(i).soh_cost,
              soh_fr_cost                     = a_tbl_update(i).soh_fr_cost,
              soh_margin                      = a_tbl_update(i).soh_margin,
              franchise_soh_margin            = a_tbl_update(i).franchise_soh_margin,
              inbound_excl_cust_ord_qty       = a_tbl_update(i).inbound_excl_cust_ord_qty,
              inbound_excl_cust_ord_selling   = a_tbl_update(i).inbound_excl_cust_ord_selling,
              inbound_excl_cust_ord_cost      = a_tbl_update(i).inbound_excl_cust_ord_cost,
              inbound_incl_cust_ord_qty       = a_tbl_update(i).inbound_incl_cust_ord_qty,
              inbound_incl_cust_ord_selling   = a_tbl_update(i).inbound_incl_cust_ord_selling,
              inbound_incl_cust_ord_cost      = a_tbl_update(i).inbound_incl_cust_ord_cost,
              boh_qty                         = a_tbl_update(i).boh_qty,
              boh_cases                       = a_tbl_update(i).boh_cases,
              boh_selling                     = a_tbl_update(i).boh_selling,
              boh_cost                        = a_tbl_update(i).boh_cost,
              boh_fr_cost                     = a_tbl_update(i).boh_fr_cost,
              reg_soh_qty                     = a_tbl_update(i).reg_soh_qty,
              reg_soh_selling                 = a_tbl_update(i).reg_soh_selling,
              reg_soh_cost                    = a_tbl_update(i).reg_soh_cost,
              reg_soh_fr_cost                 = a_tbl_update(i).reg_soh_fr_cost,
--MULTI CURRENCY --              
              inbnd_excl_cust_ord_cost_local  = a_tbl_update(i).inbnd_excl_cust_ord_cost_local,
              inbnd_excl_cust_ord_sell_local  = a_tbl_update(i).inbnd_excl_cust_ord_sell_local,
              inbnd_incl_cust_ord_cost_local  = a_tbl_update(i).inbnd_incl_cust_ord_cost_local,
              inbnd_incl_cust_ord_sell_local  = a_tbl_update(i).inbnd_incl_cust_ord_sell_local,
              sit_cost_local                  = a_tbl_update(i).sit_cost_local,
              sit_selling_local               = a_tbl_update(i).sit_selling_local,
              sit_fr_cost_local               = a_tbl_update(i).sit_fr_cost_local,
              sit_margin_local                = a_tbl_update(i).sit_margin_local,
              soh_cost_local                  = a_tbl_update(i).soh_cost_local,
              soh_selling_local               = a_tbl_update(i).soh_selling_local,
              soh_fr_cost_local               = a_tbl_update(i).soh_fr_cost_local,
              soh_margin_local                = a_tbl_update(i).soh_margin_local,
              franchise_soh_margin_local      = a_tbl_update(i).franchise_soh_margin_local,
              boh_selling_local               = a_tbl_update(i).boh_selling_local,
              boh_cost_local                  = a_tbl_update(i).boh_cost_local,
              boh_fr_cost_local               = a_tbl_update(i).boh_fr_cost_local,
              clear_soh_cost_local            = a_tbl_update(i).clear_soh_cost_local,
              clear_soh_selling_local         = a_tbl_update(i).clear_soh_selling_local,
              clear_soh_margin_local          = a_tbl_update(i).clear_soh_margin_local,
              clear_soh_fr_cost_local         = a_tbl_update(i).clear_soh_fr_cost_local,
              reg_soh_selling_local           = a_tbl_update(i).reg_soh_selling_local,
              reg_soh_cost_local              = a_tbl_update(i).reg_soh_cost_local,
              reg_soh_fr_cost_local           = a_tbl_update(i).reg_soh_fr_cost_local,
              
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no      and
              post_date                       = a_tbl_update(i).post_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).post_date;
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
   from   rtl_mc_loc_item_dy_rms_stock
   where  sk1_location_no    = g_rec_out.sk1_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no      and
          post_date          = g_rec_out.post_date;

   if g_count = 1 then
      g_found := TRUE;
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
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    
    g_forall_limit := 20000;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD rtl_mc_loc_item_dy_rms_stock FOODS EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := g_date -1;        --XXXREMOVE!!
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_dy_rms_stk;
    fetch c_fnd_rtl_loc_item_dy_rms_stk bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_item_dy_rms_stk bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_dy_rms_stk;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_mc_100u;
