--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_118U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_118U_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JAN 2009
--  Author:      Alastair de Wet
--  Purpose:     Load Allocation fact table in performance layer
--               with input ex RMS Allocation table from foundation layer (Foods Only).
--  Tables:      Input  - fnd_rtl_allocation
--               Output - rtl_loc_item_dy_rms_alloc
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  29 april 2015 wendy lyttle  DAVID JONES - do not load where  chain_code = 'DJ'
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
g_rec_out            rtl_loc_item_dy_rms_alloc%rowtype;
g_wac                rtl_loc_item_dy_rms_price.wac%type                  := 0;
g_reg_rsp_excl_vat   rtl_loc_item_dy_rms_price.reg_rsp_excl_vat%type     := 0;
g_num_units_per_tray rtl_loc_item_dy_rms_price.num_units_per_tray%type   := 1;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate) - 14;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_118U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS ALLOC DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_rms_alloc%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rms_alloc%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_allocation is
   select /*+ FULL(aloc) full(fnd_li) parallel(aloc,8) parallel(fnd_li,8) */
          sum(nvl(aloc.alloc_qty,0)) alloc_qty,
          sum(nvl(aloc.apportion_qty,0)) apportion_qty,
          sum(nvl(aloc.sdn_qty,0)) sdn_qty,
          sum(nvl(aloc.orig_alloc_qty,0)) orig_alloc_qty,
          sum(nvl(aloc.dist_qty,0)) dist_qty,
          sum(nvl(aloc.received_qty,0)) received_qty,
          sum(nvl(aloc.alloc_cancel_qty,0)) alloc_cancel_qty,
          sum(nvl(aloc.special_qty,0)) special_qty,
          sum(nvl(aloc.safety_qty,0)) safety_qty,
          sum(nvl(aloc.priority1_qty,0)) priority1_qty,
          sum(nvl(aloc.overstock_qty,0)) overstock_qty,
          max(nvl(reg_rsp_excl_vat,0)) reg_rsp_excl_vat,
          max(nvl(wac,0)) wac,
          trunc(aloc.into_loc_date) into_loc_date,
          di.sk1_item_no,
          dl.sk1_location_no,
          max(aloc.item_no) item_no,
          max(aloc.to_loc_no) location_no,
          max(di.standard_uom_code) standard_uom_code,
          max(nvl(di.static_mass,1)) static_mass,
          max(nvl(di.random_mass_ind,0)) random_mass_ind,
          max(nvl(di.vat_rate_perc,0)) vat_rate_perc,
          max(nvl(fnd_li.num_units_per_tray,1)) num_units_per_tray,
--          max(nvl(fnd_li.reg_rsp,0) * 100 / (100 + di.vat_rate_perc)) reg_rsp_excl_vat,
          max(dlh.sk2_location_no) sk2_location_no,
          max(dih.sk2_item_no) sk2_item_no
   from   fnd_rtl_allocation aloc,
          fnd_location_item fnd_li,
          dim_item di,
          dim_item_hist dih,
          dim_location dl,
          dim_location_hist dlh
   where  aloc.item_no                = di.item_no          and
          aloc.item_no                = dih.item_no         and
          aloc.into_loc_date         between dih.sk2_active_from_date and dih.sk2_active_to_date and
          aloc.to_loc_no              = dl.location_no      and
          aloc.to_loc_no              = dlh.location_no     and
          aloc.into_loc_date         between dlh.sk2_active_from_date and dlh.sk2_active_to_date  and
          aloc.item_no                = fnd_li.item_no(+) and
          aloc.to_loc_no              = fnd_li.location_no(+) and
          aloc.into_loc_date         between '26 SEP 2016'  and g_date and
 --         di.business_unit_no        = 50 and
          aloc.ITEM_NO IN 
          (
          157636,
          42299837,
          42299851,
          42299868 
          ) and
          aloc.into_loc_date         IS NOT NULL and
          (
          nvl(aloc.alloc_qty,0) <> 0 or
          nvl(aloc.apportion_qty,0) <> 0 or
          nvl(aloc.sdn_qty,0) <> 0 or
          nvl(aloc.orig_alloc_qty,0) <> 0 or
          nvl(aloc.dist_qty,0) <> 0 or
          nvl(aloc.received_qty,0) <> 0 or
          nvl(aloc.alloc_cancel_qty,0) <> 0 or
          nvl(aloc.special_qty,0) <> 0 or
          nvl(aloc.safety_qty,0) <> 0 or
          nvl(aloc.priority1_qty,0) <> 0 or
          nvl(aloc.overstock_qty,0) <> 0
          )
          AND (CHAIN_CODE <> 'DJ' or chain_code is null)
   group by di.sk1_item_no, dl.sk1_location_no,  trunc(aloc.into_loc_date);

g_rec_in             c_fnd_rtl_allocation%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_allocation%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.calendar_date                   := g_rec_in.into_loc_date;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.fd_alloc_qty                    := g_rec_in.alloc_qty;
   g_rec_out.fd_apportion_qty                := g_rec_in.apportion_qty;
   g_rec_out.fd_sdn_qty                      := g_rec_in.sdn_qty;
   g_rec_out.fd_orig_alloc_qty               := g_rec_in.orig_alloc_qty;
   g_rec_out.fd_dist_qty                     := g_rec_in.dist_qty;
   g_rec_out.fd_received_qty                 := g_rec_in.received_qty;
   g_rec_out.fd_alloc_cancel_qty             := g_rec_in.alloc_cancel_qty;
   g_rec_out.fd_p1_picking_qty               := g_rec_in.special_qty;
   g_rec_out.fd_p2_picking_qty               := g_rec_in.safety_qty;
   g_rec_out.fd_p3_picking_qty               := g_rec_in.priority1_qty;
   g_rec_out.fd_p4_picking_qty               := g_rec_in.overstock_qty;
   g_rec_out.last_updated_date               := g_date;
   g_wac                                     := g_rec_in.wac;
   g_reg_rsp_excl_vat                        := g_rec_in.reg_rsp_excl_vat;
   g_num_units_per_tray                      := g_rec_in.num_units_per_tray;

/*
   begin
      select wac,reg_rsp_excl_vat,num_units_per_tray
      into   g_wac,g_reg_rsp_excl_vat,g_num_units_per_tray
      from   rtl_loc_item_dy_rms_price
      where  sk1_item_no     =  g_rec_out.sk1_item_no and
             sk1_location_no =  g_rec_out.sk1_location_no and
             calendar_date   =  g_date;
      exception
      when no_data_found then
             g_wac                := 0;
             g_reg_rsp_excl_vat   := 0;
             g_num_units_per_tray := 1;
   end;
*/
   if g_num_units_per_tray <> 0 then
   g_rec_out.fd_alloc_cases         := round(nvl(g_rec_out.fd_alloc_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_apportion_cases     := round(nvl(g_rec_out.fd_apportion_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_sdn_cases           := round(nvl(g_rec_out.fd_sdn_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_orig_alloc_cases    := round(nvl(g_rec_out.fd_orig_alloc_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_dist_cases          := round(nvl(g_rec_out.fd_dist_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_received_cases      := round(nvl(g_rec_out.fd_received_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_alloc_cancel_cases  := round(nvl(g_rec_out.fd_alloc_cancel_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_p1_picking_cases    := round(nvl(g_rec_out.fd_p1_picking_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_p2_picking_cases    := round(nvl(g_rec_out.fd_p2_picking_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_p3_picking_cases    := round(nvl(g_rec_out.fd_p3_picking_qty,0)/g_num_units_per_tray,0);
   g_rec_out.fd_p4_picking_cases    := round(nvl(g_rec_out.fd_p4_picking_qty,0)/g_num_units_per_tray,0);
   end if;



/*
    begin
      select wac
      into   g_wac
      from   fnd_rtl_loc_item_dy_rms_wac wac
      where  wac.item_no     =  g_rec_in.item_no and
             wac.location_no =  g_rec_in.location_no and
             wac.tran_date   =
         (
         select max(wac_sub.tran_date)
         from   fnd_rtl_loc_item_dy_rms_wac wac_sub
         where  wac_sub.location_no  = g_rec_in.location_no and
                wac_sub.item_no      = g_rec_in.item_no and
                wac_sub.tran_date   <= g_rec_out.calendar_date
         );
      exception
      when no_data_found then
             g_wac              := 0;
   end;
*/

   if g_rec_in.standard_uom_code = 'EA' and g_rec_in.random_mass_ind = 1 then
      g_rec_out.fd_alloc_selling              := g_rec_out.fd_alloc_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_apportion_selling          := g_rec_out.fd_apportion_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_sdn_selling                := g_rec_out.fd_sdn_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_orig_alloc_selling         := g_rec_out.fd_orig_alloc_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_dist_selling               := g_rec_out.fd_dist_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_received_selling           := g_rec_out.fd_received_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_alloc_cancel_selling       := g_rec_out.fd_alloc_cancel_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_p1_picking_selling         := g_rec_out.fd_p1_picking_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_p2_picking_selling         := g_rec_out.fd_p2_picking_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_p3_picking_selling         := g_rec_out.fd_p3_picking_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_p4_picking_selling         := g_rec_out.fd_p4_picking_qty * g_reg_rsp_excl_vat * g_rec_in.static_mass;
      g_rec_out.fd_alloc_cost                 := g_rec_out.fd_alloc_qty * g_wac;
      g_rec_out.fd_apportion_cost             := g_rec_out.fd_apportion_qty * g_wac;
      g_rec_out.fd_sdn_cost                   := g_rec_out.fd_sdn_qty * g_wac;
      g_rec_out.fd_orig_alloc_cost            := g_rec_out.fd_orig_alloc_qty * g_wac;
      g_rec_out.fd_dist_cost                  := g_rec_out.fd_dist_qty * g_wac;
      g_rec_out.fd_received_cost              := g_rec_out.fd_received_qty * g_wac;
      g_rec_out.fd_alloc_cancel_cost          := g_rec_out.fd_alloc_cancel_qty * g_wac;
      g_rec_out.fd_p1_picking_cost            := g_rec_out.fd_p1_picking_qty * g_wac;
      g_rec_out.fd_p2_picking_cost            := g_rec_out.fd_p2_picking_qty * g_wac;
      g_rec_out.fd_p3_picking_cost            := g_rec_out.fd_p3_picking_qty * g_wac;
      g_rec_out.fd_p4_picking_cost            := g_rec_out.fd_p4_picking_qty * g_wac;
   else
      g_rec_out.fd_alloc_selling              := g_rec_out.fd_alloc_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_apportion_selling          := g_rec_out.fd_apportion_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_sdn_selling                := g_rec_out.fd_sdn_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_orig_alloc_selling         := g_rec_out.fd_orig_alloc_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_dist_selling               := g_rec_out.fd_dist_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_received_selling           := g_rec_out.fd_received_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_alloc_cancel_selling       := g_rec_out.fd_alloc_cancel_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_p1_picking_selling         := g_rec_out.fd_p1_picking_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_p2_picking_selling         := g_rec_out.fd_p2_picking_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_p3_picking_selling         := g_rec_out.fd_p3_picking_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_p4_picking_selling         := g_rec_out.fd_p4_picking_qty * g_reg_rsp_excl_vat;
      g_rec_out.fd_alloc_cost                 := g_rec_out.fd_alloc_qty * g_wac;
      g_rec_out.fd_apportion_cost             := g_rec_out.fd_apportion_qty * g_wac;
      g_rec_out.fd_sdn_cost                   := g_rec_out.fd_sdn_qty * g_wac;
      g_rec_out.fd_orig_alloc_cost            := g_rec_out.fd_orig_alloc_qty * g_wac;
      g_rec_out.fd_dist_cost                  := g_rec_out.fd_dist_qty * g_wac;
      g_rec_out.fd_received_cost              := g_rec_out.fd_received_qty * g_wac;
      g_rec_out.fd_alloc_cancel_cost          := g_rec_out.fd_alloc_cancel_qty * g_wac;
      g_rec_out.fd_p1_picking_cost            := g_rec_out.fd_p1_picking_qty * g_wac;
      g_rec_out.fd_p2_picking_cost            := g_rec_out.fd_p2_picking_qty * g_wac;
      g_rec_out.fd_p3_picking_cost            := g_rec_out.fd_p3_picking_qty * g_wac;
      g_rec_out.fd_p4_picking_cost            := g_rec_out.fd_p4_picking_qty * g_wac;
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
       insert into rtl_loc_item_dy_rms_alloc values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
       update rtl_loc_item_dy_rms_alloc
       set    fd_alloc_selling                = a_tbl_update(i).fd_alloc_selling,
              fd_alloc_cost                   = a_tbl_update(i).fd_alloc_cost,
              fd_alloc_qty                    = a_tbl_update(i).fd_alloc_qty,
              fd_alloc_cases                  = a_tbl_update(i).fd_alloc_cases,
              fd_apportion_selling            = a_tbl_update(i).fd_apportion_selling,
              fd_apportion_cost               = a_tbl_update(i).fd_apportion_cost,
              fd_apportion_qty                = a_tbl_update(i).fd_apportion_qty,
              fd_apportion_cases              = a_tbl_update(i).fd_apportion_cases,
              fd_sdn_selling                  = a_tbl_update(i).fd_sdn_selling,
              fd_sdn_cost                     = a_tbl_update(i).fd_sdn_cost,
              fd_sdn_qty                      = a_tbl_update(i).fd_sdn_qty,
              fd_sdn_cases                    = a_tbl_update(i).fd_sdn_cases,
              fd_orig_alloc_selling           = a_tbl_update(i).fd_orig_alloc_selling,
              fd_orig_alloc_cost              = a_tbl_update(i).fd_orig_alloc_cost,
              fd_orig_alloc_qty               = a_tbl_update(i).fd_orig_alloc_qty,
              fd_orig_alloc_cases             = a_tbl_update(i).fd_orig_alloc_cases,
              fd_dist_selling                 = a_tbl_update(i).fd_dist_selling,
              fd_dist_cost                    = a_tbl_update(i).fd_dist_cost,
              fd_dist_qty                     = a_tbl_update(i).fd_dist_qty,
              fd_dist_cases                   = a_tbl_update(i).fd_dist_cases,
              fd_received_selling             = a_tbl_update(i).fd_received_selling,
              fd_received_cost                = a_tbl_update(i).fd_received_cost,
              fd_received_qty                 = a_tbl_update(i).fd_received_qty,
              fd_received_cases               = a_tbl_update(i).fd_received_cases,
              fd_alloc_cancel_selling         = a_tbl_update(i).fd_alloc_cancel_selling,
              fd_alloc_cancel_cost            = a_tbl_update(i).fd_alloc_cancel_cost,
              fd_alloc_cancel_qty             = a_tbl_update(i).fd_alloc_cancel_qty,
              fd_alloc_cancel_cases           = a_tbl_update(i).fd_alloc_cancel_cases,
              fd_p1_picking_selling           = a_tbl_update(i).fd_p1_picking_selling,
              fd_p1_picking_cost              = a_tbl_update(i).fd_p1_picking_cost,
              fd_p1_picking_qty               = a_tbl_update(i).fd_p1_picking_qty,
              fd_p1_picking_cases             = a_tbl_update(i).fd_p1_picking_cases,
              fd_p2_picking_selling           = a_tbl_update(i).fd_p2_picking_selling,
              fd_p2_picking_cost              = a_tbl_update(i).fd_p2_picking_cost,
              fd_p2_picking_qty               = a_tbl_update(i).fd_p2_picking_qty,
              fd_p2_picking_cases             = a_tbl_update(i).fd_p2_picking_cases,
              fd_p4_picking_selling           = a_tbl_update(i).fd_p4_picking_selling,
              fd_p3_picking_selling           = a_tbl_update(i).fd_p3_picking_selling,
              fd_p3_picking_cost              = a_tbl_update(i).fd_p3_picking_cost,
              fd_p4_picking_cost              = a_tbl_update(i).fd_p4_picking_cost,
              fd_p3_picking_qty               = a_tbl_update(i).fd_p3_picking_qty,
              fd_p4_picking_qty               = a_tbl_update(i).fd_p4_picking_qty,
              fd_p4_picking_cases             = a_tbl_update(i).fd_p4_picking_cases,
              fd_p3_picking_cases             = a_tbl_update(i).fd_p3_picking_cases,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no      and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no    and
              calendar_date                   = a_tbl_update(i).calendar_date ;

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
                       ' '||a_tbl_update(g_error_index).calendar_date;
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
   from   rtl_loc_item_dy_rms_alloc
   where  sk1_location_no     = g_rec_out.sk1_location_no      and
          sk1_item_no         = g_rec_out.sk1_item_no    and
          calendar_date       = g_rec_out.calendar_date;

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
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD rtl_loc_item_dy_rms_alloc EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'RANGE BEING PROCESSED - '||g_start_date||' Through '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_allocation;
    fetch c_fnd_rtl_allocation bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_allocation bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_allocation;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
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

end wh_prf_corp_118u_fix;
