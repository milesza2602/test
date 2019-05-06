--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_208U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_208U" (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as

--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RMS Sales fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_sales_cpy
--               Output - fnd_rtl_loc_item_dy_rms_sale
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  27 Feb 2009 - TD983 SLR - validate prom_discount_no against fnd_prom
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  1000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_sale_hsp.sys_process_msg%type;
g_item_level_no      fnd_item.item_level_no%type;
g_tran_level_no      fnd_item.tran_level_no%type;
g_rec_out            fnd_rtl_loc_item_dy_rms_sale%rowtype;
g_rec_in             stg_rms_sale_cpy%rowtype;
g_found              boolean;
g_insert_rec         boolean;

g_date               date          := trunc(sysdate);
g_old_date           date          ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_208U'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS SALES FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For input bulk collect --
type stg_array is table of stg_rms_sale_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_loc_item_dy_rms_sale%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_loc_item_dy_rms_sale%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_sale_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_sale_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_stg_rms_sale is
   select *
   from stg_rms_sale_cpy
   where sys_process_code = 'N' and
--QC4099 exclude old records fed
         post_date        >  g_old_date and
         location_no      between p_from_loc_no and p_to_loc_no
   order by sys_source_batch_id,sys_source_sequence_no;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                          := 'N';
   g_rec_out.location_no                     := g_rec_in.location_no;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sales_qty                       := g_rec_in.sales_qty;
   g_rec_out.sales                           := g_rec_in.sales;
   g_rec_out.sales_cost                      := g_rec_in.sales_cost;
   g_rec_out.reg_sales_qty                   := g_rec_in.reg_sales_qty;
   g_rec_out.reg_sales                       := g_rec_in.reg_sales;
   g_rec_out.reg_sales_cost                  := g_rec_in.reg_sales_cost;
   g_rec_out.prom_sales_qty                  := g_rec_in.prom_sales_qty;
   g_rec_out.prom_sales                      := g_rec_in.prom_sales;
   g_rec_out.prom_sales_cost                 := g_rec_in.prom_sales_cost;
   g_rec_out.clear_sales_qty                 := g_rec_in.clear_sales_qty;
   g_rec_out.clear_sales                     := g_rec_in.clear_sales;
   g_rec_out.clear_sales_cost                := g_rec_in.clear_sales_cost;
   g_rec_out.waste_qty                       := g_rec_in.waste_qty;
   g_rec_out.waste_selling                   := g_rec_in.waste_selling;
   g_rec_out.waste_cost                      := g_rec_in.waste_cost;
   g_rec_out.shrink_qty                      := g_rec_in.shrink_qty;
   g_rec_out.shrink_selling                  := g_rec_in.shrink_selling;
   g_rec_out.shrink_cost                     := g_rec_in.shrink_cost;
   g_rec_out.gain_qty                        := g_rec_in.gain_qty;
   g_rec_out.gain_selling                    := g_rec_in.gain_selling;
   g_rec_out.gain_cost                       := g_rec_in.gain_cost;
   g_rec_out.sdn_in_qty                      := g_rec_in.sdn_in_qty;
   g_rec_out.sdn_in_selling                  := g_rec_in.sdn_in_selling;
   g_rec_out.sdn_in_cost                     := g_rec_in.sdn_in_cost;
   g_rec_out.grn_qty                         := g_rec_in.grn_qty;
   g_rec_out.grn_selling                     := g_rec_in.grn_selling;
   g_rec_out.grn_cost                        := g_rec_in.grn_cost;
   g_rec_out.claim_qty                       := g_rec_in.claim_qty;
   g_rec_out.claim_selling                   := g_rec_in.claim_selling;
   g_rec_out.claim_cost                      := g_rec_in.claim_cost;
   g_rec_out.sales_returns_qty               := g_rec_in.sales_returns_qty;
   g_rec_out.sales_returns                   := g_rec_in.sales_returns;
   g_rec_out.sales_returns_cost              := g_rec_in.sales_returns_cost;
   g_rec_out.self_supply_qty                 := g_rec_in.self_supply_qty;
   g_rec_out.self_supply_selling             := g_rec_in.self_supply_selling;
   g_rec_out.self_supply_cost                := g_rec_in.self_supply_cost;
   g_rec_out.wac_adj_amt                     := g_rec_in.wac_adj_amt;
   g_rec_out.invoice_adj_qty                 := g_rec_in.invoice_adj_qty;
   g_rec_out.invoice_adj_selling             := g_rec_in.invoice_adj_selling;
   g_rec_out.invoice_adj_cost                := g_rec_in.invoice_adj_cost;
   g_rec_out.rndm_mass_pos_var               := g_rec_in.rndm_mass_pos_var;
   g_rec_out.mkup_selling                    := g_rec_in.mkup_selling;
   g_rec_out.mkup_cancel_selling             := g_rec_in.mkup_cancel_selling;
   g_rec_out.mkdn_selling                    := g_rec_in.mkdn_selling;
   g_rec_out.mkdn_cancel_selling             := g_rec_in.mkdn_cancel_selling;
   g_rec_out.clear_mkdn_selling              := g_rec_in.clear_mkdn_selling;
   g_rec_out.rtv_qty                         := g_rec_in.rtv_qty;
   g_rec_out.rtv_selling                     := g_rec_in.rtv_selling;
   g_rec_out.rtv_cost                        := g_rec_in.rtv_cost;
   g_rec_out.sdn_out_qty                     := g_rec_in.sdn_out_qty;
   g_rec_out.sdn_out_selling                 := g_rec_in.sdn_out_selling;
   g_rec_out.sdn_out_cost                    := g_rec_in.sdn_out_cost;
   g_rec_out.ibt_in_qty                      := g_rec_in.ibt_in_qty;
   g_rec_out.ibt_in_selling                  := g_rec_in.ibt_in_selling;
   g_rec_out.ibt_in_cost                     := g_rec_in.ibt_in_cost;
   g_rec_out.ibt_out_qty                     := g_rec_in.ibt_out_qty;
   g_rec_out.ibt_out_selling                 := g_rec_in.ibt_out_selling;
   g_rec_out.ibt_out_cost                    := g_rec_in.ibt_out_cost;
   g_rec_out.prom_discount_no                := g_rec_in.prom_discount_no;
   g_rec_out.ho_prom_discount_amt            := g_rec_in.ho_prom_discount_amt;
   g_rec_out.ho_prom_discount_qty            := g_rec_in.ho_prom_discount_qty;
   g_rec_out.st_prom_discount_amt            := g_rec_in.st_prom_discount_amt;
   g_rec_out.st_prom_discount_qty            := g_rec_in.st_prom_discount_qty;
--   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;

   g_rec_out.last_updated_date         := g_date;

   if not  dwh_valid.fnd_calendar(g_rec_out.post_date) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_date_not_found;
     l_text          := dwh_constants.vc_date_not_found||g_rec_out.post_date ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
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

   if g_rec_out.prom_discount_no is not null then
      if not dwh_valid.fnd_prom(g_rec_out.prom_discount_no) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_prom_not_found ;
         l_text          := dwh_constants.vc_prom_not_found||g_rec_out.prom_discount_no ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
      end if;
   end if;

   begin
         select item_level_no,tran_level_no
         into   g_item_level_no,g_tran_level_no
         from   fnd_item
         where  item_no        = g_rec_out.item_no  ;
         exception
            when no_data_found then
              g_item_level_no := 1;
              g_tran_level_no := 2;
    end;

    if g_item_level_no <> g_tran_level_no then
         g_hospital      := 'Y';
         g_hospital_text := 'Item must be at T_Level';
         l_text          := 'Item must be at T_Level'||g_rec_out.item_no ;
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

   insert into stg_rms_sale_hsp values g_rec_in;
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
       insert into fnd_rtl_loc_item_dy_rms_sale values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).item_no||
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
       update fnd_rtl_loc_item_dy_rms_sale
       set    sales_qty                       = a_tbl_update(i).sales_qty,
              sales                           = a_tbl_update(i).sales,
              sales_cost                      = a_tbl_update(i).sales_cost,
              reg_sales_qty                   = a_tbl_update(i).reg_sales_qty,
              reg_sales                       = a_tbl_update(i).reg_sales,
              reg_sales_cost                  = a_tbl_update(i).reg_sales_cost,
              prom_sales_qty                  = a_tbl_update(i).prom_sales_qty,
              prom_sales                      = a_tbl_update(i).prom_sales,
              prom_sales_cost                 = a_tbl_update(i).prom_sales_cost,
              clear_sales_qty                 = a_tbl_update(i).clear_sales_qty,
              clear_sales                     = a_tbl_update(i).clear_sales,
              clear_sales_cost                = a_tbl_update(i).clear_sales_cost,
              waste_qty                       = a_tbl_update(i).waste_qty,
              waste_selling                   = a_tbl_update(i).waste_selling,
              waste_cost                      = a_tbl_update(i).waste_cost,
              shrink_qty                      = a_tbl_update(i).shrink_qty,
              shrink_selling                  = a_tbl_update(i).shrink_selling,
              shrink_cost                     = a_tbl_update(i).shrink_cost,
              gain_qty                        = a_tbl_update(i).gain_qty,
              gain_selling                    = a_tbl_update(i).gain_selling,
              gain_cost                       = a_tbl_update(i).gain_cost,
              sdn_in_qty                      = a_tbl_update(i).sdn_in_qty,
              sdn_in_selling                  = a_tbl_update(i).sdn_in_selling,
              sdn_in_cost                     = a_tbl_update(i).sdn_in_cost,
              grn_qty                         = a_tbl_update(i).grn_qty,
              grn_selling                     = a_tbl_update(i).grn_selling,
              grn_cost                        = a_tbl_update(i).grn_cost,
              claim_qty                       = a_tbl_update(i).claim_qty,
              claim_selling                   = a_tbl_update(i).claim_selling,
              claim_cost                      = a_tbl_update(i).claim_cost,
              sales_returns_qty               = a_tbl_update(i).sales_returns_qty,
              sales_returns                   = a_tbl_update(i).sales_returns,
              sales_returns_cost              = a_tbl_update(i).sales_returns_cost,
              self_supply_qty                 = a_tbl_update(i).self_supply_qty,
              self_supply_selling             = a_tbl_update(i).self_supply_selling,
              self_supply_cost                = a_tbl_update(i).self_supply_cost,
              wac_adj_amt                     = a_tbl_update(i).wac_adj_amt,
              invoice_adj_qty                 = a_tbl_update(i).invoice_adj_qty,
              invoice_adj_selling             = a_tbl_update(i).invoice_adj_selling,
              invoice_adj_cost                = a_tbl_update(i).invoice_adj_cost,
              rndm_mass_pos_var               = a_tbl_update(i).rndm_mass_pos_var,
              mkup_selling                    = a_tbl_update(i).mkup_selling,
              mkup_cancel_selling             = a_tbl_update(i).mkup_cancel_selling,
              mkdn_selling                    = a_tbl_update(i).mkdn_selling,
              mkdn_cancel_selling             = a_tbl_update(i).mkdn_cancel_selling,
              clear_mkdn_selling              = a_tbl_update(i).clear_mkdn_selling,
              rtv_qty                         = a_tbl_update(i).rtv_qty,
              rtv_selling                     = a_tbl_update(i).rtv_selling,
              rtv_cost                        = a_tbl_update(i).rtv_cost,
              sdn_out_qty                     = a_tbl_update(i).sdn_out_qty,
              sdn_out_selling                 = a_tbl_update(i).sdn_out_selling,
              sdn_out_cost                    = a_tbl_update(i).sdn_out_cost,
              ibt_in_qty                      = a_tbl_update(i).ibt_in_qty,
              ibt_in_selling                  = a_tbl_update(i).ibt_in_selling,
              ibt_in_cost                     = a_tbl_update(i).ibt_in_cost,
              ibt_out_qty                     = a_tbl_update(i).ibt_out_qty,
              ibt_out_selling                 = a_tbl_update(i).ibt_out_selling,
              ibt_out_cost                    = a_tbl_update(i).ibt_out_cost,
              prom_discount_no                = a_tbl_update(i).prom_discount_no,
              ho_prom_discount_amt            = a_tbl_update(i).ho_prom_discount_amt,
              ho_prom_discount_qty            = a_tbl_update(i).ho_prom_discount_qty,
              st_prom_discount_amt            = a_tbl_update(i).st_prom_discount_amt,
              st_prom_discount_qty            = a_tbl_update(i).st_prom_discount_qty,
              last_updated_date          = a_tbl_update(i).last_updated_date
       where  location_no                = a_tbl_update(i).location_no  and
              item_no                    = a_tbl_update(i).item_no      and
              post_date                  = a_tbl_update(i).post_date;

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
                       ' '||a_tbl_update(g_error_index).location_no||
                       ' '||a_tbl_update(g_error_index).item_no||
                       ' '||a_tbl_update(g_error_index).post_date;
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
       update stg_rms_sale_cpy
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
   from   fnd_rtl_loc_item_dy_rms_sale
   where  location_no    = g_rec_out.location_no  and
          item_no        = g_rec_out.item_no      and
          post_date      = g_rec_out.post_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).location_no = g_rec_out.location_no and
            a_tbl_insert(i).item_no     = g_rec_out.item_no and
            a_tbl_insert(i).post_date   = g_rec_out.post_date then
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

    l_text := 'LOAD OF FND_RTL_LOC_ITEM_DY_RMS_SALE EX ITB STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_old_date          := g_date - 90;

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date||g_old_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_sale;
    fetch c_stg_rms_sale bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_rms_sale bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_sale;
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
end wh_fnd_corp_208u;
