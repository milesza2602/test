--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_105U_FIX_SUN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_105U_FIX_SUN" 
                       (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
-- datafix  - copy from wh_PRF_CORP_105U
-- fix SAT data, batch=sat
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RMS C&H Stock fact table in the performance layer
--               with input ex RMS Stock table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_stk_ch_FIX
--               Output - rtl_loc_item_dy_rms_stock
--  Packages:    constants, dwh_log, dwh_valid
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
g_recs_merged        integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_dy_rms_stock%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_Pdate               date          := trunc(sysdate);

g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                     rtl_loc_item_dy_rms_price.wac%type             := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_105U_FIX_SUN'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS C&H STOCK FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_m is table of rtl_loc_item_dy_rms_stock%rowtype index by binary_integer;
a_tbl_merge         tbl_array_m;
a_empty_set_m       tbl_array_m;

a_count             integer       := 0;
a_count_m           integer       := 0;
-- /*+ parallel(fnd_lid,4) */
cursor c_fnd_rtl_loc_item_dy_rms_stk is
   select 
          fnd_lid.*,
          di.standard_uom_code,di.business_unit_no,di.vat_rate_perc,di.sk1_department_no,di.sk1_item_no,
          dl.chain_no,dl.sk1_location_no,
          dih.sk2_item_no,
          dlh.sk2_location_no
   from   DWH_FOUNDATION.fnd_rtl_loc_item_dy_stk_ch_FIX fnd_lid,
          dim_item di,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh
   where  fnd_lid.last_updated_date  = g_date and
          fnd_lid.location_no        between p_from_loc_no and p_to_loc_no and
          fnd_lid.item_no            = di.item_no and
          fnd_lid.location_no        = dl.location_no and
          fnd_lid.item_no            = dih.item_no and
          dih.sk2_active_to_date     = dwh_constants.sk_to_date and
          fnd_lid.location_no        = dlh.location_no and
          dlh.sk2_active_to_date     = dwh_constants.sk_to_date;

g_rec_in                   c_fnd_rtl_loc_item_dy_rms_stk%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_rms_stk%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.soh_qty                         := g_rec_in.soh_qty;
   g_rec_out.soh_selling                     := g_rec_in.soh_selling;
   g_rec_out.soh_cost                        := g_rec_in.soh_cost;
   g_rec_out.sit_qty                         := g_rec_in.sit_qty;
   g_rec_out.sit_selling                     := g_rec_in.sit_selling;
   g_rec_out.sit_cost                        := g_rec_in.sit_cost;
   g_rec_out.non_sellable_qty                := g_rec_in.non_sellable_qty;
   g_rec_out.inbound_excl_cust_ord_qty       := g_rec_in.inbound_excl_cust_ord_qty;
   g_rec_out.inbound_excl_cust_ord_selling   := g_rec_in.inbound_excl_cust_ord_selling;
   g_rec_out.inbound_excl_cust_ord_cost      := g_rec_in.inbound_excl_cust_ord_cost;
   g_rec_out.inbound_incl_cust_ord_qty       := g_rec_in.inbound_incl_cust_ord_qty;
   g_rec_out.inbound_incl_cust_ord_selling   := g_rec_in.inbound_incl_cust_ord_selling;
   g_rec_out.inbound_incl_cust_ord_cost      := g_rec_in.inbound_incl_cust_ord_cost;
   g_rec_out.clear_soh_qty                   := g_rec_in.clear_soh_qty;
   g_rec_out.clear_soh_selling               := g_rec_in.clear_soh_selling;
   g_rec_out.clear_soh_cost                  := g_rec_in.clear_soh_cost;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;

   g_rec_out.com_flag_ind                    := 0;
   g_rec_out.sit_cases                       := '';
   g_rec_out.soh_cases                       := '';
   g_rec_out.sit_fr_cost                     := '';
   g_rec_out.soh_fr_cost                     := '';
   g_rec_out.franchise_soh_margin            := '';
   g_rec_out.clear_soh_fr_cost               := '';
   g_rec_out.reg_soh_fr_cost                 := '';

-- Value add and calculated fields added to performance layer
   g_rec_out.sit_margin          := nvl(g_rec_out.sit_selling,0) - nvl(g_rec_out.sit_cost,0);
   g_rec_out.soh_margin          := nvl(g_rec_out.soh_selling,0) - nvl(g_rec_out.soh_cost,0);
   g_rec_out.reg_soh_qty         := nvl(g_rec_out.soh_qty,0) - nvl(g_rec_out.clear_soh_qty,0);
   g_rec_out.reg_soh_selling     := nvl(g_rec_out.soh_selling,0) - nvl(g_rec_out.clear_soh_selling,0);
   g_rec_out.reg_soh_cost        := nvl(g_rec_out.soh_cost,0) - nvl(g_rec_out.clear_soh_cost,0);
   g_rec_out.clear_soh_margin    := nvl(g_rec_out.clear_soh_selling,0) - nvl(g_rec_out.clear_soh_cost,0);
   g_rec_out.reg_soh_margin      := nvl(g_rec_out.reg_soh_selling,0) - nvl(g_rec_out.reg_soh_cost,0);

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
      g_rec_out.clear_soh_fr_cost            := nvl(g_rec_out.clear_soh_cost,0) + round((nvl(g_rec_out.clear_soh_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.reg_soh_fr_cost              := nvl(g_rec_out.soh_fr_cost,0) - nvl(g_rec_out.clear_soh_fr_cost,0);
   end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

/*
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_loc_item_dy_rms_stock values a_tbl_insert(i);

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

*/
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk merge  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin

    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions
       merge /*+ USE_HASH(rtl_lid ,mer_lid)*/ into rtl_loc_item_dy_rms_stock rtl_lid
       using
       (select  a_tbl_merge(i).SK1_LOCATION_NO	as	SK1_LOCATION_NO,
                a_tbl_merge(i).SK1_ITEM_NO	as	SK1_ITEM_NO,
                a_tbl_merge(i).POST_DATE	as	POST_DATE,
                a_tbl_merge(i).SK2_LOCATION_NO	as	SK2_LOCATION_NO,
                a_tbl_merge(i).SK2_ITEM_NO	as	SK2_ITEM_NO,
                a_tbl_merge(i).COM_FLAG_IND	as	COM_FLAG_IND,
                a_tbl_merge(i).SIT_QTY	as	SIT_QTY,
                a_tbl_merge(i).SIT_CASES	as	SIT_CASES,
                a_tbl_merge(i).SIT_SELLING	as	SIT_SELLING,
                a_tbl_merge(i).SIT_COST	as	SIT_COST,
                a_tbl_merge(i).SIT_FR_COST	as	SIT_FR_COST,
                a_tbl_merge(i).SIT_MARGIN	as	SIT_MARGIN,
                a_tbl_merge(i).NON_SELLABLE_QTY	as	NON_SELLABLE_QTY,
                a_tbl_merge(i).SOH_QTY	as	SOH_QTY,
                a_tbl_merge(i).SOH_CASES	as	SOH_CASES,
                a_tbl_merge(i).SOH_SELLING	as	SOH_SELLING,
                a_tbl_merge(i).SOH_COST	as	SOH_COST,
                a_tbl_merge(i).SOH_FR_COST	as	SOH_FR_COST,
                a_tbl_merge(i).SOH_MARGIN	as	SOH_MARGIN,
                a_tbl_merge(i).FRANCHISE_SOH_MARGIN	as	FRANCHISE_SOH_MARGIN,
                a_tbl_merge(i).INBOUND_EXCL_CUST_ORD_QTY	as	INBOUND_EXCL_CUST_ORD_QTY,
                a_tbl_merge(i).INBOUND_EXCL_CUST_ORD_SELLING	as	INBOUND_EXCL_CUST_ORD_SELLING,
                a_tbl_merge(i).INBOUND_EXCL_CUST_ORD_COST	as	INBOUND_EXCL_CUST_ORD_COST,
                a_tbl_merge(i).INBOUND_INCL_CUST_ORD_QTY	as	INBOUND_INCL_CUST_ORD_QTY,
                a_tbl_merge(i).INBOUND_INCL_CUST_ORD_SELLING	as	INBOUND_INCL_CUST_ORD_SELLING,
                a_tbl_merge(i).INBOUND_INCL_CUST_ORD_COST	as	INBOUND_INCL_CUST_ORD_COST,
                a_tbl_merge(i).BOH_QTY	as	BOH_QTY,
                a_tbl_merge(i).BOH_CASES	as	BOH_CASES,
                a_tbl_merge(i).BOH_SELLING	as	BOH_SELLING,
                a_tbl_merge(i).BOH_COST	as	BOH_COST,
                a_tbl_merge(i).BOH_FR_COST	as	BOH_FR_COST,
                a_tbl_merge(i).CLEAR_SOH_QTY	as	CLEAR_SOH_QTY,
                a_tbl_merge(i).CLEAR_SOH_SELLING	as	CLEAR_SOH_SELLING,
                a_tbl_merge(i).CLEAR_SOH_COST	as	CLEAR_SOH_COST,
                a_tbl_merge(i).CLEAR_SOH_FR_COST	as	CLEAR_SOH_FR_COST,
                a_tbl_merge(i).REG_SOH_QTY	as	REG_SOH_QTY,
                a_tbl_merge(i).REG_SOH_SELLING	as	REG_SOH_SELLING,
                a_tbl_merge(i).REG_SOH_COST	as	REG_SOH_COST,
                a_tbl_merge(i).REG_SOH_FR_COST	as	REG_SOH_FR_COST,
                a_tbl_merge(i).LAST_UPDATED_DATE	as	LAST_UPDATED_DATE,
                a_tbl_merge(i).CLEAR_SOH_MARGIN	as	CLEAR_SOH_MARGIN,
                a_tbl_merge(i).REG_SOH_MARGIN	as	REG_SOH_MARGIN
       from dual) mer_lid
       on (mer_lid.SK1_LOCATION_NO = rtl_lid.SK1_LOCATION_NO and
           mer_lid.SK1_ITEM_NO     = rtl_lid.SK1_ITEM_NO and
           mer_lid.POST_DATE       = rtl_lid.POST_DATE)
       when matched then
       update
       set    sit_qty                         = mer_lid.sit_qty,
              sit_cases                       = mer_lid.sit_cases,
              sit_selling                     = mer_lid.sit_selling,
              sit_cost                        = mer_lid.sit_cost,
              sit_fr_cost                     = mer_lid.sit_fr_cost,
              sit_margin                      = mer_lid.sit_margin,
              non_sellable_qty                = mer_lid.non_sellable_qty,
              soh_qty                         = mer_lid.soh_qty,
              soh_cases                       = mer_lid.soh_cases,
              soh_selling                     = mer_lid.soh_selling,
              soh_cost                        = mer_lid.soh_cost,
              soh_fr_cost                     = mer_lid.soh_fr_cost,
              soh_margin                      = mer_lid.soh_margin,
              franchise_soh_margin            = mer_lid.franchise_soh_margin,
              inbound_excl_cust_ord_qty       = mer_lid.inbound_excl_cust_ord_qty,
              inbound_excl_cust_ord_selling   = mer_lid.inbound_excl_cust_ord_selling,
              inbound_excl_cust_ord_cost      = mer_lid.inbound_excl_cust_ord_cost,
              inbound_incl_cust_ord_qty       = mer_lid.inbound_incl_cust_ord_qty,
              inbound_incl_cust_ord_selling   = mer_lid.inbound_incl_cust_ord_selling,
              inbound_incl_cust_ord_cost      = mer_lid.inbound_incl_cust_ord_cost,
              clear_soh_qty                   = mer_lid.clear_soh_qty,
              clear_soh_selling               = mer_lid.clear_soh_selling,
              clear_soh_cost                  = mer_lid.clear_soh_cost,
              clear_soh_fr_cost               = mer_lid.clear_soh_fr_cost,
              reg_soh_qty                     = mer_lid.reg_soh_qty,
              reg_soh_selling                 = mer_lid.reg_soh_selling,
              reg_soh_cost                    = mer_lid.reg_soh_cost,
              reg_soh_fr_cost                 = mer_lid.reg_soh_fr_cost,
              last_updated_date               = mer_lid.last_updated_date,
              clear_soh_margin                = mer_lid.clear_soh_margin,
              reg_soh_margin                  = mer_lid.reg_soh_margin

   when not matched then
   insert values  a_tbl_merge(i);
   g_recs_merged := g_recs_merged + a_tbl_merge.count;

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
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_merge;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   a_count_m               := a_count_m + 1;
   a_tbl_merge(a_count_m)  := g_rec_out;
   a_count                 := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_merge;
      a_tbl_merge   := a_empty_set_m;
      a_count_m     := 0;
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_loc_item_dy_rms_stock EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '10 AUG 2014';
    
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_Pdate := '10 AUG 2014';
    
    l_text := 'POST DATE BEING PROCESSED IS:- '||g_Pdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'INPUT = fnd_rtl_loc_item_dy_stk_ch_fix';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'OUTPUT = rtl_loc_item_dy_rms_stock';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
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
         if g_recs_read mod 100000 = 0 then
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
    local_bulk_merge;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED '||g_recs_merged;
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



END WH_PRF_CORP_105U_FIX_SUN;
