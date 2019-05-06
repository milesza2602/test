--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_660U_BCK11092015
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_660U_BCK11092015" (p_forall_limit in integer,p_success out boolean,p_start_date in date,p_end_date in date) as
--**************************************************************************************************
--  Date:         February 2009
--  Author:       M Munnik
--  Purpose:      Creates a list in the performance layer, of all po_no, item_no combinations
--                where the records on the RMS Purchase Order and Shipment tables,
--                have been changed during the selected period.
--                This program can be called with 2 extra input parameters, p_start_date and p_end_date.
--                When these 2 parameters are supplied, the list is created for all po_no, item_no combinations,
--                where the PO not_before_date between p_start_date and p_end_date.
--  NB !! NB !!   A decision was made and aggreed with Business, to only load PO's from Fin Year 2008 (starting at 25 June 2007) and onwards.
--  NB !! NB !!   There are converted data (before 25 June 2007) on the PO and Shipment tables, that do not comply to the current business rules.
--  Tables:       Input  - fnd_rtl_purchase_order, fnd_rtl_shipment
--                Output - temp_po_list
--  Packages:     constants, dwh_log, dwh_valid
--
--  Maintenance:
--  09 Jul 2010 - M Munnik
--                In wh_prf_corp_661u and _662u, the join between Purchase Orders and Shipments is changed to include location in the join criteria.
--                This will prevent the join to have duplicate records in the resultset.
--                The test to exclude PO's with more than one location, (causing duplicates), has been removed.
--                There are records on the PO table with a null supply_chain_type.
--                A list is created in this procedure, to get the distinct not null supply_chain_type per PO.
--                However, there are PO's where all supply_chain_type's for the PO are null. In this case, a null is selected.
--                The distinct supply_chain_type is then used for all the records of the PO, regardless of the item and location (in wh_prf_corp_661u and _662u).
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_param_ind          integer       :=  0;
g_date               date          := trunc(sysdate);
g_start_date         date;
g_end_date           date;
g_rec_out            temp_po_list%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_660U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DISTINCT PO/ITEM LIST FROM PO AND SHIPMENT EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of temp_po_list%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_empty_set_i       tbl_array_i;

a_count             integer       := 0;
a_count_i           integer       := 0;

-- Before June 2007, it was allowed to completely cancel a Purchase Order
-- and then re-use the PO Number with a different location, not_before_date, etc. (therefore more than 1 location per PO).
-- The procedures joining Purchase Orders with Shipments, used to only join on po_no and item_no.
-- When doing this join on records where there are more than 1 location per PO, the resultset will contain duplicate records.
-- To prevent this, all PO's with more than one location, were excluded from all the wh_prf_corp_66* programs.
-- The join between Purchase Orders and Shipments were changed to include location in the join criteria.
-- First join to dim_location with the location_no on the PO.
-- If the location on the PO is a store (loc_type 'S') then use location_no to join to Shipment else use wh_physical_wh_no to join to Shipment.
-- The test for duplicates has been removed.
-- A decision was made and aggreed with Business, to only load PO's from Fin Year 2008 (starting at 25 June 2007) and onwards.

-- Currently, the business rule exists that one PO can only have one not null supply_chain_type.
-- If the business decides to have more than one supply_chain_type per PO, only the max supply_chain_type will be shown.
-- There are PO's with no supply_chain_type - all supply_chain_type for the PO are null - therefore the union.

cursor c_temp_po_list is
   with po_suppl_chain as
  (select po_no, max(supply_chain_type) supply_chain_type
   from   fnd_rtl_purchase_order
   where  not_before_date > '24 Jun 2007'
   and   (supply_chain_type is not null)
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po_no
   union all
   select po_no, null supply_chain_type
   from   fnd_rtl_purchase_order
   where  not_before_date > '24 Jun 2007'
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po_no
   having max(nvl(supply_chain_type,'AAA')) = 'AAA')

   select po.po_no, dp.sk1_po_no, po.item_no, di.sk1_item_no, ps.supply_chain_type, g_param_ind param_ind
   from   fnd_rtl_purchase_order po
   join   dim_purchase_order dp       on po.po_no   = dp.po_no
   join   dim_item di                 on po.item_no = di.item_no
   join   po_suppl_chain ps           on po.po_no   = ps.po_no
   where  po.last_updated_date = g_date
   and    po.not_before_date between '25 Jun 2007' and g_date + 540   -- partitions for rtl_po_supchain_loc_item_dy, up till 18 months into future
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po.po_no, dp.sk1_po_no, po.item_no, di.sk1_item_no, ps.supply_chain_type
   union
   select s.po_no, dp.sk1_po_no, s.item_no, di.sk1_item_no, ps.supply_chain_type, g_param_ind param_ind
   from   fnd_rtl_shipment s
   join   dim_purchase_order dp       on s.po_no    = dp.po_no
   join   dim_item di                 on s.item_no  = di.item_no
   join   po_suppl_chain ps           on s.po_no    = ps.po_no
   where  s.last_updated_date = g_date
   group  by s.po_no, dp.sk1_po_no, s.item_no, di.sk1_item_no, ps.supply_chain_type;

cursor c_temp_po_list_hist is
   with po_suppl_chain as
  (select po_no, max(supply_chain_type) supply_chain_type
   from   fnd_rtl_purchase_order
   where  not_before_date > '24 Jun 2007'
   and   (supply_chain_type is not null)
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po_no
   union all
   select po_no, null supply_chain_type
   from   fnd_rtl_purchase_order
   where  not_before_date > '24 Jun 2007'
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po_no
   having max(nvl(supply_chain_type,'AAA')) = 'AAA')

   select po.po_no, dp.sk1_po_no, po.item_no, di.sk1_item_no, ps.supply_chain_type, g_param_ind param_ind
   from   fnd_rtl_purchase_order po
   join   dim_purchase_order dp       on po.po_no   = dp.po_no
   join   dim_item di                 on po.item_no = di.item_no
   join   po_suppl_chain ps           on po.po_no   = ps.po_no
   where  po.not_before_date > '24 Jun 2007'
   and    po.not_before_date between g_start_date and g_end_date
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po.po_no, dp.sk1_po_no, po.item_no, di.sk1_item_no, ps.supply_chain_type;

cursor c_temp_po_list_old_hist is
   with non_dupl_po as
  (select   po_no, max(reccnt) from
  (select   po_no, item_no, count(*) reccnt
   from     fnd_rtl_purchase_order
   where   (chain_code <> 'DJ' or chain_code is null)
   group by po_no, item_no)
   group by po_no
   having   max(reccnt) = 1),

   po_suppl_chain as
  (select p.po_no, max(p.supply_chain_type) supply_chain_type
   from   fnd_rtl_purchase_order p join non_dupl_po d on p.po_no = d.po_no
   where  p.not_before_date > '24 Jun 2007'
   and   (p.supply_chain_type is not null)
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by p.po_no
   union all
   select p.po_no, null supply_chain_type
   from   fnd_rtl_purchase_order p join non_dupl_po d on p.po_no = d.po_no
   where  p.not_before_date > '24 Jun 2007'
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by p.po_no
   having max(nvl(p.supply_chain_type,'AAA')) = 'AAA')

   select po.po_no, dp.sk1_po_no, po.item_no, di.sk1_item_no, ps.supply_chain_type, g_param_ind param_ind
   from   fnd_rtl_purchase_order po
   join   dim_purchase_order dp       on po.po_no   = dp.po_no
   join   dim_item di                 on po.item_no = di.item_no
   join   po_suppl_chain ps           on po.po_no   = ps.po_no
   where  po.not_before_date > '24 Jun 2007'
   and    po.not_before_date between g_start_date and g_end_date
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by po.po_no, dp.sk1_po_no, po.item_no, di.sk1_item_no, ps.supply_chain_type;

cursor c_temp_po_list_ship is
   with non_dupl_po as
  (select   po_no, max(reccnt) from
  (select   po_no, item_no, count(*) reccnt
   from     fnd_rtl_purchase_order
   where   (chain_code <> 'DJ' or chain_code is null)
   group by po_no, item_no)
   group by po_no
   having   max(reccnt) = 1),

   po_suppl_chain as
  (select p.po_no, max(p.supply_chain_type) supply_chain_type
   from   fnd_rtl_purchase_order p join non_dupl_po d on p.po_no = d.po_no
   where (p.supply_chain_type is not null)
   and   (chain_code <> 'DJ' or chain_code is null)
   group  by p.po_no
   union all
   select p.po_no, null supply_chain_type
   from   fnd_rtl_purchase_order p join non_dupl_po d on p.po_no = d.po_no
   where (chain_code <> 'DJ' or chain_code is null) -- please ensure filter works correctly
   group  by p.po_no
   having max(nvl(p.supply_chain_type,'AAA')) = 'AAA'),

   old_po_nos as
  (select distinct po_no
   from   fnd_rtl_purchase_order
   where  not_before_date < '25 Jun 2007'
   and   (chain_code <> 'DJ' or chain_code is null))

   select s.po_no, dp.sk1_po_no, s.item_no, di.sk1_item_no, ps.supply_chain_type, g_param_ind param_ind
   from   fnd_rtl_shipment s
   join   dim_purchase_order dp       on s.po_no    = dp.po_no
   join   old_po_nos op               on s.po_no    = op.po_no
   join   dim_item di                 on s.item_no  = di.item_no
   join   po_suppl_chain ps           on s.po_no    = ps.po_no
   where  s.shipment_status_code = 'R'
   and    s.actl_rcpt_date       > '24 Jun 2007'
   group  by s.po_no, dp.sk1_po_no, s.item_no, di.sk1_item_no, ps.supply_chain_type;

g_rec_in             c_temp_po_list%rowtype;
-- For input bulk collect --
type stg_array is table of c_temp_po_list%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                   := g_rec_in;

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
       insert into temp_po_list values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).po_no||
                       ' '||a_tbl_insert(g_error_index).item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Write valid data out to output table
--**************************************************************************************************
procedure local_write_output as
begin
-- Place data into and array for later writing to table in bulk
   a_count_i               := a_count_i + 1;
   a_tbl_insert(a_count_i) := g_rec_out;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;
      a_tbl_insert  := a_empty_set_i;
      a_count_i     := 0;
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
    l_text := 'LOAD OF temp_po_list STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    if p_start_date is not null and p_end_date is not null then
       g_start_date := p_start_date;
       g_end_date   := p_end_date;
       if g_end_date < '25 Jun 2007' then
          g_param_ind := 3;
       else
          if g_end_date < '25 Jun 2008' then
             g_param_ind := 4;
          else
             g_param_ind := 2;
          end if;
       end if;
    else
       g_param_ind := 1;
    end if;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    case g_param_ind
       when 1 then
          l_text := 'PO LIST CREATED FOR LAST_UPDATED_DATE = '||g_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       when 2 then
          l_text := 'PO LIST CREATED FOR PO NOT_BEFORE_DATE BETWEEN '||g_start_date||' and '||g_end_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       when 3 then
          l_text := 'PO LIST CREATED FOR ACTL RCPT DATE >= 25 JUN 2007 BUT PO NOT BEFORE DATE < 25 JUN 2007';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       when 4 then
          l_text := 'PO LIST CREATED FOR PO NOT_BEFORE_DATE BETWEEN '||g_start_date||' and '||g_end_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end case;

    execute immediate 'truncate table dwh_performance.temp_po_list';
    l_text := 'TABLE temp_po_list TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    case g_param_ind
       when 1 then
          open c_temp_po_list;
          fetch c_temp_po_list bulk collect into a_stg_input limit g_forall_limit;
       when 2 then
          open c_temp_po_list_hist;
          fetch c_temp_po_list_hist bulk collect into a_stg_input limit g_forall_limit;
       when 3 then
          open c_temp_po_list_ship;
          fetch c_temp_po_list_ship bulk collect into a_stg_input limit g_forall_limit;
       when 4 then
          open c_temp_po_list_old_hist;
          fetch c_temp_po_list_old_hist bulk collect into a_stg_input limit g_forall_limit;
    end case;

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
       case g_param_ind
          when 1 then
             fetch c_temp_po_list bulk collect into a_stg_input limit g_forall_limit;
          when 2 then
             fetch c_temp_po_list_hist bulk collect into a_stg_input limit g_forall_limit;
          when 3 then
             fetch c_temp_po_list_ship bulk collect into a_stg_input limit g_forall_limit;
          when 4 then
             fetch c_temp_po_list_old_hist bulk collect into a_stg_input limit g_forall_limit;
       end case;
    end loop;
    case g_param_ind
       when 1 then
          close c_temp_po_list;
       when 2 then
          close c_temp_po_list_hist;
       when 3 then
          close c_temp_po_list_ship;
       when 4 then
          close c_temp_po_list_old_hist;
    end case;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;

    ww_dbms_stats.gather_table_stats('dwh_performance','temp_po_list');
    l_text := 'TABLE temp_po_list STATISTICS UPDATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end wh_prf_corp_660u_BCK11092015;
