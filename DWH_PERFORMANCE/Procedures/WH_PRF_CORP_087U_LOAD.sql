--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_087U_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_087U_LOAD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2014
--  Author:      Quentin Smit
--  Purpose:     Create Item Supplier History fact table in the performance layer
--               with input ex RMS fnd_zone_item_supp_hist_om_hist and fnd_zone_item (Foods Renewal) tables from foundation layer.
--  Tables:      Input  - fnd_zone_item_supp_hist_om_hist
--                        fnd_zone_item_supp_hist
--               Output - rtl_zone_item_supp_hist_hist
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dwh_performance.rtl_zone_item_supp_hist%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_087U_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE ITEM SUPPLIER HISTORY FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_supp_hist%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_supp_hist%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_zone_item_supp_hist_hist is
   with om as (
select fis.zone_group_no zone_group_no,
          fis.zone_no zone_no,
          fis.item_no as item_no,
          fis.supplier_no as supplier_no,
          g_date as calendar_date,
          fis.to_loc_no  as to_loc_no,
          fis.num_lead_time_days as num_lead_time_days,
          fis.next_wk_cost_price as next_wk_cost_price,
          fis.this_wk_cost_price as this_wk_cost_price,
          fis.next_wk_trunk_cost as next_wk_trunk_cost,
          fis.this_wk_trunk_cost as this_wk_trunk_cost,
          fis.next_wk_day_1_daily_perc as next_wk_day_1_daily_perc,
          fis.next_wk_day_2_daily_perc as next_wk_day_2_daily_perc,
          fis.next_wk_day_3_daily_perc as next_wk_day_3_daily_perc,
          fis.next_wk_day_4_daily_perc as next_wk_day_4_daily_perc,
          fis.next_wk_day_5_daily_perc as next_wk_day_5_daily_perc,
          fis.next_wk_day_6_daily_perc as next_wk_day_6_daily_perc,
          fis.next_wk_day_7_daily_perc as next_wk_day_7_daily_perc,
          fis.this_wk_day_1_daily_perc as this_wk_day_1_daily_perc,
          fis.this_wk_day_2_daily_perc as this_wk_day_2_daily_perc,
          fis.this_wk_day_3_daily_perc as this_wk_day_3_daily_perc,
          fis.this_wk_day_4_daily_perc as this_wk_day_4_daily_perc,
          fis.this_wk_day_5_daily_perc as this_wk_day_5_daily_perc,
          fis.this_wk_day_6_daily_perc as this_wk_day_6_daily_perc,
          fis.this_wk_day_7_daily_perc as this_wk_day_7_daily_perc,
          0 as split_perc
         
   from   dwh_foundation.fnd_zone_item_supp_om_h fis,
          dim_zone dz ,
          --dim_item di ,
          dim_supplier ds 
   where dz.zone_group_no = fis.zone_group_no
     and dz.zone_no       = fis.zone_no
     --and di.item_no       = fis.item_no
     and ds.supplier_no   = fis.supplier_no),
   --  and di.item_desc not like '%DO NOT USE%'),  -- select * from om;
   
   ff as (select dz.zone_group_no,
          fis.zone_no zone_no,
          fis.item_no as item_no,
          fis.supplier_no as supplier_no,
          g_date as calendar_date,
          fis.to_loc_no  as to_loc_no,
          fis.num_lead_time_days as num_lead_time_days,
          fis.next_wk_cost_price as next_wk_cost_price,
          fis.this_wk_cost_price as this_wk_cost_price,
          fis.next_wk_trunk_cost as next_wk_trunk_cost,
          fis.this_wk_trunk_cost as this_wk_trunk_cost,
          fis.next_wk_day_1_daily_perc as next_wk_day_1_daily_perc,
          fis.next_wk_day_2_daily_perc as next_wk_day_2_daily_perc,
          fis.next_wk_day_3_daily_perc as next_wk_day_3_daily_perc,
          fis.next_wk_day_4_daily_perc as next_wk_day_4_daily_perc,
          fis.next_wk_day_5_daily_perc as next_wk_day_5_daily_perc,
          fis.next_wk_day_6_daily_perc as next_wk_day_6_daily_perc,
          fis.next_wk_day_7_daily_perc as next_wk_day_7_daily_perc,
          fis.this_wk_day_1_daily_perc as this_wk_day_1_daily_perc,
          fis.this_wk_day_2_daily_perc as this_wk_day_2_daily_perc,
          fis.this_wk_day_3_daily_perc as this_wk_day_3_daily_perc,
          fis.this_wk_day_4_daily_perc as this_wk_day_4_daily_perc,
          fis.this_wk_day_5_daily_perc as this_wk_day_5_daily_perc,
          fis.this_wk_day_6_daily_perc as this_wk_day_6_daily_perc,
          fis.this_wk_day_7_daily_perc as this_wk_day_7_daily_perc,
          fis.split_perc               as split_perc
         
   from   dwh_foundation.fnd_zone_item_supp_hist fis,
          dim_zone dz ,
          --dim_item di ,
          dim_supplier ds 
   where dz.zone_no       = fis.zone_no
     --and di.item_no       = fis.item_no
     and ds.supplier_no   = fis.supplier_no),
   --  and di.item_desc not like '%DO NOT USE%') ,  -- select * from ff;
   
  all_together  as (
    select nvl(om.zone_group_no, ff.zone_group_no)  as zone_group_no,
          nvl(om.zone_no, ff.zone_no) as zone_no,
          nvl(om.item_no, ff.item_no)  as item_no,
          nvl(om.supplier_no, ff.supplier_no)  as supplier_no,
          nvl(om.calendar_date, ff.calendar_date) as calendar_date,
          nvl(om.to_loc_no, ff.to_loc_no)  as to_loc_no,
          nvl(om.num_lead_time_days, ff.num_lead_time_days) as num_lead_time_days,
          nvl(om.next_wk_cost_price, ff.next_wk_cost_price) as next_wk_cost_price,
          nvl(om.this_wk_cost_price, ff.this_wk_cost_price) as this_wk_cost_price,
          nvl(om.next_wk_trunk_cost, ff.next_wk_trunk_cost) as next_wk_trunk_cost,
          nvl(om.this_wk_trunk_cost, ff.this_wk_trunk_cost) as this_wk_trunk_cost,
          nvl(om.next_wk_day_1_daily_perc, ff.next_wk_day_1_daily_perc) as next_wk_day_1_daily_perc,
          nvl(om.next_wk_day_2_daily_perc, ff.next_wk_day_2_daily_perc) as next_wk_day_2_daily_perc,
          nvl(om.next_wk_day_3_daily_perc, ff.next_wk_day_3_daily_perc) as next_wk_day_3_daily_perc,
          nvl(om.next_wk_day_4_daily_perc, ff.next_wk_day_4_daily_perc) as next_wk_day_4_daily_perc,
          nvl(om.next_wk_day_5_daily_perc, ff.next_wk_day_5_daily_perc) as next_wk_day_5_daily_perc,
          nvl(om.next_wk_day_6_daily_perc, ff.next_wk_day_6_daily_perc) as next_wk_day_6_daily_perc,
          nvl(om.next_wk_day_7_daily_perc, ff.next_wk_day_7_daily_perc) as next_wk_day_7_daily_perc,
          nvl(om.this_wk_day_1_daily_perc, ff.this_wk_day_1_daily_perc) as this_wk_day_1_daily_perc,
          nvl(om.this_wk_day_2_daily_perc, ff.this_wk_day_2_daily_perc) as this_wk_day_2_daily_perc,
          nvl(om.this_wk_day_3_daily_perc, ff.this_wk_day_3_daily_perc) as this_wk_day_3_daily_perc,
          nvl(om.this_wk_day_4_daily_perc, ff.this_wk_day_4_daily_perc) as this_wk_day_4_daily_perc,
          nvl(om.this_wk_day_5_daily_perc, ff.this_wk_day_5_daily_perc) as this_wk_day_5_daily_perc,
          nvl(om.this_wk_day_6_daily_perc, ff.this_wk_day_6_daily_perc) as this_wk_day_6_daily_perc,
          nvl(om.this_wk_day_7_daily_perc, ff.this_wk_day_7_daily_perc) as this_wk_day_7_daily_perc,
          nvl(om.split_perc, ff.split_perc) as split_perc
           
   from   om om
   full outer join ff ff on om.zone_no     = ff.zone_no
                        and om.zone_group_no = ff.zone_group_no
                        and om.item_no     = ff.item_no
                        and om.to_loc_no   = ff.to_loc_no
                        and om.supplier_no = ff.supplier_no) -- select * from all_together;
                        
   select dz.sk1_zone_group_zone_no,
          di.sk1_item_no,
          ds.sk1_supplier_no,
          atg.calendar_date,
          atg.to_loc_no,
          atg.NUM_LEAD_TIME_DAYS,
          atg.NEXT_WK_COST_PRICE,
          atg.THIS_WK_COST_PRICE,
          atg.NEXT_WK_TRUNK_COST,
          atg.THIS_WK_TRUNK_COST,
          atg.NEXT_WK_DAY_1_DAILY_PERC,
          atg.NEXT_WK_DAY_2_DAILY_PERC,
          atg.NEXT_WK_DAY_3_DAILY_PERC,
          atg.NEXT_WK_DAY_4_DAILY_PERC,
          atg.NEXT_WK_DAY_5_DAILY_PERC,
          atg.NEXT_WK_DAY_6_DAILY_PERC,
          atg.NEXT_WK_DAY_7_DAILY_PERC,
          atg.THIS_WK_DAY_1_DAILY_PERC,
          atg.THIS_WK_DAY_2_DAILY_PERC,
          atg.THIS_WK_DAY_3_DAILY_PERC,
          atg.THIS_WK_DAY_4_DAILY_PERC,
          atg.THIS_WK_DAY_5_DAILY_PERC,
          atg.THIS_WK_DAY_6_DAILY_PERC,
          atg.THIS_WK_DAY_7_DAILY_PERC,
          atg.split_perc
   from all_together atg,
        dim_zone dz,
        dim_item di,
        dim_supplier ds
  where atg.zone_group_no = dz.zone_group_no
    and atg.zone_no       = dz.zone_no
    and atg.item_no       = di.item_no
    and atg.supplier_no   = ds.supplier_no;
 
-- For input bulk collect --
type stg_array is table of c_fnd_zone_item_supp_hist_hist%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_zone_item_supp_hist_hist%rowtype;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.to_loc_no                       := g_rec_in.to_loc_no;
   g_rec_out.num_lead_time_days              := g_rec_in.num_lead_time_days;
   g_rec_out.next_wk_cost_price              := g_rec_in.next_wk_cost_price;
   g_rec_out.this_wk_cost_price              := g_rec_in.this_wk_cost_price;
   g_rec_out.next_wk_trunk_cost              := g_rec_in.next_wk_trunk_cost;
   g_rec_out.this_wk_trunk_cost              := g_rec_in.this_wk_trunk_cost;
   g_rec_out.next_wk_day_1_daily_perc        := g_rec_in.next_wk_day_1_daily_perc;
   g_rec_out.next_wk_day_2_daily_perc        := g_rec_in.next_wk_day_2_daily_perc;
   g_rec_out.next_wk_day_3_daily_perc        := g_rec_in.next_wk_day_3_daily_perc;
   g_rec_out.next_wk_day_4_daily_perc        := g_rec_in.next_wk_day_4_daily_perc;
   g_rec_out.next_wk_day_5_daily_perc        := g_rec_in.next_wk_day_5_daily_perc;
   g_rec_out.next_wk_day_6_daily_perc        := g_rec_in.next_wk_day_6_daily_perc;
   g_rec_out.next_wk_day_7_daily_perc        := g_rec_in.next_wk_day_7_daily_perc;
   g_rec_out.this_wk_day_1_daily_perc        := g_rec_in.this_wk_day_1_daily_perc;
   g_rec_out.this_wk_day_2_daily_perc        := g_rec_in.this_wk_day_2_daily_perc;
   g_rec_out.this_wk_day_3_daily_perc        := g_rec_in.this_wk_day_3_daily_perc;
   g_rec_out.this_wk_day_4_daily_perc        := g_rec_in.this_wk_day_4_daily_perc;
   g_rec_out.this_wk_day_5_daily_perc        := g_rec_in.this_wk_day_5_daily_perc;
   g_rec_out.this_wk_day_6_daily_perc        := g_rec_in.this_wk_day_6_daily_perc;
   g_rec_out.this_wk_day_7_daily_perc        := g_rec_in.this_wk_day_7_daily_perc;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_zone_group_zone_no          := g_rec_in.sk1_zone_group_zone_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
   g_rec_out.calendar_date                   := g_rec_in.calendar_date;

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
       insert into rtl_zone_item_supp_hist values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no||
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
       update rtl_zone_item_supp_hist
       set    to_loc_no                       = a_tbl_update(i).to_loc_no,
              num_lead_time_days              = a_tbl_update(i).num_lead_time_days,
              next_wk_cost_price              = a_tbl_update(i).next_wk_cost_price,
              this_wk_cost_price              = a_tbl_update(i).this_wk_cost_price,
              next_wk_trunk_cost              = a_tbl_update(i).next_wk_trunk_cost,
              this_wk_trunk_cost              = a_tbl_update(i).this_wk_trunk_cost,
              next_wk_day_1_daily_perc        = a_tbl_update(i).next_wk_day_1_daily_perc,
              next_wk_day_2_daily_perc        = a_tbl_update(i).next_wk_day_2_daily_perc,
              next_wk_day_3_daily_perc        = a_tbl_update(i).next_wk_day_3_daily_perc,
              next_wk_day_4_daily_perc        = a_tbl_update(i).next_wk_day_4_daily_perc,
              next_wk_day_5_daily_perc        = a_tbl_update(i).next_wk_day_5_daily_perc,
              next_wk_day_6_daily_perc        = a_tbl_update(i).next_wk_day_6_daily_perc,
              next_wk_day_7_daily_perc        = a_tbl_update(i).next_wk_day_7_daily_perc,
              this_wk_day_1_daily_perc        = a_tbl_update(i).this_wk_day_1_daily_perc,
              this_wk_day_2_daily_perc        = a_tbl_update(i).this_wk_day_2_daily_perc,
              this_wk_day_3_daily_perc        = a_tbl_update(i).this_wk_day_3_daily_perc,
              this_wk_day_4_daily_perc        = a_tbl_update(i).this_wk_day_4_daily_perc,
              this_wk_day_5_daily_perc        = a_tbl_update(i).this_wk_day_5_daily_perc,
              this_wk_day_6_daily_perc        = a_tbl_update(i).this_wk_day_6_daily_perc,
              this_wk_day_7_daily_perc        = a_tbl_update(i).this_wk_day_7_daily_perc,
              --calendar_date                   = a_tbl_update(i).calendar_date,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_item_no                     = a_tbl_update(i).sk1_item_no  and
              sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no and
              sk1_zone_group_zone_no          = a_tbl_update(i).sk1_zone_group_zone_no and
              calendar_date                   = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
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
   from   rtl_zone_item_supp_hist
   where  sk1_item_no            = g_rec_out.sk1_item_no  and
          sk1_supplier_no        = g_rec_out.sk1_supplier_no and
          sk1_zone_group_zone_no = g_rec_out.sk1_zone_group_zone_no and
          calendar_date          = g_rec_out.calendar_date;
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

    l_text := 'LOAD OF rtl_zone_item_supp_hist EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '02/MAR/14';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

    open c_fnd_zone_item_supp_hist_hist;

    fetch c_fnd_zone_item_supp_hist_hist bulk collect into a_stg_input limit g_forall_limit;

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
    fetch c_fnd_zone_item_supp_hist_hist bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_zone_item_supp_hist_hist;
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
end wh_prf_corp_087u_load;
