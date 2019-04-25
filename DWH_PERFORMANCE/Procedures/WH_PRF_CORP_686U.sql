--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_686U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_686U" 
                                                                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        February 2010
--  Author:      M Munnik
--  Purpose:     Calculates 6wk sales from Sales Dense to Promotions fact table for promotions that have been approved.
--  Tables:      Input  - rtl_loc_item_wk_rms_dense
--               Output - rtl_prom_loc_item_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 
--  wendy lyttle 5 july 2012 removed to allow thru -and      pl.prom_no <>  313801xx
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date;
g_rec_out            rtl_prom_loc_item_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_686U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CALC 6WK SALES ONTO PROM/LOC/ITEM/DY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_u is table of rtl_prom_loc_item_dy%rowtype index by binary_integer;
a_tbl_update        tbl_array_u;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_u           integer       := 0;

cursor c_prom_loc_item_dy is
   with prom as
  (select   sk1_prom_no, prom_no, (dc.this_week_start_date - 7) complwk
   from     dim_calendar dc
   join     dim_prom dp                   on  dc.calendar_date = dp.prom_start_date
   Where    G_Date                        Between Dp.Approval_Date And Dp.Prom_End_Date
    -- removed to allow thru
-- 5 july 2012
 --     and dp.prom_no <> 313801
 ),

   promweeks as
  (select   dc.fin_year_no, dc.fin_week_no, dl.sk1_location_no, ia.sk1_item_no, dp.sk1_prom_no
   from     dim_calendar dc
   join     prom dp                       on  dc.this_week_start_date between (dp.complwk - 35) and (dp.complwk)
   join     fnd_prom_location pl          on  dp.prom_no         = pl.prom_no
   join     dim_location dl               on  dl.location_no     = pl.location_no
   join     rtl_prom_item_all ia          on  ia.sk1_prom_no     = dp.sk1_prom_no
   Where    Dc.Fin_Day_No                 = 1
     -- removed to allow thru
-- 5 july 2012
   --   and pl.prom_no <> 313801
      ),
   
   dense as
   (select  /*+ full(dn) parallel (dn,6)  */  dn.sk1_location_no, dn.sk1_item_no, p.sk1_prom_no,
            sum(dn.sales) sales_6wk_back
   from     rtl_loc_item_wk_rms_dense dn
   join     promweeks p                   on  dn.fin_year_no     = p.fin_year_no
                                          and dn.fin_week_no     = p.fin_week_no
                                          and dn.sk1_item_no     = p.sk1_item_no
                                          and dn.sk1_location_no = p.sk1_location_no
   group by dn.sk1_location_no, dn.sk1_item_no, p.sk1_prom_no)
   
   select   /*+ full(prm) parallel (prm,6)*/  dns.sk1_location_no, dns.sk1_item_no, dns.sk1_prom_no,prm.post_date,dns.sales_6wk_back
   from     dense dns
   join     rtl_prom_loc_item_dy prm           on dns.sk1_item_no     = prm.sk1_item_no
                                              and dns.sk1_location_no = prm.sk1_location_no
                                              and dns.sk1_prom_no     = prm.sk1_prom_no
                                              and nvl(prm.sales_6wk_back,0)          <> dns.sales_6wk_back
    order by  prm.post_date                                         ;
  


g_rec_in             c_prom_loc_item_dy%rowtype;
-- For input bulk collect --
type stg_array is table of c_prom_loc_item_dy%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out          			                     := null;

   g_rec_out.sk1_location_no                     := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                         := g_rec_in.sk1_item_no;
   g_rec_out.sk1_prom_no                         := g_rec_in.sk1_prom_no;
      g_rec_out.post_date                         := g_rec_in.post_date;
   g_rec_out.sales_6wk_back                      := g_rec_in.sales_6wk_back;
   g_rec_out.last_updated_date                   := g_date;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_prom_loc_item_dy
       set    sales_6wk_back                  = a_tbl_update(i).sales_6wk_back,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  post_date                       = a_tbl_update(i).post_date
       and    sk1_location_no                 = a_tbl_update(i).sk1_location_no
       and    sk1_item_no                     = a_tbl_update(i).sk1_item_no
       and    sk1_prom_no                     = a_tbl_update(i).sk1_prom_no ;


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
                       ' '||a_tbl_update(g_error_index).post_date||
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_prom_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_write_output as
begin

-- Place data into and array for later writing to table in bulk
   a_count_u               := a_count_u + 1;
   a_tbl_update(a_count_u) := g_rec_out;

   a_count                 := a_count + 1;

   if a_count > g_forall_limit then
      local_bulk_update;
      a_tbl_update  := a_empty_set_u;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
--    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
--       g_forall_limit := p_forall_limit;
--    end if;
--  Because the update statement in this procedure updates many records on the table 
--  for each one record in the update array, the forall_limit must be smaller.
--    g_forall_limit := 25;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'CALC 6WK SALES ONTO RTL_PROM_LOC_ITEM_DY STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--    l_text := 'RETURNED WITHOUT PROCESSING';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text := dwh_constants.vc_log_draw_line;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    p_success := true;
--    return;

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'alter session enable parallel dml';
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_prom_loc_item_dy;
    fetch c_prom_loc_item_dy bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 20000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := null;
         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_prom_loc_item_dy bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_prom_loc_item_dy;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,'',g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
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
      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

end wh_prf_corp_686u;
