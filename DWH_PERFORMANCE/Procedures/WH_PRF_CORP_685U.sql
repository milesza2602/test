--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_685U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_685U" 
                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        February 2010
--  Author:      M Munnik
--  Purpose:     Rollup sales_dly_app_fcst to Promotions fact table for promotions that have been approved.
--  Tables:      Input  - rtl_loc_item_dy_rdf_fcst
--               Output - rtl_prom_loc_item_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  30 nov 2011 - wendy - change delete to a cursor to help performance
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
--  09 Nov 2016 - A Joshua - Level1/2 rename (chg-2218)
--
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_recs               number        :=   0;
g_found              boolean;
g_date               date;
g_sk1_prom_period_1  rtl_prom_loc_item_dy.sk1_prom_period_no%type;
g_sk1_prom_period_2  rtl_prom_loc_item_dy.sk1_prom_period_no%type;
g_rec_out            rtl_prom_loc_item_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_685U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLS SALES FCST TO PROM/LOC/ITEM/DY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_prom_loc_item_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_prom_loc_item_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_prom_loc_item_dy is
   select   df.post_date, df.sk1_location_no, df.sk1_item_no, ia.sk1_prom_no,
            (case when df.post_date between dp.prom_start_date and dp.prom_end_date then g_sk1_prom_period_2
                  else g_sk1_prom_period_1 end) sk1_prom_period_no,
            df.sales_dly_app_fcst_qty, df.sales_dly_app_fcst
   from     RTL_LOC_ITEM_RDF_DYFCST_L2 df
   --from     rtl_loc_item_dy_rdf_fcst df  -- level1/2 remapping chg-2218
   join     dim_location dl               on  df.sk1_location_no     = dl.sk1_location_no
   join     rtl_prom_item_all ia          on  df.sk1_item_no         = ia.sk1_item_no
   join     dim_prom dp                   on  ia.sk1_prom_no         = dp.sk1_prom_no
   join     fnd_prom_location pl          on  dp.prom_no             = pl.prom_no
                                          and dl.location_no         = pl.location_no
   where    DF.POST_DATE                  between DP.APPROVAL_DATE and DP.PROM_END_DATE
   and    DF.LAST_UPDATED_DATE          = G_DATE
 ;
   
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

   g_rec_out.post_date                           := g_rec_in.post_date;
   g_rec_out.sk1_location_no                     := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                         := g_rec_in.sk1_item_no;
   g_rec_out.sk1_prom_no                         := g_rec_in.sk1_prom_no;
   g_rec_out.sk1_prom_period_no                  := g_rec_in.sk1_prom_period_no;
   g_rec_out.sales_dly_app_fcst_qty              := g_rec_in.sales_dly_app_fcst_qty;
   g_rec_out.sales_dly_app_fcst                  := g_rec_in.sales_dly_app_fcst;
   g_rec_out.last_updated_date                   := g_date;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_prom_loc_item_dy values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).post_date||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_prom_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_prom_loc_item_dy
       set    sk1_prom_period_no              = a_tbl_update(i).sk1_prom_period_no,
              sales_dly_app_fcst_qty          = a_tbl_update(i).sales_dly_app_fcst_qty,
              sales_dly_app_fcst              = a_tbl_update(i).sales_dly_app_fcst,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  post_date                       = a_tbl_update(i).post_date
       and    sk1_location_no                 = a_tbl_update(i).sk1_location_no
       and    sk1_item_no                     = a_tbl_update(i).sk1_item_no
       and    sk1_prom_no                     = a_tbl_update(i).sk1_prom_no;

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

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_prom_loc_item_dy
   where  post_date             = g_rec_out.post_date
   and    sk1_location_no       = g_rec_out.sk1_location_no
   and    sk1_item_no           = g_rec_out.sk1_item_no
   and    sk1_prom_no           = g_rec_out.sk1_prom_no;

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

   a_count                    := a_count + 1;

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
-- Forecast records exist on rtl_loc_item_dy_rdf_fcst into the future.
-- If the end date of the promotion is changed, there might be records on rtl_prom_loc_item_dy,
-- where post_date > prom_end_date
--**************************************************************************************************
procedure delete_redundant_recs as
begin

--   delete from rtl_prom_loc_item_dy r 
--   where r.post_date > (select p.prom_end_date from dim_prom p where p.sk1_prom_no = r.sk1_prom_no);
   
  FOR v_cur IN
  (with selext
as 
  (select /*+ PARALLEL(a 4)  */
   a.sk1_prom_no, post_date
   from dwh_performance.rtl_prom_loc_item_dy a
   where post_date > g_date
   group by a.sk1_prom_no, post_date)
  select r.sk1_prom_no, prom_end_date, post_date
   from selext r,
        dwh_performance.dim_prom p
   where  p.sk1_prom_no = r.sk1_prom_no
   group by r.sk1_prom_no, prom_end_date, post_date
   having  post_date > prom_end_date
  )
  LOOP
     DELETE FROM dwh_performance.rtl_prom_loc_item_dy r 
     WHERE r.sk1_prom_no = v_cur.sk1_prom_no
     and r.post_date = v_cur.post_date;
     g_recs := sql%rowcount;
         l_text := 'deleted - '||g_recs||' '||v_cur.sk1_prom_no||'  '||v_cur.post_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     g_recs_deleted  := g_recs_deleted  + g_recs;
     COMMIT;
  END LOOP;
  COMMIT;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_redundant_recs;

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
    l_text := 'LOAD RTL_PROM_LOC_ITEM_DY from SALES FCST STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--      g_date := '29 nov 2011';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'alter session enable parallel dml';
--**************************************************************************************************
-- Once-off retrieval of data to improve performance
--**************************************************************************************************
    select sk1_prom_period_no
    into   g_sk1_prom_period_1
    from   dim_prom_period
    where  prom_period_no = '1';

    select sk1_prom_period_no
    into   g_sk1_prom_period_2
    from   dim_prom_period
    where  prom_period_no = '2';

--**************************************************************************************************
-- Process Deletes
--**************************************************************************************************
    l_text := 'Delete Started - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    delete_redundant_recs;
    l_text := 'Delete Ended - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
         if g_recs_read mod 300000 = 0 then
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
    local_bulk_insert;
    local_bulk_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_685u;
