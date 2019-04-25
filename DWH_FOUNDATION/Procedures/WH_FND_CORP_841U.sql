--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_841U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_841U" (p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
--  Date:        April 2010
--  Author:      W Lyttle
--  Purpose:     Update allocation tracker table with the correct first_dc_no for supply_chain_code 'WH'.
--               The wh_no on fnd_rtl_allocation, is used to populate first_dc_no,
--               but for supply_chain_code 'WH', this is not the correct DC.
--               For CHBD only.
--  Tables:      Input  - fnd_alloc_tracker_alloc, fnd_rtl_shipment
--               Output - fnd_alloc_tracker_alloc
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  7 may 2010 - wendy - change first dc_no update
--  14 MAY 2010 - WENDY - add first_whse_supplier_no processing
--                      and 'second' location as first_dc_no
--  28 may 2010 - wendy - ensure that extract query is same in live and full_live
--  22 Oct 2010 - wendy - have removed all the hints
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_date               date;
g_start_date         date;
g_rec_out            fnd_alloc_tracker_alloc%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_841U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Updates ALLOC TRACKER TABLE with correct first_dc_no';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_alloc_tracker_alloc%rowtype index by binary_integer;
type tbl_array_u is table of fnd_alloc_tracker_alloc%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_alloc is
WITH selship AS
  (SELECT
    /*+ full(a) full(s) parallel(a,4) parallel(s,4) */
            a.alloc_no alloc_no,
            s.from_loc_no ,
            s.to_loc_no
  FROM fnd_alloc_tracker_alloc a
  JOIN fnd_rtl_shipment s
      ON A.alloc_no             = s.tsf_alloc_no
      AND A.item_no             = s.item_no
  WHERE a.supply_chain_code = 'WH'
    AND a.release_date BETWEEN g_start_date AND g_date
  GROUP BY  a.alloc_no ,
            s.from_loc_no ,
            s.to_loc_no
  ),
  selwh AS
    (SELECT TRUNC(sysdate) release_date,
            alloc_no,
            0 to_loc_no,
            from_loc_no first_dc_no
  FROM selship s
  JOIN dim_location l
      ON l.location_no = s.from_loc_no
  WHERE l.loc_type = 'W'
    GROUP BY alloc_no,
            from_loc_no
  MINUS
    SELECT TRUNC(sysdate) release_date,
            alloc_no,
            0 to_loc_no,
            to_loc_no first_dc_no
  FROM selship s
  JOIN dim_location l
      ON l.location_no = s.to_loc_no
  WHERE l.loc_type = 'W'
    GROUP BY alloc_no,
              to_loc_no
  )
SELECT
  /*+ full(b) parallel(b,4) */
      a.alloc_no ,
      a.first_dc_no first_whse_supplier_no,
      b.to_loc_no first_dc_no
FROM selwh a,
      fnd_rtl_shipment b
WHERE a.alloc_no  = b.tsf_alloc_no
AND a.first_dc_no = b.from_loc_no
GROUP BY
      a.alloc_no ,
      a.first_dc_no ,
      b.to_loc_no ;

g_rec_in             c_fnd_alloc%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_alloc%rowtype;
a_stg_input      stg_array;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.alloc_no                             := g_rec_in.alloc_no;
   g_rec_out.first_dc_no                          := g_rec_in.first_dc_no;
   g_rec_out.first_whse_supplier_no               := g_rec_in.first_whse_supplier_no;

-- no need to update last_updated_date. fnd_alloc_tracker_alloc gets fully refreshed every day.

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
--       update /*+ parallel(su,4) */ fnd_alloc_tracker_alloc su
       update  fnd_alloc_tracker_alloc su
          set first_dc_no                  = a_tbl_update(i).first_dc_no,
              first_whse_supplier_no       = a_tbl_update(i).first_whse_supplier_no
        where su.alloc_no                  = a_tbl_update(i).alloc_no
          AND su.release_date BETWEEN g_start_date AND g_date
      ;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
       commit;

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
                       ' '||a_tbl_update(g_error_index).release_date||
                       ' '||a_tbl_update(g_error_index).alloc_no||
                       ' '||a_tbl_update(g_error_index).to_loc_no;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to output table
--**************************************************************************************************
procedure local_write_output as
begin

   a_count_u               := a_count_u + 1;
   a_tbl_update(a_count_u) := g_rec_out;
   a_count                 := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_update;
      a_tbl_update  := a_empty_set_u;
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'UPDATE ALLOC TRACKER TABLE WITH CORRECT FIRST_DC_NO STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   g_start_date := g_date - 90;

     l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA UPDATED FOR PERIOD '||g_start_date||' TO '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    begin
--    execute immediate '  alter session set events ''10046 trace name context forever, level 12''   ';
--    end;

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

--    update /*+ parallel(alloc,4) */ fnd_alloc_tracker_alloc allooc
    update  fnd_alloc_tracker_alloc allooc
    set first_dc_no = null
    where release_date between g_start_date and g_date
    and supply_chain_code = 'WH'
    AND FIRST_DC_NO IS NOT NULL;

    g_recs_updated := sql%rowcount;
    commit;

    l_text := 'RECORDS FOR SUPPLY_CHAIN_CODE WH SET TO NULL - '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_recs_updated := 0;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_alloc;
    fetch c_fnd_alloc bulk collect into a_stg_input limit g_forall_limit;

    if a_stg_input.count > 0 then
       g_rec_in    := a_stg_input(1);
    end if;

    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read||' updated='||g_recs_updated ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := null;
         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_alloc bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_alloc;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_update;
    commit;

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


END WH_FND_CORP_841U;
