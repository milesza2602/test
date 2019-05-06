--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_005A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_005A" 
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2008
--  Author:      Alfonso Joshua
--  Purpose:     Create the weekly temp swing system forecast table in the performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_wk_rdf_wkfcst
--               Output - temp_loc_item_wk_rdf_sysfcst
--  Packages:    constants, dwh_log, dwh_valid
--  Maintenance
--  04 May 2009: TD-1469 - check for data duplication to prevent unique constraint as this program is insert only
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
g_fnd_count          number        :=  0;
g_today_fin_day_no   number        :=  0;
g_fd_num_catlg_days  number        :=  0;
g_fin_week_no        number        :=  0;
g_max_week           number        :=  0;
g_rec_out            temp_loc_item_wk_rdf_sysfcst%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_calendar_date      date          := trunc(sysdate);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_005A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TEMP LOAD RDF WEEKLY SYS FCST FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of temp_loc_item_wk_rdf_sysfcst%rowtype index by binary_integer;
type tbl_array_u is table of temp_loc_item_wk_rdf_sysfcst%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_wk_wkfcst is

        select location_no,
               item_no,
               fin_year_no,
               fin_week_no,
               (case when (fin_week_no + to_number(substr(syscol,4,2))) > g_max_week
                     then fin_year_no + 1
                     else fin_year_no
                end) fin_year_no_calc,
               (case when (fin_week_no + to_number(substr(syscol,4,2))) > g_max_week
                     then fin_week_no + (to_number(substr(syscol,4,2))- g_max_week)
                     else fin_week_no + (to_number(substr(syscol,4,2))-1)
                end) fin_week_no_calc,
               syscol,
--               sysdata
               sysfcst,appfcst
        from   fnd_rtl_loc_item_wk_rdf_wkfcst
               unpivot include nulls ((sysfcst,appfcst) for syscol in (
                                                            (wk_01_sys_fcst_qty,wk_01_app_fcst_qty),
                                                            (wk_02_sys_fcst_qty,wk_02_app_fcst_qty),
                                                            (wk_03_sys_fcst_qty,wk_03_app_fcst_qty),
                                                            (wk_04_sys_fcst_qty,wk_04_app_fcst_qty),
                                                            (wk_05_sys_fcst_qty,wk_05_app_fcst_qty),
                                                            (wk_06_sys_fcst_qty,wk_06_app_fcst_qty),
                                                            (wk_07_sys_fcst_qty,wk_07_app_fcst_qty),
                                                            (wk_08_sys_fcst_qty,wk_08_app_fcst_qty),
                                                            (wk_09_sys_fcst_qty,wk_09_app_fcst_qty),
                                                            (wk_10_sys_fcst_qty,wk_10_app_fcst_qty),
                                                            (wk_11_sys_fcst_qty,wk_11_app_fcst_qty),
                                                            (wk_12_sys_fcst_qty,wk_12_app_fcst_qty),
                                                            (wk_13_sys_fcst_qty,wk_13_app_fcst_qty) /*,
                                                            (wk_14_sys_fcst_qty,wk_14_app_fcst_qty),
                                                            (wk_15_sys_fcst_qty,wk_15_app_fcst_qty),
                                                            (wk_16_sys_fcst_qty,wk_16_app_fcst_qty),
                                                            (wk_17_sys_fcst_qty,wk_17_app_fcst_qty),
                                                            (wk_18_sys_fcst_qty,wk_18_app_fcst_qty),
                                                            (wk_19_sys_fcst_qty,wk_19_app_fcst_qty),
                                                            (wk_20_sys_fcst_qty,wk_20_app_fcst_qty),
                                                            (wk_21_sys_fcst_qty,wk_21_app_fcst_qty),
                                                            (wk_22_sys_fcst_qty,wk_22_app_fcst_qty),
                                                            (wk_23_sys_fcst_qty,wk_23_app_fcst_qty),
                                                            (wk_24_sys_fcst_qty,wk_24_app_fcst_qty),
                                                            (wk_25_sys_fcst_qty,wk_25_app_fcst_qty),
                                                            (wk_26_sys_fcst_qty,wk_26_app_fcst_qty),
                                                            (wk_27_sys_fcst_qty,wk_27_app_fcst_qty),
                                                            (wk_28_sys_fcst_qty,wk_28_app_fcst_qty),
                                                            (wk_29_sys_fcst_qty,wk_29_app_fcst_qty),
                                                            (wk_30_sys_fcst_qty,wk_30_app_fcst_qty),
                                                            (wk_31_sys_fcst_qty,wk_31_app_fcst_qty),
                                                            (wk_32_sys_fcst_qty,wk_32_app_fcst_qty),
                                                            (wk_33_sys_fcst_qty,wk_33_app_fcst_qty),
                                                            (wk_34_sys_fcst_qty,wk_34_app_fcst_qty),
                                                            (wk_35_sys_fcst_qty,wk_35_app_fcst_qty),
                                                            (wk_36_sys_fcst_qty,wk_36_app_fcst_qty),
                                                            (wk_37_sys_fcst_qty,wk_37_app_fcst_qty),
                                                            (wk_38_sys_fcst_qty,wk_38_app_fcst_qty),
                                                            (wk_39_sys_fcst_qty,wk_39_app_fcst_qty),
                                                            (wk_40_sys_fcst_qty,wk_40_app_fcst_qty),
                                                            (wk_41_sys_fcst_qty,wk_41_app_fcst_qty),
                                                            (wk_42_sys_fcst_qty,wk_42_app_fcst_qty),
                                                            (wk_43_sys_fcst_qty,wk_43_app_fcst_qty),
                                                            (wk_44_sys_fcst_qty,wk_44_app_fcst_qty),
                                                            (wk_45_sys_fcst_qty,wk_45_app_fcst_qty),
                                                            (wk_46_sys_fcst_qty,wk_46_app_fcst_qty),
                                                            (wk_47_sys_fcst_qty,wk_47_app_fcst_qty),
                                                            (wk_48_sys_fcst_qty,wk_48_app_fcst_qty),
                                                            (wk_49_sys_fcst_qty,wk_49_app_fcst_qty),
                                                            (wk_50_sys_fcst_qty,wk_50_app_fcst_qty),
                                                            (wk_51_sys_fcst_qty,wk_51_app_fcst_qty),
                                                            (wk_52_sys_fcst_qty,wk_52_app_fcst_qty) */
                                                             ))
        where  last_updated_date = g_date --and
--               to_number(substr(syscol,4,2)) >= fin_week_no and
--               to_number(substr(syscol,4,2)) <= fin_week_no + 12
        order by location_no, item_no, fin_year_no, fin_week_no desc;  /* TD-1469 */

g_rec_in                   c_fnd_rtl_loc_item_wk_wkfcst%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_wk_wkfcst%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.location_no                     := g_rec_in.location_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no_calc;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no_calc;
   g_rec_out.fin_week_no_orig                := g_rec_in.fin_week_no;
   g_rec_out.sales_wkly_sys_fcst_qty         := g_rec_in.sysfcst;
   g_rec_out.sales_wkly_app_fcst_qty         := g_rec_in.appfcst;
   g_rec_out.last_updated_date               := g_date;

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
       insert into temp_loc_item_wk_rdf_sysfcst values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;
/*
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update temp_loc_item_wk_rdf_sysfcst
       set    sales_dly_sys_fcst_qty        = a_tbl_update(i).sales_dly_sys_fcst_qty,
              post_date_orig                = a_tbl_update(i).post_date_orig,
              last_updated_date             = a_tbl_update(i).last_updated_date
       where  location_no                   = a_tbl_update(i).location_no  and
              item_no                       = a_tbl_update(i).item_no      and
              fin_year_no                   = a_tbl_update(i).fin_year_no  and
              fin_week_no                   = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;
*/
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := FALSE;
   g_count :=0;

-- Check to see if item is present on table and update/insert accordingly
/*
   if g_fnd_count > 0 then  /* check done to determine multiple staging loads into foundation - TD-1169
      select count(1)
      into   g_count
      from   temp_loc_item_wk_rdf_sysfcst
      where  location_no    = g_rec_out.location_no  and
             item_no        = g_rec_out.item_no      and
             fin_year_no    = g_rec_out.fin_year_no  and
             fin_week_no    = g_rec_out.fin_week_no;

      if g_count = 1 then
         g_found := TRUE;
      end if;

      if a_count_i > 0 and not g_found then
         for i in a_tbl_insert.first .. a_tbl_insert.last
         loop
            if  a_tbl_insert(i).location_no = g_rec_out.location_no and
                a_tbl_insert(i).item_no     = g_rec_out.item_no and
                a_tbl_insert(i).fin_year_no = g_rec_out.fin_year_no and
                a_tbl_insert(i).fin_week_no = g_rec_out.fin_week_no
               then
               g_found := TRUE;
            end if;
         end loop;
      end if;
   end if;
*/
-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
--   else
--      a_count_u               := a_count_u + 1;
--      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
--      local_bulk_update;
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
    l_text := 'LOAD TEMP_LOC_ITEM_WK_RDF_SYSFCST EX FOUNDATION STARTED '||
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

    select fin_week_no, calendar_date
    into g_fin_week_no, g_calendar_date
    from dim_calendar
    where calendar_date = (select trunc(sysdate) from dual) ;
    l_text := 'CURRENT DATE BEING PROCESSED - '||g_calendar_date||' g_week_no - '||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    ----------------------------------------------------------------------------------------------

    select max(fin_week_no) + 1
    into   g_max_week
    from   dim_calendar
    where  fin_year_no =
           (
           select fin_year_no
           from   dim_calendar
           where  calendar_date = g_date
           );


    l_text := 'No of weeks in the Fin_year + 1 for calculation in the code  - ' ||g_max_week;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    ----------------------------------------------------------------------------------------------

    execute immediate 'truncate table dwh_performance.temp_loc_item_wk_rdf_sysfcst';
    l_text := 'TABLE temp_loc_item_wk_rdf_sysfcst TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    /*  TD-1469
    This check will determine whether multiple loads are present on the foundation table (fnd_rtl_loc_item_wk_rdf_wkfcst)
    Should there be duplicate item/location combinations then a lookup will be done on the temp table
    prior to data population (temp_loc_item_wk_rdf_sysfcst)
*/
    select count(*)
    into g_fnd_count
    from
      (select count(*), item_no, location_no
       from fnd_rtl_loc_item_wk_rdf_wkfcst
       where last_updated_date = g_date
       group by item_no, location_no
       having count(*) > 1);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_wk_wkfcst;
    fetch c_fnd_rtl_loc_item_wk_wkfcst bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_loc_item_wk_wkfcst bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_wk_wkfcst;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
--    local_bulk_update;
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

end wh_prf_rdf_005a;
