--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_023U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_023U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load Hyperion S4S business_unit week information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_HYP_LOC_NTCOE_WK
--               Output   - DWH_PERFORMANCE.RTL_LOC_NTCOE_WK
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            dwh_performance.RTL_LOC_NTCOE_WK%rowtype;
g_found              boolean;
g_date               date;
G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_constr_end_date  date;



l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_023U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_LOC_NTCOE_WK data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.RTL_LOC_NTCOE_WK%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.RTL_LOC_NTCOE_WK%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_LOCATION is
-- SELECT *FROM DIM_HR_EMPLOYEE WHERE SK1_JOB_ID IN (396371,
--404245)
--  JOB_ID IN (1002194, 1002198, 1002609)
 --179463    1002194
--179464    1002198
--179630    1002609
--396371    7072436
--404245    7075640
/* Formatted on 09/09/2014 10:38:01 AM (QP5 v5.185.11230.41888) */
with selext as (SELECT  /*+ full(flr) parallel(flr,6) */              FLR.LOCATION_NO
                   , FLR.business_date
                  , FLR.ntcoe_budget_fte
                  , FLR.ntcoe_budget_cost
                  , flr.fin_year_no
                  ,flr.fin_week_no
                  , SK1_LOCATION_NO
           FROM dwh_foundation.FND_S4S_HYP_LOC_NTCOE_WK flr,
                dwh_performance.DIM_LOCATION DL
          WHERE FLR.LOCATION_NO = DL.LOCATION_NO
          and flr.last_updated_date = g_date
          GROUP BY    FLR.LOCATION_NO
                  , FLR.business_date
                  , FLR.ntcoe_budget_fte
                  , FLR.ntcoe_budget_cost
                  , flr.fin_year_no
                  ,flr.fin_week_no
                  , SK1_LOCATION_NO
          )

SELECT   /*+ full(rtl) parallel(rtl,6) */  DISTINCT      se.*
                  , RTL.SK1_LOCATION_NO RTL_EXISTS
           FROM selext se,
                DWH_PERFORMANCE.RTL_LOC_NTCOE_WK rtl
          WHERE se.sk1_LOCATION_NO = rtl.sk1_LOCATION_NO(+)
          and se.fin_year_no = rtl.fin_year_no(+)
          and se.fin_week_no = rtl.fin_week_no(+)
          ORDER BY SE.FIN_YEAR_NO,
                    SE.FIN_WEEK_NO,
                    SE.SK1_LOCATION_NO

;
type stg_array is table of c_fnd_LOCATION%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_LOCATION%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin
 /* Formatted on 09/09/2014 01:01:31 PM (QP5 v5.185.11230.41888) */
   /* Formatted on 09/09/2014 01:03:45 PM (QP5 v5.185.11230.41888) */
   g_rec_out.sk1_LOCATION_NO :=  g_rec_in.sk1_LOCATION_NO;
   g_rec_out.fin_year_no :=  g_rec_in.fin_year_no;
   g_rec_out.fin_week_no :=  g_rec_in.fin_week_no;
   g_rec_out.ntcoe_budget_fte :=  g_rec_in.ntcoe_budget_fte;
   g_rec_out.ntcoe_budget_cost :=  g_rec_in.ntcoe_budget_cost;

    g_rec_out.last_updated_date         := g_date;


   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into dwh_performance.RTL_LOC_NTCOE_WK  values a_tbl_insert(i);

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
                       ' '||a_tbl_INSERT(g_error_index).FIN_YEAR_NO||
                       ' '||a_tbl_INSERT(g_error_index).FIN_WEEK_NO||
                       ' '||a_tbl_INSERT(g_error_index).SK1_LOCATION_NO
;
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
             UPDATE dwh_performance.RTL_LOC_NTCOE_WK
                    SET
                      ntcoe_budget_FTE              = a_tbl_update(i).ntcoe_budget_FTE  ,
                      ntcoe_budget_COSt            = a_tbl_update(i).ntcoe_budget_COSt    ,
                      LAST_UPDATED_DATE       = a_tbl_update(i).LAST_UPDATED_DATE
                    WHERE SK1_location_NO     = a_tbl_update(i).SK1_location_NO
                    AND FIN_YEAR_NO = a_tbl_update(i).FIN_YEAR_NO
                    AND FIN_WEEK_NO = a_tbl_update(i).FIN_WEEK_NO;


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
                       ' '||a_tbl_update(g_error_index).FIN_YEAR_NO||
                       ' '||a_tbl_update(g_error_index).FIN_WEEK_NO||
                                ' '||a_tbl_update(g_error_index).SK1_LOCATION_NO
;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
 procedure local_write_output as
begin
/*   g_found := FALSE;
   -- Check to see if Business Unit is present on table and update/insert accordingly
   select count(1)
     into g_count
     from DIM_JOB
    where
    JOB_ID = g_rec_out.JOB_ID
	   and JOBGROUP_ID = g_rec_out.JOBGROUP_ID
	   and WORKGROUP_ID = g_rec_out.WORKGROUP_ID
	   and JOB_ID = g_rec_out.JOB_ID;
*/
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
     G_COUNT := 0;
   IF G_REC_IN.RTL_EXISTS IS NOT NULL
   THEN G_COUNT := 1;
   g_found := TRUE;
   END IF;

/*   l_text := 'G_COUNT='||g_COUNT||' -G_REC_IN.RTL_EXISTS='||G_REC_IN.RTL_EXISTS
   ||'-'||g_rec_out.SK1_JOB_ID
   ||'-'||g_rec_out.SK1_location_NO
   ||'-'||g_rec_out.FIN_YEAR_NO
   ||'-'||g_rec_out.FIN_WEEK_NO
                    ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
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
--~~~~~~~~~~`````````````````````****************************````````````````````````~~~~~~~~~~~~~~~~~~~
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_LOC_NTCOE_WK  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


  --  execute immediate 'alter session set workarea_size_policy=manual';
 --   execute immediate 'alter session set sort_area_size=100000000';
 --   execute immediate 'alter session enable parallel dml';

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_HYP_LOC_NTCOE_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_HYP_LOC_NTCOE_WK', DEGREE => 8);



--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_FND_LOCATION;
    fetch c_FND_LOCATION bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 5000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_FND_LOCATION bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_FND_LOCATION;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
       l_text := 'AFTER CURSOR';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   local_bulk_insert;
       l_text := 'AFTER INS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   local_bulk_update;
    l_text := 'Running GATHER_TABLE_STATS ON RTL_LOC_NTCOE_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_NTCOE_WK', DEGREE => 8);

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



END WH_PRF_S4S_023U;
