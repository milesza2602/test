--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_022U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_022U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Update Hyperion S4S JOB week information for Scheduling for Staff(S4S)
--
--  Tables:      AIT load - STG_S4S_HYP_JOB_WK
--               Input    - STG_S4S_HYP_JOB_WK_CPY
--               Output   - FND_S4S_HYP_LOC_JOB_WK
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
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      DWH_FOUNDATION.STG_S4S_HYP_JOB_WK_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.FND_S4S_HYP_LOC_JOB_WK%rowtype;

g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_022U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE Hyperion S4S JOB week data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  -- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_FOUNDATION.FND_S4S_HYP_LOC_JOB_WK%rowtype index by binary_integer;
type tbl_array_u is table of DWH_FOUNDATION.FND_S4S_HYP_LOC_JOB_WK%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of DWH_FOUNDATION.STG_S4S_HYP_JOB_WK_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of DWH_FOUNDATION.STG_S4S_HYP_JOB_WK_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_STG_S4S_HYP_JOB_WK is
SELECT /*+ full(STG) parallel(STG,6)*/ SYS_SOURCE_BATCH_ID,
          SYS_SOURCE_SEQUENCE_NO,
          SYS_LOAD_DATE,
          SYS_PROCESS_CODE,
          SYS_LOAD_SYSTEM_NAME,
          SYS_MIDDLEWARE_BATCH_ID,
          SYS_PROCESS_MSG,
          STG.SOURCE_DATA_STATUS_CODE,
          STG.LOCATION_NO stg_LOCATION_NO,
          STG.JOB_ID stg_JOB_ID,
          STG.business_date stg_business_date,
          STG.budget_fte stg_budget_fte,
          STG.budget_cost stg_budget_cost,
          fl.LOCATION_NO fl_LOCATION_NO,
          fe.JOB_ID fe_JOB_ID,
          dc.THIS_WEEK_START_DATE dc_business_date,
          dc.fin_year_no dc_fin_year_no,
          dc.fin_week_no dc_fin_week_no
  FROM DWH_FOUNDATION.STG_S4S_HYP_JOB_WK_cpy stg,
        DWH_FOUNDATION.fnd_location fl,
        DWH_FOUNDATION.fnd_s4s_JOB fe,
        DWH_PERFORMANCE.dim_calendar dc
  WHERE stg.location_no        = fl.location_no(+)
  AND stg.JOB_ID = fe.JOB_ID(+)
  AND stg.business_date        = dc.this_week_start_date(+)
 -- and stg.location_no  in (3091,123)
 -- and stg.business_date between '1 april 2014' and '29 april 2014'
  GROUP BY  SYS_SOURCE_BATCH_ID,
            SYS_SOURCE_SEQUENCE_NO,
            SYS_LOAD_DATE,
            SYS_PROCESS_CODE,
            SYS_LOAD_SYSTEM_NAME,
            SYS_MIDDLEWARE_BATCH_ID,
            SYS_PROCESS_MSG,
            STG.SOURCE_DATA_STATUS_CODE,
            STG.LOCATION_NO ,
            STG.JOB_ID ,
            STG.business_date ,
            STG.budget_fte ,
            STG.budget_cost ,
            fl.LOCATION_NO ,
            fe.JOB_ID ,
            dc.THIS_WEEK_START_DATE ,
            dc.fin_year_no ,
            dc.fin_week_no
 
ORDER BY sys_source_batch_id,
  sys_source_sequence_no;




g_rec_in                   c_STG_S4S_HYP_JOB_WK%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_S4S_HYP_JOB_WK%rowtype;
a_stg_input      stg_array;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.LOCATION_NO :=  g_rec_in.STG_LOCATION_NO;
   g_rec_out.JOB_ID :=  g_rec_in.STG_JOB_ID;
   g_rec_out.fin_year_no :=  g_rec_in.dc_fin_year_no;
   g_rec_out.fin_week_no :=  g_rec_in.dc_fin_week_no;
   g_rec_out.business_date :=  g_rec_in.STG_business_date;
   g_rec_out.budget_fte :=  g_rec_in.STG_budget_fte;
   g_rec_out.budget_cost :=  g_rec_in.STG_budget_cost;


  g_rec_out.last_updated_date               := g_date;


   if G_REC_IN.fl_LOCATION_NO IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'LOCATION_NO NOT FOUND';
     return;
   end if;
   if G_REC_IN.fe_JOB_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'JOB_ID NOT FOUND';
     return;
   end if;
   if G_REC_IN.dc_business_date IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'BUSINESS_DATE NOT FOUND';
     return;
   end if;
   if G_REC_IN.dc_FIN_YEAR_NO IS NULL OR G_REC_IN.dc_FIN_WEEK_NO IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'FIN_YEAR AND FIN_WEEK NOT FOUND';
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

      insert into dwh_foundation.STG_S4S_HYP_JOB_WK_hsp values
   ( g_rec_in.SYS_SOURCE_BATCH_ID
     ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
     ,g_rec_in.SYS_LOAD_DATE
     ,g_rec_in.SYS_PROCESS_CODE
     ,g_rec_in.SYS_LOAD_SYSTEM_NAME
     ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
     ,g_rec_in.SYS_PROCESS_MSG
     ,g_rec_in.SOURCE_DATA_STATUS_CODE
     ,g_rec_in.STG_LOCATION_NO
     ,g_rec_in.STG_JOB_ID
     ,g_rec_in.STG_business_date
     ,g_rec_in.STG_budget_fte
     ,g_rec_in.STG_budget_cost

    );


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
       insert into DWH_FOUNDATION.FND_S4S_HYP_LOC_JOB_WK  values a_tbl_insert(i);

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
                      ' '||a_tbl_insert(g_error_index).LOCATION_NO||
                      ' '||a_tbl_insert(g_error_index).JOB_ID||
' '||a_tbl_insert(g_error_index).fin_year_no||' '||a_tbl_insert(g_error_index).fin_WEEK_no||
' '||' INS';

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
      UPDATE DWH_FOUNDATION.FND_S4S_HYP_LOC_JOB_WK
          SET budget_fte      = a_tbl_update(i).budget_fte,
                budget_cost       = a_tbl_update(i).budget_cost,
                business_date     = a_tbl_update(i).business_date,
                LAST_UPDATED_DATE = a_tbl_update(i).LAST_UPDATED_DATE
          WHERE LOCATION_NO   = a_tbl_update(i).LOCATION_NO
          AND JOB_ID          = a_tbl_update(i).JOB_ID
          AND fin_year_no     = a_tbl_update(i).fin_year_no
          AND fin_week_no     = a_tbl_update(i).fin_week_no
;

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
' '||a_tbl_update(g_error_index).LOCATION_NO||
' '||a_tbl_update(g_error_index).JOB_ID||
' '||a_tbl_update(g_error_index).business_date||
' '||' UPD';

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

v_count integer := 0;

begin

   g_found := false;

   select count(1)
      into   v_count
   from   DWH_FOUNDATION.FND_S4S_HYP_LOC_JOB_WK
   where  LOCATION_NO =  g_rec_out.LOCATION_NO
      AND JOB_ID =  g_rec_out.JOB_ID
             AND business_date = g_rec_out.business_date;



   if v_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).LOCATION_NO = g_rec_out.location_no
      AND a_tbl_insert(i).JOB_ID =  g_rec_out.JOB_ID
             AND a_tbl_insert(i).business_date = g_rec_out.business_date
           then
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
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD THE HYP_JOB_WK data ex S4S STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_S4S_HYP_JOB_WK;
    fetch c_STG_S4S_HYP_JOB_WK bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
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
    fetch c_STG_S4S_HYP_JOB_WK bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_S4S_HYP_JOB_WK;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    l_text := 'end of cursor';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    local_bulk_insert;
        l_text := 'end of insert';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    local_bulk_update;

    l_text := 'end of last bulk';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_HYP_LOC_JOB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_HYP_LOC_JOB_WK', DEGREE => 8);

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


END WH_FND_S4S_022U;
