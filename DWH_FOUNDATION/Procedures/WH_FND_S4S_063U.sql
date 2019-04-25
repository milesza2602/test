--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_063U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_063U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        December 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Update actual exception employee day information for Scheduling for Staff(S4S)
--
--  Tables:      AIT load - STG_S4S_ACTL_XCPTN_EMP_DY 
--               Input    - STG_S4S_ACTL_XCPTN_EMP_DY_cpy
--               Output   - FND_S4S_ACTL_XCPTN_EMP_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  w lyttle 24 feb 2015 - SRS from Teddy
--                        a.	S4S SP: usp_Stg_Outbound_Exception_Schedule_Emp 
--                        b.	Trans Type:  SCRAEE
--                        c.	BI Interface: 
--                        d.	BI Logic:
--                              i.	Take-On: Clear all records and load as per payload received from source
--                            ii.	No deletes are required at the moment
--                            iii.	Insert data based on primary key and update if the record already exists


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
g_hospital_text      DWH_FOUNDATION.STG_S4S_ACTL_XCPTN_EMP_DY_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY%rowtype;

g_SYS_SOURCE_BATCH_ID number;
g_max                number;

g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         		 number        :=  0;
g_recs_deleted       integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_063U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE actual exception employee day  data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  -- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_S4S_ACTL_XCPTN_EMP_DY%rowtype index by binary_integer;
type tbl_array_u is table of FND_S4S_ACTL_XCPTN_EMP_DY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_S4S_ACTL_XCPTN_EMP_DY_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_S4S_ACTL_XCPTN_EMP_DY_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_STG_S4S_EMP_JOB is
   select SYS_SOURCE_BATCH_ID,
          SYS_SOURCE_SEQUENCE_NO,
          SYS_LOAD_DATE,
          SYS_PROCESS_CODE,
          SYS_LOAD_SYSTEM_NAME,
          SYS_MIDDLEWARE_BATCH_ID,
          SYS_PROCESS_MSG,
          STG.SOURCE_DATA_STATUS_CODE,
          STG.EMPLOYEE_ID stg_EMPLOYEE_ID,
          STG.EXCEPTION_TYPE_ID STG_EXCEPTION_TYPE_ID,
          STG.EXCEPTION_DATE stg_EXCEPTION_DATE,
          STG.EXCEPTION_START_TIME stg_EXCEPTION_START_TIME,
          STG.EXCEPTION_END_TIME stg_EXCEPTION_END_TIME,
          STG.JOB_ID STG_JOB_ID,
          STG.LOCATION_NO STG_LOCATION_NO,
          DE.EMPLOYEE_ID DE_EMPLOYEE_ID,
          DC2.CALENDAR_DATE DC2_EXCEPTION_DATE,
       --   DC2.THIS_WEEK_START_DATE DC2_WK_START,
       --   DC2.THIS_WEEK_END_DATE DC2_WK_END,
          EI.EXCEPTION_TYPE_ID EI_EXCEPTION_TYPE_ID,
          DJ.JOB_ID DJ_JOB_ID,
          DL.LOCATION_NO DL_LOCATION_NO
   from DWH_FOUNDATION.STG_S4S_ACTL_XCPTN_EMP_DY_cpy stg,
        DWH_HR_PERFORMANCE.DIM_EMPLOYEE DE,
        dim_calendar dc2,
        FND_S4S_EXCEPTION_TYPE EI,
        FND_LOCATION DL,
        FND_S4S_JOB DJ
   where stg.EMPLOYEE_ID = DE.EMPLOYEE_ID(+)
     and stg.EXCEPTION_DATE = dc2.calendar_date(+)
     and stg.EXCEPTION_TYPE_ID  = EI.EXCEPTION_TYPE_ID(+)
     and stg.JOB_ID  = DJ.JOB_ID(+)
     and stg.LOCATION_NO  = DL.LOCATION_NO(+)
	 --  and DC2.THIS_WEEK_START_DATE >= (select DC2.THIS_WEEK_START_DATE
   --                                   FROM dim_calendar dc2
    --                                  WHERE DC2.CALENDAR_DATE = trunc(sysdate)-7)
   order by sys_source_batch_id,sys_source_sequence_no;


g_rec_in                   c_STG_S4S_EMP_JOB%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_S4S_EMP_JOB%rowtype;
a_stg_input      stg_array;
/*
------------------------------------------------------------------------------------------------------------------------
 --DELETE PROC-   Added on requirement to delete records outside of the reporting dates | see e-mail by kim on 18 December 2014
 -----------------------------------------------------------------------------------------------------------------------
procedure delete_fnd as
begin
      g_recs_inserted := 0;

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4SACTL_XCPTN_EMPDELETE;
      
      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      
	  g_run_date := trunc(sysdate);

     BEGIN
	    insert /*+ append  into dwh_foundation.FND_S4SACTL_XCPTN_EMPDELETE
			select f.*, g_run_date, g_date, g_run_seq_no 
			from DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY f
			where f.EXCEPTION_TYPE_ID NOT IN (905, 910, 911, 912)
			AND f.EXCEPTION_DATE NOT BETWEEN (select THIS_WEEK_START_DATE from dim_calendar where Calendar_date = trunc(sysdate)-7)
                                and (select THIS_WEEK_END_DATE from dim_calendar where Calendar_date = trunc(sysdate));
          g_recs :=SQL%ROWCOUNT;
          COMMIT;
          g_recs_inserted := g_recs;
		  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
		  l_text := 'Insert into FND_S4SACTL_XCPTN_EMPDELETE recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

         delete from DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY
         where (EMPLOYEE_ID, LOCATION_NO, EXCEPTION_TYPE_ID, EXCEPTION_DATE, JOB_ID) in (select distinct EMPLOYEE_ID, LOCATION_NO, EXCEPTION_TYPE_ID, EXCEPTION_DATE, JOB_ID from dwh_foundation.FND_S4SACTL_XCPTN_EMPDELETE
         where run_seq_no = g_run_seq_no);
     
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_deleted := g_recs;
                l_text := 'Deleted from DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
			dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      
      exception
         when no_data_found then
                l_text := 'No deletions done for DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   end;          

   g_recs_inserted  :=0;
   g_recs_deleted := 0;
   
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_fnd;
*/
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

    g_hospital                     := 'N';
    g_rec_out.EMPLOYEE_ID          := g_rec_in.stg_EMPLOYEE_ID;
    g_rec_out.LOCATION_NO          := g_rec_in.stg_LOCATION_NO;
    g_rec_out.JOB_ID               := g_rec_in.stg_JOB_ID;
    g_rec_out.EXCEPTION_DATE       := g_rec_in.STG_EXCEPTION_DATE;
    g_rec_out.EXCEPTION_START_TIME := g_rec_in.STG_EXCEPTION_START_TIME;
    g_rec_out.EXCEPTION_END_TIME   := g_rec_in.STG_EXCEPTION_END_TIME;
    g_rec_out.EXCEPTION_TYPE_ID         := g_rec_in.STG_EXCEPTION_TYPE_ID;

   g_rec_out.last_updated_date               := g_date;

 --   l_text := ' G_REC_IN.fl_EMPLOYEE_ID:- '||G_REC_IN.fl_EMPLOYEE_ID;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if G_REC_IN.DE_EMPLOYEE_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'EMPLOYEE_ID NOT FOUND';
     return;
   end if;
   
      if G_REC_IN.DL_LOCATION_NO IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'LOCATION_NO NOT FOUND';
     return;
   end if;

   if G_REC_IN.DJ_JOB_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'JOB_ID NOT FOUND';
     return;
   end if;
      if G_REC_IN.STG_EXCEPTION_END_TIME IS NOT NULL
      AND  G_REC_IN.STG_EXCEPTION_END_TIME < G_REC_IN.STG_EXCEPTION_START_TIME
      then
     g_hospital      := 'Y';
     g_hospital_text := 'EXCEPTION_END_TIME < EXCEPTION_START_TIME';
     return;
   end if;
       if G_REC_IN.DC2_EXCEPTION_DATE IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'EXCEPTION_DATE NOT FOUND';
     return;
   end if;
       if G_REC_IN.EI_EXCEPTION_TYPE_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'EXCEPTION_TYPE_ID NOT FOUND';
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

      insert into dwh_foundation.STG_S4S_ACTL_XCPTN_EMP_DY_hsp values
   ( g_rec_in.SYS_SOURCE_BATCH_ID
     ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
     ,g_rec_in.SYS_LOAD_DATE
     ,g_rec_in.SYS_PROCESS_CODE
     ,g_rec_in.SYS_LOAD_SYSTEM_NAME
     ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
     ,g_rec_in.SYS_PROCESS_MSG
     ,g_rec_in.SOURCE_DATA_STATUS_CODE
     ,g_rec_in.stg_EMPLOYEE_ID
     ,g_rec_in.STG_EXCEPTION_TYPE_ID
     ,g_rec_in.STG_EXCEPTION_DATE
     ,g_rec_in.STG_LOCATION_NO
     ,g_rec_in.STG_JOB_ID
     ,g_rec_in.STG_EXCEPTION_START_TIME
     ,g_rec_in.STG_EXCEPTION_END_TIME
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
       insert into FND_S4S_ACTL_XCPTN_EMP_DY  values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i
                       || ' '||g_error_index
                       || ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)
                       || ' '||a_tbl_insert(g_error_index).EMPLOYEE_ID
                       || ' '||a_tbl_insert(g_error_index).jOB_ID
                       || ' '||a_tbl_insert(g_error_index).LOCATION_NO
                       || ' '||a_tbl_insert(g_error_index).EXCEPTION_TYPE_ID
                       || ' '||a_tbl_insert(g_error_index).EXCEPTION_DATE;

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
             UPDATE FND_S4S_ACTL_XCPTN_EMP_DY
                     SET  
                        
                          EXCEPTION_START_TIME = a_tbl_update(i).EXCEPTION_START_TIME,
                          EXCEPTION_END_TIME       = a_tbl_update(i).EXCEPTION_END_TIME,
                          LAST_UPDATED_DATE        = a_tbl_update(i).LAST_UPDATED_DATE 
              WHERE EMPLOYEE_ID = a_tbl_update(i).EMPLOYEE_ID 
              AND EXCEPTION_DATE = a_tbl_update(i).EXCEPTION_DATE 
              AND EXCEPTION_TYPE_ID = a_tbl_update(i).EXCEPTION_TYPE_ID
              and jOB_ID = a_tbl_update(i).JOB_ID
              and LOCATION_NO = a_tbl_update(i).LOCATION_NO;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
      FOR i IN 1 .. g_error_count
      LOOP
        g_error_index := sql%bulk_exceptions(i).error_index;
        l_message     := dwh_constants.vc_err_lb_loop||i
        ||' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)
        || ' '||a_tbl_update(g_error_index).EMPLOYEE_ID
        || ' '||a_tbl_update(g_error_index).jOB_ID
        || ' '||a_tbl_update(g_error_index).LOCATION_NO
        || ' '||a_tbl_update(g_error_index).EXCEPTION_TYPE_ID
        || ' '||a_tbl_update(g_error_index).EXCEPTION_DATE;
      
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
   from   DWH_FOUNDATION.FND_S4S_ACTL_XCPTN_EMP_DY
   where  EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID
      AND EXCEPTION_DATE =  g_rec_out.EXCEPTION_DATE
      AND EXCEPTION_TYPE_ID =  g_rec_out.EXCEPTION_TYPE_ID
      and jOB_ID =  g_rec_out.JOB_ID
     and LOCATION_NO =  g_rec_out.LOCATION_NO;


   if v_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if   a_tbl_insert(i).EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID
           AND a_tbl_insert(i).EXCEPTION_DATE =  g_rec_out.EXCEPTION_DATE
           AND a_tbl_insert(i).EXCEPTION_TYPE_ID =  g_rec_out.EXCEPTION_TYPE_ID
           AND a_tbl_insert(i).jOB_ID =  g_rec_out.jOB_ID
           AND a_tbl_insert(i).LOCATION_NO =  g_rec_out.LOCATION_NO
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

    l_text := 'LOAD THE actual exception employee day  data ex S4S STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    -- hardcoding batch_date for testing
 --   g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     select min(sys_source_batch_id), max(sys_source_batch_id) into g_SYS_SOURCE_BATCH_ID, g_max from DWH_FOUNDATION.STG_S4S_ACTL_XCPTN_EMP_DY_cpy;

   l_text := 'batch='||g_SYS_SOURCE_BATCH_ID||'-'||g_max;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   
--**************************************************************************************************
-- Delete records
--**************************************************************************************************
--delete_fnd;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_S4S_EMP_JOB;
    fetch c_STG_S4S_EMP_JOB bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_STG_S4S_EMP_JOB bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_S4S_EMP_JOB;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
    local_bulk_update;
--**************************************************************************************************

    local_bulk_insert;

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_ACTL_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_ACTL_XCPTN_EMP_DY', DEGREE => 8);
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


END WH_FND_S4S_063U;
