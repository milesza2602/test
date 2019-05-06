--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_014E_SS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_014E_SS" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        10 July 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Load LabourRole heirachy data to DIM_LABOUR_ROLE from STG Heirachy'.
--
--  DIM Tables:  Input  - FND_S4S_LABOUR_ROLE_SS
--               Output - RTL_LABOUR_ROLE_SS

--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  ************* - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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

g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;

g_rec_out            RTL_LABOUR_ROLE_SS%rowtype;

g_found              boolean;
g_insert_rec         boolean;
g_invalid_plan_type_no boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_014E_SS';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_pln_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD LABOUR_ROLE dimenstion Data';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_LABOUR_ROLE_SS%rowtype index by binary_integer;
type tbl_array_u is table of RTL_LABOUR_ROLE_SS%rowtype index by binary_integer;

a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_JOBGROUP is
    with
    workgroup as (
    select sk1_workgroup_id,workgroup_id,sk1_s4s_business_unit_no, count(*)
     from (Select Distinct
                  dmj.sk1_workgroup_id,
                  fnd.workgroup_id,
                  dmj.sk1_s4s_business_unit_no,
                  fnd.s4s_business_unit_no,
                  fnd.effective_from_date
             from FND_S4S_LABOUR_ROLE_SS fnd,
                  rtl_job_ss dmj
            where fnd.workgroup_id = dmj.workgroup_id
              and fnd.s4s_business_unit_no = dmj.s4s_business_unit_no 
              and fnd.s4s_business_unit_name = dmj.s4s_business_unit_name
            order by workgroup_id,s4s_business_unit_no,effective_from_date
          )
    group by sk1_workgroup_id,workgroup_id,sk1_s4s_business_unit_no
--    having count(*) > 1
    ),
    jobgroup as (
    select sk1_jobgroup_id,jobgroup_id, count(*)
     from (Select Distinct
                  dmj.sk1_jobgroup_id,
                  fnd.jobgroup_id,
                  dmj.sk1_workgroup_id,
                  fnd.workgroup_id,
                  fnd.effective_from_date
             from FND_S4S_LABOUR_ROLE_SS fnd,
                  rtl_job_ss dmj
            where fnd.jobgroup_id = dmj.jobgroup_id
              and fnd.workgroup_id = dmj.workgroup_id 
            order by jobgroup_id,workgroup_id,fnd.effective_from_date
          )
    group by sk1_jobgroup_id,jobgroup_id
--    having count(*) > 1
    ),
    jobid as (
    select sk1_job_id,job_id, effective_from_date,count(*)
     from (Select Distinct
                  dmj.sk1_job_id,
                  fnd.job_id,
                  dmj.sk1_jobgroup_id,
                  fnd.jobgroup_id,
                  fnd.effective_from_date
             from FND_S4S_LABOUR_ROLE_SS fnd,
                  rtl_job_ss dmj
            where fnd.job_id = dmj.job_id
              and fnd.jobgroup_id = dmj.jobgroup_id
              and fnd.effective_from_date = dmj.sk1_effective_from_date
            order by fnd.job_id,fnd.jobgroup_id,fnd.effective_from_date
          )
    group by sk1_job_id,job_id,effective_from_date
--    having count(*) > 1
    )
    SELECT
        fnd.labour_role_id,
        fnd.labour_role_name,
        null labour_role_seq,
        jd.sk1_job_id,
        fnd.job_id,
        fnd.job_name,
        jg.sk1_jobgroup_id,
        fnd.jobgroup_id,
        fnd.jobgroup_name,
        wg.sk1_workgroup_id,
        fnd.workgroup_id,
        fnd.workgroup_name,    
        wg.sk1_s4s_business_unit_no,
        fnd.s4s_business_unit_no,
        fnd.s4s_business_unit_name,
        fnd.effective_from_date,
        fnd.effective_to_date
    FROM FND_S4S_LABOUR_ROLE_SS fnd
         ,workgroup wg
         ,jobgroup jg
         ,jobid jd
    WHERE fnd.workgroup_id = wg.workgroup_id(+)
      and fnd.jobgroup_id = jg.jobgroup_id(+)
      and fnd.job_id = jd.job_id(+)   
      and jd.sk1_job_id is not null
      and fnd.effective_from_date = jd.effective_from_date
--      and fnd.effective_to_date >= '30/DEC/3999'
--      and fnd.effective_from_date = '01/OCT/2018'
    ;

g_rec_in    c_JOBGROUP%rowtype;

-- For input bulk collect --
type stg_array is table of c_JOBGROUP%rowtype;
a_stg_input      stg_array;

---------  SK1_WORKGROUP_ID, SK1_S4S_BUSINESS_UNIT_NO

-----------------------------------------------------------------------------------------------------
-- order by only where sequencing is essential to the correct loading of data
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

      g_rec_out.LABOUR_ROLE_ID           := g_rec_in.LABOUR_ROLE_ID;
      g_rec_out.LABOUR_ROLE_NAME         := g_rec_in.LABOUR_ROLE_NAME ;
      g_rec_out.LABOUR_ROLE_SEQ          := g_rec_in.LABOUR_ROLE_SEQ;
      g_rec_out.SK1_JOB_ID               := g_rec_in.SK1_JOB_ID;
      g_rec_out.JOB_ID                   := g_rec_in.JOB_ID;
      g_rec_out.JOB_NAME                 := g_rec_in.JOB_NAME;
      g_rec_out.SK1_JOBGROUP_ID          := g_rec_in.SK1_JOBGROUP_ID;
      g_rec_out.JOBGROUP_ID              := g_rec_in.JOBGROUP_ID;
      g_rec_out.JOBGROUP_NAME            := g_rec_in.JOBGROUP_NAME;
      g_rec_out.SK1_WORKGROUP_ID         := g_rec_in.SK1_WORKGROUP_ID;
      g_rec_out.WORKGROUP_ID             := g_rec_in.WORKGROUP_ID;
      g_rec_out.WORKGROUP_NAME           := g_rec_in.WORKGROUP_NAME;
      g_rec_out.SK1_S4S_BUSINESS_UNIT_NO := g_rec_in.SK1_S4S_BUSINESS_UNIT_NO;
      g_rec_out.S4S_BUSINESS_UNIT_NO     := g_rec_in.S4S_BUSINESS_UNIT_NO;
      g_rec_out.S4S_BUSINESS_UNIT_NAME   := g_rec_in.S4S_BUSINESS_UNIT_NAME;
      g_rec_out.SK1_EFFECTIVE_FROM_DATE  := g_rec_in.EFFECTIVE_FROM_DATE;
      g_rec_out.SK1_EFFECTIVE_TO_DATE    := g_rec_in.EFFECTIVE_TO_DATE;
      g_rec_out.TOTAL                    := 'TOTAL';
      g_rec_out.TOTAL_DESC               := 'ALL LABOUR_ROLE ID';
      g_rec_out.last_updated_date        := g_date;



  exception
    when others then
     l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
     dwh_log.record_error(l_module_name,sqlcode,l_message);
dbms_output.put_line('Message XY'||l_message);
    raise;

end local_address_variables;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into RTL_LABOUR_ROLE_SS values a_tbl_insert(i);
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
                       ' '||a_tbl_INSERT(g_error_index).LABOUR_ROLE_ID||
                       ' '||a_tbl_insert(g_error_index).JOB_ID||
                       ' '||a_tbl_insert(g_error_index).JOBGROUP_ID||
                       ' '||a_tbl_insert(g_error_index).WORKGROUP_ID||
                       ' '||a_tbl_insert(g_error_index).S4S_BUSINESS_UNIT_NO||
                       ' '||a_tbl_insert(g_error_index).SK1_EFFECTIVE_FROM_DATE||
                       ' '||'INS';
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;

dbms_output.put_line('Message XY1 - Insert '||l_message); 
       
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       UPDATE RTL_LABOUR_ROLE_SS
          SET LABOUR_ROLE_NAME         = a_tbl_update(i).LABOUR_ROLE_NAME,
              LABOUR_ROLE_SEQ          = a_tbl_update(i).LABOUR_ROLE_SEQ,
              SK1_JOB_ID               = a_tbl_update(i).SK1_JOB_ID,
           --   JOB_ID                   = a_tbl_update(i).JOB_ID,
              JOB_NAME                 = a_tbl_update(i).JOB_NAME,
              SK1_JOBGROUP_ID          = a_tbl_update(i).SK1_JOBGROUP_ID,
           --   JOBGROUP_ID              = a_tbl_update(i).JOBGROUP_ID,
              JOBGROUP_NAME            = a_tbl_update(i).JOBGROUP_NAME,
              SK1_WORKGROUP_ID         = a_tbl_update(i).SK1_WORKGROUP_ID,
           --   WORKGROUP_ID             = a_tbl_update(i).WORKGROUP_ID,
              WORKGROUP_NAME           = a_tbl_update(i).WORKGROUP_NAME,
              SK1_S4S_BUSINESS_UNIT_NO = a_tbl_update(i).SK1_S4S_BUSINESS_UNIT_NO,
           --   S4S_BUSINESS_UNIT_NO     = a_tbl_update(i).S4S_BUSINESS_UNIT_NO,
              S4S_BUSINESS_UNIT_NAME   = a_tbl_update(i).S4S_BUSINESS_UNIT_NAME,
--              EFFECTIVE_FROM_DATE      = a_tbl_update(i).EFFECTIVE_FROM_DATE,
              SK1_EFFECTIVE_TO_DATE    = a_tbl_update(i).SK1_EFFECTIVE_TO_DATE,
              TOTAL                    = a_tbl_update(i).TOTAL,
              TOTAL_DESC               = a_tbl_update(i).TOTAL_DESC,
              last_updated_date        = a_tbl_update(i).LAST_UPDATED_DATE
              
        WHERE LABOUR_ROLE_ID           = a_tbl_update(i).LABOUR_ROLE_ID
         AND JOB_ID                   = a_tbl_update(i).JOB_ID
         and JOBGROUP_ID               = a_tbl_update(i).JOBGROUP_ID
         and WORKGROUP_ID              = a_tbl_update(i).WORKGROUP_ID
         and S4S_BUSINESS_UNIT_NO      = a_tbl_update(i).S4S_BUSINESS_UNIT_NO
         and SK1_EFFECTIVE_FROM_DATE   = a_tbl_update(i).SK1_EFFECTIVE_FROM_DATE;

--dbms_output.put_line('Message XY1 - Update '||l_message); 

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
                       ' '||a_tbl_update(g_error_index).LABOUR_ROLE_ID||
                       ' '||a_tbl_update(g_error_index).JOB_ID||
                       ' '||a_tbl_update(g_error_index).JOBGROUP_ID||        
                       ' '||a_tbl_update(g_error_index).WORKGROUP_ID||
                       ' '||a_tbl_UPDATE(g_error_index).S4S_BUSINESS_UNIT_NO||
                       ' '||a_tbl_UPDATE(g_error_index).SK1_EFFECTIVE_FROM_DATE||
                       ' '||'UPD';
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
   -- Check to see if Business Unit is present on table and update/insert accordingly
   select count(1)
     into g_count
     from RTL_LABOUR_ROLE_SS
    where LABOUR_ROLE_ID        = g_rec_out.LABOUR_ROLE_ID
	  and JOB_ID                = g_rec_out.JOB_ID
	  and JOBGROUP_ID           = g_rec_out.JOBGROUP_ID
	  and WORKGROUP_ID          = g_rec_out.WORKGROUP_ID
	  and S4S_BUSINESS_UNIT_NO  = g_rec_out.S4S_BUSINESS_UNIT_NO
      and sk1_effective_from_date = g_rec_out.sk1_effective_from_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

--dbms_output.put_line(g_count||':'||g_rec_out.JOB_NAME||':'||g_rec_out.LABOUR_ROLE_NAME);

-- Check if insert of Business Unit number is already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).LABOUR_ROLE_ID          = g_rec_out.LABOUR_ROLE_ID
         and a_tbl_insert(i).JOB_ID                  = g_rec_out.JOB_ID
         and a_tbl_insert(i).JOBGROUP_ID             = g_rec_out.JOBGROUP_ID
         and a_tbl_insert(i).WORKGROUP_ID            = g_rec_out.WORKGROUP_ID
         and a_tbl_insert(i).S4S_BUSINESS_UNIT_NO    = g_rec_out.S4S_BUSINESS_UNIT_NO 
         and a_tbl_insert(i).sk1_effective_from_date = g_rec_out.sk1_effective_from_date
            then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
	  g_rec_out.SK1_LABOUR_ROLE_ID := LABOUR_HIERACHY_SEQ.nextval;
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
  if p_forall_limit is not null and p_forall_limit > 1000  then ------dwh_constants.vc_forall_minimum
       g_forall_limit := p_forall_limit;
    end if;

	dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOADING PRF LABOUR_ROLE data'|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_JOBGROUP ;
    fetch c_JOBGROUP bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 1000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

	   local_address_variables;
       local_write_output;

      end loop;
    fetch c_JOBGROUP bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_JOBGROUP ;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
       
end WH_PRF_S4S_014E_SS;
