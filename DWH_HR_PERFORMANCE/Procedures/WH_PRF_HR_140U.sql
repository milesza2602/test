--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_140U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_140U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        June 2014
--  Author:      Kgomotso Lehabe
--  Purpose:     Create dim_hr_poi dimension table in the performance layer
--  Tables:      Input  - fnd_hr_poi
--               Output - dim_hr_poi
--  Packages:    dwh_hr_constants, dwh_log, dwh_hr_valid
--
--  Maintenance:
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  10000;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_hr_poi%rowtype;
g_rec_in             DWH_HR_FOUNDATION.fnd_hr_poi%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_140U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_HR_poi EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of DWH_HR_FOUNDATION.fnd_hr_poi%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_hr_poi%rowtype index by binary_integer;
type tbl_array_u is table of dim_hr_poi%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_hr_poi is
   select *
   from DWH_HR_FOUNDATION.fnd_hr_poi;

-- No where clause used as we need to refresh all records for better continuity. Volumes are very small so no impact


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.poi_id	                     :=  g_rec_in.poi_id;
   g_rec_out.id_no 	                     :=  g_rec_in.id_no;
   g_rec_out.first_name	  	             :=  g_rec_in.first_name;
   g_rec_out.last_name 	                 :=  g_rec_in.last_name;
   g_rec_out.gender_code	 	             :=  g_rec_in.gender_code;
   g_rec_out.race_code	                 :=  g_rec_in.race_code;
   g_rec_out.citizenship_status_code     :=  g_rec_in.citizenship_status_code;
   g_rec_out.disabled_ind    	           :=  g_rec_in.disabled_ind;
   g_rec_out.level_3_code	   	           :=  nvl(g_rec_in.level_3_code,'OTHER BU');
   g_rec_out.company_code      	         :=  g_rec_in.company_code;
   g_rec_out.poi_type      	             :=  g_rec_in.poi_type;
   g_rec_out.poi_status                  :=  g_rec_in.poi_status;
   g_rec_out.effective_date 	           :=  g_rec_in.effective_date;
   g_rec_out.last_updated_date           :=  g_date;

---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------

   g_rec_out.poi_long_desc  := g_rec_in.poi_id||' - ' ||g_rec_in.first_name||' '||g_rec_in.last_name;

   begin
     select sk1_poi_type
     into   g_rec_out.sk1_poi_type
     from   dim_hr_poi_type
     where  poi_type          = g_rec_out.poi_type ;

     exception
         when no_data_found then
              g_rec_out.sk1_poi_type := 0;
   end;

      begin
     select sk1_gender_code
     into   g_rec_out.sk1_gender_code
     from   dim_hr_gender
     where  gender_code          = g_rec_out.gender_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_gender_code := 0;
   end;

    begin
     select sk1_race_code
     into   g_rec_out.sk1_race_code
     from   dim_hr_race
     where  race_code          = g_rec_out.race_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_race_code := 0;
   end;

    begin
     select sk1_citizenship_status_code
     into   g_rec_out.sk1_citizenship_status_code
     from   dim_hr_citizenship_status
     where  citizenship_status_code          = g_rec_out.citizenship_status_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_citizenship_status_code := 0;
   end;

   begin
      select bu.sk1_bee_business_unit_code
     into   g_rec_out.sk1_bee_business_unit_code
     from   fnd_hr_ps_bee_bu_map psbu,
            dim_hr_bee_bu bu
     where  bu.bee_business_unit_code =  psbu.bee_business_unit_code
     and    psbu.company_code  = g_rec_out.company_code
     and ps_business_unit_code =  g_rec_out.level_3_code ;

         exception
            when no_data_found then
              g_rec_out.sk1_bee_business_unit_code := 176326;
   end;


   begin
     select sk1_company_code
     into   g_rec_out.sk1_company_code
     from   dim_hr_company
     where  company_code          = g_rec_out.company_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_company_code := 0;
   end;

    exception
      when others then
       l_message := dwh_hr_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into dim_hr_poi values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).poi_id;
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
      update dim_hr_poi
      set   sk1_poi_type                    = a_tbl_update(i).sk1_poi_type,
            sk1_gender_code                 = a_tbl_update(i).sk1_gender_code,
            sk1_race_code                   = a_tbl_update(i).sk1_race_code,
            sk1_citizenship_status_code     = a_tbl_update(i).sk1_citizenship_status_code,
            sk1_bee_business_unit_code      = a_tbl_update(i).sk1_bee_business_unit_code,
            sk1_company_code                = a_tbl_update(i).sk1_company_code,
           id_no 	                          = a_tbl_update(i).id_no,
           first_name	  	                  = a_tbl_update(i).first_name,
           last_name 	                      = a_tbl_update(i).last_name,
           poi_long_desc                    = a_tbl_update(i).poi_long_desc,
           gender_code	 	                  = a_tbl_update(i).gender_code,
           race_code	                      = a_tbl_update(i).race_code,
           citizenship_status_code          = a_tbl_update(i).citizenship_status_code,
           disabled_ind    	                = a_tbl_update(i).disabled_ind,
           level_3_code	   	                = a_tbl_update(i).level_3_code,
           company_code      	              = a_tbl_update(i).company_code,
           poi_type      	                  = a_tbl_update(i).poi_type,
           poi_status                       = a_tbl_update(i).poi_status,
           effective_date 	                = a_tbl_update(i).effective_date,
           last_updated_date                = a_tbl_update(i).last_updated_date
         where  poi_id                      = a_tbl_update(i).poi_id  ;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).poi_id;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_hr_valid.dim_hr_poi(g_rec_out.poi_id);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_poi_id  := hr_seq.nextval;
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
       l_message := dwh_hr_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dim_hr_poi EX fnd_hr_poi STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_hr_poi;
    fetch c_fnd_hr_poi bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_hr_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_hr_poi bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_hr_poi;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_hr_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_HR_140U;
