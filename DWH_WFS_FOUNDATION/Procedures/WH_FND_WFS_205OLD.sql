--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_205OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_205OLD" (p_forall_limit in integer,p_success out boolean) AS 


--**************************************************************************************************
--  Date:        MARCH 2013
--  Author:      Alastair de Wet
--  Purpose:     Create PRODUCT dimention table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_cust_collect_cpy
--               Output - fnd_wfs_cust_perf_60dy
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   
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

g_hospital_text      stg_vsn_cust_collect_hsp.sys_process_msg%type;
g_rec_out            fnd_wfs_cust_perf_60dy%rowtype;
g_rec_in             stg_vsn_cust_collect_cpy%rowtype;
g_found              boolean;
g_count              number        :=  0;
g05_day              integer       := TO_CHAR(current_date,'DD');

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%TYPE;
L_MODULE_NAME        SYS_DWH_ERRLOG.LOG_PROCEDURE_NAME%TYPE    := 'WH_FND_WFS_205U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_wfs_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_wfs_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_wfs_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUST_PERF_60DY EX VISION COLLECT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_vsn_cust_collect_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_wfs_cust_perf_60dy%rowtype index by binary_integer;
type tbl_array_u is table of fnd_wfs_cust_perf_60dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_vsn_cust_collect_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_vsn_cust_collect_cpy.sys_source_sequence_no%type 
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_vsn_cust_collect is
   select /*+ FULL(cpy)  parallel (cpy,4) */  *
   from stg_vsn_cust_collect_cpy cpy
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data
   
--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin


   g_rec_out.run_date                      := trunc(sysdate);
   g_rec_out.product_code_no               := g_rec_in.product_code_no;
   g_rec_out.wfs_customer_no               := g_rec_in.wfs_customer_no;
   g_rec_out.wfs_account_no                := g_rec_in.wfs_account_no;
   g_rec_out.collector_id                  := g_rec_in.wfs_rep_id;
   g_rec_out.cta_class                     := g_rec_in.cta_class;
   g_rec_out.pending_chgoff_ind            := g_rec_in.pending_chgoff_ind;   
   g_rec_out.last_updated_date             := g_date;

g_found := TRUE; 
begin  
   select account_status, 
          block_code_1, 
          billing_cycle, 
          cycle_due,
          chgoff_date,
          delinquency_cycle
   into   g_rec_out.account_status, 
          g_rec_out.block_code_1, 
          g_rec_out.billing_cycle, 
          g_rec_out.cycle_due,
          g_rec_out.chgoff_date,
          g_rec_out.delinquency_cycle
   from   fnd_wfs_cust_perf_60dy
   where  run_date         = g_rec_out.run_date and
          product_code_no  = g_rec_out.product_code_no and
          wfs_customer_no  = g_rec_out.wfs_customer_no and
          wfs_account_no   = g_rec_out.wfs_account_no ;
   exception
            when no_data_found then
              g_found := FALSE;
end; 

IF g_found THEN
  case
   when g_rec_out.pending_chgoff_ind = 'Y' then
        g_rec_out.pend_chgoff_code :='P';
     if g_rec_out.block_code_1  = 'S' then
        g_rec_out.pend_chgoff_code :='S';
     end if;
     if g_rec_out.block_code_1 ='A' or g_rec_out.block_code_1 ='I' or g_rec_out.block_code_1 ='D' or g_rec_out.block_code_1 ='J' then
         g_rec_out.pend_chgoff_code :='A';
     end if;
   else
       g_rec_out.pend_chgoff_code  := '0';
  end case;

   if g_rec_out.pend_chgoff_code  = 'S' or g_rec_out.pend_chgoff_code ='P' or g_rec_out.pend_chgoff_code ='A' then
      if g05_day >= 10 and g05_day <= 16 and g_rec_out.billing_cycle < g05_day then
            g_rec_out.delinquency_cycle  := 'N';
      else
            g_rec_out.delinquency_cycle  := g_rec_out.delinquency_cycle;
      end if;
   else
       g_rec_out.delinquency_cycle  := g_rec_out.delinquency_cycle;
   end if;
        
END IF;

   exception
      when others then
       l_message  := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
       UPDATE fnd_wfs_cust_perf_60dy
       SET    cta_class                       = a_tbl_update(i).cta_class,
              collector_id                    = a_tbl_update(i).collector_id,
              pending_chgoff_ind              = a_tbl_update(i).pending_chgoff_ind,
              pend_chgoff_code                = a_tbl_update(i).pend_chgoff_code,
              delinquency_cycle               = a_tbl_update(i).delinquency_cycle,
              last_updated_date               = a_tbl_update(i).last_updated_date
       WHERE  run_date                        = a_tbl_update(i).run_date AND
              wfs_customer_no                 = a_tbl_update(i).wfs_customer_no AND
              wfs_account_no                  = a_tbl_update(i).wfs_account_no and
              product_code_no                 = a_tbl_update(i).product_code_no;
       
       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
               
   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-SQL%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).wfs_customer_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);  
       end loop;   
       raise;        
end local_bulk_update;


--************************************************************************************************** 
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
 
BEGIN
   -- Place data into and array for later writing to table in bulk   
IF g_found THEN   
   a_count_u               := a_count_u + 1;
   a_tbl_update(a_count_u) := g_rec_out;
   a_count := a_count + 1;
END IF;   
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   IF a_count > g_forall_limit THEN   
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
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
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
    g_forall_limit := 5000;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF cust_perf_60dy EX VISION COLLECT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date||' day'||G05_DAY;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_vsn_cust_collect;
    fetch c_stg_vsn_cust_collect bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;
   
         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
--         if g_hospital = 'Y' then
--            local_write_hospital;
--         else
            local_write_output;
--         end if;
      end loop;
    fetch c_stg_vsn_cust_collect bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_stg_vsn_cust_collect;
--**************************************************************************************************  
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_update;    
    
--************************************************************************************************** 
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');       
       rollback;
       p_success := false;
       raise;

END WH_FND_WFS_205OLD;
