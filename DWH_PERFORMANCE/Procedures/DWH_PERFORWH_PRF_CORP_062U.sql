--------------------------------------------------------
--  DDL for Procedure DWH_PERFORWH_PRF_CORP_062U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."DWH_PERFORWH_PRF_CORP_062U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Like fo Like location_day fact table in the performance layer
--               with input ex fnd_loc_dy_like_4_like table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_dy_like_4_like
--               Output - rtl_loc_dy
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 July 2009 - defect 2017 - Add field LIKE_FOR_LIKE_ADJ_IND to tables 
--                               FND_RTL_LOC_DY_LIKE_4_LIKE and RTL_LOC_DY
--  14 August 2009 - defect 2252 - Ensure that check for valid location_no is 
--                                 done in FND and not PRF for Like4Like
--  25 September 2018 - A.Ugolini - Ensure that the forward projections are based on the 
--                                  latest values loaded in fnd_rtl_loc_dy_like_4_like
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
g_count              number        :=  0;
g_rec_out            rtl_loc_dy%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_date_ly            date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_max_date           date          := trunc(sysdate);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_062U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LIKE FOR LIKE FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_dy_like_4_like is
   select l4l.*, dl.sk1_location_no, dlh.sk2_location_no
   from   fnd_rtl_loc_dy_like_4_like l4l,
          dim_location dl,
          dim_location_hist dlh   
   where  
          l4l.location_no         = dl.location_no and
          dl.location_no          = dlh.location_no and
          g_date                  between dlh.sk2_active_from_date and dlh.sk2_active_to_date 
   and  l4l.last_updated_date = g_date ;  
   
cursor c_rtl_loc_dy_l4l_distinct is
   select 
        loc.sk1_location_no,
        lfl.calendar_date,
        lfl.LIKE_FOR_LIKE_IND,
        lfl.LIKE_FOR_LIKE_ADJ_IND,
        loc.SUNDAY_STORE_TRADE_IND,
        lfl.LAST_UPDATED_DATE
    from fnd_rtl_loc_dy_like_4_like lfl, dim_location loc
      where 
       lfl.location_no||lfl.calendar_date in (
                 select location_no||max(rtl.calendar_date)
                   from fnd_rtl_loc_dy_like_4_like rtl,
                        dim_calendar cal
                  where rtl.calendar_date = cal.this_week_start_date
                  group by location_no)
   and lfl.location_no = loc.location_no ; 
     
-- Input bulk collect table declared
type stg_array is table of c_fnd_rtl_loc_dy_like_4_like%rowtype;
a_stg_input      stg_array;  


g_rec_in             c_fnd_rtl_loc_dy_like_4_like%rowtype;

l4l_upd              c_rtl_loc_dy_l4l_distinct%rowtype;
--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_rec_out.post_date           := g_rec_in.calendar_date;
   g_rec_out.last_updated_date   := g_date;
   g_rec_out.sk1_location_no     := g_rec_in.sk1_location_no ;
   g_rec_out.sk2_location_no     := g_rec_in.sk2_location_no ;
  
   g_rec_out.like_for_like_ind       := g_rec_in.like_for_like_ind ;
   g_rec_out.like_for_like_adj_ind   := g_rec_in.like_for_like_adj_ind ;


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
       insert into rtl_loc_dy values a_tbl_insert(i);
       
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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update rtl_loc_dy
       set    like_for_like_ind    = a_tbl_update(i).like_for_like_ind,
              like_for_like_adj_ind = a_tbl_update(i).like_for_like_adj_ind,
              last_updated_date    = a_tbl_update(i).last_updated_date
       where  sk1_location_no      = a_tbl_update(i).sk1_location_no  and
 --             sk2_location_no      = a_tbl_update(i).sk2_location_no  and
              post_date            = a_tbl_update(i).post_date;
       
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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).post_date;
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
-- Check to see if item is present on table and update/insert accordingly   
   select count(1)
   into   g_count
   from   rtl_loc_dy
   where  sk1_location_no    = g_rec_out.sk1_location_no  and
          post_date          = g_rec_out.post_date;
  
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
    l_text := 'LOAD OF RTL_LOC_DY EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
-- dwh_lookup.dim_control(g_date);
 g_date := '24/SEP/2018';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_dy_like_4_like;
    fetch c_fnd_rtl_loc_dy_like_4_like bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_loc_dy_like_4_like bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_fnd_rtl_loc_dy_like_4_like;
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

--**************************************************************************************************
----  update existing forward projections 
--**************************************************************************************************

  OPEN c_rtl_loc_dy_l4l_distinct;
   LOOP							/*  Loop  */ 
    FETCH c_rtl_loc_dy_l4l_distinct into l4l_upd;
     EXIT WHEN c_rtl_loc_dy_l4l_distinct%NOTFOUND;
						
  update dwh_performance.rtl_loc_dy
     set like_for_like_ind	    = l4l_upd.like_for_like_ind,
	     like_for_like_adj_ind	= l4l_upd.like_for_like_adj_ind,
         sunday_store_trade_ind = l4l_upd.sunday_store_trade_ind,
         last_updated_date      = l4l_upd.last_updated_date
  WHERE
       sk1_location_no	= l4l_upd.sk1_location_no
   AND post_date 	   >= l4l_upd.calendar_date;

END LOOP;					       /*  end loop  */ 
CLOSE c_rtl_loc_dy_l4l_distinct;
--    
    select max(post_date) 
    into   g_max_date
    from   rtl_loc_dy;  
   
    if g_max_date - g_date < 8 then
       l_text :=  'Inserting records into the future to ensure data present '||g_max_date;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       insert into rtl_loc_dy
       select  SK1_LOCATION_NO,
              (POST_DATE + 1) as Post_date,
               SK2_LOCATION_NO,
               MYSCHOOL_SALES,
               LAYA_SALES,
               UTILITY_SALES,
               LIKE_FOR_LIKE_IND,
               LAST_UPDATED_DATE,
               SUNDAY_STORE_TRADE_IND,
               LIKE_FOR_LIKE_ADJ_IND 
       from    rtl_loc_dy 
       where   post_date = g_max_date;
    end if;
    
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

commit;

end wh_prf_corp_062u;
