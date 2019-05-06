--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_186U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_186U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sept 2016
--  Author:      Lwazi ntloko
--  Purpose:     Create product link day fact table in the performance layer
--               with added value ex foundation layer.
--  Tables:      Input  -   RTL_apx_item_link_chn_item_dy
--               Output -   RTL_apx_item_link_chn_item_wk
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:    Single DML could be considered for this program.
--
--  Maintenance:
--  
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            RTL_apx_item_link_chn_item_wk%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_186U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_RTL_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_RTL_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_RTL_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_apx_item_link_chn_item_wk EX RTL_apx_item_link_chn_item_dy';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_apx_item_link_chn_item_wk%rowtype index by binary_integer;
type tbl_array_u is table of RTL_apx_item_link_chn_item_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_apx_item_link_chn is
   select SK1_CHAIN_CODE_IND
          ,PROD_LINK_REF_NO
          ,max(PROD_LINK_IND) PROD_LINK_IND
          ,SK1_CHAIN_NO
          ,SK1_ITEM_NO
          ,fin_year_no
          , fin_week_no
          ,SIZE_ID
          ,max(CREATE_DATE) create_date
          ,max(LINK_START_DATE) link_start_date
          ,max(LINK_EXPIRED_DATE) link_expired_date
          ,max(SK1_GROUP_ITEM_NO) SK1_GROUP_ITEM_NO
          ,THIS_WEEK_END_DATE
    from DWH_PERFORMANCE.RTL_apx_item_link_chn_item_dy RTL,
        DWH_PERFORMANCE.DIM_calendar dc
   where RTL.post_date = dc.calendar_date
   and RTL.last_updated_date = g_date
   GROUP BY SK1_CHAIN_CODE_IND
          ,PROD_LINK_REF_NO
          ,SK1_CHAIN_NO
          ,SK1_ITEM_NO
          ,fin_year_no
          , fin_week_no
          ,SIZE_ID
          ,THIS_WEEK_END_DATE
 ;
  
-- Input record declared as cursor%rowtype
g_rec_in             c_apx_item_link_chn%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_apx_item_link_chn%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
--    NB. in PRD
--     Post_date  Last_updated_date  
--     10/OCT/16	09/OCT/16	 
--     11/OCT/16	10/OCT/16	 
--   hence always make sure that LUD = post_date - 1
--   as important for Merge uncatalogued children into catalog table
--**************************************************************************************************
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.SK1_CHAIN_CODE_IND         := g_rec_in.SK1_CHAIN_CODE_IND;
   g_rec_out.SK1_CHAIN_NO              	:= g_rec_in.SK1_CHAIN_NO;
   g_rec_out.SK1_ITEM_NO           			:= g_rec_in.SK1_ITEM_NO;
   g_rec_out.SIZE_ID                		:= g_rec_in.SIZE_ID;
   g_rec_out.PROD_LINK_REF_NO          	:= g_rec_in.PROD_LINK_REF_NO;
   g_rec_out.PROD_LINK_IND          		:= g_rec_in.PROD_LINK_IND;
   g_rec_out.FIN_YEAR_NO             		:= g_rec_in.FIN_YEAR_NO;
   g_rec_out.FIN_WEEK_NO             		:= g_rec_in.FIN_WEEK_NO;
   g_rec_out.CREATE_DATE          			:= g_rec_in.CREATE_DATE;
   g_rec_out.LINK_START_DATE      			:= g_rec_in.LINK_START_DATE;
   g_rec_out.LINK_EXPIRED_DATE    			:= g_rec_in.LINK_EXPIRED_DATE;
    g_rec_out.last_updated_date         := g_date;
   g_rec_out.last_updated_date          := g_rec_in.THIS_WEEK_END_DATE;
   g_rec_out.SK1_GROUP_ITEM_NO     			:= g_rec_in.SK1_GROUP_ITEM_NO;

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
      insert into DWH_PERFORMANCE.RTL_apx_item_link_chn_item_wk values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).SK1_CHAIN_CODE_IND||
                       ' '||a_tbl_insert(g_error_index).PROD_LINK_REF_NO||
                       ' '||a_tbl_insert(g_error_index).PROD_LINK_IND||
                       ' '||a_tbl_insert(g_error_index).SK1_CHAIN_NO||
                       ' '||a_tbl_insert(g_error_index).SK1_ITEM_NO||
                       ' '||a_tbl_insert(g_error_index).FIN_YEAR_NO||
                       ' '||a_tbl_insert(g_error_index).FIN_WEEK_NO;
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
      update DWH_PERFORMANCE.RTL_apx_item_link_chn_item_wk
       set    CREATE_DATE                  = a_tbl_update(i).CREATE_DATE ,
              LINK_START_DATE              = a_tbl_update(i).LINK_START_DATE,
              LINK_EXPIRED_DATE            = a_tbl_update(i).LINK_EXPIRED_DATE,
              last_updated_date            = a_tbl_update(i).last_updated_date,
              SIZE_ID                      = a_tbl_update(i).SIZE_ID,
              SK1_GROUP_ITEM_NO            = a_tbl_update(i).SK1_GROUP_ITEM_NO
       where  SK1_CHAIN_CODE_IND           = a_tbl_update(i).SK1_CHAIN_CODE_IND and
              PROD_LINK_REF_NO             = a_tbl_update(i).PROD_LINK_REF_NO     and
              PROD_LINK_IND                = a_tbl_update(i).PROD_LINK_IND    and
              SK1_CHAIN_NO                 = a_tbl_update(i).SK1_CHAIN_NO and
              SK1_ITEM_NO                  = a_tbl_update(i).SK1_ITEM_NO and 
              FIN_YEAR_NO                  = a_tbl_update(i).FIN_YEAR_NO AND
              FIN_WEEK_NO                  = a_tbl_update(i).FIN_WEEK_NO ;
              
      g_recs_updated := g_recs_updated + a_tbl_update.count;

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
                       ' '||a_tbl_UPDATE(g_error_index).SK1_CHAIN_CODE_IND||
                       ' '||a_tbl_UPDATE(g_error_index).PROD_LINK_REF_NO||
                       ' '||a_tbl_UPDATE(g_error_index).PROD_LINK_IND||
                       ' '||a_tbl_UPDATE(g_error_index).SK1_CHAIN_NO||
                       ' '||a_tbl_UPDATE(g_error_index).SK1_ITEM_NO||
                       ' '||a_tbl_UPDATE(g_error_index).FIN_YEAR_NO||
                       ' '||a_tbl_UPDATE(g_error_index).FIN_WEEK_NO;
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
   into g_count
   from DWH_PERFORMANCE.RTL_apx_item_link_chn_item_wk
   where  SK1_CHAIN_CODE_IND   = g_rec_out.SK1_CHAIN_CODE_IND and
          PROD_LINK_REF_NO     = g_rec_out.PROD_LINK_REF_NO      and
          SK1_CHAIN_NO         = g_rec_out.SK1_CHAIN_NO  and
          SK1_ITEM_NO          = g_rec_out.SK1_ITEM_NO and
          PROD_LINK_IND        = g_rec_out.PROD_LINK_IND AND 
          fin_year_no            = g_rec_out.fin_year_no and 
          fin_week_no            = g_rec_out.fin_week_no;
   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
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
-- Main process loop
--**************************************************************************************************
begin

    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_apx_item_link_chn_item_wk EX RTL_apx_item_link_chn_item_dy STARTED AT '||
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
  
   -- g_date := '3 oct 2016';
   -- l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date||' THRU 30 OCT 2016';
   -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_apx_item_link_chn;
    fetch c_apx_item_link_chn bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_apx_item_link_chn bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_apx_item_link_chn;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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
end WH_PRF_CORP_186U;
