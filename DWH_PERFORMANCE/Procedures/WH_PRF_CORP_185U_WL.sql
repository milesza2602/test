--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_185U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_185U_WL" 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
---- FOR PRODLINK DATAFIX 2 AUG 2017 - POSTDATES = 3,10 JULY 2017
--**************************************************************************************************
--  Date:        aUGUST 2017
--  Author:      Wendy Lyttle
--**************************************************************************************************
--  Date:        Sept 2016
--  Author:      Lwazi ntloko
--  Purpose:     Create product link day fact table in the performance layer
--               with added value ex foundation layer.
--*********************************************************************************************************
--  FYI          Eventhough the catalog data from source comes in 1 day ahead of time,
--               all processing is done for post_date = g_date
--               ie. runs one day 'behind' received data
--*********************************************************************************************************
--  Tables:      Input  -   FND_apx_item_link_chn_item_dy
--               Output -   RTL_apx_item_link_chn_item_dy
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
g_read          integer       :=  0;
g_inserted      integer       :=  0;
g_updated      integer       :=  0;


g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            RTL_apx_item_link_chn_item_dy%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_185U_WL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_apx_item_link_chn_item_dy EX FND_apx_item_link_chn_item_dy';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_apx_item_link_chn_item_dy%rowtype index by binary_integer;
type tbl_array_u is table of RTL_apx_item_link_chn_item_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_apx_item_link_chn is
   select 
            DC.SK1_CHAIN_NO
          , DI.SK1_ITEM_NO
          , CASE WHEN FND.CHAIN_CODE = 'DJ' THEN 1 ELSE 0 END SK1_CHAIN_CODE_IND
          , FND.PROD_LINK_REF_NO
          , FND.PROD_LINK_IND
          , FND.CHAIN_NO
          , FND.ITEM_NO
          , FND.POST_DATE
          , di.SIZE_ID
          , FND.CREATE_DATE
          , FND.LINK_START_DATE
          , FND.LINK_EXPIRED_DATE
          , FND.LAST_UPDATED_DATE
          , DI.SK1_ITEM_NO SK1_GROUP_ITEM_NO --- this is the default
   from DWH_FOUNDATION.FND_apx_item_link_chn_item_dy FND,
        DWH_PERFORMANCE.dim_chain DC,
        DWH_PERFORMANCE.dim_item DI
   where FND.chain_no = DC.chain_no
     and FND.item_no = DI.item_no
     and fnd.last_updated_date = g_date
--   for testing  and fnd.last_updated_date = POST_date
 ;
  
-- Input record declared as cursor%rowtype
g_rec_in             c_apx_item_link_chn%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_apx_item_link_chn%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.SK1_CHAIN_CODE_IND         := g_rec_in.SK1_CHAIN_CODE_IND;
   g_rec_out.SK1_CHAIN_NO              	:= g_rec_in.SK1_CHAIN_NO;
   g_rec_out.SK1_ITEM_NO           			:= g_rec_in.SK1_ITEM_NO;
   g_rec_out.SIZE_ID                		:= g_rec_in.SIZE_ID;
   g_rec_out.PROD_LINK_REF_NO          	:= g_rec_in.PROD_LINK_REF_NO;
   g_rec_out.PROD_LINK_IND          		:= g_rec_in.PROD_LINK_IND;
   g_rec_out.POST_DATE              		:= g_rec_in.POST_DATE;
   g_rec_out.CREATE_DATE          			:= g_rec_in.CREATE_DATE;
   g_rec_out.LINK_START_DATE      			:= g_rec_in.LINK_START_DATE;
   g_rec_out.LINK_EXPIRED_DATE    			:= g_rec_in.LINK_EXPIRED_DATE;
   g_rec_out.last_updated_date          := g_date;
-- for testing   g_rec_out.last_updated_date          := g_rec_in.POST_DATE;
   ------

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
      insert into DWH_PERFORMANCE.RTL_APX_ITEM_LINK_CHN_ITEM_DY values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).POST_DATE;
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
      update DWH_PERFORMANCE.RTL_apx_item_link_chn_item_dy
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
              post_date                    = a_tbl_update(i).post_date ;

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
                       ' '||a_tbl_UPDATE(g_error_index).POST_DATE;
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
   from DWH_PERFORMANCE.RTL_apx_item_link_chn_item_dy
   where  SK1_CHAIN_CODE_IND   = g_rec_out.SK1_CHAIN_CODE_IND and
          PROD_LINK_REF_NO     = g_rec_out.PROD_LINK_REF_NO      and
          SK1_CHAIN_NO         = g_rec_out.SK1_CHAIN_NO  and
          SK1_ITEM_NO          = g_rec_out.SK1_ITEM_NO and
          PROD_LINK_IND        = g_rec_out.PROD_LINK_IND AND 
          POST_DATE            = g_rec_out.POST_DATE ;
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
-- Update parent group_item_no on children records
-- This will be used to link a parent and child.
-- The match is done on dim_item.size_id
-- as only 'like' sizes will result in a match.
--**************************************************************************************************

procedure UPDATE_GROUP_ITEM as
begin


    If g_recs_read > 0
        then 
              l_text := 'Truncate TEMP_APX_ITEM_LINK ';
              dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
              execute immediate('Truncate table dwh_performance.TEMP_APX_ITEM_LINK');
          
              INSERT /*+ APPEND */ INTO DWH_performance.TEMP_APX_ITEM_LINK
               WITH SELPAR AS (SELECT *
                              FROM dWH_PERFORMANCE.RTL_apx_item_link_chn_item_dy FND
                              WHERE LAST_UPDATED_DATE = G_DATE
                             AND FND.PROD_LINK_IND = 1
 
                         -- for testing WHERELAST_UPDATED_DATE = POST_DATE - 1
                          -- for testing   AND  FND.PROD_LINK_IND = 1
                              )
               SELECT DISTINCT a.SK1_CHAIN_CODE_IND
                    , a.PROD_LINK_REF_NO
                    , a.PROD_LINK_IND
                    , a.SK1_CHAIN_NO
                    , a.SK1_ITEM_NO
                    , a.POST_DATE
                    , a.SIZE_ID
                    , a.CREATE_DATE
                    , a.LINK_START_DATE
                    , a.LINK_EXPIRED_DATE
                    , a.LAST_UPDATED_DATE
                    , NVL(SP.SK1_ITEM_NO,a.SK1_ITEM_NO) SK1_GROUP_ITEM_NO
               FROM dWH_PERFORMANCE.RTL_apx_item_link_chn_item_dy A, SELPAR SP
               WHERE A.SK1_CHAIN_NO = sp.SK1_CHAIN_NO(+)
                 and    A.PROD_LINK_REF_NO  = sp.PROD_LINK_REF_NO(+)
                 and    A.PROD_LINK_IND = 0
                 and    A.SK1_CHAIN_CODE_IND = sp.SK1_CHAIN_CODE_IND(+)
                 and    A.POST_DATE  = sp.POST_DATE(+) -- CHECK IF POST_DATE SAME WITHIN PROD_LINK_REF_NO
                 and    A.SIZE_ID = SP.SIZE_ID(+)
                 and    a.last_updated_date = g_date
   --for testing              and    a.last_updated_date = A.POST_date
                 ;
                g_read     := sql%rowcount;
                g_inserted := sql%rowcount;
                commit;
                l_text := 'Group_item_no Recs written to TEMP_APX_ITEM_LINK = '||g_inserted;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);      
          
                If g_inserted > 0 then 
                      UPDATE dWH_PERFORMANCE.RTL_apx_item_link_chn_item_dy a
                      SET sk1_group_ITEM_NO = (SELECT distinct B.sk1_GROUP_ITEM_NO
                                               FROM DWH_performance.TEMP_APX_ITEM_LINK b
                                               WHERE (A.SK1_CHAIN_CODE_IND = b.SK1_CHAIN_CODE_IND
                                                       and    A.PROD_LINK_REF_NO  = b.PROD_LINK_REF_NO
                                                       and    A.PROD_LINK_IND = b.PROD_LINK_IND
                                                       -- ITEM_NO NOT REQUIRED HERE IN JOIN
                                                       and    A.sk1_CHAIN_NO = b.sk1_CHAIN_NO
                                                       and    A.POST_DATE  = b.POST_DATE -- CHECK IF POST_DATE SAME WITHIN PROD_LINK_REF_NO
                                                       and    A.SIZE_ID = b.SIZE_ID
                                                       and    b.sk1_group_item_no > 0)
                                               OR (A.SK1_CHAIN_CODE_IND = b.SK1_CHAIN_CODE_IND
                                                     and    A.PROD_LINK_REF_NO  = b.PROD_LINK_REF_NO
                                                     and    A.PROD_LINK_IND = 1
                                                     -- ITEM_NO NOT REQUIRED HERE IN JOIN
                                                     and    A.sk1_CHAIN_NO = b.sk1_CHAIN_NO
                                                     and    A.POST_DATE  = b.POST_DATE -- CHECK IF POST_DATE SAME WITHIN PROD_LINK_REF_NO
                                                     and    b.sk1_group_item_no= A.sk1_ITEM_NO)
                                           )
                           where a.last_updated_date = g_date
---   FOR TESTING                   where a.last_updated_date = POST_date - 1
---   FOR TESTING            where a.last_updated_date = POST_date
                        ;
                      
                      g_read     := sql%rowcount;
                      g_updated  := sql%rowcount;
                
                      commit;
                l_text := 'Group_item_no Recs updated on RTL_apx_item_link_chn_item_dy = '||g_updated;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
                end if;
       else
                l_text := 'NO Group_item_no Recs to be updated on RTL_apx_item_link_chn_item_dy ';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
       end if;
    l_text := '------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end UPDATE_GROUP_ITEM;

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

    l_text := 'LOAD OF RTL_apx_item_link_chn_item_dy EX FND_apx_item_link_chn_item_dy STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    G_DATE := '10 JULY 2017';
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text := '------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    l_text := 'Starting Insert/Update..... ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
 --   g_date := '3 oct 2016';
--    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date||' THRU 30 OCT 2016';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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

    l_text := 'Insert/Update complete';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
--**************************************************************************************************
-- Update parent group_item_no on children records
-- This will be used to link a parent and child.
-- The match is done on dim_item.size_id
-- as only 'like' sizes will result in a match.
--**************************************************************************************************
    l_text := '------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    l_text := 'Starting parent_group_item update..... ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

      UPDATE_GROUP_ITEM;

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
end WH_PRF_CORP_185U_WL;
