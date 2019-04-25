--------------------------------------------------------
--  DDL for Procedure WH_FND_AST_042U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_AST_042U" 
(p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
--  Date:        September 2016
--  Author:      Lwazi Ntloko
--  Purpose:     Insert Uncatalogued Linked items into  C&H catalog fact table 
--                and update Catalogued linked items.
-----------------------------------------------------
--Due to the fact that  fnd_ast_loc_item_dy_catlg  will contain only catalogued items 
--and therefore the linked-children which are marked as uncatalogued in source systems will not be coming through
--but we still need them in BI as catalogued,
--means that we have to generate and insert these records into  fnd_ast_loc_item_dy_catlg 
--using a combination of fnd_apx_item_link_chn_item_dy(parent and children data)  and fnd_ast_loc_item_dy_catlg (parent data)
-------
--Match fnd_apx_item_link_chn_item_dy to fnd_ast_loc_item_dy_catlg 
--on item_no, post_date, chain_no (join fnd_ast_loc_item_dy_catlg .location_no to dim_location.location_no to get chain_no)
--where prod_link_ind = 1 to get the parents.
--Use the parent records to pick-up the children (where prod_linl_ind = 0) to get the children columns(item_no, post_date, chain_no )
--from fnd_apx_item_link_chn_item_dy 
--and then the parent values from fnd_ast_loc_item_dy_catlg  columns(location_no, active_from_date, active_to_date, source_data_status_code) 
-- to generate and then insert the missing children into  fnd_ast_loc_item_dy_catlg .
------------------------------------------------------
--  FYI          Eventhough the catalog data from source comes in 1 day ahead of time,
--               all processing is done for post_date = g_date
--               ie. runs one day 'behind' received data
------------------------------------------------------
--
--  Tables:      Input  - fnd_ast_loc_item_dy_catlg, fnd_apx_itm_link_chn_item_dy
--               Output - fnd_ast_loc_item_dy_catlg
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  oct/nov 2016 - wendy - changes for child inserts - broke up cursor as taking too long
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
g_recs_merged        integer       :=  0;
g_recs_INSERTED        integer       :=  0;
g_date               date;
g_loop_date               date;
g_date1               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_sub                integer       :=  0;

   g_date_min_2       date := trunc(sysdate);
   g_date_add_1       date := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_AST_042U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ITEM LINK CATALOG DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Merge uncatalogued children into catalog table.
--
-- Eventhough the business spec stated that 'child' items would not be catalogued
--             and hence not be sent to BI via the catalogue interface
--                and thus we had to insert the child items into the FNd catalogue table
--                      using the parent catalogue data,
--      for the initial implementation, it was decided that they would not uncatalogue the child items
-- This means that we should only be updating the FND catalogue table for now.
-- It was decided that we would still run this procedure as we still need the update part
--  for further prod-link processing.
-- We are leaving the insert part as this will be used at a later date once the business process
--  for uncataloguing child items is implemented at source.
--**************************************************************************************************

--**************************************************************************************************
-- insert parent recs
--**************************************************************************************************

procedure UPDATE_parentS as
begin

    merge /*+ APPEND parallel (ast,2) */ into dwh_foundation.fnd_ast_loc_item_dy_catlg ast
    using
        (        
               WITH SELITEM AS (SELECT /*+ materialize full(b) parallel(b,8) */  ITEM_NO, post_date, chain_no , PROD_LINK_IND
                              FROM DWH_FOUNDATION.fnd_apx_item_link_chn_item_dy b 
                              WHERE PROD_LINK_IND = 1
                                AND post_date = g_date --between g_date_min_2 and g_date_add_1
                            --  and post_date between g_date_min_2 and g_date_add_1
                            )
              SELECT /*+ full(a)  parallel(a,8) full(c) */  DISTINCT A.LOCATION_NO
              ,A.ITEM_NO
              ,A.POST_DATE
              ,A.ACTIVE_FROM_DATE
              ,A.ACTIVE_TO_DATE
              ,A.SOURCE_DATA_STATUS_CODE
              ,A.LAST_UPDATED_DATE
              ,'CL' PROD_LINK_TYPE
              ,A.ITEM_NO GROUP_ITEM_NO
              , 2 RECAT_IND
              , b.PROD_LINK_IND
              FROM dwh_foundation.fnd_ast_loc_item_dy_catlg A , SELITEM B, dim_location c
              WHERE A.POST_DATE = G_DATE
              and a.item_no = b.item_no
              and a.post_date = B.post_date
              and a.location_no = c.location_no
              and B.chain_no = c.chain_no
) g3
                   ON (AST.POST_DATE = G3.POST_DATE 
                   and AST.location_NO = G3.location_NO
                   and AST.ITEM_NO = G3.ITEM_NO
                   )
         when matched then
                 update set 
                            last_updated_date = g_date, 
                            PROD_LINK_TYPE = g3.Prod_link_type, 
                            group_item_no  = g3.GROUP_ITEM_NO,
                            recat_ind = g3.recat_ind,
                            PROD_LINK_IND = g3.PROD_LINK_IND
                       ;
            

           g_recs_inserted :=  0;
           g_recs_inserted :=  SQL%ROWCOUNT;
      
      
           l_text := 'Update PARENTS:- RECS =  '||g_recs_inserted;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end UPDATE_parents;

--*************************************************************************************************
--  select parent records
--*************************************************************************************************

procedure select_parent as
begin

   insert /*+ APPEND parallel (ast,4) */ into dwh_foundation.temp_ast_parent ast
  with
                           selparent as  (select /*+ materialize FULL(DI) FULL(C) FULL(APX) parallel (aPX,8) */ distinct 
                                      APX.SK1_CHAIN_CODE_IND
                                    , APX.PROD_LINK_REF_NO
                                    , APX.PROD_LINK_IND
                                    , APX.SK1_CHAIN_NO
                                    , APX.SK1_ITEM_NO  
                                    , APX.POST_DATE 
                                    , APX.CREATE_DATE
                                    , APX.LINK_START_DATE
                                    , APX.LINK_EXPIRED_DATE
                                    , APX.LAST_UPDATED_DATE
                                    , APX.SK1_GROUP_ITEM_NO
                                    , sk1_location_no 
                                    , item_no group_item_no
                                    , location_no
                                    , chain_no
                              from dwh_performance.rtl_apx_item_link_chn_item_dy APX, dim_item di, dim_location c
                               where aPX.sk1_item_no = di.sk1_item_no 
                               and aPX.sk1_chain_no = c.sk1_chain_no
                               and apx.prod_link_ind = 1
                                AND post_date = g_date --between g_date_min_2 and g_date_add_1
                                order by POST_DATE,ITEM_NO,LOCATION_NO
 --   for testing              and apx.last_updated_date = g_date
                            )
                                select /*+  full(cat) parallel (CAT,4) */ distinct 
                                       cat.LOCATION_NO
                                      , cat.ITEM_NO
                                      , cat.POST_DATE
                                      , cat.ACTIVE_FROM_DATE
                                      , cat.ACTIVE_TO_DATE
                                      , cat.SOURCE_DATA_STATUS_CODE
                                      , cat.LAST_UPDATED_DATE
                                      , sp.group_item_no
                                      , cat.RECAT_IND
                                      , cat.PROD_LINK_IND
                                      , cat.PROD_LINK_TYPE
                                      , sp.sk1_chain_no
                                      , sp.sk1_location_no
                                      , sp.sk1_item_no
                                      , sp.SK1_GROUP_ITEM_NO
                                from dwh_foundation.fnd_ast_loc_item_dy_catlg cat, selparent sp
                                 where sp.GROUP_ITEM_NO = cat.item_no
                                 and sp.location_no = cat.location_no
                                 and sp.post_date = cat.post_date
                       --          order by sp.sk1_item_no, cat.post_date, sp.sk1_location_no
     ; 
                    
      g_recs_read := sql%rowcount;
      g_recs_merged := sql%rowcount;
      
      commit;
                 l_text := 'temp_ast_parent - recs  = '||g_recs_read||' - '||g_date;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end select_parent;

--*************************************************************************************************
--  select child records
--*************************************************************************************************
procedure select_child as
begin

   insert /*+ APPEND parallel (ast,4) */ into dwh_foundation.temp_ast_child ast
   with
                selchild as (select /*+ materialize FULL(DI) FULL(C) FULL(APX) parallel (aPX,4) */ distinct 
                                      APX.SK1_CHAIN_CODE_IND
                                    , APX.PROD_LINK_REF_NO
                                    , APX.PROD_LINK_IND
                                    , APX.SK1_CHAIN_NO
                                    , APX.SK1_ITEM_NO  
                                    , APX.POST_DATE 
                                    , APX.CREATE_DATE
                                    , APX.LINK_START_DATE
                                    , APX.LINK_EXPIRED_DATE
                                    , APX.LAST_UPDATED_DATE
                                    , APX.SK1_GROUP_ITEM_NO
                                    , sk1_location_no 
                                    , item_no group_item_no
                                    , location_no
                                    , chain_no
                                    , item_no item_no
                              from dwh_performance.rtl_apx_item_link_chn_item_dy APX, dim_item di, dim_location c
                               where aPX.sk1_item_no = di.sk1_item_no 
                               and aPX.sk1_chain_no = c.sk1_chain_no
                               and apx.prod_link_ind = 0
                                AND post_date = g_date) --between g_date_min_2 and g_date_add_1)
          select  SK1_CHAIN_CODE_IND	
                  , PROD_LINK_REF_NO
                  , PROD_LINK_IND	
                  , SK1_CHAIN_NO	
                  , SK1_ITEM_NO	
                  , POST_DATE 
                  , CREATE_DATE 
                  , LINK_START_DATE 
                  , LINK_EXPIRED_DATE 
                  , LAST_UPDATED_DATE 
                  , SK1_GROUP_ITEM_NO
                  , SK1_LOCATION_NO	
                  , group_item_no 
                  , location_no 
                  , CHAIN_NO	
                  , item_no 
          from  selchild apx
          where apx.sk1_item_no <> apx.sk1_group_item_no
            and apx.prod_link_ind = 0   
     --     order by apx.sk1_group_item_no, apx.post_date, apx.sk1_location_no
         ;

                                 
      g_recs_read := sql%rowcount;
      g_recs_merged := sql%rowcount;
      
      commit;
                 l_text := 'temp_ast_child - recs  = '||g_recs_read||' - '||g_date;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end select_child;

--*************************************************************************************************
--  generate child records
--*************************************************************************************************
procedure merge_ins_upd as
begin

   merge /*+ APPEND parallel (ast,4) */ into dwh_foundation.fnd_ast_loc_item_dy_catlg ast
    using
                    (      select  /*+ full(scp) parallel(scp,4) full(apx) parallel(apx,4) */
                                            SCP.LOCATION_NO
                                        ,apx.ITEM_NO
                                        ,SCP.POST_DATE
                                        ,SCP.ACTIVE_FROM_DATE
                                        ,SCP.ACTIVE_TO_DATE
                                        ,SCP.SOURCE_DATA_STATUS_CODE
                                        ,'UL' PROD_LINK_TYPE
                                        ,SCP.GROUP_ITEM_NO
                                        ,1 RECAT_IND
                                        , apx.prod_link_ind
                                  from dwh_foundation.temp_ast_parent scp, dwh_foundation.temp_ast_child apx
                                  where scp.sk1_item_no = apx.sk1_group_item_no
                                  and scp.post_date = apx.post_date
                                  and scp.sk1_location_no = apx.sk1_location_no
) g3
                   ON (AST.POST_DATE = G3.POST_DATE 
                   and AST.location_NO = G3.location_NO
                   and AST.ITEM_NO = G3.ITEM_NO)
         when matched then
                 update set 
                            last_updated_date = g_date, 
                            PROD_LINK_TYPE = g3.Prod_link_type, 
                            group_item_no  = g3.GROUP_ITEM_NO,
                            recat_ind = g3.RECAT_IND,
                            PROD_LINK_IND = g3.PROD_LINK_IND
         when not matched then
                 insert (LOCATION_NO
                       , ITEM_NO
                       , POST_DATE
                       , ACTIVE_FROM_DATE
                       , ACTIVE_TO_DATE
                       , SOURCE_DATA_STATUS_CODE
                       , LAST_UPDATED_DATE
                       , PROD_LINK_TYPE
                       , GROUP_ITEM_NO
                       , recat_ind
                       , PROD_LINK_IND)
                 values (G3.LOCATION_NO
                       , G3.ITEM_NO
                       , G3.POST_DATE
                       , G3.ACTIVE_FROM_DATE
                       , G3.ACTIVE_TO_DATE
                       , G3.SOURCE_DATA_STATUS_CODE
                       , G_date
                       , G3.PROD_LINK_TYPE
                       , G3.GROUP_ITEM_NO
                       , G3.recat_ind
                       , g3.PROD_LINK_IND)
                                 ;

                                 
      g_recs_read := sql%rowcount;
      g_recs_merged := sql%rowcount;
      
      commit;
                 l_text := 'recs merged = '||g_recs_read||' - '||g_date;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end merge_ins_upd;


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
   l_text := 'LOAD OF fnd_ast_loc_item_dy_catlg STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   --g_date := '14 nov 2016';
   l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --  g_date_min_2 := g_date-2;
 --  g_date_add_1 := g_date + 1;
   
    execute immediate 'alter session enable parallel dml';

 --   l_text := 'update stats - fnd_AST_LOC_ITEM_DY_CATLG';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_AST_LOC_ITEM_DY_CATLG', DEGREE => 32);
 
 ----------------------------
     
     l_text := 'update parents.......';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    update_parents;
----------------------------

    l_text := 'truncate table temp_ast_parent';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     execute immediate 'truncate table dwh_foundation.temp_ast_parent';
      
     l_text := 'select parents.......';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    select_parent;

----------------------------
    l_text := 'truncate table temp_ast_child';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     execute immediate 'truncate table dwh_foundation.temp_ast_child';
     
    l_text := 'select children.......';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
   
    select_child;

---------------------------- 
    l_text := 'generate children.......';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    merge_ins_upd;

 

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_merged,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  'RECORDS MERGED '||g_recs_merged;
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

end WH_FND_AST_042U;
