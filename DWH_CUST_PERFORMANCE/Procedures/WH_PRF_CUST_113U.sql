--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_113U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_113U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2017
--  Author:      A DE WET
--  Purpose:     Update dim_customer_master  with additional information
--
--  Tables:      Input  - cust_basket_tender,
--                        cust_interaction,
--                        apex_ap3_param_1
--               Output - dim_customer_master
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--               Theo Filander  08/Oct/2018
--               Added the functionality to cater for multiple records being returned
--               from dim_customer_mapping - Order by SOURCE_KEY and LAST_UPDATED_DATE desc 
--
--  Naming conventions
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
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_date               date          := trunc(sysdate);
g_stmt               varchar(500);

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_113U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE ADDITIONAL CUSTOMER FIELDS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

procedure reset_ucount_cust_type as
begin


  update /*+ PARALLEL(dim,4)*/ dwh_cust_performance.dim_customer_master dim
  set ucount_cust_type = ''
  where ucount_cust_type in ('UCount Customer','UCount Redeemed');
  commit;
exception
  when others then
    l_message := 'RESET UCOUNT CUSTOMER TYPE - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end reset_ucount_cust_type;

--***************************************************************************************************************
procedure update_ucount_cust_type as
begin
  l_text := 'UPDATE UCOUNT_CUST_TYPE TO UCOUNT CUSTOMER ON DWH_CUST_PERFORMANCE.dim_customer_master STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  update /*+ PARALLEL(dim,8)*/ dwh_cust_performance.dim_customer_master dim
  set ucount_cust_type = 'UCount Potential'
  where dim.master_subscriber_key in 
                          (select /*+ Parallel(8) */
                                  subscriber_key 
                             from (
                                   select /*+ Parallel(cbt,8) Parallel(mp,8) Full(cbt) */
                                          mp.subscriber_key,
                                          rank() over (partition by mp.source_key, mp.last_updated_date order by mp.last_updated_date desc) mp_seq
                                   from  dwh_cust_performance.cust_basket_tender cbt
                                   inner join dwh_cust_performance.dim_customer_mapping mp on (cbt.customer_no = mp.source_key)
                                   where cbt.customer_no = mp.source_key 
                                     and mp.source = 'C2'
                                     and cbt.tran_date between add_months(g_date,-6) and g_date
                                     and cbt.customer_no is not null
                                     and floor(cbt.tender_no/10000000000) in (222126,445120,445121,445122,454858,489061,489074,503615,503615,503615,510432,510433,519612,519613,522100,
                                                                              522126,522134,522159,522167,522175,522191,522250,522262,523982,523983,526440,532657,533822,535949,552057,
                                                                              552065,559200)
                                   ) 
                            where mp_seq = 1
                          ) 
    and   nvl(ucount_cust_type,'X') <> 'UCount Potential';
    
  g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;
  l_text := 'UPDATED '|| g_recs_updated ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;

  g_recs_updated:= 0;

  l_text := 'UPDATE UCOUNT_CUST_TYPE TO UCOUNT REDEEMED ON DWH_CUST_PERFORMANCE.dim_customer_master STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 
   update dwh_cust_performance.dim_customer_master dc
   set ucount_cust_type = 'UCount Redeemed'
   where dc.master_subscriber_key in 
                        (
                         select /*+ Parallel(cbt,8) Full(cbt) Parallel(apx,8) Full(apx) Parallel(mp,8)  */
                                         distinct mp.subscriber_key
                                   from  dwh_cust_performance.cust_basket_tender cbt  
                                   inner join 
                                         (
                                          select /*+ Parallel(cm,8) */
                                                 subscriber_key,
                                                 source_key,
                                                 rank() over (partition by cm.source_key, cm.last_updated_date order by cm.last_updated_date desc) mp_seq
                                            from dwh_cust_performance.dim_customer_mapping cm
                                           where cm.source = 'C2'
                                         ) mp on (cbt.customer_no = mp.source_key) 
                                   inner join
                                        (
                                         select /*+ Parallel(aa,8) Full(aa) */
                                                aa.start_measurement_date,
                                                aa.end_measurement_date,
                                                ROW_NUMBER() OVER (ORDER BY aa.end_measurement_date desc ) row_id
                                           from apex_app_cust_01.apex_ap3_param_1 aa
                                          where substr(aa.campaign_description,1,2) = 'UC'and  
                                                aa.end_measurement_date <=trunc(sysdate)
                                          order by aa.end_measurement_date desc
                                         ) apx
                                         on cbt.tran_date between apx.start_measurement_date and apx.end_measurement_date
                                  where floor(cbt.tender_no/10000000000) in (533823)
                                    and cbt.customer_no is not null
                                    and apx.row_id <=2
                                    and mp_seq = 1
                         );

--     and ucount_cust_type = 'UCount Customer'; Removed 23/May/17 As per Louis P
  g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;
  l_text := 'UPDATED '|| g_recs_updated ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     

  commit;
  g_recs_updated:= 0;
  
exception
  when others then
    l_message := 'UPDATE UCOUNT CUSTOMER TYPE - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end update_ucount_cust_type;
--******************************************************************************************************************************
procedure update_wfs_app_reg_ind as
begin
  update /*+ PARALLEL(dim,4)*/ dwh_cust_performance.dim_customer_master dim
     set wfs_app_reg_ind = 1
   where dim.master_subscriber_key  in 
                        (select /*+ Parallel(cust,8) Parallel(mp,8) Full(cust)*/
                                mp.subscriber_key
                           from dwh_cust_performance.cust_interaction cust
                           inner join     
                                 (
                                  select /*+ Parallel(cm,8) */
                                         subscriber_key,
                                         source_key,
                                         rank() over (partition by cm.source_key, cm.last_updated_date order by cm.last_updated_date desc) mp_seq
                                    from dwh_cust_performance.dim_customer_mapping cm
                                   where cm.source = 'C2'
                                 ) mp on (cust.account_contact_id = mp.source_key )
                          where inquiry_type_desc in ('CUSTAUTH-RAMP-APP','CUSTAUTH-RAMP-USSD','CUSTAUTH-RAMP-WAP')
                          and   mp_seq = 1
--                          and   logged_date > g_date - 30
                          )
     and nvl(wfs_app_reg_ind,0) <> 1; --limit how many records get updated
     
  g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;
  l_text := 'UPDATED '|| g_recs_updated ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
   
  commit;
exception
  when others then
    l_message := 'UPDATE WFS APPLICATION REGISTRATION INDICATOR - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end update_wfs_app_reg_ind;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
  execute immediate 'alter session enable parallel dml';

  l_text := dwh_constants.vc_log_draw_line;
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
-- Call the bulk routines
--**************************************************************************************************

  l_text := 'RESET UNCOUNT CUSTOMER TYPE STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  reset_ucount_cust_type;
  


  l_text := 'UPDATE UNCOUNT CUSTOMER TYPE STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  update_ucount_cust_type;

  l_text := 'UPDATE WFS APPLICATION REGISTRATION INDICATOR STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  update_wfs_app_reg_ind;

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
  l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
  l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
end WH_PRF_CUST_113U;
