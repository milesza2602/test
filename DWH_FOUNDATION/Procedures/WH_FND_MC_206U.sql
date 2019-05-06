--------------------------------------------------------
--  DDL for Procedure WH_FND_MC_206U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MC_206U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2017
--  Author:      Bhavesh Valodia
--  Purpose:     Update STG_RMS_MC_STOCK_CH dimention table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_mc_stock_CH_cpy
--               Output - fnd_mc_rtl_loc_item_dy_rms_stk_ch
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  
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
g_recs_duplicate     integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      dwh_foundation.stg_rms_mc_stock_ch_hsp.sys_process_msg%type;
mer_mart             dwh_foundation.stg_rms_mc_stock_ch_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MC_206U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STOCK_CH MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no     stg_rms_mc_stock_ch_cpy.location_no%type; 
g_item_no           stg_rms_mc_stock_ch_cpy.item_no%type; 
g_post_date           stg_rms_mc_stock_ch_cpy.post_date%type;


   cursor stg_dup is
      select * 
        from dwh_foundation.stg_rms_mc_stock_ch_cpy
       where (location_no, item_no, post_date)
          in
     (select location_no, item_no, post_date
        from stg_rms_mc_stock_ch_cpy 
    group by location_no, item_no, post_date
      having count(*) > 1) 
    order by location_no, item_no, post_date, sys_source_batch_id desc ,sys_source_sequence_no desc ;
    

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

    l_text := 'LOAD OF MC_STOCK_CH EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 
    select count(*)
    into   g_recs_read
    from   DWH_FOUNDATION.stg_rms_mc_stock_ch_cpy                                                                                                                                                                                              
    where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no  := 0; 
   g_item_no      := 0;
   g_post_date    :='1 JAN 2000';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no     = g_location_no and
            dupp_record.item_no         = g_item_no and 
            dupp_record.post_date       = g_post_date
            then
            update stg_rms_mc_stock_ch_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no   := dupp_record.location_no; 
        g_item_no       := dupp_record.item_no;
        g_post_date     := dupp_record.post_date;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


merge /*+ parallel (fnd_mart,6) */ 
    into DWH_FOUNDATION.FND_MC_LOC_ITEM_DY_RMS_STK_CH fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
    from DWH_FOUNDATION.STG_RMS_MC_STOCK_CH_CPY  tmp 
    where tmp.sys_process_code = 'N'
--      and (tmp.source_data_status_code in ('U', 'I', 'D', 'P') or tmp.source_data_status_code is null)
     and tmp.item_no     in (select item_no     from fnd_item itm     where itm.item_no = tmp.item_no) 
     and tmp.location_no in (select location_no from fnd_location loc where loc.location_no = tmp.location_no)      
     ) mer_mart
  
on  (mer_mart.location_no   = fnd_mart.location_no
and  mer_mart.item_no       = fnd_mart.item_no
and  mer_mart.post_date     = fnd_mart.post_date
    )
when matched then
update
set           SOH_QTY                        = mer_mart.soh_qty,
              SIT_QTY                        = mer_mart.sit_qty,
              NON_SELLABLE_QTY               = mer_mart.non_sellable_qty,        
              INBOUND_EXCL_CUST_ORD_QTY      = mer_mart.inbound_excl_cust_ord_qty,
              INBOUND_INCL_CUST_ORD_QTY      = mer_mart.inbound_incl_cust_ord_qty,
              CLEAR_SOH_QTY                  = mer_mart.clear_soh_qty,
              LAST_UPDATED_DATE              = g_date,
              CLEAR_SOH_COST_LOCAL           = mer_mart.clear_soh_cost_local,
              CLEAR_SOH_COST_OPR             = mer_mart.clear_soh_cost_opr,
              CLEAR_SOH_SELLING_LOCAL        = mer_mart.clear_soh_selling_local,
              CLEAR_SOH_SELLING_OPR          = mer_mart.clear_soh_selling_opr,
              INBND_EXCL_CUST_ORD_COST_LOCAL = mer_mart.inbnd_excl_cust_ord_cost_local,
              INBND_EXCL_CUST_ORD_COST_OPR   = mer_mart.inbnd_excl_cust_ord_cost_opr,
              INBND_EXCL_CUST_ORD_SELL_LOCAL = mer_mart.inbnd_excl_cust_ord_sell_local,
              INBND_EXCL_CUST_ORD_SELL_OPR   = mer_mart.inbnd_excl_cust_ord_sell_opr,
              INBND_INCL_CUST_ORD_COST_LOCAL = mer_mart.inbnd_incl_cust_ord_cost_local,
              INBND_INCL_CUST_ORD_COST_OPR   = mer_mart.inbnd_incl_cust_ord_cost_opr,
              INBND_INCL_CUST_ORD_SELL_LOCAL = mer_mart.inbnd_incl_cust_ord_sell_local,
              INBND_INCL_CUST_ORD_SELL_OPR   = mer_mart.inbnd_incl_cust_ord_sell_opr,
              SIT_COST_LOCAL                 = mer_mart.sit_cost_local,
              SIT_COST_OPR                   = mer_mart.sit_cost_opr,
              SIT_SELLING_LOCAL              = mer_mart.sit_selling_local,
              SIT_SELLING_OPR                = mer_mart.sit_selling_opr,
              SOH_COST_LOCAL                 = mer_mart.soh_cost_local,
              SOH_COST_OPR                   = mer_mart.soh_cost_opr,
              SOH_SELLING_LOCAL              = mer_mart.soh_selling_local,
              SOH_SELLING_OPR                = mer_mart.soh_selling_opr


          
WHEN NOT MATCHED THEN
INSERT
(         LOCATION_NO,
          ITEM_NO,
          POST_DATE,
          SOH_QTY,
          SIT_QTY,
          NON_SELLABLE_QTY,
          INBOUND_EXCL_CUST_ORD_QTY,
          INBOUND_INCL_CUST_ORD_QTY,
          CLEAR_SOH_QTY,
          LAST_UPDATED_DATE,
          CLEAR_SOH_COST_LOCAL,
          CLEAR_SOH_COST_OPR,
          CLEAR_SOH_SELLING_LOCAL,
          CLEAR_SOH_SELLING_OPR,
          INBND_EXCL_CUST_ORD_COST_LOCAL,
          INBND_EXCL_CUST_ORD_COST_OPR,
          INBND_EXCL_CUST_ORD_SELL_LOCAL,
          INBND_EXCL_CUST_ORD_SELL_OPR,
          INBND_INCL_CUST_ORD_COST_LOCAL,
          INBND_INCL_CUST_ORD_COST_OPR,
          INBND_INCL_CUST_ORD_SELL_LOCAL,
          INBND_INCL_CUST_ORD_SELL_OPR,
          SIT_COST_LOCAL,
          SIT_COST_OPR,
          SIT_SELLING_LOCAL,
          SIT_SELLING_OPR,
          SOH_COST_LOCAL,
          SOH_COST_OPR,
          SOH_SELLING_LOCAL,
          SOH_SELLING_OPR

          )
  values
(         mer_mart.LOCATION_NO,
          mer_mart.ITEM_NO,
          mer_mart.POST_DATE,
          mer_mart.SOH_QTY,
          mer_mart.SIT_QTY,
          mer_mart.NON_SELLABLE_QTY,
          mer_mart.INBOUND_EXCL_CUST_ORD_QTY,
          mer_mart.INBOUND_INCL_CUST_ORD_QTY,
          mer_mart.CLEAR_SOH_QTY,
          g_date,
          mer_mart.CLEAR_SOH_COST_LOCAL,
          mer_mart.CLEAR_SOH_COST_OPR,
          mer_mart.CLEAR_SOH_SELLING_LOCAL,
          mer_mart.CLEAR_SOH_SELLING_OPR,
          mer_mart.INBND_EXCL_CUST_ORD_COST_LOCAL,
          mer_mart.INBND_EXCL_CUST_ORD_COST_OPR,
          mer_mart.INBND_EXCL_CUST_ORD_SELL_LOCAL,
          mer_mart.INBND_EXCL_CUST_ORD_SELL_OPR,
          mer_mart.INBND_INCL_CUST_ORD_COST_LOCAL,
          mer_mart.INBND_INCL_CUST_ORD_COST_OPR,
          mer_mart.INBND_INCL_CUST_ORD_SELL_LOCAL,
          mer_mart.INBND_INCL_CUST_ORD_SELL_OPR,
          mer_mart.SIT_COST_LOCAL,
          mer_mart.SIT_COST_OPR,
          mer_mart.SIT_SELLING_LOCAL,
          mer_mart.SIT_SELLING_OPR,
          mer_mart.SOH_COST_LOCAL,
          mer_mart.SOH_COST_OPR,
          mer_mart.SOH_SELLING_LOCAL,
          mer_mart.SOH_SELLING_OPR

          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into STG_RMS_MC_STOCK_CH_HSP hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,
                            'Y',
                            'DWH',
                            TMP.sys_middleware_batch_id,
                            'REFERENCIAL ERROR ON  LOCATION_NO, ITEM_NO, POST_DATE',
                            TMP.SOURCE_DATA_STATUS_CODE,
                            TMP.LOCATION_NO,
                            TMP.ITEM_NO,
                            TMP.POST_DATE,
                            TMP.SOH_QTY,
                            TMP.SIT_QTY,
                            TMP.NON_SELLABLE_QTY,
                            TMP.INBOUND_EXCL_CUST_ORD_QTY,
                            TMP.INBOUND_INCL_CUST_ORD_QTY,
                            TMP.CLEAR_SOH_QTY,
                            TMP.CLEAR_SOH_COST_LOCAL,
                            TMP.CLEAR_SOH_COST_OPR,
                            TMP.CLEAR_SOH_SELLING_LOCAL,
                            TMP.CLEAR_SOH_SELLING_OPR,
                            TMP.INBND_EXCL_CUST_ORD_COST_LOCAL,
                            TMP.INBND_EXCL_CUST_ORD_COST_OPR,
                            TMP.INBND_EXCL_CUST_ORD_SELL_LOCAL,
                            TMP.INBND_EXCL_CUST_ORD_SELL_OPR,
                            TMP.INBND_INCL_CUST_ORD_COST_LOCAL,
                            TMP.INBND_INCL_CUST_ORD_COST_OPR,
                            TMP.INBND_INCL_CUST_ORD_SELL_LOCAL,
                            TMP.INBND_INCL_CUST_ORD_SELL_OPR,
                            TMP.SIT_COST_LOCAL,
                            TMP.SIT_COST_OPR,
                            TMP.SIT_SELLING_LOCAL,
                            TMP.SIT_SELLING_OPR,
                            TMP.SOH_COST_LOCAL,
                            TMP.SOH_COST_OPR,
                            TMP.SOH_SELLING_LOCAL,
                            TMP.SOH_SELLING_OPR


 from  STG_RMS_MC_STOCK_CH_CPY  TMP 
   where tmp.item_no     not in (select item_no     from fnd_item itm     where itm.item_no = tmp.item_no) 
   or    tmp.location_no not in (select location_no from fnd_location loc where loc.location_no = tmp.location_no)   
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;          
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
end WH_FND_MC_206U;
