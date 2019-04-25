--------------------------------------------------------
--  DDL for Procedure WH_FND_MC_208U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MC_208U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2017
--  Author:      Bhavesh Valodia
--  Purpose:     Update STG_RMS_MC_SALE table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_mc_sale_cpy
--               Output - fnd_mc_loc_item_dy_rms_sale
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
g_hospital_text      dwh_foundation.stg_rms_mc_SALE_hsp.sys_process_msg%type;
mer_mart             dwh_foundation.stg_rms_mc_SALE_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MC_208U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE MC_SALE MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no     stg_rms_mc_sale_cpy.location_no%type; 
g_item_no           stg_rms_mc_sale_cpy.item_no%type; 
g_post_date           stg_rms_mc_sale_cpy.post_date%type;


   cursor stg_dup is
      select * 
        from dwh_foundation.stg_rms_mc_sale_cpy
       where (location_no, item_no, post_date)
          in
     (select location_no, item_no, post_date
        from stg_rms_mc_sale_cpy 
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

    l_text := 'LOAD OF MC_SALES EX RMS STARTED AT '||
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
    from   DWH_FOUNDATION.STG_RMS_MC_SALE_CPY                                                                                                                                                                                               
    where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no  := 0; 
   g_item_no      := 0;
   g_post_date    := '1 JAN 2000';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no    = g_location_no and
            dupp_record.item_no        = g_item_no and 
            dupp_record.post_date      = g_post_date
            then
            update stg_rms_mc_sale_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no     := dupp_record.location_no; 
        g_item_no         := dupp_record.item_no;
        g_post_date       := dupp_record.post_date;
    
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
    into DWH_FOUNDATION.FND_MC_LOC_ITEM_DY_RMS_SALE fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
    from DWH_FOUNDATION.STG_RMS_MC_SALE_CPY  tmp 
    where tmp.sys_process_code = 'N'
    and tmp.item_no     in (select item_no     from fnd_item itm     where itm.item_no = tmp.item_no) 
    and tmp.location_no in (select location_no from fnd_location loc where loc.location_no = tmp.location_no) 
     
     ) mer_mart
  
on  (mer_mart.location_no   = fnd_mart.location_no
and  mer_mart.item_no       = fnd_mart.item_no
and  mer_mart.post_date     = fnd_mart.post_date
    )
when matched then
update
set                       SALES_QTY                 = mer_mart.SALES_QTY,
                          REG_SALES_QTY             = mer_mart.REG_SALES_QTY,
                          PROM_SALES_QTY            = mer_mart.PROM_SALES_QTY,
                          CLEAR_SALES_QTY           = mer_mart.CLEAR_SALES_QTY,
                          WASTE_QTY                 = mer_mart.WASTE_QTY,
                          SHRINK_QTY                = mer_mart.SHRINK_QTY,
                          GAIN_QTY                  = mer_mart.GAIN_QTY,
                          SDN_IN_QTY                = mer_mart.SDN_IN_QTY,
                          GRN_QTY                   = mer_mart.GRN_QTY,
                          CLAIM_QTY                 = mer_mart.CLAIM_QTY,
                          SALES_RETURNS_QTY         = mer_mart.SALES_RETURNS_QTY,
                          SELF_SUPPLY_QTY           = mer_mart.SELF_SUPPLY_QTY,
                          INVOICE_ADJ_QTY           = mer_mart.INVOICE_ADJ_QTY,
                          RNDM_MASS_POS_VAR         = mer_mart.RNDM_MASS_POS_VAR,
                          RTV_QTY                   = mer_mart.RTV_QTY,
                          SDN_OUT_QTY               = mer_mart.SDN_OUT_QTY,
                          IBT_IN_QTY                = mer_mart.IBT_IN_QTY,
                          IBT_OUT_QTY               = mer_mart.IBT_OUT_QTY,
                          PROM_DISCOUNT_NO          = mer_mart.PROM_DISCOUNT_NO,
                          HO_PROM_DISCOUNT_QTY      = mer_mart.HO_PROM_DISCOUNT_QTY,
                          ST_PROM_DISCOUNT_QTY      = mer_mart.ST_PROM_DISCOUNT_QTY,
                          SOURCE_DATA_STATUS_CODE   = mer_mart.SOURCE_DATA_STATUS_CODE,
                          LAST_UPDATED_DATE         = g_date,
                          CLAIM_COST_LOCAL          = mer_mart.CLAIM_COST_LOCAL,
                          CLAIM_COST_OPR            = mer_mart.CLAIM_COST_OPR,
                          CLAIM_SELLING_LOCAL       = mer_mart.CLAIM_SELLING_LOCAL,
                          CLAIM_SELLING_OPR         = mer_mart.CLAIM_SELLING_OPR,
                          CLEAR_MKDN_SELLING_LOCAL  = mer_mart.CLEAR_MKDN_SELLING_LOCAL,
                          CLEAR_MKDN_SELLING_OPR    = mer_mart.CLEAR_MKDN_SELLING_OPR,
                          CLEAR_SALES_COST_LOCAL    = mer_mart.CLEAR_SALES_COST_LOCAL,
                          CLEAR_SALES_COST_OPR      = mer_mart.CLEAR_SALES_COST_OPR,
                          CLEAR_SALES_LOCAL         = mer_mart.CLEAR_SALES_LOCAL,
                          CLEAR_SALES_OPR           = mer_mart.CLEAR_SALES_OPR,
                          GAIN_COST_LOCAL           = mer_mart.GAIN_COST_LOCAL,
                          GAIN_COST_OPR             = mer_mart.GAIN_COST_OPR,
                          GAIN_SELLING_LOCAL        = mer_mart.GAIN_SELLING_LOCAL,
                          GAIN_SELLING_OPR          = mer_mart.GAIN_SELLING_OPR,
                          GRN_COST_LOCAL            = mer_mart.GRN_COST_LOCAL,
                          GRN_COST_OPR              = mer_mart.GRN_COST_OPR,
                          GRN_SELLING_LOCAL         = mer_mart.GRN_SELLING_LOCAL,
                          GRN_SELLING_OPR           = mer_mart.GRN_SELLING_OPR,
                          HO_PROM_DISCOUNT_AMT_LOCAL = mer_mart.HO_PROM_DISCOUNT_AMT_LOCAL,
                          HO_PROM_DISCOUNT_AMT_OPR  = mer_mart.HO_PROM_DISCOUNT_AMT_OPR,
                          IBT_IN_COST_LOCAL         = mer_mart.IBT_IN_COST_LOCAL,
                          IBT_IN_COST_OPR           = mer_mart.IBT_IN_COST_OPR,
                          IBT_IN_SELLING_LOCAL      = mer_mart.IBT_IN_SELLING_LOCAL,
                          IBT_IN_SELLING_OPR        = mer_mart.IBT_IN_SELLING_OPR,
                          IBT_OUT_COST_LOCAL        = mer_mart.IBT_OUT_COST_LOCAL,
                          IBT_OUT_COST_OPR          = mer_mart.IBT_OUT_COST_OPR,
                          IBT_OUT_SELLING_LOCAL     = mer_mart.IBT_OUT_SELLING_LOCAL,
                          IBT_OUT_SELLING_OPR       = mer_mart.IBT_OUT_SELLING_OPR,
                          INVOICE_ADJ_COST_LOCAL    = mer_mart.INVOICE_ADJ_COST_LOCAL,
                          INVOICE_ADJ_COST_OPR      = mer_mart.INVOICE_ADJ_COST_OPR,
                          INVOICE_ADJ_SELLING_LOCAL = mer_mart.INVOICE_ADJ_SELLING_LOCAL,
                          INVOICE_ADJ_SELLING_OPR   = mer_mart.INVOICE_ADJ_SELLING_OPR,
                          MKDN_CANCEL_SELLING_LOCAL = mer_mart.MKDN_CANCEL_SELLING_LOCAL,
                          MKDN_CANCEL_SELLING_OPR   = mer_mart.MKDN_CANCEL_SELLING_OPR,
                          MKDN_SELLING_LOCAL        = mer_mart.MKDN_SELLING_LOCAL,
                          MKDN_SELLING_OPR          = mer_mart.MKDN_SELLING_OPR,
                          MKUP_CANCEL_SELLING_LOCAL = mer_mart.MKUP_CANCEL_SELLING_LOCAL,
                          MKUP_CANCEL_SELLING_OPR   = mer_mart.MKUP_CANCEL_SELLING_OPR,
                          MKUP_SELLING_LOCAL        = mer_mart.MKUP_SELLING_LOCAL,
                          MKUP_SELLING_OPR          = mer_mart.MKUP_SELLING_OPR,
                          PROM_SALES_COST_LOCAL     = mer_mart.PROM_SALES_COST_LOCAL,
                          PROM_SALES_COST_OPR       = mer_mart.PROM_SALES_COST_OPR,
                          PROM_SALES_LOCAL          = mer_mart.PROM_SALES_LOCAL,
                          PROM_SALES_OPR            = mer_mart.PROM_SALES_OPR,
                          REG_SALES_COST_LOCAL      = mer_mart.REG_SALES_COST_LOCAL,
                          REG_SALES_COST_OPR        = mer_mart.REG_SALES_COST_OPR,
                          REG_SALES_LOCAL           = mer_mart.REG_SALES_LOCAL,
                          REG_SALES_OPR             = mer_mart.REG_SALES_OPR,
                          RTV_COST_LOCAL            = mer_mart.RTV_COST_LOCAL,
                          RTV_COST_OPR              = mer_mart.RTV_COST_OPR,
                          RTV_SELLING_LOCAL         = mer_mart.RTV_SELLING_LOCAL,
                          RTV_SELLING_OPR           = mer_mart.RTV_SELLING_OPR,
                          SALES_COST_LOCAL          = mer_mart.SALES_COST_LOCAL,
                          SALES_COST_OPR            = mer_mart.SALES_COST_OPR,
                          SALES_LOCAL               = mer_mart.SALES_LOCAL,
                          SALES_OPR                 = mer_mart.SALES_OPR,
                          SALES_RETURNS_COST_LOCAL  = mer_mart.SALES_RETURNS_COST_LOCAL,
                          SALES_RETURNS_COST_OPR    = mer_mart.SALES_RETURNS_COST_OPR,
                          SALES_RETURNS_LOCAL       = mer_mart.SALES_RETURNS_LOCAL,
                          SALES_RETURNS_OPR         = mer_mart.SALES_RETURNS_OPR,
                          SDN_IN_COST_LOCAL         = mer_mart.SDN_IN_COST_LOCAL,
                          SDN_IN_COST_OPR           = mer_mart.SDN_IN_COST_OPR,
                          SDN_IN_SELLING_LOCAL      = mer_mart.SDN_IN_SELLING_LOCAL,
                          SDN_IN_SELLING_OPR        = mer_mart.SDN_IN_SELLING_OPR,
                          SDN_OUT_COST_LOCAL        = mer_mart.SDN_OUT_COST_LOCAL,
                          SDN_OUT_COST_OPR          = mer_mart.SDN_OUT_COST_OPR,
                          SDN_OUT_SELLING_LOCAL     = mer_mart.SDN_OUT_SELLING_LOCAL,
                          SDN_OUT_SELLING_OPR       = mer_mart.SDN_OUT_SELLING_OPR,
                          SELF_SUPPLY_COST_LOCAL    = mer_mart.SELF_SUPPLY_COST_LOCAL,
                          SELF_SUPPLY_COST_OPR      = mer_mart.SELF_SUPPLY_COST_OPR,
                          SELF_SUPPLY_SELLING_LOCAL = mer_mart.SELF_SUPPLY_SELLING_LOCAL,
                          SELF_SUPPLY_SELLING_OPR   = mer_mart.SELF_SUPPLY_SELLING_OPR,
                          SHRINK_COST_LOCAL         = mer_mart.SHRINK_COST_LOCAL,
                          SHRINK_COST_OPR           = mer_mart.SHRINK_COST_OPR,
                          SHRINK_SELLING_LOCAL      = mer_mart.SHRINK_SELLING_LOCAL,
                          SHRINK_SELLING_OPR        = mer_mart.SHRINK_SELLING_OPR,
                          ST_PROM_DISCOUNT_AMT_LOCAL = mer_mart.ST_PROM_DISCOUNT_AMT_LOCAL,
                          ST_PROM_DISCOUNT_AMT_OPR  = mer_mart.ST_PROM_DISCOUNT_AMT_OPR,
                          WAC_ADJ_AMT_LOCAL         = mer_mart.WAC_ADJ_AMT_LOCAL,
                          WAC_ADJ_AMT_OPR           = mer_mart.WAC_ADJ_AMT_OPR,
                          WASTE_COST_LOCAL          = mer_mart.WASTE_COST_LOCAL,
                          WASTE_COST_OPR            = mer_mart.WASTE_COST_OPR,
                          WASTE_SELLING_LOCAL       = mer_mart.WASTE_SELLING_LOCAL,
                          WASTE_SELLING_OPR         = mer_mart.WASTE_SELLING_OPR




          
WHEN NOT MATCHED THEN
INSERT
(             LOCATION_NO,
              ITEM_NO,
              POST_DATE,
              SALES_QTY,
              REG_SALES_QTY,
              PROM_SALES_QTY,
              CLEAR_SALES_QTY,
              WASTE_QTY,
              SHRINK_QTY,
              GAIN_QTY,
              SDN_IN_QTY,
              GRN_QTY,
              CLAIM_QTY,
              SALES_RETURNS_QTY,
              SELF_SUPPLY_QTY,
              INVOICE_ADJ_QTY,
              RNDM_MASS_POS_VAR,
              RTV_QTY,
              SDN_OUT_QTY,
              IBT_IN_QTY,
              IBT_OUT_QTY,
              PROM_DISCOUNT_NO,
              HO_PROM_DISCOUNT_QTY,
              ST_PROM_DISCOUNT_QTY,
              SOURCE_DATA_STATUS_CODE,
              LAST_UPDATED_DATE,
              CLAIM_COST_LOCAL,
              CLAIM_COST_OPR,
              CLAIM_SELLING_LOCAL,
              CLAIM_SELLING_OPR,
              CLEAR_MKDN_SELLING_LOCAL,
              CLEAR_MKDN_SELLING_OPR,
              CLEAR_SALES_COST_LOCAL,
              CLEAR_SALES_COST_OPR,
              CLEAR_SALES_LOCAL,
              CLEAR_SALES_OPR,
              GAIN_COST_LOCAL,
              GAIN_COST_OPR,
              GAIN_SELLING_LOCAL,
              GAIN_SELLING_OPR,
              GRN_COST_LOCAL,
              GRN_COST_OPR,
              GRN_SELLING_LOCAL,
              GRN_SELLING_OPR,
              HO_PROM_DISCOUNT_AMT_LOCAL,
              HO_PROM_DISCOUNT_AMT_OPR,
              IBT_IN_COST_LOCAL,
              IBT_IN_COST_OPR,
              IBT_IN_SELLING_LOCAL,
              IBT_IN_SELLING_OPR,
              IBT_OUT_COST_LOCAL,
              IBT_OUT_COST_OPR,
              IBT_OUT_SELLING_LOCAL,
              IBT_OUT_SELLING_OPR,
              INVOICE_ADJ_COST_LOCAL,
              INVOICE_ADJ_COST_OPR,
              INVOICE_ADJ_SELLING_LOCAL,
              INVOICE_ADJ_SELLING_OPR,
              MKDN_CANCEL_SELLING_LOCAL,
              MKDN_CANCEL_SELLING_OPR,
              MKDN_SELLING_LOCAL,
              MKDN_SELLING_OPR,
              MKUP_CANCEL_SELLING_LOCAL,
              MKUP_CANCEL_SELLING_OPR,
              MKUP_SELLING_LOCAL,
              MKUP_SELLING_OPR,
              PROM_SALES_COST_LOCAL,
              PROM_SALES_COST_OPR,
              PROM_SALES_LOCAL,
              PROM_SALES_OPR,
              REG_SALES_COST_LOCAL,
              REG_SALES_COST_OPR,
              REG_SALES_LOCAL,
              REG_SALES_OPR,
              RTV_COST_LOCAL,
              RTV_COST_OPR,
              RTV_SELLING_LOCAL,
              RTV_SELLING_OPR,
              SALES_COST_LOCAL,
              SALES_COST_OPR,
              SALES_LOCAL,
              SALES_OPR,
              SALES_RETURNS_COST_LOCAL,
              SALES_RETURNS_COST_OPR,
              SALES_RETURNS_LOCAL,
              SALES_RETURNS_OPR,
              SDN_IN_COST_LOCAL,
              SDN_IN_COST_OPR,
              SDN_IN_SELLING_LOCAL,
              SDN_IN_SELLING_OPR,
              SDN_OUT_COST_LOCAL,
              SDN_OUT_COST_OPR,
              SDN_OUT_SELLING_LOCAL,
              SDN_OUT_SELLING_OPR,
              SELF_SUPPLY_COST_LOCAL,
              SELF_SUPPLY_COST_OPR,
              SELF_SUPPLY_SELLING_LOCAL,
              SELF_SUPPLY_SELLING_OPR,
              SHRINK_COST_LOCAL,
              SHRINK_COST_OPR,
              SHRINK_SELLING_LOCAL,
              SHRINK_SELLING_OPR,
              ST_PROM_DISCOUNT_AMT_LOCAL,
              ST_PROM_DISCOUNT_AMT_OPR,
              WAC_ADJ_AMT_LOCAL,
              WAC_ADJ_AMT_OPR,
              WASTE_COST_LOCAL,
              WASTE_COST_OPR,
              WASTE_SELLING_LOCAL,
              WASTE_SELLING_OPR



          )
  values
(                   mer_mart.LOCATION_NO,
                    mer_mart.ITEM_NO,
                    mer_mart.POST_DATE,
                    mer_mart.SALES_QTY,
                    mer_mart.REG_SALES_QTY,
                    mer_mart.PROM_SALES_QTY,
                    mer_mart.CLEAR_SALES_QTY,
                    mer_mart.WASTE_QTY,
                    mer_mart.SHRINK_QTY,
                    mer_mart.GAIN_QTY,
                    mer_mart.SDN_IN_QTY,
                    mer_mart.GRN_QTY,
                    mer_mart.CLAIM_QTY,
                    mer_mart.SALES_RETURNS_QTY,
                    mer_mart.SELF_SUPPLY_QTY,
                    mer_mart.INVOICE_ADJ_QTY,
                    mer_mart.RNDM_MASS_POS_VAR,
                    mer_mart.RTV_QTY,
                    mer_mart.SDN_OUT_QTY,
                    mer_mart.IBT_IN_QTY,
                    mer_mart.IBT_OUT_QTY,
                    mer_mart.PROM_DISCOUNT_NO,
                    mer_mart.HO_PROM_DISCOUNT_QTY,
                    mer_mart.ST_PROM_DISCOUNT_QTY,
                    mer_mart.SOURCE_DATA_STATUS_CODE,
                    g_date,
                    mer_mart.CLAIM_COST_LOCAL,
                    mer_mart.CLAIM_COST_OPR,
                    mer_mart.CLAIM_SELLING_LOCAL,
                    mer_mart.CLAIM_SELLING_OPR,
                    mer_mart.CLEAR_MKDN_SELLING_LOCAL,
                    mer_mart.CLEAR_MKDN_SELLING_OPR,
                    mer_mart.CLEAR_SALES_COST_LOCAL,
                    mer_mart.CLEAR_SALES_COST_OPR,
                    mer_mart.CLEAR_SALES_LOCAL,
                    mer_mart.CLEAR_SALES_OPR,
                    mer_mart.GAIN_COST_LOCAL,
                    mer_mart.GAIN_COST_OPR,
                    mer_mart.GAIN_SELLING_LOCAL,
                    mer_mart.GAIN_SELLING_OPR,
                    mer_mart.GRN_COST_LOCAL,
                    mer_mart.GRN_COST_OPR,
                    mer_mart.GRN_SELLING_LOCAL,
                    mer_mart.GRN_SELLING_OPR,
                    mer_mart.HO_PROM_DISCOUNT_AMT_LOCAL,
                    mer_mart.HO_PROM_DISCOUNT_AMT_OPR,
                    mer_mart.IBT_IN_COST_LOCAL,
                    mer_mart.IBT_IN_COST_OPR,
                    mer_mart.IBT_IN_SELLING_LOCAL,
                    mer_mart.IBT_IN_SELLING_OPR,
                    mer_mart.IBT_OUT_COST_LOCAL,
                    mer_mart.IBT_OUT_COST_OPR,
                    mer_mart.IBT_OUT_SELLING_LOCAL,
                    mer_mart.IBT_OUT_SELLING_OPR,
                    mer_mart.INVOICE_ADJ_COST_LOCAL,
                    mer_mart.INVOICE_ADJ_COST_OPR,
                    mer_mart.INVOICE_ADJ_SELLING_LOCAL,
                    mer_mart.INVOICE_ADJ_SELLING_OPR,
                    mer_mart.MKDN_CANCEL_SELLING_LOCAL,
                    mer_mart.MKDN_CANCEL_SELLING_OPR,
                    mer_mart.MKDN_SELLING_LOCAL,
                    mer_mart.MKDN_SELLING_OPR,
                    mer_mart.MKUP_CANCEL_SELLING_LOCAL,
                    mer_mart.MKUP_CANCEL_SELLING_OPR,
                    mer_mart.MKUP_SELLING_LOCAL,
                    mer_mart.MKUP_SELLING_OPR,
                    mer_mart.PROM_SALES_COST_LOCAL,
                    mer_mart.PROM_SALES_COST_OPR,
                    mer_mart.PROM_SALES_LOCAL,
                    mer_mart.PROM_SALES_OPR,
                    mer_mart.REG_SALES_COST_LOCAL,
                    mer_mart.REG_SALES_COST_OPR,
                    mer_mart.REG_SALES_LOCAL,
                    mer_mart.REG_SALES_OPR,
                    mer_mart.RTV_COST_LOCAL,
                    mer_mart.RTV_COST_OPR,
                    mer_mart.RTV_SELLING_LOCAL,
                    mer_mart.RTV_SELLING_OPR,
                    mer_mart.SALES_COST_LOCAL,
                    mer_mart.SALES_COST_OPR,
                    mer_mart.SALES_LOCAL,
                    mer_mart.SALES_OPR,
                    mer_mart.SALES_RETURNS_COST_LOCAL,
                    mer_mart.SALES_RETURNS_COST_OPR,
                    mer_mart.SALES_RETURNS_LOCAL,
                    mer_mart.SALES_RETURNS_OPR,
                    mer_mart.SDN_IN_COST_LOCAL,
                    mer_mart.SDN_IN_COST_OPR,
                    mer_mart.SDN_IN_SELLING_LOCAL,
                    mer_mart.SDN_IN_SELLING_OPR,
                    mer_mart.SDN_OUT_COST_LOCAL,
                    mer_mart.SDN_OUT_COST_OPR,
                    mer_mart.SDN_OUT_SELLING_LOCAL,
                    mer_mart.SDN_OUT_SELLING_OPR,
                    mer_mart.SELF_SUPPLY_COST_LOCAL,
                    mer_mart.SELF_SUPPLY_COST_OPR,
                    mer_mart.SELF_SUPPLY_SELLING_LOCAL,
                    mer_mart.SELF_SUPPLY_SELLING_OPR,
                    mer_mart.SHRINK_COST_LOCAL,
                    mer_mart.SHRINK_COST_OPR,
                    mer_mart.SHRINK_SELLING_LOCAL,
                    mer_mart.SHRINK_SELLING_OPR,
                    mer_mart.ST_PROM_DISCOUNT_AMT_LOCAL,
                    mer_mart.ST_PROM_DISCOUNT_AMT_OPR,
                    mer_mart.WAC_ADJ_AMT_LOCAL,
                    mer_mart.WAC_ADJ_AMT_OPR,
                    mer_mart.WASTE_COST_LOCAL,
                    mer_mart.WASTE_COST_OPR,
                    mer_mart.WASTE_SELLING_LOCAL,
                    mer_mart.WASTE_SELLING_OPR


          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into DWH_FOUNDATION.STG_RMS_MC_SALE_HSP hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,
                            'Y',
                            'DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON  LOCATION_NO, ITEM_NO, TRAN_DATE',
                            TMP.LOCATION_NO,
                            TMP.ITEM_NO,
                            TMP.POST_DATE,
                            TMP.SALES_QTY,
                            TMP.REG_SALES_QTY,
                            TMP.PROM_SALES_QTY,
                            TMP.CLEAR_SALES_QTY,
                            TMP.WASTE_QTY,
                            TMP.SHRINK_QTY,
                            TMP.GAIN_QTY,
                            TMP.SDN_IN_QTY,
                            TMP.GRN_QTY,
                            TMP.CLAIM_QTY,
                            TMP.SALES_RETURNS_QTY,
                            TMP.SELF_SUPPLY_QTY,
                            TMP.INVOICE_ADJ_QTY,
                            TMP.RNDM_MASS_POS_VAR,
                            TMP.RTV_QTY,
                            TMP.SDN_OUT_QTY,
                            TMP.IBT_IN_QTY,
                            TMP.IBT_OUT_QTY,
                            TMP.PROM_DISCOUNT_NO,
                            TMP.HO_PROM_DISCOUNT_QTY,
                            TMP.ST_PROM_DISCOUNT_QTY,
                            TMP.SOURCE_DATA_STATUS_CODE,
                            
                            TMP.CLAIM_COST_LOCAL,
                            TMP.CLAIM_COST_OPR,
                            TMP.CLAIM_SELLING_LOCAL,
                            TMP.CLAIM_SELLING_OPR,
                            TMP.CLEAR_MKDN_SELLING_LOCAL,
                            TMP.CLEAR_MKDN_SELLING_OPR,
                            TMP.CLEAR_SALES_COST_LOCAL,
                            TMP.CLEAR_SALES_COST_OPR,
                            TMP.CLEAR_SALES_LOCAL,
                            TMP.CLEAR_SALES_OPR,
                            TMP.GAIN_COST_LOCAL,
                            TMP.GAIN_COST_OPR,
                            TMP.GAIN_SELLING_LOCAL,
                            TMP.GAIN_SELLING_OPR,
                            TMP.GRN_COST_LOCAL,
                            TMP.GRN_COST_OPR,
                            TMP.GRN_SELLING_LOCAL,
                            TMP.GRN_SELLING_OPR,
                            TMP.HO_PROM_DISCOUNT_AMT_LOCAL,
                            TMP.HO_PROM_DISCOUNT_AMT_OPR,
                            TMP.IBT_IN_COST_LOCAL,
                            TMP.IBT_IN_COST_OPR,
                            TMP.IBT_IN_SELLING_LOCAL,
                            TMP.IBT_IN_SELLING_OPR,
                            TMP.IBT_OUT_COST_LOCAL,
                            TMP.IBT_OUT_COST_OPR,
                            TMP.IBT_OUT_SELLING_LOCAL,
                            TMP.IBT_OUT_SELLING_OPR,
                            TMP.INVOICE_ADJ_COST_LOCAL,
                            TMP.INVOICE_ADJ_COST_OPR,
                            TMP.INVOICE_ADJ_SELLING_LOCAL,
                            TMP.INVOICE_ADJ_SELLING_OPR,
                            TMP.MKDN_CANCEL_SELLING_LOCAL,
                            TMP.MKDN_CANCEL_SELLING_OPR,
                            TMP.MKDN_SELLING_LOCAL,
                            TMP.MKDN_SELLING_OPR,
                            TMP.MKUP_CANCEL_SELLING_LOCAL,
                            TMP.MKUP_CANCEL_SELLING_OPR,
                            TMP.MKUP_SELLING_LOCAL,
                            TMP.MKUP_SELLING_OPR,
                            TMP.PROM_SALES_COST_LOCAL,
                            TMP.PROM_SALES_COST_OPR,
                            TMP.PROM_SALES_LOCAL,
                            TMP.PROM_SALES_OPR,
                            TMP.REG_SALES_COST_LOCAL,
                            TMP.REG_SALES_COST_OPR,
                            TMP.REG_SALES_LOCAL,
                            TMP.REG_SALES_OPR,
                            TMP.RTV_COST_LOCAL,
                            TMP.RTV_COST_OPR,
                            TMP.RTV_SELLING_LOCAL,
                            TMP.RTV_SELLING_OPR,
                            TMP.SALES_COST_LOCAL,
                            TMP.SALES_COST_OPR,
                            TMP.SALES_LOCAL,
                            TMP.SALES_OPR,
                            TMP.SALES_RETURNS_COST_LOCAL,
                            TMP.SALES_RETURNS_COST_OPR,
                            TMP.SALES_RETURNS_LOCAL,
                            TMP.SALES_RETURNS_OPR,
                            TMP.SDN_IN_COST_LOCAL,
                            TMP.SDN_IN_COST_OPR,
                            TMP.SDN_IN_SELLING_LOCAL,
                            TMP.SDN_IN_SELLING_OPR,
                            TMP.SDN_OUT_COST_LOCAL,
                            TMP.SDN_OUT_COST_OPR,
                            TMP.SDN_OUT_SELLING_LOCAL,
                            TMP.SDN_OUT_SELLING_OPR,
                            TMP.SELF_SUPPLY_COST_LOCAL,
                            TMP.SELF_SUPPLY_COST_OPR,
                            TMP.SELF_SUPPLY_SELLING_LOCAL,
                            TMP.SELF_SUPPLY_SELLING_OPR,
                            TMP.SHRINK_COST_LOCAL,
                            TMP.SHRINK_COST_OPR,
                            TMP.SHRINK_SELLING_LOCAL,
                            TMP.SHRINK_SELLING_OPR,
                            TMP.ST_PROM_DISCOUNT_AMT_LOCAL,
                            TMP.ST_PROM_DISCOUNT_AMT_OPR,
                            TMP.WAC_ADJ_AMT_LOCAL,
                            TMP.WAC_ADJ_AMT_OPR,
                            TMP.WASTE_COST_LOCAL,
                            TMP.WASTE_COST_OPR,
                            TMP.WASTE_SELLING_LOCAL,
                            TMP.WASTE_SELLING_OPR


   from  DWH_FOUNDATION.STG_RMS_MC_SALE_CPY  TMP 
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
end WH_FND_MC_208u;
