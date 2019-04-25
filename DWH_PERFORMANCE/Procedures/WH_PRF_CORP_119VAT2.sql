--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_119VAT2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_119VAT2" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        AUG 2015
--  Author:      Quentin Smit
--  Purpose:     FIX VAT on RTL_LOC_ITEM_DY_RMS_SPARSE
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
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
g_run_date           date          := trunc(sysdate);
g05_day              integer       := TO_CHAR(current_date,'DD');

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_119VAT2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'FIX VAT ON SPARSE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_from_rel_date      date;
g_to_rel_date        date;
g_from_post_date     date;
g_to_post_date       date;


  
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
    g_date := '02/AUG/15';
    
    g_to_rel_date := g_date;
    g_from_rel_date := g_to_rel_date - 91;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --g_to_rel_date := 'Moo';
  --========================================================================================
 
 --========================================================================================
 
    WHILE G_TO_REL_DATE < '11/AUG/15' LOOP
    
      l_text := 'BATCH DATE BEING PROCESSED IS:- '||G_TO_REL_DATE;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'ALLOC FROM RELEASE DATE '||g_from_rel_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'ALLOC TO RELEASE DATE '||g_to_rel_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'SPARSE DATE '||g_to_rel_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       merge  into rtl_loc_item_dy_rms_sparse  dnsw 
          using ( with aa as (  
                select /*+ parallel(dnsr,4) full(dnsr) parallel(aloc,4) full(aloc) */  
                 dnsr.SK1_ITEM_NO, DNSR.SK1_LOCATION_NO, DNSR.POST_DATE, CH_ALLOC_SELLING, 
                 sum(nvl(aloc.alloc_qty,0)) * max(aloc.reg_rsp_excl_vat) vat_fix
      
         from   rtl_loc_item_dy_rms_sparse dnsr,
                fnd_rtl_allocation aloc,
                dim_item di,
                dim_item_hist dih,
                dim_location dl,
                dim_location_hist dlh
         where  aloc.item_no                = di.item_no          
           and  aloc.item_no                = dih.item_no         
           and  aloc.release_date         between dih.sk2_active_from_date and dih.sk2_active_to_date 
           and  aloc.to_loc_no              = dl.location_no       
           and  aloc.to_loc_no              = dlh.location_no      
           and  aloc.release_date         between dlh.sk2_active_from_date and dlh.sk2_active_to_date   
           and  di.business_unit_no        <> 50  
           and  aloc.release_date between g_from_rel_date and g_to_rel_date
           and  aloc.release_date         IS NOT NULL  
           and  (aloc.CHAIN_CODE <> 'DJ' or aloc.chain_code is null)  
        
         --  and  dnsr.last_updated_date   between '29 JUN 2015' and '29 JUN 2015'  
           and  dnsr.sk1_item_no          = di.sk1_item_no  
           and  dnsr.sk1_location_no      = dl.sk1_location_no 
           and  dnsr.post_date            = aloc.release_date  
           and  dnsr.post_date            = g_to_rel_date 
           and  dnsr.CH_ALLOC_SELLING <> (nvl(aloc.alloc_qty,0) * (aloc.reg_rsp_excl_vat))
           GROUP BY dnsr.SK1_ITEM_NO, DNSR.SK1_LOCATION_NO, DNSR.POST_DATE, CH_ALLOC_SELLING
              )
               select *  from aa where ch_alloc_selling <> vat_fix
              )mer_rec
        on    (  dnsw.post_date        = mer_rec.post_date        and
                 dnsw.sk1_location_no  = mer_rec.sk1_location_no  and
                 dnsw.sk1_item_no      = mer_rec.sk1_item_no   )
        when matched then 
        update set CH_ALLOC_SELLING      = mer_rec.vat_fix;
                   
        g_recs_updated := g_recs_updated +  sql%rowcount;       

      commit;

 
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

    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    p_success := true;
    
    g_to_rel_date := g_to_rel_date + 1;
    g_from_rel_date := g_to_rel_date - 91;
    
END LOOP;

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
       RAISE;

END WH_PRF_CORP_119VAT2;
