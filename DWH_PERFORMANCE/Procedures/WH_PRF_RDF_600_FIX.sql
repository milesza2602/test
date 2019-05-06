--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_600_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_600_FIX" as

  pdate     date := '28 jun 16';
  lupdates  int;
  
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_600_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FIX OF RDF DAILY FCST';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

begin
    execute immediate 'ALTER SESSION enable PARALLEL DML';
--    EXECUTE IMMEDIATE 'set serveroutput on';
  
    merge into dwh_performance.RTL_LOC_ITEM_RDF_DYFCST_L2 tgt 
    using (
            with err_recs as
                  ( select  /*+ parallel (t,4) full(t) */
                            sk1_ITEM_NO, 
                            sk1_LOCATION_NO, 
                            POST_DATE
            --                    
                           ,SALES_DLY_APP_FCST
                           ,SALES_DLY_SYS_FCST 
                    from    dwh_performance.RTL_LOC_ITEM_RDF_DYFCST_L2 t
                    where   POST_DATE = pdate   
                    and    (SALES_DLY_APP_FCST = 0.01 or SALES_DLY_SYS_FCST = 0.01)
                    order by 1,2
                  ),
                  
                 cor_recs as
                  ( select  /*+ parallel (tgt,4) full(tgt) */
                            sk1_ITEM_NO, 
                            sk1_LOCATION_NO, 
                            POST_DATE, 
                            
                            SALES_DLY_APP_FCST, 
                            SALES_DLY_SYS_FCST 
                   from dwh_performance.rtl_loc_item_dy_rdf_fcst tgt
                   where   exists 
                              (
                                select  /*+ parallel (t,4) full(t) */
                                        1
                                from    err_recs t
                                
                                where   t.sk1_ITEM_NO       = tgt.sk1_ITEM_NO 
                                and     t.sk1_LOCATION_NO   = tgt.sk1_LOCATION_NO 
                                and     t.POST_DATE         = tgt.POST_DATE
                              )
                    and   tgt.POST_DATE = pdate 
                    order by 1, 2
                  )
            
            select  b.*
            from    err_recs a
            join    cor_recs b on (a.sk1_ITEM_NO = b.sk1_ITEM_NO and a.sk1_LOCATION_NO = b.sk1_LOCATION_NO)
            where  (a.SALES_DLY_APP_FCST <> b.SALES_DLY_APP_FCST)
            or     (a.SALES_DLY_SYS_FCST <> b.SALES_DLY_SYS_FCST) 
          ) src
    on (tgt.sk1_item_no = src.sk1_item_no and tgt.sk1_location_no = src.sk1_location_no and tgt.POST_DATE = src.POST_DATE)
    
    when matched then
    update set   tgt.SALES_DLY_APP_FCST = src.SALES_DLY_APP_FCST,
                 tgt.SALES_DLY_SYS_FCST = src.SALES_DLY_SYS_FCST;
    lupdates := sql%rowcount;
   
    l_text := 'FIX for DATE IS:- '||pdate||' '||lupdates;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dbms_output.put_line('updates: ' || lupdates);
    commit;
    
end WH_PRF_RDF_600_FIX;
