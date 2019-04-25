--------------------------------------------------------
--  DDL for Procedure WH_PRF_FPI_006U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_FPI_006U" -- STORE PROC CHANGE
  (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Francisca de Vaal
--               Extracting the woolworths branded item for the GRN items
--
--  Tables:      Input  - temp_fpi_grn_item_data,                                                       -- TABLE NAME CHANGE 
--                      - temp_fpi_grnytd_item_data,
--               Output - temp_fpi_grnytd_item_data                                                     -- TABLE NAME CHANGE 
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       
--  
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
g_recs_read           integer       :=  0;
g_recs_updated        integer       :=  0;
g_recs_inserted       integer       :=  0;
g_recs_hospital       integer       :=  0;
g_recs_duplicate      integer       :=  0;
g_recs_dummy          integer       :=  0;
g_truncate_count      integer       :=  0;
g_physical_updated    integer       :=  0;

g_date                date          := trunc(sysdate);
g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_last_wk_start_date  date;
g_last_wk_end_date    date;
g_calendar_date       date;
g_loop_date           date ;
g_start_date          date ;
g_end_date            date ;
g_fin_week_no         number        :=  0;
g_fin_year_no         number        :=  0;

l_message             sys_dwh_errlog.log_text%type;
l_module_name         sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_FPI_006U';                              -- STORE PROC CHANGE
l_name                sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name         sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name         sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name      sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                sys_dwh_log.log_text%type ;
l_description         sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_ITEM_SUP_WK_BRTH_LIST EX BRTH';    -- TABLE NAME CHANGE
l_process_type        sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

v_YR                  int     := 0;
v_WK                  int     := 0;
    
v_QRT                 int;
v_CYR                 int;
v_CWK                 int;
v_CSDTE               date;
v_HSDTE               date;

v_LWYR                int     := 0;
v_LWWK                int     := 0;
--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure do_update as
begin
            
    select  FIN_YEAR_NO, 
            FIN_WEEK_NO, 
            FIN_QUARTER_NO, 
            THIS_WEEK_START_DATE
    into    v_CYR, 
            v_CWK, 
            v_QRT, 
            v_CSDTE
    from    dim_calendar
    where   CALENDAR_DATE = (select THIS_WK_START_DATE 
                             from dim_control);
    
    -- get the 6 week refresh starting YR/WK (6 weeks back) ...
    select  FIN_YEAR_NO, 
            FIN_WEEK_NO, 
            THIS_WEEK_START_DATE
    into    v_YR, 
            v_WK, 
            v_HSDTE
    from    dim_calendar
    where   CALENDAR_DATE = (select THIS_WK_START_DATE - 42
                             from dim_control);

    -- set YTD table initial read point ...    
    v_LWYR := v_YR;
    v_LWWK := v_WK - 1;

    if v_WK = 1 then 
       v_LWYR := v_YR-1; 
       v_LWWK := 52;
    end if;
    
    -- Roll forward previous weeks balances for the new week (01 WK is just skeleton records with no values)...
    if  v_CWK = 1 then
        insert  into dwh_performance.rtl_item_wk_sales_ytd
        select  SK1_ITEM_NO, 
                v_CYR, 
                v_CWK, 
                v_QRT, 
                v_CYR || v_QRT, 
                v_CSDTE,
                
                0,0,0, 
                0,0,0, 
                0,0,0,
                to_date('03 Jan 1900')
        from    dwh_performance.rtl_item_wk_sales_ytd
        where   fin_year_no = v_CYR - 1
        and     fin_week_no = 52;
    else
        insert  into dwh_performance.rtl_item_wk_sales_ytd
        select  SK1_ITEM_NO, 
                v_CYR, 
                v_CWK, 
                v_QRT, 
                v_CYR || v_QRT, 
                v_CSDTE,
                
                0,0,0, 
                SALES_YTD, 
                SALES_QTY_YTD, 
                SALES_MARGIN_YTD, 
                0,0,0,
                to_date('03 Jan 1900')
        from    dwh_performance.rtl_item_wk_sales_ytd
        where   fin_year_no = v_CYR
        and     fin_week_no = v_CWK - 1
--      and sk1_item_no in (28842824, 28920758, 25945512, 28402659, 23883364, 23977512)
        ;
    end if;
    
    g_recs_inserted := sql%rowcount;
    
    commit;
    
    -- add in the LYTD values ...
    merge /*+ parallel(TGT,4) */ into dwh_performance.rtl_item_wk_sales_ytd TGT   
       using (
                select  /*+ parallel(6) */
                        SK1_ITEM_NO,
                        FIN_YEAR_NO,
                        FIN_WEEK_NO,
                        SALES_YTD,
                        SALES_QTY_YTD,
                        SALES_MARGIN_YTD
    
                from    dwh_performance.rtl_item_wk_sales_ytd  a
                where   FIN_YEAR_NO = v_CYR-1
                and     FIN_WEEK_NO = v_CWK
                
               ) SRC
       on (TGT.SK1_ITEM_NO = SRC.SK1_ITEM_NO and TGT.FIN_WEEK_NO = SRC.FIN_WEEK_NO and TGT.FIN_YEAR_NO = v_CYR)
       
       when matched then update
       set  TGT.SALES_LYTD        = SRC.SALES_YTD,
            TGT.SALES_QTY_LYTD    = SRC.SALES_QTY_YTD,
            TGT.SALES_MARGIN_LYTD = SRC.SALES_MARGIN_YTD;
            
       g_recs_updated := sql%rowcount; 

--    DBMS_OUTPUT.put_line ('Current WK: ' || CWK || ' - Op. Balance rows created: ' || l_ins);          
--    DBMS_OUTPUT.put_line ('    - LYTD Merge done: ' || l_upd);

    l_text := 'Current WK: ' || v_CWK || ' - Op. Balance rows created: ' || g_recs_inserted || '    - LYTD Merge done: ' || g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    commit;
    
    --------------------------------------------------------------------    
    -- refresh last 6 weeks data plus the current week just loaded ...   
    g_recs_inserted := 0;
    g_recs_updated  := 0;
    
    for i in 1..7 LOOP
       merge into dwh_performance.rtl_item_wk_sales_ytd TGT
        using 
        (
            with 
            locs as 
            (
                select sk1_location_no,sk1_area_no,area_no
                from   dwh_performance.dim_location 
                where  area_no = 9951
            ),
            
            items as 
            (
               select /*+ parallel (a,4) */
                       a.item_no,a.sk1_item_no
                from   dim_item a
                join   rtl_item_sup_wk_wwbrand b on a.sk1_item_no = b.sk1_item_no and a.business_unit_no = 50
            --and a.sk1_item_no in (28842824, 28920758, 25945512, 28402659, 23883364, 23977512)
                group by 
                        a.item_no,a.sk1_item_no
            ),
                        
            cur_wk as 
            ( 
                select /*+ parallel (6) */        
                       a.sk1_item_no,
                       a.fin_year_no,
                       a.fin_week_no,
    
                       sum(nvl(a.sales,0))              sales,
                       sum(nvl(a.sales_qty,0))          sales_qty,
                       sum(nvl(a.sales_margin,0))       sales_margin
                                     
                from    rtl_loc_item_wk_rms_dense a
                join    locs  dl on (a.sk1_location_no = dl.sk1_location_no)
                join    items di on (a.sk1_item_no = di.sk1_item_no)
                where   a.fin_year_no = v_YR 
                and     a.fin_week_no = v_WK                 
                group by 
                        a.sk1_item_no, a.fin_year_no, a.fin_week_no 
            ),
                                  
            Hist as
            (
                 select /*+ parallel (6) */        
                        a.sk1_item_no,
                        a.fin_year_no,
                        a.fin_week_no,
                       
                       (nvl(a.sales_ytd,0))                     sales_ytd,
                       (nvl(a.sales_qty_YTD,0))                 sales_qty_ytd,
                       (nvl(a.sales_margin_ytd,0))              sales_margin_ytd
                       
                 from   dwh_performance.rtl_item_wk_sales_ytd a
                 where  fin_year_no = v_LWYR 
                 and    fin_week_no = v_LWWK
            )
                          
            select  /*+ parallel (4) */ 
                    a.sk1_item_no,
                    a.fin_year_no,
                    a.fin_week_no,
                    
                    a.sales                                     sales,
                    a.sales_qty                                 sales_qty,
                    a.sales_margin                              sales_margin,
                    
                    a.sales + nvl(b.sales_ytd,0)                sales_ytd,
                    a.sales_qty + nvl(b.sales_qty_ytd,0)        sales_qty_ytd,
                    a.sales_margin + nvl(b.sales_margin_ytd,0)  sales_margin_ytd
            from    cur_wk         a  
            left join    
                    hist           b on (a.sk1_item_no = b.sk1_item_no and a.FIN_YEAR_NO = B.FIN_YEAR_NO)
        ) SRC
          on (TGT.SK1_ITEM_NO = SRC.SK1_ITEM_NO and TGT.FIN_WEEK_NO = SRC.FIN_WEEK_NO and TGT.FIN_YEAR_NO = SRC.FIN_YEAR_NO)
          
          when matched then update
          set   TGT.SALES            = SRC.SALES,
                TGT.SALES_QTY        = SRC.SALES_QTY,
                TGT.SALES_MARGIN     = SRC.SALES_MARGIN,
                
                TGT.SALES_YTD        = SRC.SALES_YTD,
                TGT.SALES_QTY_YTD    = SRC.SALES_QTY_YTD,
                TGT.SALES_MARGIN_YTD = SRC.SALES_MARGIN_YTD,
                
                last_updated_date    = '01 Jan 1900'
          where TGT.SALES            <> SRC.SALES 
          or    TGT.SALES_QTY        <> SRC.SALES_QTY 
          or    TGT.SALES_MARGIN     <> SRC.SALES_MARGIN

          when not matched then insert                                                                                                          
          values                                                                                                           
             (          
              SRC.sk1_item_no,
              SRC.fin_year_no,
              SRC.fin_week_no,
              
              v_QRT,
              SRC.fin_year_no || v_QRT,
              v_HSDTE,
              
              SRC.sales,
              SRC.sales_qty,
              SRC.sales_margin,
              
              SRC.sales_ytd,
              SRC.sales_qty_ytd,
              SRC.sales_margin_ytd,
              
              0,0,0,
             '02 Jan 1900'
             );  
        commit;
        
        select count(*) + g_recs_updated
        into   g_recs_updated
        from dwh_performance.rtl_item_wk_sales_ytd 
        where LAST_UPDATED_DATE = '01 Jan 1900';
        
        select count(*) + g_recs_inserted
        into   g_recs_inserted 
        from   dwh_performance.rtl_item_wk_sales_ytd 
        where  LAST_UPDATED_DATE = '02 Jan 1900';
        
--        DBMS_OUTPUT.put_line ('            ' || WK || ' - Updates: ' || l_upd);
--        DBMS_OUTPUT.put_line ('            ' || WK || ' - Inserts: ' || l_ins);
        
--        select count(*) 
--        into   g_recs_inserted 
--        from   dwh_performance.rtl_item_wk_sales_ytd 
--        where  FIN_YEAR_NO = YR 
--        and    fin_week_no = WK;
        
--        DBMS_OUTPUT.put_line ('Tot rows for WK: ' || WK || ' - ' || l_ins);

    
        -- set last update data (G_DATE)  
        update  /*+ parallel (a,4) */ dwh_performance.rtl_item_wk_sales_ytd a 
        set     LAST_UPDATED_DATE = sysdate 
        where   FIN_YEAR_NO = v_YR 
        and     fin_week_no = v_WK 
        and     LAST_UPDATED_DATE in ('01 Jan 1900', '02 Jan 1900', '03 Jan 1900');
        commit;

        l_text := 'Refreshing WK: ' || v_WK || ' - rows created: ' || g_recs_inserted || '    - Update done: ' || g_recs_updated;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
        -- increment
        v_WK      := v_WK + 1;
        v_LWWK    := v_LWWK + 1;
        v_HSDTE   := v_HSDTE + 7;
        
        if (v_YR = 2019 and v_WK > 53) then 
            v_YR := v_YR + 1; 
            v_WK := 01;
        else 
            if (v_YR <> 2019 and v_WK > 52) then 
                v_YR := v_YR + 1; 
                v_WK := 01;
            end if;
        end if; 
        
        if  v_LWWK > 52 then 
            v_LWYR := v_LWYR + 1; 
            v_LWWK := 0;
        end if;   
        
    end loop;
    
    commit;
      
  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
 
end do_update;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

--    dbms_output.put_line('Execute Parallel ');
    execute immediate 'alter session enable parallel dml';
 
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
--    dbms_output.put_line('Control Date ');
    dwh_lookup.dim_control(g_date);
--    g_date := g_date - 7;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
--     select last_wk_fin_year_no, last_wk_fin_week_no, last_wk_start_date  
--     into   g_last_wk_fin_year_no, g_last_wk_fin_week_no, g_last_wk_start_date 
--     from dim_control;
     
     select fin_year_no, fin_week_no, this_week_start_date  , this_week_end_date
     into   g_last_wk_fin_year_no, g_last_wk_fin_week_no, g_last_wk_start_date , g_last_wk_end_date
     from   dim_calendar 
     where  calendar_date = g_date;
     
/*     select calendar_date
     into   g_calendar_date
     from   dim_calendar 
     where  calendar_date between g_last_wk_start_date and g_last_wk_end_date;
*/     
    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    l_text := 'Procedure do_update STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    dbms_output.put_line('Start Merge ');
    do_update;
   
    l_text := 'Procedure do_update COMPLETED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    dbms_output.put_line('End - Procedure do_update ');

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;                              --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;                                 --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;               --Bulk load--
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
end wh_prf_fpi_006u                                                                                             -- STORE PROC CHANGE 
;
