--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_324SOPH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_324SOPH" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Aug 2017
--  Author:      Alastair de Wet
--  Purpose:     Create the SOP history - Once off load program
--  Tables:      Input  - cust_basket_item
--               Output - customer_store_of_pref
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;

g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number          ;
g_start_date         date         ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_324SOPH';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE SOP HISTORY - ONCE OFF';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'SOP HISTORY STARTED AT '||
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


--**************************************************************************************************
-- Main loop
--**************************************************************************************************


--    select fin_year_no,fin_quarter_no
--    into   g_yr_00,g_qt_00
--    from   dim_calendar
--    where  calendar_date = g_date - 80;

g_qt_00 := 5; 
g_yr_00 := 2014;
for sub in 1..4
loop
     g_qt_00 := g_qt_00 - 1;  
     
    if g_qt_00 = 1 then
      g_start_week := 1;
      g_end_week   := 13;
    end if; 
    if g_qt_00 = 2 then
      g_start_week := 14;
      g_end_week   := 26;
    end if; 
    if g_qt_00 = 3 then
      g_start_week := 27;
      g_end_week   := 39;
    end if; 
    if g_qt_00 = 4 then
      g_start_week := 40;
       SELECT MAX(FIN_WEEK_NO) INTO g_end_week FROM DIM_CALENDAR_WK WHERE FIN_YEAR_NO = g_yr_00;
    end if;     
    
    select calendar_date,fin_quarter_end_date 
    into   g_start_date,g_end_date
    from   dim_calendar 
    where  fin_day_no  = 1  
    and    fin_week_no = g_start_week
    and    fin_year_no = g_yr_00;

    l_text := 'ROLLUP DATE RANGE IS:- '||g_start_date||'  to '||g_end_date||' of '|| g_yr_00;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';


--=============================================================================================================================
-- create store of preference over time for C&H and Foods
--=============================================================================================================================
    l_text := ' STORE OF PREFERENCE OVER TIME CALC AND WRITE 1:- ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

     MERGE /*+ parallel(pref,4) */ INTO  customer_store_of_pref pref  using  (
     with     sop as  (
     select   /*+ FULL(cbi) parallel(cbi,8)  full(di) */ 
              cbi.location_no,
              cbi.primary_customer_identifier,
              sum(item_tran_selling-discount_selling) basket_value 
     from     cust_basket_item cbi, 
              dim_item di
     where    cbi.tran_date       between G_START_DATE and G_END_DATE
     and      cbi.item_no         = di.item_no  
     and      primary_customer_identifier not in (998,0)
     and      tran_type           not in ('P','N','L','R','Q')
     and      di.business_unit_no between 51 and 55    
     group by primary_customer_identifier,
              location_no
                      ),
              sop1 as (
     select   /*+ parallel(8) */ 
              primary_customer_identifier,
              max(basket_value) max_basket_value
     from     sop
     group by primary_customer_identifier
                       )
     select   /*+ parallel(8) */ 
              sop.primary_customer_identifier,
              min(sop.basket_value) basket_value,
              min(location_no) location_no
     from     sop, sop1
     where    sop.basket_value                = sop1.max_basket_value
     and      sop.primary_customer_identifier = sop1.primary_customer_identifier
     group by sop.primary_customer_identifier
     order by sop.primary_customer_identifier         ) mer_rec
 
   ON    (  pref.	primary_customer_identifier	 =	mer_rec.	primary_customer_identifier and
            pref. fin_year_no                  =  g_yr_00 and
            pref. fin_quarter_no               =  g_qt_00 and
            pref. fd_ch                        =  2)
   WHEN MATCHED THEN 
   UPDATE SET
            pref.	basket_value	        =	mer_rec.	basket_value	,
            pref.	location_no	          =	mer_rec.	location_no	,
            pref. last_updated_date     = g_date
            WHERE pref.	basket_value	        <>	mer_rec.	basket_value  
            OR    pref.	location_no	          <>	mer_rec.	location_no
   WHEN NOT MATCHED THEN
   INSERT
          (         
          fin_year_no,
          fin_quarter_no,
          primary_customer_identifier,
          fd_ch,   
          location_no	,
          basket_value,
          last_updated_date
          )
  values
          ( 
          g_yr_00,
          g_qt_00,
          mer_rec.	primary_customer_identifier,
          2,
          mer_rec.	location_no,
          mer_rec.	basket_value,
          g_date
          )           
          ; 

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
commit;          
--------------------------------------------------------------------------------  
--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUSTOMER_STORE_OF_PREF',estimate_percent=>1, DEGREE => 32);
--    COMMIT;

    l_text := ' STORE OF PREFERENCE OVER TIME CALC AND WRITE 2:- ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 


     MERGE  /*+ parallel(pref,4) */ INTO  customer_store_of_pref pref using (
     with     sop as  (
     select   /*+ FULL(cbi) parallel(cbi,8)  full(di) */ 
              cbi.location_no,
              cbi.primary_customer_identifier,
              sum(item_tran_selling-discount_selling) basket_value 
     from     cust_basket_item cbi, 
              dim_item di
     where    cbi.tran_date       between G_START_DATE and G_END_DATE
     and      cbi.item_no         = di.item_no  
     and      primary_customer_identifier not in (998,0)
     and      tran_type           not in ('P','N','L','R','Q')
     and      di.business_unit_no = 50    
     group by primary_customer_identifier,
              location_no
                      ),
              sop1 as (
     select   /*+ parallel(8) */ 
              primary_customer_identifier,
              max(basket_value) max_basket_value
     from     sop
     group by primary_customer_identifier
                       )
     select   /*+ parallel(8) */ 
              sop.primary_customer_identifier,
              min(sop.basket_value) basket_value,
              min(location_no) location_no
     from     sop, sop1
     where    sop.basket_value                = sop1.max_basket_value
     and      sop.primary_customer_identifier = sop1.primary_customer_identifier
     group by sop.primary_customer_identifier
     order by sop.primary_customer_identifier ) mer_rec
 
   ON    (  pref.	primary_customer_identifier	 =	mer_rec.	primary_customer_identifier and
            pref. fin_year_no                  =  g_yr_00 and
            pref. fin_quarter_no               =  g_qt_00 and
            pref. fd_ch                        =  1)
   WHEN MATCHED THEN 
   UPDATE SET
            pref.	basket_value	        =	mer_rec.	basket_value	,
            pref.	location_no	          =	mer_rec.	location_no	,
            pref. last_updated_date     = g_date
            WHERE pref.	basket_value	        <>	mer_rec.	basket_value  
            OR    pref.	location_no	          <>	mer_rec.	location_no
   WHEN NOT MATCHED THEN
   INSERT
          (         
          fin_year_no,
          fin_quarter_no,
          primary_customer_identifier,
          fd_ch,   
          location_no	,
          basket_value,
          last_updated_date
          )
  values
          ( 
          g_yr_00,
          g_qt_00,
          mer_rec.	primary_customer_identifier,
          1,
          mer_rec.	location_no,
          mer_rec.	basket_value,
          g_date
          )           
          ;            

        
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;
END LOOP;    

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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_cust_324soph;
