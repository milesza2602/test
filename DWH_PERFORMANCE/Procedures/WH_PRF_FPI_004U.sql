--------------------------------------------------------
--  DDL for Procedure WH_PRF_FPI_004U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_FPI_004U" -- STORE PROC CHANGE 
  (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        April 2018
--  Author:      Francisca de Vaal
--               Extracting the woolworths branded item for the Sales items
--
--  Tables:      Input  - fnd_item_sup_spec_picklist,                                                       -- TABLE NAME CHANGE 
--                      - dim_item,dim_supplier
--                      - rtl_item_sup_wk_wwbrand, rtl_item_sup_wk_gbj_code                    
--               Output - rtl_item_sup_wk_gbj_code, rtl_item_sup_wk_gbj_brand                                       -- TABLE NAME CHANGE 
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

g_date                date          :=trunc(sysdate);
g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_fin_year_no         number(4);
g_fin_Week_no         number(2);
g_last_wk_start_date  date;
g_last_wk_end_date    date;
g_calendar_date       date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_FPI_004U';                              -- STORE PROC CHANGE
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_ITEM_SUP_WK_BRTH_LIST EX BRTH';    -- TABLE NAME CHANGE
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure do_merge_update as
begin
--/*+ first_rows parallel(rtl) parallel(upd_rec) */
 
   merge into  rtl_item_sup_wk_gbj_code tmp
   using (
       With PickList as (
                select distinct 
                      di.sk1_item_no
                     ,ds.sk1_Supplier_no
                     ,pl.item_no
                     ,pl.Supplier_no
                     ,pl.spec_version
                     ,upper(pl.attribute_code) as attribute_code
                     ,upper(pl.attribute_value) as attribute_value
                     ,case when pl.attribute_code  like 'GBJ%' then 1 else 0 end gbj_count
                from  fnd_item_sup_spec_picklist pl,
                      dim_item di,
                      dim_supplier ds
                where pl.item_no = di.item_no
                  and pl.Supplier_no = ds.Supplier_no
                  and di.business_unit_no = 50
                  and pl.attribute_code like 'GBJ%'
                order by pl.item_no,pl.Supplier_no,pl.spec_version,upper(pl.attribute_value)
                ),
            calendar_list as (
                select distinct fin_year_no, fin_week_no,fin_quarter_no,
                        fin_year_no||fin_quarter_no as fin_year_quarter_no,
                        this_week_start_date, this_week_end_date, calendar_date
                  from dwh_performance.dim_calendar
                where calendar_date = g_date
--                where calendar_date between '04 feb 19' and '10 feb 19'
--                where calendar_date between '07/JAN/2019' and '13/JAN/2019' -- Week 29
                )
                select distinct
                    bi.sk1_item_no
                    ,bi.sk1_supplier_no    
                    ,pl.spec_version
                    ,bi.spec_status
                    ,bi.spec_type
                    ,pl.attribute_code
                    ,gbj.actual_value as attribute_value
--                    ,max(pl.attribute_value) attribute_value
                    ,bi.spec_active_from_dte
                    ,bi.spec_active_to_dte
                    ,bi.fin_year_no
                    ,bi.fin_week_no
                    ,cal.fin_quarter_no
                    ,cal.fin_year_quarter_no                    
                    ,g_date as last_updated_date
                    ,nvl(pl.gbj_count,0)gbj_count
                from rtl_item_sup_wk_wwbrand bi
                    ,pickList pl
                    ,rtl_fpi_gbj_lookup gbj
                    ,calendar_list cal
                where bi.sk1_item_no     = pl.sk1_item_no
                  and bi.sk1_supplier_no = pl.sk1_supplier_no
                  and bi.spec_version    = pl.spec_version
                  and pl.attribute_code  = gbj.attribute_code
                  and bi.spec_type       = gbj.spec_type
                  and bi.fin_year_no     = cal.fin_year_no
                  and bi.fin_week_no     = cal.fin_week_no
                  and cal.calendar_date between bi.spec_active_from_dte and bi.spec_active_to_dte 
--                  group by bi.sk1_item_no, bi.sk1_supplier_no, pl.spec_version, bi.spec_status, bi.spec_type, 
--                    pl.attribute_code, bi.spec_active_from_dte, bi.spec_active_to_dte, bi.fin_year_no, bi.fin_week_no, 
--                    cal.fin_quarter_no, cal.fin_year_quarter_no, g_date, nvl(pl.gbj_count,0)
--                order by bi.fin_year_no,bi.fin_week_no,bi.sk1_item_no,bi.sk1_supplier_no,pl.spec_version,pl.attribute_code ,bi.spec_active_from_dte,bi.spec_active_to_dte
        ) mer_rec
         
   on    (tmp.sk1_item_no	      =	mer_rec.sk1_item_no     and
          tmp.sk1_supplier_no  	  =	mer_rec.sk1_supplier_no and 
          tmp.attribute_code  	  =	mer_rec.attribute_code  and
          tmp.attribute_VALUE  	  =	mer_rec.attribute_VALUE  and   --ADDED
          tmp.spec_version        =	mer_rec.spec_version    and 
          tmp.spec_TYPE         =	mer_rec.spec_TYPE     and --CHANGED FROM SPEC_STATUS
          tmp.fin_year_no	      =	mer_rec.fin_year_no     and   
          tmp.fin_week_no         =	mer_rec.fin_week_no
         ) 
            
   when matched then 
   update set                                                                                                      -- COLUNM NAME CHANGE 
--          tmp.attribute_value      =	mer_rec.attribute_value,
--          tmp.spec_type            =	mer_rec.spec_type,
          tmp.spec_STATUS            =	mer_rec.spec_STATUS,     -- ADDED
          tmp.spec_active_from_dte =	mer_rec.spec_active_from_dte,
          tmp.spec_active_to_dte   =	mer_rec.spec_active_to_dte, 
          tmp.fin_quarter_no       =	mer_rec.fin_quarter_no,
          tmp.fin_year_quarter_no         =	mer_rec.fin_year_quarter_no,
          tmp.gbj_count            =	mer_rec.gbj_count,
          tmp.last_updated_date    =    g_date
            
   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
         (sk1_item_no,
          sk1_supplier_no,
          spec_version,
          spec_status,
          attribute_code,
          attribute_value,
          spec_type,
          spec_active_from_dte,
          spec_active_to_dte,
          fin_year_no,          
          fin_week_no,
          fin_quarter_no,
          fin_year_quarter_no,
          gbj_count,
          last_updated_date
         )
  values                                                                                                           -- COLUNM NAME CHANGE 
         (          
          mer_rec.sk1_item_no,
          mer_rec.sk1_supplier_no,
          mer_rec.spec_version,
          mer_rec.spec_status,
          mer_rec.attribute_code,
          mer_rec.attribute_value,
          mer_rec.spec_type,
          mer_rec.spec_active_from_dte,
          mer_rec.spec_active_to_dte,
          mer_rec.fin_year_no,          
          mer_rec.fin_week_no,
          mer_rec.fin_quarter_no,
          mer_rec.fin_year_quarter_no,
          mer_rec.gbj_count,
          g_date
          )           
          ;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

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
 
end do_merge_update;
--**************************************************************************************************
-- Final Merge process
--**************************************************************************************************
procedure do_final_update as
begin
-- Only going to insert the GBJ items that got a GBJ code linked to it 
   merge into  rtl_item_sup_wk_gbj_brand tmp
   using (
   with calendar_list as (
                select distinct fin_year_no, fin_week_no,fin_quarter_no,
                        fin_year_no||fin_quarter_no as fin_year_quarter_no,
                        this_week_start_date, this_week_end_date, calendar_date
                  from dwh_performance.dim_calendar 
                where calendar_date = g_date    
--                where calendar_date between '04 feb 19' and '10 feb 19'
                ),
    GBJCode as (
       Select distinct
             ps.sk1_item_no
            ,ps.sk1_supplier_no
            ,ps.fin_year_no
            ,ps.fin_week_no
            ,ps.fin_quarter_no
            ,ps.fin_year_quarter_no            
            ,ps.brand
            ,ps.spec_version
            ,ps.spec_type
            ,gbj.attribute_code
            ,gbj.attribute_value
            ,nvl(gbj.gbj_count,0) gbj_count
      from   rtl_item_sup_wk_wwbrand ps
             ,rtl_item_sup_wk_gbj_code gbj
             ,calendar_list cal
     Where  ps.sk1_item_no     = gbj.sk1_item_no 
       and  ps.sk1_supplier_no = gbj.sk1_supplier_no 
       and  ps.fin_year_no     = gbj.fin_year_no 
       and  ps.fin_week_no     = gbj.fin_week_no 
       and  ps.spec_version    = gbj.spec_version 
       and  ps.spec_type       = gbj.spec_type
       and  ps.fin_year_no     = cal.fin_year_no
       and  ps.fin_week_no     = cal.fin_week_no
       )
       Select * from GBJCode
       ) mer_rec
         
   on    (tmp.sk1_item_no	    = mer_rec.sk1_item_no     and
          tmp.sk1_supplier_no   = mer_rec.sk1_supplier_no and
          tmp.fin_year_no	    = mer_rec.fin_year_no     and    
          tmp.fin_week_no       = mer_rec.fin_week_no     and
          tmp.spec_version	    = mer_rec.spec_version    and
          tmp.spec_type         = mer_rec.spec_type       and 
          tmp.attribute_code    = mer_rec.attribute_code
          )
            
   when matched then 
   update set                                                                                                      -- COLUNM NAME CHANGE 
          tmp.fin_quarter_no    = mer_rec.fin_quarter_no,
          tmp.fin_year_quarter_no      = mer_rec.fin_year_quarter_no,
          tmp.brand             = mer_rec.brand,
--          tmp.attribute_code    = mer_rec.attribute_code,
          tmp.attribute_value   = mer_rec.attribute_value,
          tmp.gbj_count         = mer_rec.gbj_count,
          tmp.last_updated_date = g_date
            
   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
         (sk1_item_no,
          sk1_supplier_no,
          fin_year_no,
          fin_week_no,
          fin_quarter_no,
          fin_year_quarter_no,
          spec_version,
          spec_type,
          attribute_code,
          attribute_value,
          brand,
          gbj_count,
          last_updated_date
         )
   values                                                                                                           -- COLUNM NAME CHANGE 
         (          
          mer_rec.sk1_item_no,
          mer_rec.sk1_supplier_no,
          mer_rec.fin_year_no,
          mer_rec.fin_week_no,
          mer_rec.fin_quarter_no,
          mer_rec.fin_year_quarter_no,
          mer_rec.spec_version,
          mer_rec.spec_type,
          mer_rec.attribute_code,
          mer_rec.attribute_value,
          mer_rec.brand,
          mer_rec.gbj_count,
          g_date
          )           
          ;  
             
   g_recs_updated := g_recs_updated +  sql%rowcount;       

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
 
end do_final_update;

--**************************************************************************************************
-- sub Final Merge process
--**************************************************************************************************
procedure do_subfinal_update as
begin
-- Only going to insert the items that do not have a GBJ code linked to it 
   merge into  rtl_item_sup_wk_gbj_brand tmp
   using (
   with calendar_list as (
                select distinct fin_year_no, fin_week_no,fin_quarter_no,
                        fin_year_no||fin_quarter_no as fin_year_quarter_no,
                        this_week_start_date, this_week_end_date, calendar_date
                  from dwh_performance.dim_calendar  
                where calendar_date = g_date  
--                where calendar_date between '04 feb 19' and '10 feb 19'
                ),
    GBJCode as (
       Select distinct
             ps.sk1_item_no
            ,ps.sk1_supplier_no
            ,ps.fin_year_no
            ,ps.fin_week_no
            ,ps.fin_quarter_no
            ,ps.fin_year_quarter_no            
            ,ps.brand
            ,ps.spec_version
            ,ps.spec_type
            ,gbj.attribute_code
            ,gbj.attribute_value
            ,nvl(gbj.gbj_count,0) gbj_count
      from   rtl_item_sup_wk_wwbrand ps
             ,rtl_item_sup_wk_gbj_code gbj
             ,calendar_list cal
     Where  ps.sk1_item_no     = gbj.sk1_item_no 
       and  ps.sk1_supplier_no = gbj.sk1_supplier_no 
       and  ps.fin_year_no     = gbj.fin_year_no 
       and  ps.fin_week_no     = gbj.fin_week_no 
       and  ps.spec_version    = gbj.spec_version 
       and  ps.spec_type       = gbj.spec_type
       and  ps.fin_year_no     = cal.fin_year_no
       and  ps.fin_week_no     = cal.fin_week_no
       ),
   MaxVersion as(
        select distinct   
            sk1_item_no,
            sk1_supplier_no,
            fin_year_no,
            fin_week_no,
            max(spec_version) spec_version
        from rtl_item_sup_wk_wwbrand 
        group by sk1_item_no, sk1_supplier_no, fin_year_no, fin_week_no
        ),
    NonGBJ as (
    SELECT distinct
        ps.sk1_item_no
       ,ps.sk1_supplier_no
       ,ps.fin_year_no
       ,ps.fin_week_no
       ,ps.fin_quarter_no
       ,ps.fin_year_quarter_no            
       ,ps.brand
       ,ps.spec_version
       ,ps.spec_status
       ,ps.spec_type
       ,nvl(gbj.attribute_code,'9999') attribute_code
       ,nvl(gbj.attribute_value,'NON_GBJ') attribute_value
       ,nvl(gbj.gbj_count,1) gbj_count
    FROM rtl_item_sup_wk_wwbrand ps
    join calendar_list cal
      on ps.fin_year_no     = cal.fin_year_no
     and ps.fin_week_no  = cal.fin_week_no
    join MaxVersion mv
      on ps.sk1_item_no     = mv.sk1_item_no 
     and ps.sk1_supplier_no = mv.sk1_supplier_no 
     and ps.fin_year_no     = mv.fin_year_no 
     and ps.fin_week_no     = mv.fin_week_no
     and ps.spec_version = mv.spec_version          
    left join rtl_item_sup_wk_gbj_code gbj --GBJCode gbj
           on ps.sk1_item_no     = gbj.sk1_item_no 
          and ps.sk1_supplier_no = gbj.sk1_supplier_no 
          and ps.fin_year_no     = gbj.fin_year_no 
          and ps.fin_week_no     = gbj.fin_week_no 
          and ps.spec_version    = gbj.spec_version 
          and ps.spec_type       = gbj.spec_type 
     where attribute_code is null 
--    order by ps.fin_year_quarter_no,ps.sk1_item_no
     ),  
     GBJDiff as (     
     select sk1_item_no,sk1_supplier_no,spec_version,spec_type, fin_year_no, fin_week_no, attribute_code  
     from NonGBJ
     minus
     select sk1_item_no,sk1_supplier_no,spec_version,spec_type, fin_year_no, fin_week_no, attribute_code 
     from GBJCode
     )
     select 
           b.sk1_item_no
          ,b.sk1_supplier_no
          ,b.fin_year_no
          ,b.fin_week_no
          ,b.fin_quarter_no
          ,b.fin_year_quarter_no
          ,b.brand
          ,b.spec_version
          ,b.spec_status
          ,b.spec_type
          ,b.attribute_code
          ,b.attribute_value
          ,b.gbj_count       
     from GBJDiff a
          ,NonGBJ b
     where a.sk1_item_no     = b.sk1_item_no
       and a.sk1_supplier_no = b.sk1_supplier_no
       and a.spec_version    = b.spec_version
       and a.spec_type       = b.spec_type
       and a.fin_year_no     = b.fin_year_no
       and a.fin_week_no     = b.fin_week_no
       ) mer_rec
         
   on    (tmp.sk1_item_no	    = mer_rec.sk1_item_no     and
          tmp.sk1_supplier_no   = mer_rec.sk1_supplier_no and
          tmp.fin_year_no	    = mer_rec.fin_year_no     and    
          tmp.fin_week_no       = mer_rec.fin_week_no     and
          tmp.spec_version	    = mer_rec.spec_version    and
          tmp.spec_type         = mer_rec.spec_type       and
          tmp.attribute_code    = mer_rec.attribute_code
          )
            
   when matched then 
   update set                                                                                                      -- COLUNM NAME CHANGE 
          tmp.fin_quarter_no    = mer_rec.fin_quarter_no,
          tmp.fin_year_quarter_no = mer_rec.fin_year_quarter_no,
          tmp.brand             = mer_rec.brand,
--          tmp.attribute_code    = mer_rec.attribute_code,
          tmp.attribute_value   = mer_rec.attribute_value,
          tmp.gbj_count         = mer_rec.gbj_count,
          tmp.last_updated_date = g_date
            
   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
         (sk1_item_no,
          sk1_supplier_no,
          fin_year_no,
          fin_week_no,
          fin_quarter_no,
          fin_year_quarter_no,
          spec_version,
          spec_type,
          attribute_code,
          attribute_value,
          brand,
          gbj_count,
          last_updated_date
         )
   values                                                                                                           -- COLUNM NAME CHANGE 
         (          
          mer_rec.sk1_item_no,
          mer_rec.sk1_supplier_no,
          mer_rec.fin_year_no,
          mer_rec.fin_week_no,
          mer_rec.fin_quarter_no,
          mer_rec.fin_year_quarter_no,
          mer_rec.spec_version,
          mer_rec.spec_type,
          mer_rec.attribute_code,
          mer_rec.attribute_value,
          mer_rec.brand,
          mer_rec.gbj_count,
          g_date
          )           
          ;  
             
   g_recs_updated := g_recs_updated +  sql%rowcount;       

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
 
end do_subfinal_update;



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

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
    select fin_year_no, fin_week_no
    into g_fin_year_no, g_fin_week_no
    from dwh_performance.dim_calendar  
    where calendar_date = g_date;
    
    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_fin_year_no||' '||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    l_text := 'INITIAL MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    dbms_output.put_line('Start Merge ');
    do_merge_update;
    
    l_text := 'INITIAL MERGE DONE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'FINAL MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    do_final_update;
    do_subfinal_update;
   
    l_text := 'FINAL MERGE DONE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    dbms_output.put_line('End Merge ');

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
end wh_prf_fpi_004u                                                                                         -- STORE PROC CHANGE 
;
