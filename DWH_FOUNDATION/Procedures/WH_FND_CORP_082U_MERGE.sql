--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_082U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_082U_MERGE" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create location_item dimention table in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - stg_om_location_item_cpy
--               Output - fnd_location_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  21 march 2012 - defect 4605 - Composites Correction: Number of Units per tray for Composite Items
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
g_recs_duplicate      integer       :=  0;
g_recs_reset         integer       :=  0;
g_stg_count          integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_om_location_item_hsp.sys_process_msg%type;
g_rec_out            fnd_location_item%rowtype;
g_rec_in             stg_om_location_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

G_Pack_Type_ind      number := 0;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
G_Date               Date          := Trunc(Sysdate);
G_CNT                NUMBER := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_082U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION_ITEM MASTERDATA EX OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


g_location_no       stg_om_location_item_cpy.location_no%type; 
g_item_no           stg_om_location_item_cpy.item_no%TYPE; 


cursor stg_dup is
      select * from stg_om_location_item_cpy
      where (location_no,item_no)
      in
      (select location_no,item_no
      from stg_om_location_item_cpy 
      group by location_no,
      item_no
      having count(*) > 1) 
      order by location_no,
      item_no,
      sys_source_batch_id desc ,sys_source_sequence_no desc;
   
  

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin
--merge /*+ parallel (fli,6) */ into w6005682.fnd_location_item_qs fli using (
  merge /*+ parallel (fli,6) */ into FND_LOCATION_ITEM fli using (
     select /*+ PARALLEL(a,6) FULL(a) */
            a.SYS_SOURCE_BATCH_ID,
            a.SYS_SOURCE_SEQUENCE_NO,
            a.SYS_LOAD_DATE,
            a.SYS_PROCESS_CODE,
            a.SYS_LOAD_SYSTEM_NAME,
            a.SYS_MIDDLEWARE_BATCH_ID,
            a.SYS_PROCESS_MSG,
            a.ITEM_NO,
            a.LOCATION_NO,
            a.NEXT_WK_DELIV_PATTERN_CODE,
            a.THIS_WK_DELIV_PATTERN_CODE,
            a.THIS_WK_CATALOG_IND,
            a.NEXT_WK_CATALOG_IND,
            a.NUM_SHELF_LIFE_DAYS,
            
            -- UNITS PER TRAY LOGIC
            --=====================
            case a.weigh_ind
              when 1 then 
                --==========================================
                -- weigh_ind = 1, num_units_per_tray to be divided by 1000
                --===========================================
                case a.NUM_UNITS_PER_TRAY
                  when 0 then null
                  else
                    -- This only to be done if UPT is not null
                    --===============================================================================
                    -- if pack_item_ind = 0 AND pack_item_simple_ind = 0 then num_units_per_tray = 1
                    --===============================================================================
                    case fi.pack_item_ind 
                      when 1 then 
                        case fi.pack_item_simple_ind
                          when 0 then 1
                        else a.num_units_per_tray/1000
                        end
                    else a.num_units_per_tray/1000
                    end
                end
                
             else
                case a.NUM_UNITS_PER_TRAY
                  when 0 then null
                  else
                    -- This only to be done if UPT is not null
                    --===============================================================================
                    -- if pack_item_ind = 0 AND pack_item_simple_ind = 0 then num_units_per_tray = 1
                    --===============================================================================
                    case fi.pack_item_ind 
                      when 1 then 
                        case fi.pack_item_simple_ind
                          when 0 then 1
                        else a.num_units_per_tray
                        end
                    else a.num_units_per_tray
                    end
                end
            end num_units_per_tray,
            
            a.DIRECT_PERC,
            a.MODEL_STOCK,
            a.THIS_WK_CROSS_DOCK_IND,
            a.NEXT_WK_CROSS_DOCK_IND,
            a.THIS_WK_DIRECT_SUPPLIER_NO,
            a.NEXT_WK_DIRECT_SUPPLIER_NO,
            a.UNIT_PICK_IND,
            a.STORE_ORDER_CALC_CODE,
            a.SAFETY_STOCK_FACTOR,
            a.MIN_ORDER_QTY,
            a.SOURCE_DATA_STATUS_CODE,
            a.WEIGH_IND
       from stg_om_location_item_cpy a, 
            dim_item b, 
            fnd_jdaff_dept_rollout c,
            fnd_item fi
      where a.sys_process_code = 'Y'
        and a.item_no = b.item_no
        and b.department_no = c.department_no
        and c.department_live_ind = 'N'
        and fi.item_no = a.item_no
        and (weigh_ind in (1, 0)               or weigh_ind is null)
        and (this_wk_catalog_ind in (1, 0)    or this_wk_catalog_ind is null)
        and (next_wk_catalog_ind in (1, 0)     or next_wk_catalog_ind is null)
        and (NEXT_WK_CROSS_DOCK_IND in (1, 0)  or NEXT_WK_CROSS_DOCK_IND is null)
        and (THIS_WK_CROSS_DOCK_IND in (1, 0)  or THIS_WK_CROSS_DOCK_IND is null)
  
  ) mer_mart
  
  on (fli.item_no     = mer_mart.item_no
  and fli.location_no = mer_mart.location_no 
      )

when matched then
  update set
           next_wk_deliv_pattern_code      = mer_mart.next_wk_deliv_pattern_code,
           this_wk_deliv_pattern_code      = mer_mart.this_wk_deliv_pattern_code,
           this_wk_catalog_ind             = mer_mart.this_wk_catalog_ind,
           next_wk_catalog_ind             = mer_mart.next_wk_catalog_ind,
           num_shelf_life_days             = mer_mart.num_shelf_life_days,
           num_units_per_tray              = mer_mart.num_units_per_tray,
           direct_perc                     = mer_mart.direct_perc,
           model_stock                     = mer_mart.model_stock,
           this_wk_cross_dock_ind          = mer_mart.this_wk_cross_dock_ind,
           next_wk_cross_dock_ind          = mer_mart.next_wk_cross_dock_ind,
           this_wk_direct_supplier_no      = nvl(mer_mart.this_wk_direct_supplier_no,0),
           next_wk_direct_supplier_no      = nvl(mer_mart.next_wk_direct_supplier_no,0),
           unit_pick_ind                   = mer_mart.unit_pick_ind,
           store_order_calc_code           = mer_mart.store_order_calc_code,
           safety_stock_factor             = mer_mart.safety_stock_factor,
           min_order_qty                   = mer_mart.min_order_qty,
           weigh_ind                       = mer_mart.weigh_ind,
           last_updated_date               = g_date,
           primary_supplier_no             = 0,
           primary_country_code            = 'ZA'
    where (
            next_wk_deliv_pattern_code      <> mer_mart.next_wk_deliv_pattern_code or
            this_wk_deliv_pattern_code      <> mer_mart.this_wk_deliv_pattern_code or
            this_wk_catalog_ind             <> mer_mart.this_wk_catalog_ind or
            next_wk_catalog_ind             <> mer_mart.next_wk_catalog_ind or
            num_shelf_life_days             <> mer_mart.num_shelf_life_days or
            num_units_per_tray              <> mer_mart.num_units_per_tray or
            direct_perc                     <> mer_mart.direct_perc or
            model_stock                     <> mer_mart.model_stock or
            this_wk_cross_dock_ind          <> mer_mart.this_wk_cross_dock_ind or
            next_wk_cross_dock_ind          <> mer_mart.next_wk_cross_dock_ind or
            this_wk_direct_supplier_no      <> mer_mart.this_wk_direct_supplier_no or
            next_wk_direct_supplier_no      <> mer_mart.next_wk_direct_supplier_no or
            unit_pick_ind                   <> mer_mart.unit_pick_ind or
            weigh_ind                       <> mer_mart.weigh_ind or
            store_order_calc_code           <> mer_mart.store_order_calc_code or
            safety_stock_factor             <> mer_mart.safety_stock_factor or
            min_order_qty                   <> mer_mart.min_order_qty or
            next_wk_deliv_pattern_code      is null or
            this_wk_deliv_pattern_code      is null or
            this_wk_catalog_ind             is null or
            next_wk_catalog_ind             is null or
            num_shelf_life_days             is null or
            num_units_per_tray              is null or
            direct_perc                     is null or
            model_stock                     is null or
            this_wk_cross_dock_ind          is null or
            next_wk_cross_dock_ind          is null or
            this_wk_direct_supplier_no      is null or
            next_wk_direct_supplier_no      is null or
            unit_pick_ind                   is null or
            weigh_ind                       is null or
            store_order_calc_code           is null or
            safety_stock_factor             is null or
            min_order_qty                   is null
        ) 
      
when not matched then
  insert (
          ITEM_NO,                
          LOCATION_NO,            
          NEXT_WK_DELIV_PATTERN_CODE, 
          THIS_WK_DELIV_PATTERN_CODE,
          THIS_WK_CATALOG_IND,    
          NEXT_WK_CATALOG_IND,    
          NUM_SHELF_LIFE_DAYS,    
          NUM_UNITS_PER_TRAY,     
          DIRECT_PERC,            
          MODEL_STOCK,           
          THIS_WK_CROSS_DOCK_IND, 
          NEXT_WK_CROSS_DOCK_IND, 
          THIS_WK_DIRECT_SUPPLIER_NO, 
          NEXT_WK_DIRECT_SUPPLIER_NO, 
          UNIT_PICK_IND,          
          STORE_ORDER_CALC_CODE, 
          SAFETY_STOCK_FACTOR,    
          MIN_ORDER_QTY,          
          SOURCE_DATA_STATUS_CODE, 
          WEIGH_IND,
          LAST_UPDATED_DATE
          )
  values (
          mer_mart.ITEM_NO,                
          mer_mart.LOCATION_NO,            
          mer_mart.NEXT_WK_DELIV_PATTERN_CODE, 
          mer_mart.THIS_WK_DELIV_PATTERN_CODE,
          mer_mart.THIS_WK_CATALOG_IND,    
          mer_mart.NEXT_WK_CATALOG_IND,    
          mer_mart.NUM_SHELF_LIFE_DAYS,    
          mer_mart.NUM_UNITS_PER_TRAY,     
          mer_mart.DIRECT_PERC,            
          mer_mart.MODEL_STOCK,           
          mer_mart.THIS_WK_CROSS_DOCK_IND, 
          mer_mart.NEXT_WK_CROSS_DOCK_IND, 
          mer_mart.THIS_WK_DIRECT_SUPPLIER_NO, 
          mer_mart.NEXT_WK_DIRECT_SUPPLIER_NO, 
          mer_mart.UNIT_PICK_IND,          
          mer_mart.STORE_ORDER_CALC_CODE, 
          mer_mart.SAFETY_STOCK_FACTOR,    
          mer_mart.MIN_ORDER_QTY,          
          mer_mart.SOURCE_DATA_STATUS_CODE, 
          mer_mart.WEIGH_IND,
          G_DATE
          )
  ;
  
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


  commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end do_merge;
  
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

    l_text := 'LOAD OF FND_LOCATION_ITEM EX OM STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- set catalog indicators to zeros prior to start for all entries not on current catalog feed
--**************************************************************************************************
--This section is not advised as it will set all catalog indicators to 0 and if no input comes in
-- then we end up with nothing cataloged. Below update is changed to only run if a certain
-- no of records are present on the input staging ( > 700k)
-- Best solution is for OM to send the de-cataloged records when they 1st become de-cataloged.


    l_text := 'STARTING CLEARDOWN OF CATALOG INDICATORS '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select count(*) into g_stg_count from stg_om_location_item_cpy;

    if g_stg_count > 500000 then

      update /*+ parallel(li, 6) */ FND_LOCATION_ITEM li
      set    this_wk_catalog_ind = 0,
             next_wk_catalog_ind = 0,
             last_updated_date   = g_date
      where  (this_wk_catalog_ind = 1  or next_wk_catalog_ind = 1) and
           not exists
           (
             select /*+ parallel(stg, 6) */ 1
             from   stg_om_location_item_cpy stg
             where  stg.location_no = li.location_no and
                    stg.item_no     = li.item_no
           );

      g_recs_reset := SQL%ROWCOUNT;
      
      l_text := 'NO OF CATALOG INDICATORS SET TO 0 = '||g_recs_reset;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'FINISHED CLEARDOWN OF CATALOG INDICATORS '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   else
      l_text := 'NO CLEARDOWN AS INPUT VOLUME IS BELOW MINIMUM REQUIRED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
   
   commit;
   
   
  --**************************************************************************************************
  -- De Duplication of the staging table to avoid Bulk insert failures
  --************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no then
            update stg_om_location_item_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no    := dupp_record.location_no; 
        g_item_no        := dupp_record.item_no;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
   
   --==========================================--
   -- lets process the data now, shall we..  ? --
   --==========================================--
   l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   do_merge;
   
   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   insert /*+ APPEND parallel (hsp,2) */ into stg_om_location_item_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON INDICATORS / ITEM / LOCATION',
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.NEXT_WK_DELIV_PATTERN_CODE, 
                            TMP.THIS_WK_DELIV_PATTERN_CODE, 
                            TMP.THIS_WK_CATALOG_IND,    
                            TMP.NEXT_WK_CATALOG_IND,    
                            TMP.NUM_SHELF_LIFE_DAYS,    
                            TMP.NUM_UNITS_PER_TRAY,     
                            TMP.DIRECT_PERC,            
                            TMP.MODEL_STOCK,            
                            TMP.THIS_WK_CROSS_DOCK_IND, 
                            TMP.NEXT_WK_CROSS_DOCK_IND, 
                            TMP.THIS_WK_DIRECT_SUPPLIER_NO, 
                            TMP.NEXT_WK_DIRECT_SUPPLIER_NO, 
                            TMP.UNIT_PICK_IND,          
                            TMP.STORE_ORDER_CALC_CODE, 
                            TMP.SAFETY_STOCK_FACTOR,    
                            TMP.MIN_ORDER_QTY,          
                            TMP.SOURCE_DATA_STATUS_CODE, 
                            TMP.WEIGH_IND
                            
    from  stg_om_location_item_cpy  TMP 
    where ( tmp.weigh_ind not in (0,1)
         or tmp.this_wk_catalog_ind not in (1, 0)
         or next_wk_catalog_ind      not in (1, 0) 
         or NEXT_WK_CROSS_DOCK_IND   not in (1, 0) 
         or THIS_WK_CROSS_DOCK_IND   not in (1, 0)
         or
         not exists
          (select *
           from   fnd_item di
           where  tmp.item_no   = di.item_no )  
         or
         not exists
           (select *
           from   fnd_location dl
           where  tmp.location_no       = dl.location_no )
         )  
          and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;


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
       Raise;
end wh_fnd_corp_082u_merge;
