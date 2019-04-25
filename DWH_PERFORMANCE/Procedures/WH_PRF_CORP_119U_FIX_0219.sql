--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_119U_FIX_0219
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_119U_FIX_0219" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Januart 2019
--  Author:      Francisca de Vaal
--  Purpose:     Create future product status and future catalog status fact table
--  Tables:      Input  - fnd_loc_item_dy_fut_catlog,
--                      - fnd_zone_item_dy_fut_prodstat,
--                      - fnd_zone_item,
--                      - fnd_location_item,
--                      - dim_location,
--                      - dim_item,
--                      - dim_zone,
--                      - rtl_location_item,
--                      - dim_item_display_grp
--               Output - rtl_loc_item_dy_prodstatus
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
g_recs_duplicate     integer       :=  0;

g_recs_dummy         integer       :=  0;
g_physical_updated   integer       :=  0;

g_error_count        number        :=  0;
g_error_index        number        :=  0;

g_count              number        :=  0;
g_sub                number        :=  0;
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;
g_rec_out            rtl_loc_item_dy_rms_sparse%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_loop_date          date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate) - 92;
g_end_date           date          := trunc(sysdate) - 92;
g_yesterday          date          := trunc(sysdate) - 1;

g_last_wk_fin_year_no number(4);
g_last_wk_fin_Week_no number(2);
g_last_wk_start_date  date;
g_last_wk_end_date    date;
g_calendar_date       date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_119U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_LOC_ITEM_DY_PRODSTATUS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--************************************************************************************************** 
-- Merge process from data sources
--**************************************************************************************************
procedure do_merge_update as
begin

--   g_loop_date := g_date;
--   g_start_date := '27 DEC 2018';
   g_start_date := '9 jul 2018';

   for g_sub in 0..150
    LOOP
    g_recs_read := 0;

    G_START_DATE := G_START_DATE + 1;


   l_text       := '-------------------------------------------------------------';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text       := 'Datafix applied for:- '||g_start_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
   merge into  rtl_loc_item_dy_rms_sparse rtl
   using (
      with selitm as (
         select /*+ parallel (DI,8)  */ 
               di.sk1_item_no,
               di.vat_rate_perc,
               item_no
         from  dim_item di
         where di.business_unit_no        <> 50)
         
         select /*+ PARALLEL(ALOC,8) PARALLEL(LI,8) PARALLEL(DIH,8)) PARALLEL(DL,8) PARALLEL(DLH,8) PARALLEL(DI,8) */
               sum(nvl(aloc.alloc_qty,0)) ch_alloc_qty,
               release_date as post_date,
               di.sk1_item_no,
               dl.sk1_location_no,
          
            max(case when li.tax_perc is null then
                 (case when dl.vat_region_no = 1000 then
                    di.vat_rate_perc
                  else
                    dl.default_tax_region_no_perc
                  end)
                else 
                    li.tax_perc                                  
                end) as vat_rate_perc,

            max(case when vr.vat_rate_perc is null then                           
                  case when dl.vat_region_no = 1000 then
                            round(nvl(li.reg_rsp,0) * 100 / (100 + di.vat_rate_perc),2) 
                       else
                            round(nvl(li.reg_rsp,0) * 100 / (100 + dl.default_tax_region_no_perc),2)
                       end
                  else 
                            round(nvl(li.reg_rsp,0) * 100 / (100 + vr.vat_rate_perc),2)     
                  end) as reg_rsp_excl_vat
 
--            aloc.alloc_qty  * reg_rsp_excl_vat as ch_alloc_selling
          
         from  fnd_rtl_allocation aloc
          join selitm di on aloc.item_no                         = di.item_no 
          join dim_item_hist dih on aloc.item_no                 = dih.item_no  
           and aloc.release_date between dih.sk2_active_from_date and dih.sk2_active_to_date
          join dim_location dl on aloc.to_loc_no                 = dl.location_no 
          join dim_location_hist dlh on aloc.to_loc_no           = dlh.location_no 
           and aloc.release_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date 
          left outer join rtl_location_item li on di.sk1_item_no = li.sk1_item_no
           and dl.sk1_location_no  = li.sk1_location_no
          left join fnd_item_vat_rate vr on vr.item_no           = di.item_no 
           and vr.vat_region_no = dl.vat_region_no 
           and aloc.RELEASE_DATE between active_from_date and active_to_date             
 
         where aloc.release_date = G_START_DATE       --between g_start_date  and g_date 
          and  aloc.release_date is not null 
         group by release_date, di.sk1_item_no, dl.sk1_location_no
        
        ) mer_rec
         
    on (rtl.sk1_item_no	           =	mer_rec.sk1_item_no     and
        rtl.sk1_location_no        =	mer_rec.sk1_location_no and
        rtl.post_date              =	mer_rec.post_date         
        )          
    when matched then 
       update set
              rtl.ch_alloc_selling = (mer_rec.ch_alloc_qty * mer_rec.reg_rsp_excl_vat) ,
              rtl.ch_alloc_qty     =  mer_rec.ch_alloc_qty
       WHERE  rtl.ch_alloc_qty     <>  mer_rec.ch_alloc_qty 
--       AND    mer_rec.ch_alloc_qty <> 0   
              
    ;
            
    g_recs_read      :=  g_recs_read + sql%rowcount;
    g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;

   
    l_text := 'RECORDS PROCESSED :- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;

  end loop;
  
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
     
     select fin_year_no, fin_week_no, this_week_start_date  , this_week_end_date
     into   g_last_wk_fin_year_no, g_last_wk_fin_week_no, g_last_wk_start_date , g_last_wk_end_date
     from   dim_calendar 
     where  calendar_date = g_date;
    
    l_text := 'YEAR-WEEK PROCESSED IS:- '||g_last_wk_fin_year_no||' '||g_last_wk_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    do_merge_update;
   
    l_text := 'MERGE DONE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end wh_prf_corp_119u_fix_0219;
