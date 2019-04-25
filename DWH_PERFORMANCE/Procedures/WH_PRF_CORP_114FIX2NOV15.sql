--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_114FIX2NOV15
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_114FIX2NOV15" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
-- DAVID JONES DATAFIX
--**************************************************************************************************
--  Date:        2 NOV 2015
--  Author:       WENDY LYTTLE
--  Purpose:     Create LIWk Dense rollup fact table in the performance layer
--               with input ex lid dense table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_dense
--               Output - rtl_loc_item_wk_rms_dense
--  Packages:    constants, dwh_log, dwh_valid
/*ROLLUP RANGE IS:- 27-APR-15  to 03-MAY-15
ROLLUP RANGE IS:- 04-MAY-15  to 10-MAY-15
ROLLUP RANGE IS:- 11-MAY-15  to 17-MAY-15
ROLLUP RANGE IS:- 18-MAY-15  to 24-MAY-15
ROLLUP RANGE IS:- 25-MAY-15  to 31-MAY-15
ROLLUP RANGE IS:- 01-JUN-15  to 07-JUN-15
ROLLUP RANGE IS:- 08-JUN-15  to 14-JUN-15
ROLLUP RANGE IS:- 15-JUN-15  to 21-JUN-15
ROLLUP RANGE IS:- 22-JUN-15  to 28-JUN-15
ROLLUP RANGE IS:- 29-JUN-15  to 05-JUL-15
ROLLUP RANGE IS:- 06-JUL-15  to 12-JUL-15
ROLLUP RANGE IS:- 13-JUL-15  to 19-JUL-15
ROLLUP RANGE IS:- 20-JUL-15  to 26-JUL-15
ROLLUP RANGE IS:- 27-JUL-15  to 02-AUG-15
ROLLUP RANGE IS:- 03-AUG-15  to 09-AUG-15
ROLLUP RANGE IS:- 10-AUG-15  to 16-AUG-15
ROLLUP RANGE IS:- 17-AUG-15  to 23-AUG-15
ROLLUP RANGE IS:- 24-AUG-15  to 30-AUG-15
*/

--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  20 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--  06 Aug 2009 - Replaced Merge with Insert into select from with a generic partition truncate prior to run.
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
g_recs_updated       NUMBER       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_TXsub                integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_114FIX2NOV15';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS sparse PERFORMANCE to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure insert_to_temp as
begin

      --for g_sub in 0..120 loop
      for g_sub in 0..120 loop
      
      g_date := g_date - 1;
      g_txsub := g_txsub + 1;
      
      if g_txsub = 11 then
          l_text := 'ROLLUP RANGE now at :'||g_date||' g_sub='||g_sub||' RECS='||G_RECS_inserted;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          g_txsub := 1;
      end if;
      
      insert /*+ append */ 
      into dwh_performance.tmp_loc_item_DY_rms_sparse
         with lid_list as
                            (
                             select /*+  parallel(shp,4)  */
                                  di.item_no,to_loc_no,actl_rcpt_date, dl1.sk1_location_no, di.sk1_item_no, dl1.chain_no
                             from   fnd_rtl_shipment shp,
                                    dim_location dl1,
                                    dim_location dl2,
                                    dim_item di
                             where  shp.last_updated_date  = g_date AND 
                                    (shp.to_loc_no          = dl1.location_no and dl1.loc_type            = 'W') and
                                    (shp.from_loc_no        = dl2.location_no and dl2.loc_type            = 'W') and
                                    shp.item_no             = di.item_no and
                                    shp.actl_rcpt_date     is not null 
                                     and (chain_code = 'DJ' OR (shp.from_loc_no in (207,400,305,4202,4201,4206)
                                    and shp.to_loc_no in (207,400,305,4202,4201,4206,212)))
                             group by di.item_no,to_loc_no,actl_rcpt_date, dl1.sk1_location_no, di.sk1_item_no, dl1.chain_no
                             )
      
         select /*+ full(di) full(dl) full(dlh) full(dih) parallel(fnd_li,4)  */
               sum(nvl(shp.sdn_qty,0)) as trunked_qty,
                sum(case when fnd_li.tax_perc is null then
                      (case when dl.vat_region_no = 1000 then
                           nvl(shp.sdn_qty,0) * (shp.reg_rsp * 100 / (100 + di.VAT_RATE_PERC)) 
                       else
                           nvl(shp.sdn_qty,0) * (shp.reg_rsp * 100 / (100 + dl.default_tax_region_no_perc)) 
                       end)
                    else 
                      nvl(shp.sdn_qty,0) * (shp.reg_rsp * 100 / (100 + fnd_li.tax_perc)) 
                 end) as trunked_selling,       
               sum(nvl(shp.sdn_qty,0) * shp.cost_price) as trunked_cost,
                shp.actl_rcpt_date POST_DATE,
                di.sk1_item_no,
                dl.sk1_location_no
         from   fnd_rtl_shipment shp
                join lid_list on shp.item_no                                          = lid_list.item_no        
                and shp.to_loc_no                                                     = lid_list.to_loc_no      
                and shp.actl_rcpt_date                                                = lid_list.actl_rcpt_date  
               
                join dim_item di on lid_list.item_no                           = di.item_no  
                join dim_location dl on lid_list.to_loc_no            = dl.location_no 
                left outer join rtl_location_item fnd_li on lid_list.sk1_item_no      = fnd_li.sk1_item_no
                                                        and lid_list.sk1_location_no  = fnd_li.sk1_location_no
         where shp.sdn_qty <> 0                  
               and shp.sdn_qty is not null           
         group by shp.actl_rcpt_date, di.sk1_item_no, dl.sk1_location_no
         ;
       
        g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
        commit;

    end loop;
    l_text := 'TEMP recs_inserted='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||'insert_to_temp';
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end insert_to_temp;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure merge_temp_to_sparse as
begin

    MERGE INTO dwh_performance.rtl_loc_item_DY_rms_sparse rtl_lidrs
    USING
          (   select * from dwh_performance.tmp_loc_item_DY_rms_sparse 
          ) mer_lidrs
              ON (rtl_lidrs.SK1_LOCATION_NO = mer_lidrs.SK1_LOCATION_NO
              AND rtl_lidrs.SK1_ITEM_NO = mer_lidrs.SK1_ITEM_NO
              AND rtl_lidrs.POST_DATE = mer_lidrs.POST_DATE
          )
    WHEN MATCHED THEN
        UPDATE SET   trunked_selling = mer_lidrs.trunked_selling,
        trunked_cost = mer_lidrs.trunked_cost,
        trunked_qty = mer_lidrs.trunked_qty
        ;
     
    g_recs_UPDATED :=  g_recs_UPDATED + SQL%ROWCOUNT;

    l_text := 'SPARSE recs_updated='||g_recs_UPDATED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||'merge_temp_to_sparse';
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end merge_temp_to_sparse;

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

    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_dense EX DAY LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
      g_date := '31 aug 2015';
 --   g_date := '11 may 2015';
      l_text := 'Start_date is '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
      execute immediate 'truncate table dwh_performance.tmp_loc_item_DY_rms_sparse';
      execute immediate 'alter session enable parallel dml';
      g_recs_inserted := 0;
      g_txsub := 0;
      
      insert_to_temp;
      
      g_recs_updated := 0;
      
      merge_temp_to_sparse;



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

end wh_prf_corp_114FIX2NOV15;
