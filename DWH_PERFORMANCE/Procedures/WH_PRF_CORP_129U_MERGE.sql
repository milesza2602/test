--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_129U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_129U_MERGE" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2014
--  Author:      Quentin Smit
--  Purpose:     Create Customer orders and ROQ fact table in the performance layer
--               with input ex JDA table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_ff_ord
--               Output - rtl_loc_item_dy_om_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_dy_om_ord%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_129M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUST ORDERS & ROQ FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin
    
--merge into rtl_loc_item_dy_om_ord rtl_lidoo USING
merge /*+ parallel(rtl_lidoo,4) */ into W6005682.RTL_LOC_ITEM_DY_OM_ORDQ rtl_lidoo USING
(select /*+ parallel(ord,4) parallel(ff_ord,4) parallel(sdn,4) */         --index(prc,PK_P_RTL_LID_RMS_PRCF) index(dir,PK_P_RTL_LC_ITM_DY_STD_ORD)  */
          ord.LOCATION_NO,
          ord.ITEM_NO,
          ord.POST_DATE,
          ord.ROQ_QTY,
          ord.CUST_ORDER_CASES,
          ord.WEIGH_IND,
          ord.LAST_UPDATED_DATE,
          di.sk1_item_no,
          di.standard_uom_code,
          di.static_mass,
          di.random_mass_ind,
          di.business_unit_no,
          dl.sk1_location_no,
          dl.sk1_fd_zone_group_zone_no,
          dih.sk2_item_no,
          dlh.sk2_location_no,
          nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)  as sod_boh_qty,  -- SDN_1_1   ==> RTL_LOC_ITEM_SO_SDN
                                                                        -- BOH_1_QTY ==> RTL_LOC_ITEM_DY_ST_DIR_ORD
          nvl(prc.case_selling_excl_vat,0) as case_selling_excl_vat,
          
          case nvl(ffd_ord.num_units_per_tray,1) 
             when 0 then 1
             else nvl(ffd_ord.num_units_per_tray,1) 
          end as num_units_per_tray,
          
          nvl(ffd_ord.num_units_per_tray2,1) as num_units_per_tray2,
          nvl(prc.reg_rsp_excl_vat,0) as reg_rsp_excl_vat,
          nvl(prc.case_cost,0) as case_cost,
          nvl(prc.wac,0) as wac,

          nvl(dir.boh_1_qty,0) boh_1_qty,
          nvl(sdn.sdn_qty,0) sdn_qty,
          
          
          -- ROQ_CASES
          --==========
          case nvl(ffd_ord.num_units_per_tray,1) 
             when 0 then 
                round(nvl(ord.roq_qty,0)) / 1
             else 
                round(nvl(ord.roq_qty,0)) / nvl(ffd_ord.num_units_per_tray,1) 
          end as roq_cases,
          
          
          -- ROQ_SELLING
          --============
          case nvl(ffd_ord.num_units_per_tray,1) 
             when 0 then 
                -- roq_cases                   
                (round(nvl(ord.roq_qty,0) / 1)) * nvl(prc.case_selling_excl_vat,0)
             else 
                -- roq_cases  
                (round(nvl(ord.roq_qty,0) / nvl(ffd_ord.num_units_per_tray,1)))  * nvl(prc.case_selling_excl_vat,0)
          end as roq_selling,
          
          
          -- ROQ_COST
          --=========
          case nvl(ffd_ord.num_units_per_tray,1) 
             when 0 then 
                -- roq_cases                   
                (round(nvl(ord.roq_qty,0) / 1)) * nvl(prc.case_cost,0)
             else 
                -- roq_cases  
                (round(nvl(ord.roq_qty,0) / nvl(ffd_ord.num_units_per_tray,1)))  * nvl(prc.case_cost,0)
          end as roq_cost,
          
          
          -- CUST_ORDER_QTY
          --===============
          case nvl(ffd_ord.num_units_per_tray,1) 
             when 0 then                -- num_units_per_tray
                ord.cust_order_cases * (round(nvl(ord.roq_qty,0)) / 1)
             else 
                ord.cust_order_cases * (round(nvl(ord.roq_qty,0)) / nvl(ffd_ord.num_units_per_tray,1))
          end as cust_order_qty,
          
          
          --CUST_ORDER_SELLING
          --==================
          ord.cust_order_cases * nvl(prc.case_selling_excl_vat,0) as cust_order_selling,
         
         
         --CUST_ORDER_COST
         --================
         ord.cust_order_cases * nvl(prc.case_cost,0) AS cust_order_cost,
          
          -- SOD_BOH_SELLING
          --================
          case di.standard_uom_code
            when 'EA' then
               case di.random_mass_ind
                  when 1 then
                                  -- sod_boh_qty
                    nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat,0) * di.static_mass,0)
               end
            else            -- sod_boh_qty
               nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat,0),0)
          end as  sod_boh_selling,
         
          -- SOD_BOH_COST
          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.wac,0),0) as sod_boh_cost
                    

   from   fnd_rtl_loc_item_dy_ff_ord ord
          join dim_item di             on ord.item_no                   = di.item_no
          join dim_location dl         on ord.location_no               = dl.location_no
          join fnd_jdaff_dept_rollout d on d.department_no              = di.department_no

          left outer join rtl_loc_item_so_sdn sdn on dl.sk1_location_no = sdn.sk1_location_no   --PK_RTL_LI_SO_SDN
                                      and di.sk1_item_no                = sdn.sk1_item_no
                                      and ord.post_date                 = sdn.receive_date

          left outer join rtl_loc_item_dy_st_dir_ord dir
                                       on dl.sk1_location_no            = dir.sk1_location_no
                                      and di.sk1_item_no                = dir.sk1_item_no
                                      and ord.post_date                 = dir.post_date

          join dim_item_hist dih       on ord.item_no                = dih.item_no
                                      and ord.post_date   between dih.sk2_active_from_date and dih.sk2_active_to_date
          join dim_location_hist dlh   on ord.location_no            = dlh.location_no
                                      and ord.post_date   between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   
          left outer join fnd_loc_item_dy_ff_ord ffd_ord
                                       on di.item_no                    = ffd_ord.item_no
                                      and dl.location_no                = ffd_ord.location_no
                                      and (ord.post_date - 1)           = ffd_ord.post_date

         left outer join RTL_LID_RMS_PRICE_FF prc
                                       on di.sk1_item_no                = prc.sk1_item_no
                                      and dl.sk1_location_no            = prc.sk1_location_no
                                      and (ord.post_date - 1)           = prc.calendar_date
                                      
   where  ord.last_updated_date      = g_date
     and  d.department_live_ind = 'Y'
     and di.sk1_item_no = 22566195 and dl.sk1_location_no = 419 and ord.post_date = '21/OCT/15'
     ) mer_lidoo
     
on  (rtl_lidoo.sk1_location_no  = mer_lidoo.sk1_location_no
and rtl_lidoo.sk1_item_no       = mer_lidoo.sk1_item_no
and rtl_lidoo.post_date         = mer_lidoo.post_date)

when matched then
update
set
       roq_qty                         = mer_lidoo.roq_qty,
       roq_cases                       = mer_lidoo.roq_cases,
       roq_selling                     = mer_lidoo.roq_selling,
       roq_cost                        = mer_lidoo.roq_cost,
       cust_order_qty                  = mer_lidoo.cust_order_qty,
       cust_order_cases                = mer_lidoo.cust_order_cases,
       cust_order_selling              = mer_lidoo.cust_order_selling,
       cust_order_cost                 = mer_lidoo.cust_order_cost,
       sod_boh_qty                     = mer_lidoo.sod_boh_qty,
       sod_boh_selling                 = mer_lidoo.sod_boh_selling,
       sod_boh_cost                    = mer_lidoo.sod_boh_cost,
       num_units_per_tray              = mer_lidoo.num_units_per_tray,
       num_units_per_tray2             = mer_lidoo.num_units_per_tray2,
       last_updated_date               = mer_lidoo.last_updated_date
       
when not matched then
insert
(      rtl_lidoo.sk1_location_no,
       rtl_lidoo.sk1_item_no,
       rtl_lidoo.post_date,
       rtl_lidoo.sk2_location_no,
       rtl_lidoo.sk2_item_no,
       rtl_lidoo.roq_qty,
       rtl_lidoo.roq_cases,
       rtl_lidoo.roq_selling,
       rtl_lidoo.roq_cost,
       rtl_lidoo.cust_order_qty,
       rtl_lidoo.cust_order_cases,
       rtl_lidoo.cust_order_selling,
       rtl_lidoo.cust_order_cost,
       rtl_lidoo.sod_boh_qty,
       rtl_lidoo.last_updated_date,
       rtl_lidoo.sod_boh_selling,
       rtl_lidoo.sod_boh_cost,
       rtl_lidoo.num_units_per_tray,
       rtl_lidoo.num_units_per_tray2
)
values
(      mer_lidoo.sk1_location_no,
       mer_lidoo.sk1_item_no,
       mer_lidoo.post_date,
       mer_lidoo.sk2_location_no,
       mer_lidoo.sk2_item_no,
       mer_lidoo.roq_qty,
       mer_lidoo.roq_cases,
       mer_lidoo.roq_selling,
       mer_lidoo.roq_cost,
       mer_lidoo.cust_order_qty,
       mer_lidoo.cust_order_cases,
       mer_lidoo.cust_order_selling,
       mer_lidoo.cust_order_cost,
       mer_lidoo.sod_boh_qty,
       mer_lidoo.last_updated_date,
       mer_lidoo.sod_boh_selling,
       mer_lidoo.sod_boh_cost,
       mer_lidoo.num_units_per_tray,
       mer_lidoo.num_units_per_tray2
);

    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
    g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
    g_recs_read :=  g_recs_read + SQL%ROWCOUNT;
      
   COMMIT;

   exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
       
end local_bulk_merge;


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

    l_text := 'LOAD OF rtl_loc_item_dy_om_ord EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    Dwh_Lookup.Dim_Control(G_Date);
    --g_date:= '21/MAR/15';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

    local_bulk_merge;


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
       raise;
end wh_prf_corp_129u_MERGE;
