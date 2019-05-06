--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_129U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_129U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2014
--  Author:      Quentin Smit
--  Purpose:     Create Customer orders and ROQ fact table in the performance layer
--               with input ex JDA table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_ff_ord
--               Output - rtl_mc_loc_item_dy_om_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  02 Mar 2016 - B Kirschner: Add new ISO and Cust Orders interface to feed it's data into target fact table - rtl_mc_loc_item_dy_om_ord
--                from source FND table: DWH_FOUNDATION.FND_LOC_ITEM_DY_FF_CUST_ORD
--                Additional measure set required (4 measures in each set - QTY, CASES, SELLINF, COST)for categories of EMERGENCY, IN_STORE, ZERO_BOH, SCANNED (added to target fact table)
--                Added as a union within a WITH to original selection criteria in merge statement
--                Ref: BK02Mar2016
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria

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
g_rec_out            rtl_mc_loc_item_dy_om_ord%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_129U';
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

merge /*+ parallel(rtl_lidoo,8) */ into rtl_mc_loc_item_dy_om_ord rtl_lidoo USING

(
  with MainDriver as                                                           -- BK02Mar2016
    (
      select /*+ parallel(ord,8) parallel(ffd_ord,8) parallel(sdn,8) */         --index(prc,PK_P_RTL_LID_RMS_PRCF) index(dir,PK_P_RTL_LC_ITM_DY_STD_ORD)  */
                ord.LOCATION_NO,
                ord.ITEM_NO,
                ord.POST_DATE,
                ord.ROQ_QTY,
                0 as cust_order_cases,
                nvl(ord.WEIGH_IND,0) WEIGH_IND,
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
                nvl(prc.case_selling_excl_vat_opr,0) as case_selling_excl_vat,
                nvl(prc.case_selling_excl_vat_local,0) as case_selling_excl_vat_local,

                case nvl(ffd_ord.num_units_per_tray,1)
                   when 0 then 1
                   else nvl(ffd_ord.num_units_per_tray,1)
                end as num_units_per_tray,

                nvl(ffd_ord.num_units_per_tray2,1) as num_units_per_tray2,
                nvl(prc.reg_rsp_excl_vat_opr,0) as reg_rsp_excl_vat,
                nvl(prc.case_cost_price_opr,0) as case_cost,
                nvl(wac.wac_opr,0) as wac,
                nvl(prc.reg_rsp_excl_vat_local,0) as reg_rsp_excl_vat_local,
                nvl(prc.case_cost_price_local,0) as case_cost_local,
                nvl(wac.wac_local,0) as wac_local,


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
                      (round(nvl(ord.roq_qty,0) / 1)) * nvl(prc.case_selling_excl_vat_opr,0)
                   else
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / nvl(ffd_ord.num_units_per_tray,1)))  * nvl(prc.case_selling_excl_vat_opr,0)
                end as roq_selling,
 
                case nvl(ffd_ord.num_units_per_tray,1)
                   when 0 then
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / 1)) * nvl(prc.case_selling_excl_vat_local,0)
                   else
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / nvl(ffd_ord.num_units_per_tray,1)))  * nvl(prc.case_selling_excl_vat_local,0)
                end as roq_selling_local,                

                -- ROQ_COST
                --=========
                case nvl(ffd_ord.num_units_per_tray,1)
                   when 0 then
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / 1)) * nvl(prc.case_cost_price_opr,0)
                   else
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / nvl(ffd_ord.num_units_per_tray,1)))  * nvl(prc.case_cost_price_opr,0)
                end as roq_cost,
                
                case nvl(ffd_ord.num_units_per_tray,1)
                   when 0 then
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / 1)) * nvl(prc.case_cost_price_local,0)
                   else
                      -- roq_cases
                      (round(nvl(ord.roq_qty,0) / nvl(ffd_ord.num_units_per_tray,1)))  * nvl(prc.case_cost_price_local,0)
                end as roq_cost_local,                

                0 as cust_order_qty,                                                                            -- BK02Mar2016
                0 as cust_order_selling,                                                                        -- BK02Mar2016
                0 as cust_order_cost,
                0 as cust_order_selling_local,                                                                        -- BK02Mar2016
                0 as cust_order_cost_local,                                                                           -- BK02Mar2016
-- BK02Mar2016

                -- SOD_BOH_SELLING
                --================
                case di.standard_uom_code
                  when 'EA' then
                     case di.random_mass_ind
                        when 1 then
                                        -- sod_boh_qty
                          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat_opr,0) * di.static_mass,0)
                        else            -- sod_boh_qty
                          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat_opr,0),0)
                     end
                  else            -- sod_boh_qty
                     nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat_opr,0),0)
                end as  sod_boh_selling,
                
                case di.standard_uom_code
                  when 'EA' then
                     case di.random_mass_ind
                        when 1 then
                                        -- sod_boh_qty
                          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat_local,0) * di.static_mass,0)
                        else            -- sod_boh_qty
                          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat_local,0),0)
                     end
                  else            -- sod_boh_qty
                     nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat_local,0),0)
                end as  sod_boh_selling_local,                

                -- SOD_BOH_COST
                nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(wac.wac_opr,0),0) as sod_boh_cost,
                nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(wac.wac_local,0),0) as sod_boh_cost_local,


                -- FF Cust Ord items - default to zeros ....
                0                       EMERGENCY_ORDER_CASES,
                0                       IN_STORE_ORDER_CASES,
                0                       ZERO_BOH_ORDER_CASES,
                0                       SCANNED_ORDER_CASES

         from   fnd_rtl_loc_item_dy_ff_ord ord
                join dim_item di             on ord.item_no                   = di.item_no
                join dim_location dl         on ord.location_no               = dl.location_no

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

               left outer join rtl_location_item prc
                                             on di.sk1_item_no                = prc.sk1_item_no
                                            and dl.sk1_location_no            = prc.sk1_location_no
                                            
               left outer join FND_MC_LOC_ITEM_DY_RMS_WAC wac    
                                             on ord.item_no                   = wac.item_no   
                                            and ord.location_no               = wac.location_no  
                                            and ord.post_date                 = wac.tran_date                                            
           
         where  ord.last_updated_date = g_date
         and    dl.region_no          = 9947   --MC AFRICA, INT FRANCHISE
     )


       select LOCATION_NO,
              ITEM_NO,
              POST_DATE,
              sum(ROQ_QTY)                  ROQ_QTY,
              sum(cust_order_cases)         cust_order_cases,
              max(WEIGH_IND) WEIGH_IND,
              LAST_UPDATED_DATE,
              sk1_item_no,
              standard_uom_code,
              sum(static_mass)              static_mass,
              sum(random_mass_ind)          random_mass_ind,
              business_unit_no,
              sk1_location_no,
              sk1_fd_zone_group_zone_no,
              sk2_item_no,
              sk2_location_no,
              sum(sod_boh_qty)              sod_boh_qty,
              avg(case_selling_excl_vat)    case_selling_excl_vat,
              avg(case_selling_excl_vat_local)    case_selling_excl_vat_local,
              avg(num_units_per_tray)       num_units_per_tray,
              sum(num_units_per_tray2)      num_units_per_tray2,
              sum(reg_rsp_excl_vat)         reg_rsp_excl_vat,
              sum(reg_rsp_excl_vat_local)   reg_rsp_excl_vat_local,
              avg(case_cost)                case_cost,
              avg(case_cost_local)          case_cost_local,
              sum(wac)                      wac,
              sum(wac_local)                wac_local,

              sum(boh_1_qty)                boh_1_qty,
              sum(sdn_qty)                  sdn_qty,
              sum(roq_cases)                roq_cases,
              sum(roq_selling)              roq_selling,
              sum(roq_cost)                 roq_cost,
              sum(cust_order_qty)           cust_order_qty,
              sum(cust_order_selling)       cust_order_selling,
              sum(cust_order_cost)          cust_order_cost,
              sum(sod_boh_selling)          sod_boh_selling,
              sum(sod_boh_cost)             sod_boh_cost,
              sum(EMERGENCY_ORDER_CASES)    EMERGENCY_ORDER_CASES,
              sum(IN_STORE_ORDER_CASES)     IN_STORE_ORDER_CASES,
              sum(ZERO_BOH_ORDER_CASES)     ZERO_BOH_ORDER_CASES,
              sum(SCANNED_ORDER_CASES)      SCANNED_ORDER_CASES,
              
              sum(roq_selling_local)              roq_selling_local,
              sum(roq_cost_local)                 roq_cost_local,
              sum(cust_order_selling_local)       cust_order_selling_local,
              sum(cust_order_cost_local)          cust_order_cost_local,
              sum(sod_boh_selling_local)          sod_boh_selling_local,
              sum(sod_boh_cost_local)             sod_boh_cost_local
              
       from   maindriver
       group by
              LOCATION_NO,
              ITEM_NO,
              POST_DATE,
              LAST_UPDATED_DATE,
              sk1_item_no,
              standard_uom_code,
              business_unit_no,
              sk1_location_no,
              sk1_fd_zone_group_zone_no,
              sk2_item_no,
              sk2_location_no
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
       cust_order_qty                  = mer_lidoo.cust_order_cases * mer_lidoo.num_units_per_tray,         -- BK02Mar2016
       cust_order_cases                = mer_lidoo.cust_order_cases,
       cust_order_selling              = mer_lidoo.cust_order_cases * mer_lidoo.case_selling_excl_vat,      -- BK02Mar2016
       cust_order_cost                 = mer_lidoo.cust_order_cases * mer_lidoo.case_cost,                  -- BK02Mar2016
       sod_boh_qty                     = mer_lidoo.sod_boh_qty,
       sod_boh_selling                 = mer_lidoo.sod_boh_selling,
       sod_boh_cost                    = mer_lidoo.sod_boh_cost,
       num_units_per_tray              = mer_lidoo.num_units_per_tray,
       num_units_per_tray2             = mer_lidoo.num_units_per_tray2,

       roq_selling_local                     = mer_lidoo.roq_selling_local,
       roq_cost_local                        = mer_lidoo.roq_cost_local,
       cust_order_selling_local              = mer_lidoo.cust_order_cases * mer_lidoo.case_selling_excl_vat_local,      -- BK02Mar2016
       cust_order_cost_local                 = mer_lidoo.cust_order_cases * mer_lidoo.case_cost_local,                  -- BK02Mar2016
       sod_boh_selling_local                 = mer_lidoo.sod_boh_selling_local,
       sod_boh_cost_local                    = mer_lidoo.sod_boh_cost_local,
       last_updated_date               = mer_lidoo.last_updated_date 

/*       -- BK02Mar2016 (c. start)
       EMERGENCY_order_qty             = mer_lidoo.EMERGENCY_order_cases * mer_lidoo.num_units_per_tray,
       EMERGENCY_order_cases           = mer_lidoo.EMERGENCY_order_cases,
       EMERGENCY_order_selling         = mer_lidoo.EMERGENCY_order_cases * mer_lidoo.case_selling_excl_vat,
       EMERGENCY_order_cost            = mer_lidoo.EMERGENCY_order_cases * mer_lidoo.case_cost,

       IN_STORE_order_qty              = mer_lidoo.IN_STORE_order_cases * mer_lidoo.num_units_per_tray,
       IN_STORE_order_cases            = mer_lidoo.IN_STORE_order_cases,
       IN_STORE_order_selling          = mer_lidoo.IN_STORE_order_cases * mer_lidoo.case_selling_excl_vat,
       IN_STORE_order_cost             = mer_lidoo.IN_STORE_order_cases * mer_lidoo.case_cost,

       ZERO_BOH_order_qty              = mer_lidoo.ZERO_BOH_order_cases * mer_lidoo.num_units_per_tray,
       ZERO_BOH_order_cases            = mer_lidoo.ZERO_BOH_order_cases,
       ZERO_BOH_order_selling          = mer_lidoo.ZERO_BOH_order_cases * mer_lidoo.case_selling_excl_vat,
       ZERO_BOH_order_cost             = mer_lidoo.ZERO_BOH_order_cases * mer_lidoo.case_cost,

       SCANNED_order_qty               = mer_lidoo.SCANNED_order_cases * mer_lidoo.num_units_per_tray,
       SCANNED_order_cases             = mer_lidoo.SCANNED_order_cases,
       SCANNED_order_selling           = mer_lidoo.SCANNED_order_cases * mer_lidoo.case_selling_excl_vat,
       SCANNED_order_cost              = mer_lidoo.SCANNED_order_cases * mer_lidoo.case_cost
       -- BK02Mar2016 (c. end) 
*/

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
       rtl_lidoo.num_units_per_tray2,
       
       rtl_lidoo. ROQ_SELLING_LOCAL,
       rtl_lidoo. ROQ_COST_LOCAL,
       rtl_lidoo. CUST_ORDER_SELLING_LOCAL,
       rtl_lidoo. CUST_ORDER_COST_LOCAL,
       rtl_lidoo. SOD_BOH_SELLING_LOCAL,
       rtl_lidoo. SOD_BOH_COST_LOCAL

/*        -- BK02Mar2016 (d. start)
       rtl_lidoo.EMERGENCY_ORDER_QTY,
       rtl_lidoo.EMERGENCY_ORDER_CASES,
       rtl_lidoo.EMERGENCY_ORDER_SELLING,
       rtl_lidoo.EMERGENCY_ORDER_COST,
       rtl_lidoo.IN_STORE_ORDER_QTY,
       rtl_lidoo.IN_STORE_ORDER_CASES,
       rtl_lidoo.IN_STORE_ORDER_SELLING,
       rtl_lidoo.IN_STORE_ORDER_COST,
       rtl_lidoo.ZERO_BOH_ORDER_QTY,
       rtl_lidoo.ZERO_BOH_ORDER_CASES,
       rtl_lidoo.ZERO_BOH_ORDER_SELLING,
       rtl_lidoo.ZERO_BOH_ORDER_COST,
       rtl_lidoo.SCANNED_ORDER_QTY,
       rtl_lidoo.SCANNED_ORDER_CASES,
       rtl_lidoo.SCANNED_ORDER_SELLING,
       rtl_lidoo.SCANNED_ORDER_COST
       -- BK02Mar2016 (d. end) */
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


       mer_lidoo.cust_order_cases * mer_lidoo.num_units_per_tray,         -- BK02Mar2016
       mer_lidoo.cust_order_cases,
       mer_lidoo.cust_order_cases * mer_lidoo.case_selling_excl_vat,      -- BK02Mar2016
       mer_lidoo.cust_order_cases * mer_lidoo.case_cost,                  -- BK02Mar2016

       mer_lidoo.sod_boh_qty,
       mer_lidoo.last_updated_date,
       mer_lidoo.sod_boh_selling,
       mer_lidoo.sod_boh_cost,
       mer_lidoo.num_units_per_tray,
       mer_lidoo.num_units_per_tray2,
       
       mer_lidoo. ROQ_SELLING_LOCAL,
       mer_lidoo. ROQ_COST_LOCAL,
       mer_lidoo.CUST_ORDER_CASES * MER_LIDOO.CASE_SELLING_EXCL_VAT_LOCAL,      
       mer_lidoo.CUST_ORDER_CASES * MER_LIDOO.CASE_COST_LOCAL,                 
       mer_lidoo. SOD_BOH_SELLING_LOCAL,
       mer_lidoo. SOD_BOH_COST_LOCAL

/*        -- BK02Mar2016 (e. start)
       mer_lidoo.EMERGENCY_order_cases * mer_lidoo.num_units_per_tray,
       mer_lidoo.EMERGENCY_order_cases,
       mer_lidoo.EMERGENCY_order_cases * mer_lidoo.case_selling_excl_vat,
       mer_lidoo.EMERGENCY_order_cases * mer_lidoo.case_cost,

       mer_lidoo.IN_STORE_order_cases * mer_lidoo.num_units_per_tray,
       mer_lidoo.IN_STORE_order_cases,
       mer_lidoo.IN_STORE_order_cases * mer_lidoo.case_selling_excl_vat,
       mer_lidoo.IN_STORE_order_cases * mer_lidoo.case_cost,

       mer_lidoo.ZERO_BOH_order_cases * mer_lidoo.num_units_per_tray,
       mer_lidoo.ZERO_BOH_order_cases,
       mer_lidoo.ZERO_BOH_order_cases * mer_lidoo.case_selling_excl_vat,
       mer_lidoo.ZERO_BOH_order_cases * mer_lidoo.case_cost,

       mer_lidoo.SCANNED_order_cases * mer_lidoo.num_units_per_tray,
       mer_lidoo.SCANNED_order_cases,
       mer_lidoo.SCANNED_order_cases * mer_lidoo.case_selling_excl_vat,
       mer_lidoo.SCANNED_order_cases * mer_lidoo.case_cost
       -- BK02Mar2016 (e. end) */
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

    l_text := 'LOAD OF rtl_mc_loc_item_dy_om_ord EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    Dwh_Lookup.Dim_Control(G_Date);
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
end wh_prf_mc_129u;
