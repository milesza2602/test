--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_016U_PRD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_016U_PRD" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        November 2012
--  Author:      A Joshua
--  Purpose:     Load Assort Location Actual A-Plan from foundation level.
--  Tables:      Input  - fnd_ast_loc_sc_wk_act
--               Output - rtl_loc_geo_grd_sc_wk_ast_act
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--create or replace PROCEDURE 
                                WH_PRF_CORP_265U_SHPD2 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  ROLLUP FOR shpd DATAFIX - wENDY - 13 SEP 2016
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
--  02 Mar 2016 - B Kirschner: Add new ISO and Cust Orders interface to feed it's data into target fact table - rtl_loc_item_dy_om_ord
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;
g_partition_name       varchar2(2000) ;
g_fin_year_no        number        :=  0;
g_fin_month_no        number        :=  0;
g_fin_week_no        number        :=  0;   
g_sql_trunc_partition  varchar2(2000) ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_265U_SHPD2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS DENSE PERFORMANCE to WEEK';
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

    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_dense EX DAY LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
 --   mindate=10/DEC/13 - to 12 sep 2016 = 145, - 6 = 139 hence 138
--    G_DATE := g_date - 48;
    G_DATE := '8 JUNE 2015';
--    G_DATE := g_date - 800;
    l_text := 'Derived ----->>>>BATCH DATE BEING PROCESSED  - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    l_text := 'STARTING GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_PROM_LOC_ITEM_DY', DEGREE => 32);
    commit;
    
    l_text := 'DONE GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 


 --         select min(this_week_start_date) , max(this_week_end_date)
 --         into   g_start_date, g_end_date
 --         from   dim_calendar
 --         where  calendar_date between  g_date - 7 and g_date;
          
          select min(this_week_start_date) , max(this_week_end_date)
          into   g_start_date, g_end_date
          from   dim_calendar
          where  calendar_date = g_date;

          l_text := 'g_start='||g_start_date||' - '||g_end_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

for g_sub in 0..77 loop
--for g_sub in 0..137 loop
--for g_sub in 0..68 loop
--for g_sub in 0..2 loop
          g_start_date := g_start_date - 7;
          g_end_date := g_end_date - 7;


 merge  into rtl_loc_item_dy_om_ord rtl_lidoo USING
--merge /*+ parallel(rtl_lidoo,8) */ into rtl_loc_item_dy_om_ord rtl_lidoo USING
--merge /*+ parallel(rtl_lidoo,4) */ into W6005682.RTL_LOC_ITEM_DY_OM_ORDQ rtl_lidoo USING
(
  with MainDriver as                                                           -- BK02Mar2016
    (
      select /*+ parallel(ord,8) parallel(ff_ord,8) parallel(sdn,8) */         --index(prc,PK_P_RTL_LID_RMS_PRCF) index(dir,PK_P_RTL_LC_ITM_DY_STD_ORD)  */
                ord.LOCATION_NO,
                ord.ITEM_NO,
                ord.POST_DATE,
                ord.ROQ_QTY,
--                ord.CUST_ORDER_CASES,
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
--                case nvl(ffd_ord.num_units_per_tray,1)
--                  when 0 then                -- num_units_per_tray
--                      ord.cust_order_cases * (round(nvl(ord.roq_qty,0)) / 1)
--                   else
--                      ord.cust_order_cases * (round(nvl(ord.roq_qty,0)) / nvl(ffd_ord.num_units_per_tray,1))
--                end as cust_order_qty,
                0 as cust_order_qty,                                                                            -- BK02Mar2016
                --CUST_ORDER_SELLING
                --==================
--                ord.cust_order_cases * nvl(prc.case_selling_excl_vat,0) as cust_order_selling,
                0 as cust_order_selling,                                                                        -- BK02Mar2016

               --CUST_ORDER_COST
               --================
--               ord.cust_order_cases * nvl(prc.case_cost,0) AS cust_order_cost,
               0 as cust_order_cost,                                                                            -- BK02Mar2016

                -- SOD_BOH_SELLING
                --================
                case di.standard_uom_code
                  when 'EA' then
                     case di.random_mass_ind
                        when 1 then
                                        -- sod_boh_qty
                          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat,0) * di.static_mass,0)
                        else            -- sod_boh_qty
                          nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat,0),0)
                     end
                  else            -- sod_boh_qty
                     nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.reg_rsp_excl_vat,0),0)
                end as  sod_boh_selling,

                -- SOD_BOH_COST
                nvl((nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)) * nvl(prc.wac,0),0) as sod_boh_cost,

                -- FF Cust Ord items - default to zeros ....
                0                       EMERGENCY_ORDER_CASES,
                0                       IN_STORE_ORDER_CASES,
                0                       ZERO_BOH_ORDER_CASES,
                0                       SCANNED_ORDER_CASES

         from   fnd_rtl_loc_item_dy_ff_ord ord
                join dim_item di             on ord.item_no                   = di.item_no
                join dim_location dl         on ord.location_no               = dl.location_no
--                join fnd_jdaff_dept_rollout d on d.department_no              = di.department_no

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

         where  ord.last_updated_date = g_date
--           and  d.department_live_ind = 'Y'
    ),

  -- BK02Mar2016 (b. start)
  Custorders as
    (
       select /*+ parallel(co,8) parallel(prc,8) parallel(ffd_ord,8) full (di), full (dl), full (dih) */
              co.LOCATION_NO,
              co.ITEM_NO,
              co.POST_DATE,

              0                                 ROQ_QTY,
              nvl(co.CUST_ORDER_CASES, 0)       CUST_ORDER_CASES,
              0                                 WEIGH_IND,
              co.LAST_UPDATED_DATE,
              di.sk1_item_no,
              di.standard_uom_code,
              di.static_mass,
              di.random_mass_ind,
              di.business_unit_no,
              dl.sk1_location_no,
              dl.sk1_fd_zone_group_zone_no,
              dih.sk2_item_no,
              dlh.sk2_location_no,

              0                                 sod_boh_qty,
              nvl(prc.case_selling_excl_vat,0)  case_selling_excl_vat,
              case nvl(ffd_ord.num_units_per_tray,1)
                 when 0 then 1
                 else nvl(ffd_ord.num_units_per_tray,1)
              end                               num_units_per_tray,
              0                                 NUM_UNITS_PER_TRAY2,
              0                                 reg_rsp_excl_vat,
              nvl(prc.case_cost,0)              case_cost,
              0                                 wac,

              0                                 boh_1_qty,
              0                                 sdn_qty,
              0                                 roq_cases,
              0                                 roq_selling,
              0                                 roq_cost,

              0                                 cust_order_qty,
              0                                 cust_order_selling,
              0                                 cust_order_cost,
              0                                 sod_boh_selling,
              0                                 sod_boh_cost,

              nvl(co.EMERGENCY_ORDER_CASES,0)   EMERGENCY_ORDER_CASES,
              nvl(co.IN_STORE_ORDER_CASES, 0)   IN_STORE_ORDER_CASES,
              nvl(co.ZERO_BOH_CASES, 0)         ZERO_BOH_ORDER_CASES,
              nvl(co.SCANNED_ORDER_CASES, 0)    SCANNED_ORDER_CASES

       from   dwh_foundation.FND_LOC_ITEM_DY_FF_CUST_ORD co
       join   dim_item di         on co.item_no     = di.item_no
       join dim_location dl       on co.location_no = dl.location_no
       join dim_item_hist dih     on co.item_no     = dih.item_no and co.post_date     between dih.sk2_active_from_date and dih.sk2_active_to_date
       join dim_location_hist dlh on co.location_no = dlh.location_no and co.post_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date

--       left join fnd_loc_item_dy_ff_ord ffd_ord on di.item_no     = ffd_ord.item_no and dl.location_no     = ffd_ord.location_no and (co.post_date - 1) = ffd_ord.post_date  -- BK02Mar2016
       left join fnd_location_item ffd_ord on di.item_no     = ffd_ord.item_no and dl.location_no     = ffd_ord.location_no                                                    -- BK02Mar2016
       left join RTL_LID_RMS_PRICE_FF prc     on di.sk1_item_no = prc.sk1_item_no and dl.sk1_location_no = prc.sk1_location_no and (co.post_date - 1) = prc.calendar_date

       where  co.last_updated_date = g_date
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
              avg(num_units_per_tray)       num_units_per_tray,
              sum(num_units_per_tray2)      num_units_per_tray2,
              sum(reg_rsp_excl_vat)         reg_rsp_excl_vat,
              avg(case_cost)                case_cost,
              sum(wac)                      wac,

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
              sum(SCANNED_ORDER_CASES)      SCANNED_ORDER_CASES
       from (
               select   *
               from     maindriver
               union
               select   *
               from     custorders 
            )
       group by
              LOCATION_NO,
              ITEM_NO,
              POST_DATE,
          --   WEIGH_IND,
              LAST_UPDATED_DATE,
              sk1_item_no,
              standard_uom_code,
              business_unit_no,
              sk1_location_no,
              sk1_fd_zone_group_zone_no,
              sk2_item_no,
              sk2_location_no
       -- BK02Mar2016 (b. end)
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
--       cust_order_qty                  = mer_lidoo.cust_order_qty,                                        -- BK02Mar2016
       cust_order_qty                  = mer_lidoo.cust_order_cases * mer_lidoo.num_units_per_tray,         -- BK02Mar2016
       cust_order_cases                = mer_lidoo.cust_order_cases,
--       cust_order_selling              = mer_lidoo.cust_order_selling,                                    -- BK02Mar2016
       cust_order_selling              = mer_lidoo.cust_order_cases * mer_lidoo.case_selling_excl_vat,      -- BK02Mar2016
--       cust_order_cost                 = mer_lidoo.cust_order_cost,                                       -- BK02Mar2016
       cust_order_cost                 = mer_lidoo.cust_order_cases * mer_lidoo.case_cost,                  -- BK02Mar2016
       sod_boh_qty                     = mer_lidoo.sod_boh_qty,
       sod_boh_selling                 = mer_lidoo.sod_boh_selling,
       sod_boh_cost                    = mer_lidoo.sod_boh_cost,
       num_units_per_tray              = mer_lidoo.num_units_per_tray,
       num_units_per_tray2             = mer_lidoo.num_units_per_tray2,
       last_updated_date               = mer_lidoo.last_updated_date,

       -- BK02Mar2016 (c. start)
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

        -- BK02Mar2016 (d. start)
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
       -- BK02Mar2016 (d. end)
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

--       mer_lidoo.cust_order_qty,
       mer_lidoo.cust_order_cases * mer_lidoo.num_units_per_tray,         -- BK02Mar2016
       mer_lidoo.cust_order_cases,
--       mer_lidoo.cust_order_selling,
       mer_lidoo.cust_order_cases * mer_lidoo.case_selling_excl_vat,      -- BK02Mar2016
--       mer_lidoo.cust_order_cost,
       mer_lidoo.cust_order_cases * mer_lidoo.case_cost,                  -- BK02Mar2016

       mer_lidoo.sod_boh_qty,
       mer_lidoo.last_updated_date,
       mer_lidoo.sod_boh_selling,
       mer_lidoo.sod_boh_cost,
       mer_lidoo.num_units_per_tray,
       mer_lidoo.num_units_per_tray2,

        -- BK02Mar2016 (e. start)
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
       -- BK02Mar2016 (e. end)
);
        g_recs_read := 0;
        g_recs_inserted :=  0;    
        g_recs_read := g_recs_read + SQL%ROWCOUNT;
        g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

          l_text := 'Period='||g_start_date||' - '||g_end_date||' Recs MERGED = '||g_recs_inserted;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 end loop;   
 
   

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

end wh_prf_corp_265U_SHPD2;
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
g_recs_inserted      integer       :=  0;
g_chain_corporate    integer       :=  0;
g_chain_franchise    integer       :=  0;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_ly_fin_week_no     dim_calendar.fin_week_no%type;
g_ly_fin_year_no     dim_calendar.fin_year_no%type;
g_lcw_fin_week_no    dim_calendar.fin_week_no%type;
g_lcw_fin_year_no    dim_calendar.fin_year_no%type;
g_date               date;
g_start_date         date;
g_end_date           date;
g_ly_start_date      date;
g_ly_end_date        date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_016U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ACTUALS ex AST FACT APLAN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit := p_forall_limit;
   end if;
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'LOAD OF RTL_LOC_GEO_GRD_SC_WK_AST_ACT STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
/*
   select (this_week_end_date - 6) - (7*6), this_week_end_date - 7
   into g_start_date, g_end_date
   from  dim_calendar
   WHERE CALENDAR_DATE = (SELECT TO_DATE(SYSDATE, 'dd mon yy') FROM DUAL);
   */

   SELECT  DISTINCT THIS_WEEK_END_DATE+1
   into g_start_date
   FROM  DIM_CALENDAR
   WHERE CALENDAR_DATE = g_date - 42;

  SELECT  DISTINCT THIS_WEEK_END_DATE
into g_end_date
   FROM  DIM_CALENDAR 
   where calendar_date = g_date;


   select min(ly_calendar_date), ly_fin_year_no , ly_fin_week_no, fin_week_no, fin_year_no
   into g_ly_start_date, g_ly_fin_year_no, g_ly_fin_week_no, g_lcw_fin_week_no, g_lcw_fin_year_no
   from dim_calendar
   where this_week_start_date = g_start_date
   group by ly_fin_year_no, ly_fin_week_no, fin_week_no, fin_year_no;
   
   select max(ly_calendar_date), ly_fin_year_no , ly_fin_week_no, fin_week_no, fin_year_no
   into g_ly_end_date, g_ly_fin_year_no, g_ly_fin_week_no, g_lcw_fin_week_no, g_lcw_fin_year_no
   from dim_calendar
   where this_week_end_date = g_end_date
   group by ly_fin_year_no, ly_fin_week_no, fin_week_no, fin_year_no;

   l_text := 'Current Data extract from '||g_start_date|| ' to '||g_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'LY Data extract from '||g_ly_start_date|| ' to '||g_ly_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'LY Fin Year '||g_ly_fin_year_no|| ' LY Fin Week '||g_ly_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'Last Completed Year '||g_lcw_fin_year_no|| ' Last Completed Week '||g_lcw_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (ast,6) */ into rtl_loc_geo_grd_sc_wk_ast_act ast

      with curr_data as (
      select /*+ full (apln) full (lsc) parallel (apln,4) parallel (lsc,4) */
             loc.sk1_location_no,
             geo.sk1_geography_no,
             nvl(lsc.grade_no,999) as sk1_grade_no,
             sc.sk1_style_colour_no,
             apln.fin_year_no,
             apln.fin_week_no,
             pln.sk1_plan_type_no,
             'W'||apln.fin_year_no||apln.fin_week_no as fin_week_code,
             apln.store_count,
             apln.sales_qty,
             apln.sales_cost,
             apln.sales,
             apln.target_stock_qty,
             apln.target_stock_cost,
             apln.target_stock_selling,
             g_date as last_updated_date
      from   fnd_ast_loc_sc_wk_act apln,
             fnd_ast_loc_sc_wk lsc,
             fnd_ast_location floc,
             dim_ast_lev1_diff1 sc,
             dim_location loc,
             dim_plan_type pln,
             dim_ast_geography geo,
             dim_calendar cal
      where apln.style_colour_no   = sc.style_colour_no
       and  apln.style_colour_no   = lsc.style_colour_no (+)
       and  apln.location_no       = lsc.location_no     (+)
       and  apln.fin_year_no       = lsc.fin_year_no     (+)
       and  apln.fin_week_no       = lsc.fin_week_no     (+)
       and  apln.location_no       = loc.location_no
       and  apln.location_no       = floc.location_no
       and  floc.geography_no      = geo.geography_no
       and  apln.plan_type_no      = pln.plan_type_no
       and  apln.fin_year_no       = cal.fin_year_no
       and  apln.fin_week_no       = cal.fin_week_no
       and  cal.fin_day_no         = 3
       and  apln.last_updated_date = g_date),
--      order by apln.fin_year_no, apln.fin_week_no) ,

      selcal as (
      select distinct fin_year_no, fin_week_no, this_week_start_date
      from dim_calendar dc
      where dc.this_week_start_date between g_ly_start_date and g_ly_end_date),
      
      ly_data as (
      select /*+ full (apln) full (lsc) parallel (apln,4) parallel (lsc,4) */
             loc.sk1_location_no,
             geo.sk1_geography_no,
             nvl(lsc.grade_no,999)                                           as sk1_grade_no,
             sc.sk1_style_colour_no,
             cal.fin_year_no                                                 as fin_year_no,
             cal.fin_week_no                                                 as fin_week_no,
             pln.sk1_plan_type_no,
             'W'||(cal.fin_year_no)||cal.fin_week_no                         as fin_week_code,
             apln.store_count,
             apln.sales_qty,
             apln.sales_cost,
             apln.sales,
             apln.target_stock_qty,
             apln.target_stock_cost,
             apln.target_stock_selling,
             g_date                                                          as last_updated_date
      from   fnd_ast_loc_sc_wk_act apln,
             fnd_ast_loc_sc_wk lsc,
             fnd_ast_location floc,
             dim_ast_lev1_diff1 sc,
             dim_location loc,
             dim_plan_type pln,
             dim_ast_geography geo,
             dim_calendar cal,
             selcal sc
      where apln.style_colour_no   = sc.style_colour_no
       and  apln.style_colour_no   = lsc.style_colour_no (+)
       and  apln.location_no       = lsc.location_no     (+)
       and  apln.fin_year_no       = lsc.fin_year_no     (+)
       and  apln.fin_week_no       = lsc.fin_week_no     (+)
       and  apln.location_no       = loc.location_no
       and  apln.location_no       = floc.location_no
       and  floc.geography_no      = geo.geography_no
       and  apln.plan_type_no      = pln.plan_type_no
       and  apln.fin_year_no       = cal.ly_fin_year_no
       and  apln.fin_week_no       = cal.ly_fin_week_no
       and  cal.fin_day_no         = 3
       and  apln.fin_year_no       = sc.fin_year_no
       and  apln.fin_week_no       = sc.fin_week_no)
--       and  cal.this_week_start_date between g_ly_start_date and g_ly_end_date
--      order by apln.fin_year_no, apln.fin_week_no)

      select sk1_location_no,
             sk1_geography_no,
             sk1_grade_no,
             sk1_style_colour_no,
             fin_year_no,
             fin_week_no,
             sk1_plan_type_no,
             fin_week_code,
             sum(store_count)             store_count,
             sum(sales_qty)               sales_qty,
             sum(sales_cost)              sales_cost,
             sum(sales)                   sales,
             sum(sales_qty_ly)            sales_qty_ly,
             sum(sales_cost_ly)           sales_cost_ly,
             sum(sales_ly)                sales_ly,
             sum(target_stock_qty)        target_stock_qty,
             sum(target_stock_cost)       target_stock_cost,
             sum(target_stock_selling)    target_stock_selling,
             sum(target_stock_qty_ly)     target_stock_qty_ly,
             sum(target_stock_cost_ly)    target_stock_cost_ly,
             sum(target_stock_selling_ly) target_stock_selling_ly,
             g_date as last_updated_date
      from   (
         select /*+ full (extr1) full (extr2) parallel (extr1,4) parallel (extr2,4) */ 
             nvl(extr1.sk1_location_no,extr2.sk1_location_no)         as sk1_location_no,
             nvl(extr1.sk1_geography_no,extr2.sk1_geography_no)       as sk1_geography_no,
             nvl(extr1.sk1_grade_no,extr2.sk1_grade_no)               as sk1_grade_no,
             nvl(extr1.sk1_style_colour_no,extr2.sk1_style_colour_no) as sk1_style_colour_no,
             nvl(extr1.fin_year_no,extr2.fin_year_no)                 as fin_year_no,
             nvl(extr1.fin_week_no,extr2.fin_week_no)                 as fin_week_no,
             nvl(extr1.sk1_plan_type_no,extr2.sk1_plan_type_no)       as sk1_plan_type_no,
             nvl(extr1.fin_week_code,extr2.fin_week_code)             as fin_week_code,
             extr1.store_count                                        as store_count,
             extr1.sales_qty                                          as sales_qty,
             extr1.sales_cost                                         as sales_cost,
             extr1.sales                                              as sales,
             extr2.sales_qty                                          as sales_qty_ly,
             extr2.sales_cost                                         as sales_cost_ly,
             extr2.sales                                              as sales_ly,
             extr1.target_stock_qty                                   as target_stock_qty,
             extr1.target_stock_cost                                  as target_stock_cost,
             extr1.target_stock_selling                               as target_stock_selling,
             extr2.target_stock_qty                                   as target_stock_qty_ly,
             extr2.target_stock_cost                                  as target_stock_cost_ly,
             extr2.target_stock_selling                               as target_stock_selling_ly,
             g_date                                                   as last_updated_date
         from   curr_data extr1
         full outer join
             ly_data extr2 on
             extr1.sk1_location_no     = extr2.sk1_location_no
         and extr1.sk1_geography_no    = extr2.sk1_geography_no
         and extr1.sk1_grade_no        = extr2.sk1_grade_no
         and extr1.sk1_style_colour_no = extr2.sk1_style_colour_no
         and extr1.fin_year_no         = extr2.fin_year_no
         and extr1.fin_week_no         = extr2.fin_week_no )

         group by sk1_location_no, sk1_geography_no, sk1_grade_no, sk1_style_colour_no, fin_year_no, fin_week_no, sk1_plan_type_no, fin_week_code, g_date;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end wh_prf_ast_016u;
