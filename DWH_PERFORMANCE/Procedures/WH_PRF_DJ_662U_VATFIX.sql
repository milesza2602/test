--------------------------------------------------------
--  DDL for Procedure WH_PRF_DJ_662U_VATFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_DJ_662U_VATFIX" 
                                                                                                                                                                                                                                                                                                                                                                                   
     (p_forall_limit in integer,p_success out boolean) 
     as
--**************************************************************************************************
--  Date:         11 SEPTEMBER 2015 -- NEW OPTIMISED VERSION
--  Author:       W LYTTLE
--  Purpose:      Load PO combination fact table in performance layer
--                with input ex Purchase Order and Shipment RMS tables from foundation layer.
--                The combination fact table combines measures from PO's and Shipments.
--                The program wh_prf_corp_661u only loads the PO info to a record keyed with the
--                not_before_date/not_after_date of the PO.
--                This program loads the Shipment info to a record keyed with the actl_rcpt_date of the Shipment.
--                If the not_before_date/not_after_date and actl_rcpt_date are equal,
--                then the PO and Shipment measures will be contained on the same record.
--                The PO measures must NOT be repeated on the records where shipments were received
--                on other dates than the PO not_before_date/not_after_date.
--                However, the PO static data, like not_before_date, not_after_date, po_status_code and cancel_code
--                are carried on all the records for the PO.
--                The table temp_po_list contains the POs updated during the selected period(today or last 5 weeks)
--  Tables:       Input  - fnd_rtl_purchase_order, fnd_rtl_shipment
--                Output - RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ
--  Packages:     constants, dwh_log, dwh_valid
--
--  Maintenance:
--  09 Jul 2010 - M Munnik
--                The join between Purchase Orders and Shipments is changed to include location in the join criteria.
--                This will prevent the join to have duplicate records in the resultset.
--                The test to exclude PO's with more than one location, (causing duplicates), has been removed from wh_prf_corp_660u.
--                There are records on the PO table with a null supply_chain_type.
--                A list is created in wh_prf_corp_660u, to get the distinct not null supply_chain_type per PO.
--                However, there are PO's where all supply_chain_type's for the PO are null. In this case, a null is selected.
--                The distinct supply_chain_type is then used for all the records of the PO, regardless of the item and location (in wh_prf_corp_661u and _662U_VATFIXD).
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
g_recs_deleted      integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ%rowtype;
g_date200    date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_DJ_662U_VATFIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'NEW ADDS SHIPMENT INFO TO PO COMBINATION FACT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ%rowtype index by binary_integer;
type tbl_array_u is table of RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- Before June 2007, it was allowed to completely cancel a Purchase Order
-- and then re-use the PO Number with a different location, not_before_date, etc. (therefore more than 1 location per PO).
-- The procedures joining Purchase Orders with Shipments, used to only join on po_no and item_no.
-- When doing this join on records where there are more than 1 location per PO, the resultset will contain duplicate records.
-- To prevent this, all PO's with more than one location (duplicate PO numbers), were excluded from all the wh_prf_corp_66* programs.
-- The join between Purchase Orders and Shipments were changed to include location in the join criteria.
-- First join to dim_location with the location_no on the PO. 
-- If the location on the PO is a store (loc_type 'S') then use location_no to join to Shipment else use wh_physical_wh_no to join to Shipment.
-- A decision was made and aggreed with Business, to only load PO's from Fin Year 2008 (starting at 25 June 2007) and onwards.

--- -- Case quantities can not contain fractions, the case quantity has to be an integer value (ie. 976.0).

--**************************************************************************************************
--
-- Do calculations and load into TEMP_POSHIP_CALC_DJ
--
--**************************************************************************************************

procedure INSERT_INTO_TEMP_POSHIP_CALC as
begin

INSERT /*+ APPEND */ INTO DWH_PERFORMANCE.TEMP_POSHIP_CALC_DJ
             
 

  with 
----------------------
------- shipments base  --------
----------------------
 selship as
 (
select         /*+ materialize parallel(s,4) full(TP)  */
                 s.po_no, s.item_no, s.to_loc_no, s.shipment_status_code,
                s.actl_rcpt_date, s.received_qty, s.reg_rsp, s.cost_price, s.du_id,
                tp.SK1_PO_NO,
                tp.SK1_ITEM_NO,
                tp.SUPPLY_CHAIN_TYPE
   from         fnd_rtl_shipment s
   join         dwh_performance.temp_po_list_DJ tp
   on           s.po_no    = tp.po_no
   and          s.item_no  = tp.item_no
   where        s.shipment_status_code = 'R'
   
   --and tp.po_no = 9787486 and tp.item_no = 6009101001798
 
 --  AND sk1_po_no = 18674306
--a-nd sk1_item_no = 229071
-- and sk1_location_no = 518
-- and s.actl_rcpt_date = '10 dec 2012'
     )  
     ,
--------------------------------
------- purchase orders --------
--------------------------------
  selpo as
(
   select    /*+ materialize parallel(p,4) full(tp)  */
                p.po_no, p.supply_chain_type po_supply_chain_type , p.item_no, 
                p.contract_no, p.supplier_no,  p.location_no, p.not_before_date, p.not_after_date, p.po_status_code, p.cancel_code,
                tp.SK1_PO_NO,
                tp.SK1_ITEM_NO,                     (case when p.chain_code is null then 0 else 1 end) sk1_chain_code_ind
   from      fnd_rtl_purchase_order p
    join      dwh_performance.temp_po_list_DJ tp           on  p.po_no              = tp.po_no
                                        and p.item_no            = tp.item_no
      where  p.chain_code = 'DJ' 
            --                            where  tp.po_no = 9787486 and tp.item_no = 6009101001798
 
--    AND sk1_po_no = 18674306
--and sk1_item_no = 229071
-- and sk1_location_no = 518
 --and s.actl_rcpt_date = '10 dec 2012'
   )
   ,
--------------------------------
------- shipments+purchase_orders --------
--------------------------------
selpoship as
(
select      /*+ materialize */   
                s.po_no
                , s.item_no
                , s.to_loc_no
                , s.actl_rcpt_date
                , s.received_qty
                , s.reg_rsp
                , s.cost_price
                , s.du_id
                , s.SK1_PO_NO
                , s.SK1_ITEM_NO
                , s.SUPPLY_CHAIN_TYPE
                , p.po_supply_chain_type 
                , p.contract_no
                , p.supplier_no
                , p.location_no
                , p.not_before_date
                , p.not_after_date
                , p.po_status_code
                , p.cancel_code
                , l.loc_type
                , l.chain_no
                , (case l.loc_type when 'S' then l.sk1_location_no else l.sk1_loc_no_wh_physical_wh_no end) sk1_location_no
                , l.VAT_REGION_NO
                , l.DEFAULT_TAX_REGION_NO_PERC
                , dlh.sk2_location_no
                ,sk1_chain_code_ind
   from selpo p 
    join      dwh_performance.dim_location l on p.location_no = l.location_no
        join      dim_location_hist dlh     on  p.location_no        = dlh.location_no
                                        and p.not_before_date    between dlh.sk2_active_from_date and dlh.sk2_active_to_date
    join       selship s
    on        p.po_no      = s.po_no 
    and       p.item_no    = s.item_no
    and       s.to_loc_no = (case l.loc_type when 'S' then l.location_no else l.wh_physical_wh_no end)

)

,
--------------------------------
------- joins --------
--------------------------------
seljoin as
 (
select         /*+ materialize parallel(li,4) parallel(ldd,4) full(di) full(dih)  full(dl) full(dlh) */
 
                ps.po_no, ps.item_no, ps.to_loc_no,
                ps.actl_rcpt_date, ps.received_qty, ps.reg_rsp, ps.cost_price, ps.du_id,
                ps.SK1_PO_NO,
                ps.SK1_ITEM_NO,
                ps.SUPPLY_CHAIN_TYPE,
                ps.po_supply_chain_type, 
                ps.contract_no, ps.supplier_no, ps.location_no, ps.not_before_date, ps.not_after_date, ps.po_status_code, ps.cancel_code,
                ps.loc_type, ps.chain_no,
                ps.sk1_location_no,
                nvl(dsc.sk1_supply_chain_no,0) sk1_supply_chain_no,
                nvl(dc.sk1_contract_no, 0) sk1_contract_no, 
                nvl(ds.sk1_supplier_no, 0) sk1_supplier_no,
                ps.sk2_location_no, 
                dih.sk2_item_no, 
                di.fd_discipline_type, 
                di.business_unit_no, 
                di.standard_uom_code,
                di.random_mass_ind, 
                case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                                then  di.static_mass
                                else 1
                                end static_mass,
                (decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray)) num_units_per_tray,
                ldd.debtors_commission_perc,
                NVL(LI.TAX_PERC, (case when ps.vat_region_no = 1000 then di.vat_rate_perc else ps.DEFAULT_TAX_REGION_NO_PERC end ) )  TAX_PERC
                ,PS.sk1_chain_code_ind
    from selpoship ps 
    join      dim_item di               on  di.item_no           = ps.item_no
    join      dim_item_hist dih         on  dih.item_no          = ps.item_no
                                        and ps.not_before_date    between dih.sk2_active_from_date and dih.sk2_active_to_date
    left join dim_supply_chain_type dsc on  ps.supply_chain_type = dsc.supply_chain_code
    left join dim_DJ_contract dc           on  ps.contract_no        = dc.contract_no
    left join dim_supplier ds           on  ps.supplier_no        = ds.supplier_no
    left join rtl_location_item li      on  li.sk1_item_no       = di.sk1_item_no
                                        and li.sk1_location_no   = ps.sk1_location_no
    left join rtl_loc_dept_dy ldd       on  ps.sk1_location_no   = ldd.sk1_location_no
                                        and di.sk1_department_no = ldd.sk1_department_no
                                        and ps.actl_rcpt_date     = ldd.post_date
                                        )
,
--------------------------------
------- calculations level 1 --------
--------------------------------
selcalc1 as ( 
SELECT   /*+ materialize */  sj.po_no, sj.item_no, sj.to_loc_no, 
                sj.actl_rcpt_date,  MAX(reg_rsp) REG_RSP, MAX(sj.cost_price) COST_PRICE,
                sj.SK1_PO_NO,
                sj.SK1_ITEM_NO,
                sj.SUPPLY_CHAIN_TYPE,
                sj.po_supply_chain_type, 
                sj.contract_no, 
                sj.supplier_no, 
                sj.location_no, 
                sj.not_before_date, 
                sj.not_after_date, 
                sj.po_status_code, 
                sj.cancel_code,
                sj.loc_type, sj.chain_no,
                sj.sk1_location_no,
                sj.sk1_supply_chain_no ,
                sj.sk1_contract_no , 
                sj.sk1_supplier_no ,
                sj.sk2_location_no, 
                sj.sk2_item_no, 
                sj.fd_discipline_type, 
                sj.business_unit_no, 
                sj.standard_uom_code,
                sj.random_mass_ind,
                sj.static_mass,
                sj.num_units_per_tray,
                sj.TAX_PERC, 
        --       sj.cnt_not_after,
       --        sj.cnt_not_before,
                count(distinct sj.du_id) num_du,
                avg(nvl(sj.debtors_commission_perc,0)) debtors_commission_perc, 
                sum(sj.received_qty) actl_grn_qty,
                sum(case when sj.business_unit_no = 50 and sj.standard_uom_code = 'EA' and sj.random_mass_ind = 1
                    then (round((sj.received_qty * (sj.reg_rsp * 100 / (100 + sj.tax_perc)) * sj.static_mass),2))
                    else (round((sj.received_qty * (sj.reg_rsp * 100 / (100 + sj.tax_perc))),2)) end) actl_grn_selling,
                sum(case when sj.business_unit_no = 50 and sj.standard_uom_code = 'EA' and sj.random_mass_ind = 1
                    then (round((sj.received_qty * sj.cost_price * sj.static_mass),2))
                    else (round((sj.received_qty * sj.cost_price),2)) end) actl_grn_cost
                    ,sk1_chain_code_ind
 from seljoin sj
 group by  sj.po_no, sj.item_no, sj.to_loc_no, 
                sj.actl_rcpt_date, 
                sj.SK1_PO_NO,
                sj.SK1_ITEM_NO,
                sj.SUPPLY_CHAIN_TYPE,
                sj.po_supply_chain_type, 
                sj.contract_no, sj.supplier_no, sj.location_no, sj.not_before_date, sj.not_after_date, sj.po_status_code, sj.cancel_code,
                sj.loc_type, sj.chain_no,
                sj.sk1_location_no,
                sj.sk1_supply_chain_no ,
                sj.sk1_contract_no , 
                sj.sk1_supplier_no ,
                sj.sk2_location_no, 
                sj.sk2_item_no, 
                sj.fd_discipline_type, 
                sj.business_unit_no,
                sj.standard_uom_code,
                sj.random_mass_ind,
                sj.static_mass,
                sj.num_units_per_tray,
                sj.TAX_PERC,
                sk1_chain_code_ind
                
                
                
  )
,
--------------------------------
------- calculations level 2 --------
--------------------------------
selcalc2A as ( 
SELECT /*+ materialize */ DISTINCT 
                sc1.actl_rcpt_date,  sc1.reg_rsp, sc1.cost_price,
                sc1.SK1_PO_NO,
                sc1.SK1_ITEM_NO,
                sc1.SUPPLY_CHAIN_TYPE,
                sc1.po_supply_chain_type, 
                sc1.contract_no, sc1.supplier_no, sc1.location_no, sc1.not_before_date, sc1.not_after_date, sc1.po_status_code, sc1.cancel_code,
                sc1.loc_type, sc1.chain_no,
                sc1.sk1_location_no,
                sc1.sk1_supply_chain_no ,
                sc1.sk1_contract_no , 
                sc1.sk1_supplier_no ,
                sc1.sk2_location_no, 
                sc1.sk2_item_no, 
                sc1.fd_discipline_type, sc1.business_unit_no, sc1.standard_uom_code,sc1.random_mass_ind,sc1.static_mass,
                sc1.num_units_per_tray,
                sc1.TAX_PERC, 
                sc1.num_du,
                sc1.debtors_commission_perc, 
                sc1.actl_grn_qty,
                sc1.actl_grn_selling,
                sc1.actl_grn_cost,
                case when sc1.business_unit_no = 50 then (sc1.actl_grn_qty/sc1.num_units_per_tray) end actl_grn_cases,
                NULL num_days_to_deliver_po,
                sk1_chain_code_ind
    from     selcalc1 sc1
     ),
selcalc2 as ( 
SELECT /*+ materialize */ DISTINCT 
                sc1.actl_rcpt_date,  sc1.reg_rsp, sc1.cost_price,
                sc1.SK1_PO_NO,
                sc1.SK1_ITEM_NO,
                sc1.SUPPLY_CHAIN_TYPE,
                sc1.po_supply_chain_type, 
                sc1.contract_no, sc1.supplier_no, sc1.location_no, sc1.not_before_date, sc1.not_after_date, sc1.po_status_code, sc1.cancel_code,
                sc1.loc_type, sc1.chain_no,
                sc1.sk1_location_no,
                sc1.sk1_supply_chain_no ,
                sc1.sk1_contract_no , 
                sc1.sk1_supplier_no ,
                sc1.sk2_location_no, 
                sc1.sk2_item_no, 
                sc1.fd_discipline_type, sc1.business_unit_no, sc1.standard_uom_code,sc1.random_mass_ind,sc1.static_mass,
                sc1.num_units_per_tray,
                sc1.TAX_PERC, 
                sc1.num_du,
                sc1.debtors_commission_perc, 
                sc1.actl_grn_qty,
                sc1.actl_grn_selling,
                sc1.actl_grn_cost,
                case when sc1.business_unit_no = 50 then (sc1.actl_grn_qty/sc1.num_units_per_tray) end actl_grn_cases,
                COUNT(DC.CALENDAR_DATE) num_days_to_deliver_po,
                sk1_chain_code_ind
    from     selcalc1 sc1,    
    DIM_CALENDAR DC
    WHERE fd_discipline_type in('SA','SF') and not_after_date is not null
        AND fin_day_no not in(6,7)
        AND rsa_public_holiday_ind = 0 
        and  calendar_date between not_after_date and ACTL_RCPT_DATE
    GROUP BY   sc1.actl_rcpt_date,  sc1.reg_rsp, sc1.cost_price,
                sc1.SK1_PO_NO,
                sc1.SK1_ITEM_NO,
                sc1.SUPPLY_CHAIN_TYPE,
                sc1.po_supply_chain_type, 
                sc1.contract_no, sc1.supplier_no, sc1.location_no, sc1.not_before_date, sc1.not_after_date, sc1.po_status_code, sc1.cancel_code,
                sc1.loc_type, sc1.chain_no,
                sc1.sk1_location_no,
                sc1.sk1_supply_chain_no ,
                sc1.sk1_contract_no , 
                sc1.sk1_supplier_no ,
                sc1.sk2_location_no, 
                sc1.sk2_item_no, 
                sc1.fd_discipline_type, sc1.business_unit_no, sc1.standard_uom_code,sc1.random_mass_ind,sc1.static_mass,
                sc1.num_units_per_tray,
                sc1.TAX_PERC, 
                sc1.num_du,
                sc1.debtors_commission_perc, 
                sc1.actl_grn_qty,
                sc1.actl_grn_selling,
                sc1.actl_grn_cost,
                case when sc1.business_unit_no = 50 then (sc1.actl_grn_qty/sc1.num_units_per_tray) end,
                sk1_chain_code_ind
      UNION ALL
      SELECT /*+ materialize */ DISTINCT 
                sc1.actl_rcpt_date,  sc1.reg_rsp, sc1.cost_price,
                sc1.SK1_PO_NO,
                sc1.SK1_ITEM_NO,
                sc1.SUPPLY_CHAIN_TYPE,
                sc1.po_supply_chain_type, 
                sc1.contract_no, sc1.supplier_no, sc1.location_no, sc1.not_before_date, sc1.not_after_date, sc1.po_status_code, sc1.cancel_code,
                sc1.loc_type, sc1.chain_no,
                sc1.sk1_location_no,
                sc1.sk1_supply_chain_no ,
                sc1.sk1_contract_no , 
                sc1.sk1_supplier_no ,
                sc1.sk2_location_no, 
                sc1.sk2_item_no, 
                sc1.fd_discipline_type, sc1.business_unit_no, sc1.standard_uom_code,sc1.random_mass_ind,sc1.static_mass,
                sc1.num_units_per_tray,
                sc1.TAX_PERC, 
                sc1.num_du,
                sc1.debtors_commission_perc, 
                sc1.actl_grn_qty,
                sc1.actl_grn_selling,
                sc1.actl_grn_cost,
                case when sc1.business_unit_no = 50 then (sc1.actl_grn_qty/sc1.num_units_per_tray) end actl_grn_cases,
                COUNT(DC.CALENDAR_DATE) num_days_to_deliver_po,
                sk1_chain_code_ind
    from     selcalc1 sc1,    
    DIM_CALENDAR DC
    WHERE NOT(fd_discipline_type in('SA','SF') and not_after_date is not null)
        AND fin_day_no not in(6,7)
        AND rsa_public_holiday_ind = 0 
        and  calendar_date between not_BEFORE_date and ACTL_RCPT_DATE
    GROUP BY   sc1.actl_rcpt_date,  sc1.reg_rsp, sc1.cost_price,
                sc1.SK1_PO_NO,
                sc1.SK1_ITEM_NO,
                sc1.SUPPLY_CHAIN_TYPE,
                sc1.po_supply_chain_type, 
                sc1.contract_no, sc1.supplier_no, sc1.location_no, sc1.not_before_date, sc1.not_after_date, sc1.po_status_code, sc1.cancel_code,
                sc1.loc_type, sc1.chain_no,
                sc1.sk1_location_no,
                sc1.sk1_supply_chain_no ,
                sc1.sk1_contract_no , 
                sc1.sk1_supplier_no ,
                sc1.sk2_location_no, 
                sc1.sk2_item_no, 
                sc1.fd_discipline_type, sc1.business_unit_no, sc1.standard_uom_code,sc1.random_mass_ind,sc1.static_mass,
                sc1.num_units_per_tray,
                sc1.TAX_PERC, 
                sc1.num_du,
                sc1.debtors_commission_perc, 
                sc1.actl_grn_qty,
                sc1.actl_grn_selling,
                sc1.actl_grn_cost,
                case when sc1.business_unit_no = 50 then (sc1.actl_grn_qty/sc1.num_units_per_tray) end,
                sk1_chain_code_ind
)
 --------------------------------
------- calculations level 3 - final select --------
--------------------------------
 select
               sc2A.actl_rcpt_date TRAN_DATE
              ,sc2A.REG_RSP
              ,sc2A.COST_PRICE
              ,sc2A.SK1_PO_NO
              ,sc2A.SK1_ITEM_NO
              ,sc2A.SUPPLY_CHAIN_TYPE
              ,sc2A.PO_SUPPLY_CHAIN_TYPE
              ,sc2A.CONTRACT_NO
              ,sc2A.SUPPLIER_NO
              ,sc2A.LOCATION_NO
              ,sc2A.NOT_BEFORE_DATE
              ,sc2A.NOT_AFTER_DATE
              ,sc2A.PO_STATUS_CODE
              ,sc2A.CANCEL_CODE
              ,sc2A.LOC_TYPE
              ,sc2A.CHAIN_NO
              ,sc2A.SK1_LOCATION_NO
              ,sc2A.SK1_SUPPLY_CHAIN_NO
              ,sc2A.SK1_CONTRACT_NO
              ,sc2A.SK1_SUPPLIER_NO
              ,sc2A.SK2_LOCATION_NO
              ,sc2A.SK2_ITEM_NO
              ,sc2A.FD_DISCIPLINE_TYPE
              ,sc2A.BUSINESS_UNIT_NO
              ,sc2A.STANDARD_UOM_CODE
              ,sc2A.RANDOM_MASS_IND
              ,sc2A.STATIC_MASS
              ,sc2A.NUM_UNITS_PER_TRAY
              ,sc2A.TAX_PERC
              ,sc2A.NUM_DU
              ,sc2A.DEBTORS_COMMISSION_PERC
              ,sc2A.ACTL_GRN_QTY
              ,sc2A.ACTL_GRN_SELLING
              ,sc2A.ACTL_GRN_COST
              ,round(sc2A.actl_grn_cases,0)  actl_grn_cases
              ,case when sc2A.actl_grn_qty is NOT null then  sc2.num_days_to_deliver_po - 1  ELSE NULL end num_days_to_deliver_po
              ,0 PO_IND
              ,case when sc2A.chain_no = 20 then round(sc2A.actl_grn_cost + (sc2A.actl_grn_cost * (sc2A.debtors_commission_perc/100)),2) end actl_grn_fr_cost
              ,case when Sc2A.actl_grn_qty is NOT nulL then  (sc2.num_days_to_deliver_po - 1) * sc2.actl_grn_qty ELSE NULL end num_weighted_days_to_deliver 
              ,case when sc2A.actl_grn_selling is not null or sc2A.actl_grn_cost is not null then nvl(sc2A.actl_grn_selling,0) - nvl(sc2A.actl_grn_cost,0) end actl_grn_margin
              ,g_date LAST_UPDATED_DATE
              , SC2A.sk1_chain_code_ind
    from     selcalC2A sc2A LEFT OUTER JOIN SELCALC2 SC2 
    ON SC2.sk1_po_no                   = SC2A.sk1_po_no
       and    SC2.sk1_supply_chain_no     = SC2A.sk1_supply_chain_no
       and    SC2.sk1_location_no         = SC2A.sk1_location_no
       and    SC2.sk1_item_no             = SC2A.sk1_item_no
       and    SC2.actl_rcpt_date              = SC2A.actl_rcpt_date ;
                      
    g_recs_inserted := 0;
    g_recs_inserted :=  SQL%ROWCOUNT;
    commit; 
    l_text := 'Recs inserted into TEMP_POSHIP_CALC_DJ='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := 'error in INSERT_TEMP_POSHIP_CALC_DJ '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end INSERT_INTO_TEMP_POSHIP_CALC;

--**************************************************************************************************
--
-- Do calculations and load into RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ
--
--**************************************************************************************************
procedure MERGE_INTO_RTL as
begin

MERGE INTO  DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ RTL
 using ( SELECT a.* FROM     DWH_PERFORMANCE.TEMP_POSHIP_CALC_DJ a ) MER_MART
on           (mer_mart.sk1_po_no                   = RTL.sk1_po_no
       and    mer_mart.sk1_supply_chain_no     = RTL.sk1_supply_chain_no
       and    mer_mart.sk1_location_no         = RTL.sk1_location_no
       and    mer_mart.sk1_item_no             = RTL.sk1_item_no
       and    mer_mart.tran_date               = RTL.tran_date
     )
 when matched then
  update
     set     sk1_contract_no		              = MER_MART.sk1_contract_no,
              sk1_supplier_no			            = MER_MART.sk1_supplier_no,
              sk2_location_no			            = MER_MART.sk2_location_no,
              sk2_item_no			                = MER_MART.sk2_item_no,
              not_before_date			            = MER_MART.not_before_date,
              not_after_date			            = MER_MART.not_after_date,
              po_status_code                  = MER_MART.po_status_code,
              cancel_code			                = MER_MART.cancel_code,
              actl_grn_qty			              = MER_MART.actl_grn_qty,
              actl_grn_selling			          = MER_MART.actl_grn_selling,
              actl_grn_cost			              = MER_MART.actl_grn_cost,
              actl_grn_cases			            = MER_MART.actl_grn_cases,
              actl_grn_fr_cost			          = MER_MART.actl_grn_fr_cost,
              actl_grn_margin			            = MER_MART.actl_grn_margin,
              num_du			                    = MER_MART.num_du,
              num_days_to_deliver_po		      = MER_MART.num_days_to_deliver_po,
              num_weighted_days_to_deliver    = MER_MART.num_weighted_days_to_deliver,
              last_updated_date               = MER_MART.last_updated_date,
              sk1_chain_code_ind              = MER_MART.sk1_chain_code_ind
      
  when not matched then
     insert 
            ( sk1_po_no
             ,sk1_supply_chain_no
             ,sk1_location_no
             ,sk1_item_no
             ,tran_date
             ,sk1_contract_no
             ,sk1_supplier_no
             ,sk2_location_no
             ,sk2_item_no
             ,po_ind
             ,not_before_date
             ,not_after_date
             ,po_status_code
             ,cancel_code
             ,actl_grn_qty
             ,actl_grn_selling	
             ,actl_grn_cost
             ,actl_grn_cases
             ,num_du	
             ,actl_grn_fr_cost 
             ,num_days_to_deliver_po
             ,num_weighted_days_to_deliver
             ,actl_grn_margin
             ,last_updated_date
             ,sk1_chain_code_ind)
   values 
          (MER_MART.sk1_po_no
         ,MER_MART.sk1_supply_chain_no
         ,MER_MART.sk1_location_no
         ,MER_MART.sk1_item_no
         ,MER_MART.tran_date
         ,MER_MART.sk1_contract_no
         ,MER_MART.sk1_supplier_no
         ,MER_MART.sk2_location_no
         ,MER_MART.sk2_item_no
         ,MER_MART.po_ind
         ,MER_MART.not_before_date
         ,MER_MART.not_after_date
         ,MER_MART.po_status_code
         ,MER_MART.cancel_code
         ,MER_MART.actl_grn_qty
         ,MER_MART.actl_grn_selling	
         ,MER_MART.actl_grn_cost
         ,MER_MART.actl_grn_cases
         ,MER_MART.num_du	
         ,MER_MART.actl_grn_fr_cost 
         ,MER_MART.num_days_to_deliver_po
         ,MER_MART.num_weighted_days_to_deliver
         ,MER_MART.actl_grn_margin
         ,MER_MART.last_updated_date 
         ,MER_MART.sk1_chain_code_ind
                  );

    g_recs_inserted := 0;
    g_recs_inserted :=  SQL%ROWCOUNT;
    commit; 
    l_text := 'Recs merged into RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := 'error in MERGE_INTO_RTL '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end MERGE_INTO_RTL;


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
    l_text := 'LOAD OF RTL_PO_SUPCHAIN_LOC_ITEM_DY_DJ EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'DAVID JONES PROCESSING';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    execute immediate 'truncate table DWH_PERFORMANCE.TEMP_POSHIP_CALC_DJ';
    l_text := 'DWH_PERFORMANCE.TEMP_POSHIP_CALC_DJ TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    INSERT_INTO_TEMP_POSHIP_CALC;

 
    MERGE_INTO_RTL;
 

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_DJ_662U_VATFIX;
