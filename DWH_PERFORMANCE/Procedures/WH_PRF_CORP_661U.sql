--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_661U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_661U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:         11 SEPTEMBER 2015 -- NEW OPTIMISED VERSION
--  Author:       W LYTTLE
--  Purpose:      Load PO combination fact table in performance layer
--                with input ex Purchase Order RMS table from foundation layer.
--                The combination fact table combines measures from PO's and Shipments.
--                This program loads ONLY the PO info.
--                This program loads the PO info to a record keyed with the not_before_date/not_after_date of the PO.
--                (not_after_date for Long Life Food items and not_before_date for all other)
--                The po_ind is set to 1 for the records loaded from the PO table.
--                The program wh_prf_corp_662u, will load the Shipment info to a record
--                keyed with the actl_rcpt_date of the Shipment.
--                If the not_before_date/not_after_date and actl_rcpt_date are equal,
--                then the PO and Shipment measures will be contained on the same record.
--                If there are no shipments received on the not_before_date/not_after_date,
--                the record will only contain the PO measures.
--                The Bulk Confirm measures are also loaded by this program,
--                because Bulk Confirm is not linked to a receipt date.
--                The table temp_po_list contains the POs updated during batch or between selected from and to not_before_dates.
--                The not_before_date/not_after_date for a PO may change,
--                but this can only happen if there are no shipments yet.
--                This program will delete the records which have not been updated today, but exist in temp_po_list.
--                These will be the records where the not_before_date/not_after_date is the tran_date for the record,
--                but the not_before_date/not_after_date has changed after the record was inserted to rtl_po_supchain_loc_item_dy.
--  Tables:       Input  - fnd_rtl_purchase_order, fnd_rtl_shipment
--                Output - RTL_PO_SUPCHAIN_LOC_ITEM_DY
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
--                The distinct supply_chain_type is then used for all the records of the PO, regardless of the item and location (in wh_prf_corp_661UD and _662u).
--  2 nov 2011 - qc 4419 - wendy lyttle
--               Procedure Change:WH_PRF_CORP_661U - Use DIM_ITEM.PACK_ITEM_IND to update
--  9 Feb 2012 - qc4603 - wendy lyttle
--               Supplier order fill % : DATA FIX
--               Note : that change is only being made to the cursor and not the actual main body of code
--              -- the default is that it IS A SINGLE_ITEM
--  9 March 2012 - qc4604 - wendy lyttle
--              Composites Correction: Supplier Fillrate for COMPONENTS of a Composite
--              Logic as per qc
--  03 Mar 2018   VAT 2018 (increase) amendements as required                                       REF03Mar2018
--
--For the incoming item:
--1. Determine, at least, if the item is part of a pack.
--Select pack_item_no from fnd_pack_item_detail where item_no = incoming_item_no;
--2. Use the pack item number: because we want to test of the incoming record
--item number is part of a composite pack
--(N.B - this is done so that we can evaluate and do something for
--composite pack component items only.
--3. Use a new function to detemine if the queried item is a
--composite via this logic (Case pack_item_no from dim_item.item_no
--When (Pack_Item_Ind = 0 And Pack_Item_Simple_Ind = 0) Then '1 - single item'
--When (Pack_Item_Ind = 1 And Pack_Item_Simple_Ind = 1) Then '2 - simple pack'
--When (Pack_Item_Ind = 1 And Pack_Item_Simple_Ind = 0) Then '3 - composite pack'
--Else '0 - not listed' End) New_item_type_Ind,
--4. If New_Item_Type_Ind = '3 - composite pack'
--Then Incoming_Item_No Is_Part_Of_Complex_Pack; And Therefore Set Fill_Rate.

--              --
                --Becks	                    SKU1	      SKU2	      SKU3
                --Pack size	                1	          6	          24
                --Pack_Item_Ind	            0	          1	          1
                --fnd_pack_item_detail	    not null	not null	    null
                --Fill rate call required	  1	          0	          0


                --Yogurt	                  SKU4	    SKU5
                --Pack size	                1	          4
                --Pack_Item_Ind	            0	          1
                --fnd_pack_item_detail	    not null	null
                --Fill rate call required	  1	          0


                --Chips	SKU6
                --Pack size	                1
                --Pack_Item_Ind	            0
                --fnd_pack_item_detail	    null
                --Fill rate call required	  0


                --Composite	SKU7
                --Pack size	                12
                --Pack_Item_Ind	            1
                --fnd_pack_item_detail	    null
                --Fill rate call required	  0
---
----  2 sept 2015   wl multiple tax rates --- change to take vat_rate_perc from rtl_location_item
--                                            instead of dim_item
--                                        --- also doing some performance tuning
--
--    March 2018  Q. Smit    VAT fix (going from 14% to 15%)
--                           Had to pull in the ship date on shipments to determine which vat rate to use to cater for 
--                           possible senario of records being process after 1 April that must use the old vat rate.
--                           The item vat rate table had to be joined to twice for different measures :
--                               - TAX_PERC (using tran_date which is either 'not before' or 'not after' date from the PO data
--                               - derived measure - bc_shipment_selling which is shipment based so has to use ship_date from shipments
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
g_recs_updated       integer       :=  0;
g_recs_deleted       integer       :=  0;
g_recs_dlet_cnt      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_param_ind          number        :=  0;
g_date               date          :=  trunc(sysdate);
g_rec_out            RTL_PO_SUPCHAIN_LOC_ITEM_DY%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_661U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'NEW LOADS PO INFO TO PO COMBINATION FACT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_PO_SUPCHAIN_LOC_ITEM_DY%rowtype index by binary_integer;
type tbl_array_u is table of RTL_PO_SUPCHAIN_LOC_ITEM_DY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


---------------------------------------------------------------------------------------------------------------------------------------------
-- Before June 2007, it was allowed to completely cancel a Purchase Order
-- and then re-use the PO Number with a different location, not_before_date, etc. (therefore more than 1 location per PO).
-- The procedures joining Purchase Orders with Shipments, used to only join on po_no and item_no.
-- When doing this join on records where there are more than 1 location per PO, the resultset will contain duplicate records.
-- To prevent this, all PO's with more than one location (duplicate PO numbers), were excluded from all the wh_prf_corp_66* programs.
-- The join between Purchase Orders and Shipments were changed to include location in the join criteria.
-- First join to dim_location with the location_no on the PO.
-- If the location on the PO is a store (loc_type 'S') then use location_no to join to Shipment else use wh_physical_wh_no to join to Shipment.
-- A decision was made and aggreed with Business, to only load PO's from Fin Year 2008 (starting at 25 June 2007) and onwards.
---------------------------------------------------------------------------------------------------------------------------------------------

--**************************************************************************************************
--
-- Do calculations and load into TEMP_PO_DATA
--
--**************************************************************************************************

procedure INSERT_INTO_TEMP_PO_DATA  as
begin

INSERT /*+ APPEND */ INTO DWH_PERFORMANCE.TEMP_PO_DATA
with
---------------------------
------- pack items --------
---------------------------
      selpack
      as
         (select    /*+ materialize full(pi) full(tp) */
                            distinct
                              tp.po_no
                            , tp.item_no
                   from dwh_performance.temp_po_list tp
                   join    fnd_pack_item_detail pi     on  pi.item_no            = tp.item_no
         order by tp.item_no)
      ,
----------------------
------- items --------
----------------------
      selitpo
      as
   (
      select    /*+ materialize parallel(di,4) full(di)
                                            parallel(tp,2) full(tp)
                                            parallel(iu501,2) full(iu501)
                                            parallel(iu601,2) full(iu601)*/
                            distinct
                              tp.po_no
                            , tp.sk1_po_no
                            , tp.item_no
                            , tp.sk1_item_no
                            , tp.param_ind
                            , TP.supply_chain_type
                             , di.business_unit_no
                            , di.standard_uom_code
                            , di.random_mass_ind
                            , case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                                then  di.static_mass
                                else 1
                                end static_mass
                            , di.Pack_Item_Ind
                            , di.Pack_Item_Simple_Ind
                            , di.fd_discipline_type
                            , (case when pi.item_no is null -- eg. yoghurt packsize=4  or composite
                                    and di.pack_item_ind = 1
                                             then 0
                                    when pi.item_no is null -- eg. chips packsize=1
                                     and di.pack_item_ind = 0
                                               then 0
                                    when pi.item_no is not null -- eg.becks packsize=6 if part of packsize=24
                                     and di.pack_item_ind = 1
                                               then 0
                                    when pi.item_no is not null -- eg. yoghurt packsize=1 or becks packsize=1
                                     and di.pack_item_ind = 0
                                                then 1
                                     else
                                        1   --- default is single_item
                                end) single_item_ind
                            , dd.dept_placeholder_02_ind
                            , dd.dept_placeholder_03_ind
                            , dd.jv_dept_ind
                            , dd.packaging_dept_ind
                            , dd.gifting_dept_ind
                            , dd.non_core_dept_ind
                            , dd.bucket_dept_ind
                            , dd.book_magazine_dept_ind
                            , dd.sk1_department_no
                            , case when iu501.item_no is null then 0 else 1 end uda_item501
                            , case when iu1601.item_no is null then 0 else to_number(iu1601.uda_value_no_or_text_or_date) end uda_item1601
                            ,nvl(dsc.sk1_supply_chain_no,0) sk1_supply_chain_no
                            , vat_rate_perc
                   from          dim_item di
                   join          dwh_performance.temp_po_list tp            on  tp.item_no            = di.item_no
                   join          dim_department dd          on  dd.department_no      = di.department_no
                   left join     fnd_item_uda iu501         on  iu501.item_no          = di.item_no
                                                                and iu501.uda_no = 501
                                                                and iu501.uda_value_no_or_text_or_date = '2'
                   left join     fnd_item_uda iu1601          on  iu1601.item_no          = di.item_no
                                                                and iu1601.uda_no = 1601
                                                                and iu1601.uda_value_no_or_text_or_date in('1','2')
                   left join    selpack pi     on  pi.item_no            = di.item_no
                   left join dim_supply_chain_type dsc  on  dsc.supply_chain_code = TP.supply_chain_type
             order by business_unit_no, tp.item_no, tp.po_no)
      ,
--------------------------
------- shipments --------
--------------------------
     selship as
       (select
                 /*+ materialize parallel(s,4) full(TP)  */
                              s.po_no
                            , s.to_loc_no
                            , s.item_no
                            , s.asn_qty
                            , s.reg_rsp
                            , s.cost_price
                            , s.ship_date                                       --VAT rate change
                 from      fnd_rtl_shipment s
                  join      DWH_PERFORMANCE.temp_po_list tp
                        on  s.po_no = tp.po_no and s.item_no = tp.item_no
                  where     s.shipment_status_code = 'I'
                  and       s.asn_id like '%BULK%'
                   )
                    ,
--------------------------------
------- purchase orders --------
--------------------------------
  selpo as
(
   select
   /*+  MATERIALIZE parallel(P,4) full(TP) */
            Tp.sk1_po_no
          , Tp.sk1_item_no
          , p.po_no
          , p.item_no
          , Tp.param_ind
          , p.Location_No
          , p.CONTRACT_No
          , p.SUPPLIER_No
          , p.supply_chain_type
          , p.not_before_date
          , p.not_after_date
          , p.po_status_code
          , p.cancel_code
          , p.reg_rsp
          , p.cost_price
          , p.supp_pack_size
          , p.buyer_edited_po_qty
          , p.buyer_edited_cancel_po_qty
          , p.buyer_edited_cancel_code
          , p.original_po_qty
          , p.amended_po_qty
          ,  p.cancel_po_qty
          , p.rejected_cases
          , p.grn_qty
          ,p.distribute_from_location_no
    from      fnd_rtl_purchase_order p
    join      DWH_PERFORMANCE.temp_po_list tp       on  Tp.item_no                   = p.item_no
                                       and Tp.po_no                     = p.po_no
      where  (p.chain_code <> 'DJ' or p.chain_code is null)
                                    --   WHERE P.PO_NO = 6031382
           )
---------------------------------------------------
------- merge purchase orders and shipments--------
---------------------------------------------------
    SELECT /*+  parallel(diT,2) full(diT) parallel(diH,2) full(diH)
                                            full(dLH)
                                            parallel(LI,4)
                                            parallel(fiv,2)*/
            Sp.sk1_po_no
          , Sp.sk1_item_no
          , Sp.po_no
          , Sp.item_no
          , Sp.param_ind
          , Sp.Location_No
          , Sp.not_before_date
          , Sp.not_after_date
          , Sp.po_status_code
          , Sp.cancel_code
          , Sp.reg_rsp
          , Sp.cost_price
          , Sp.supp_pack_size
          , Sp.buyer_edited_po_qty
          , Sp.buyer_edited_cancel_po_qty
          , Sp.buyer_edited_cancel_code
          , Sp.original_po_qty
          , Sp.amended_po_qty
          , Sp.cancel_po_qty
          , Sp.rejected_cases
          , Sp.grn_qty
          , SP.distribute_from_location_no
          , IP.sk1_supply_chain_no
          , Ip.supply_chain_type
          , IP.business_unit_no
          , IP.standard_uom_code
          , IP.random_mass_ind
          , IP.static_mass
          , IP.Pack_Item_Ind
          , IP.Pack_Item_Simple_Ind
          , IP.fd_discipline_type
          , IP.dept_placeholder_02_ind
          , IP.dept_placeholder_03_ind
          , IP.jv_dept_ind
          , IP.packaging_dept_ind
          , IP.gifting_dept_ind
          , IP.non_core_dept_ind
          , IP.bucket_dept_ind
          , IP.book_magazine_dept_ind
          , IP.sk1_department_no
          , IP.uda_item501
          , IP.uda_item1601
          , IP.single_item_ind
          
          , case when IP.fd_discipline_type in('SA','SF') and (SP.not_after_date is not null) then SP.not_after_date else SP.not_before_date end    tran_date
          
          ,(Case
                When (Dit.Pack_Item_Ind = 0 And Dit.Pack_Item_Simple_Ind = 0) Then '1'-- - single item
                When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 1) Then '2'-- - simple pack
                When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 0) Then '3'-- - composite pack
                Else '0'                                                              -- - not listed
            End)  New_Item_Type_Ind
          , Case dL.Loc_Type When 'S' Then dl.Location_No Else dL.Wh_Physical_Wh_No End      location_no_other
          , dl.sk1_location_no
          , dl.chain_no
          , dl.sk1_chain_no
          , DLH.SK2_LOCATION_NO
          , DIH.SK2_ITEM_NO
          , nvl(dc.sk1_contract_no, 0) SK1_CONTRACT_NO
          , nvl(ds.sk1_supplier_no, 0) SK1_SUPPLIER_NO
           , decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray)                                                          num_units_per_tray
           
          -- , NVL(LI.TAX_PERC, (case when vat_region_no = 1000 then ip.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end ) )  TAX_PERC
           , NVL(fi_vr1.vat_rate_perc, (case when dl.vat_region_no = 1000 then ip.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end ) )  TAX_PERC    --VAT rate change
           
           
          , sum(sS.asn_qty) bc_shipment_qty
          
          --, sum((round((sS.asn_qty * (sS.reg_rsp * 100 / (100 + NVL(LI.TAX_PERC, (case when vat_region_no = 1000 then ip.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end)  ) )) * IP.static_mass),2))) bc_shipment_selling
          , sum((round((sS.asn_qty * (sS.reg_rsp * 100 / (100 + NVL(fi_vr.vat_rate_perc, (case when dl.vat_region_no = 1000 then ip.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end)  ) )) * IP.static_mass),2))) bc_shipment_selling  -- VAT rate change
          
          , sum((round((sS.asn_qty * sS.cost_price * IP.static_mass),2))) bc_shipment_cost
          
from selpo sp
join selitpo ip on ip.item_no = sp.item_no
               and ip.po_no   = sp.po_no
               
join  dim_location dl on  dl.location_no = sp.location_no
join      dim_item_hist dih          on  DIH.item_no           = SP.item_no
                                     and sp.not_before_date    between dih.sk2_active_from_date and dih.sk2_active_to_date
join      dim_location_hist dlh      on  DLH.location_no        = SP.location_no
                                     and Sp.not_before_date    between dlh.sk2_active_from_date and dlh.sk2_active_to_date
                                     
left outer join selship ss on ss.item_no = sp.item_no
                          and ss.po_no = sp.po_no
                          and ss.to_loc_no = case dl.loc_type when 'S' then dl.location_no else dl.wh_physical_wh_no end
        
left outer join selpack sk on sk.item_no = sp.item_no
                          and sk.po_no   = sp.po_no
                          
left join fnd_pack_item_detail pdit on pdit.item_no    = sp.item_no
left join dim_item dit              on  dit.item_no    = pdit.pack_item_no
left join dim_contract dc           on  dc.contract_no = sp.contract_no
left join dim_supplier ds           on  ds.supplier_no = sp.supplier_no
left join rtl_location_item li      on  LI.sk1_item_no = SP.sk1_item_no  
                                   and  LI.sk1_location_no   = DL.sk1_location_no

LEFT OUTER JOIN FND_ITEM_VAT_RATE  fi_vr  on (sp.item_no        = fi_vr.item_no                                      -- VAT rate change
                                         and  dl.vat_region_no  = fi_vr.vat_region_no                                -- VAT rate change
                                         and  ss.ship_date between fi_vr.active_from_date and fi_vr.active_to_date)  -- VAT rate change
                                         
LEFT OUTER JOIN FND_ITEM_VAT_RATE  fi_vr1 on (sp.item_no        = fi_vr1.item_no                                      -- VAT rate change
                                         and  dl.vat_region_no  = fi_vr1.vat_region_no                                -- VAT rate change
                                         and  (case when IP.fd_discipline_type in('SA','SF') and (SP.not_after_date is not null) then 
                                               SP.not_after_date else SP.not_before_date end)  between fi_vr1.active_from_date and fi_vr1.active_to_date)  -- VAT rate change

GROUP BY Sp.sk1_po_no
          , Sp.sk1_item_no
          , Sp.po_no
          , Sp.item_no
          , Sp.param_ind
          , Sp.Location_No
          , Sp.not_before_date
          , Sp.not_after_date
          , Sp.po_status_code
          , Sp.cancel_code
          , Sp.reg_rsp
          , Sp.cost_price
          , Sp.supp_pack_size
          , Sp.buyer_edited_po_qty
          , Sp.buyer_edited_cancel_po_qty
          , Sp.buyer_edited_cancel_code
          , Sp.original_po_qty
          , Sp.amended_po_qty
          , Sp.cancel_po_qty
          , Sp.rejected_cases
          , Sp.grn_qty
          , SP.distribute_from_location_no
          , IP.sk1_supply_chain_no
          , Ip.supply_chain_type
          , IP.business_unit_no
          , IP.standard_uom_code
          , IP.random_mass_ind
          , IP.static_mass
          , IP.Pack_Item_Ind
          , IP.Pack_Item_Simple_Ind
          , IP.fd_discipline_type
          , IP.dept_placeholder_02_ind
          , IP.dept_placeholder_03_ind
          , IP.jv_dept_ind
          , IP.packaging_dept_ind
          , IP.gifting_dept_ind
          , IP.non_core_dept_ind
          , IP.bucket_dept_ind
          , IP.book_magazine_dept_ind
          , IP.sk1_department_no
          , IP.uda_item501
          , IP.uda_item1601
          , IP.single_item_ind
          , case when IP.fd_discipline_type in('SA','SF') and (SP.not_after_date is not null) then SP.not_after_date else SP.not_before_date end
          ,(Case
                When (Dit.Pack_Item_Ind = 0 And Dit.Pack_Item_Simple_Ind = 0) Then '1'-- - single item
                When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 1) Then '2'-- - simple pack
                When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 0) Then '3'-- - composite pack
                Else '0'                                                              -- - not listed
            End)
          , Case dL.Loc_Type When 'S' Then dl.Location_No Else dL.Wh_Physical_Wh_No End
          , dl.sk1_location_no
          , dl.chain_no
          , dl.sk1_chain_no
          , DLH.SK2_LOCATION_NO
          , DIH.SK2_ITEM_NO
          , nvl(dc.sk1_contract_no, 0)
          , nvl(ds.sk1_supplier_no, 0)
           , decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray)
           , NVL(fi_vr1.vat_rate_perc, (case when dl.vat_region_no = 1000 then ip.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end ) )  --VAT rate change
           --, NVL(LI.TAX_PERC, (case when dl.vat_region_no = 1000 then ip.vat_rate_perc else dl.DEFAULT_TAX_REGION_NO_PERC end ) )         --VAT rate change
;

    g_recs_inserted := 0;
    g_recs_inserted := SQL%ROWCOUNT;
    commit;
    l_text := 'Recs inserted into TEMP_PO_DATA='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := 'error in INSERT_TEMP_PO_DATA '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end INSERT_INTO_TEMP_PO_DATA ;

--**************************************************************************************************
--
-- Do calculations and load into TEMP_PO_CALC
--
--**************************************************************************************************
procedure INSERT_INTO_TEMP_PO_CALC as
begin

INSERT /*+ APPEND */ INTO  DWH_PERFORMANCE.TEMP_PO_CALC
with selext1 as ( SELECT
            P.*
            , 1 po_ind
           , nvl(ldd.debtors_commission_perc,0) debtors_commission_perc
           , zif.reg_rsp fd_from_loc_reg_rsp
--                           ----------
           , (case p.business_unit_no when 50 then nvl(zif.case_selling_excl_vat,0)  else 0 end)                                                          case_selling_excl_vat
           , (case p.business_unit_no when 50 then nvl(zif.case_cost,0)  else 0 end)                                                                      case_cost
--                           ----------
           , CASE WHEN BUSINESS_UNIT_NO = 50 THEN round((p.original_po_qty * (zif.reg_rsp * 100 / (100 + P.tax_perc)) * P.static_mass),2)
           ELSE round((p.original_po_qty * (P.reg_rsp * 100 / (100 + P.tax_perc)) ),2) END  original_po_selling
           , round((p.original_po_qty * p.cost_price * P.static_mass),2)                                                                                    original_po_cost
           ,  (case when p.business_unit_no = 50 then round((p.original_po_qty/(decode(nvl(P.num_units_per_tray,0),0,1,P.num_units_per_tray))),0) end)    original_po_cases
--                           ----------
           , CASE WHEN BUSINESS_UNIT_NO = 50 THEN round((p.amended_po_qty * (zif.reg_rsp * 100 / (100 + P.tax_perc)) * P.static_mass),2)
           ELSE round((p.amended_po_qty * (P.reg_rsp * 100 / (100 + P.tax_perc))),2)  END amended_po_selling
           , round((p.amended_po_qty * p.cost_price * P.static_mass),2)                                                                                    amended_po_cost
           , (case when p.business_unit_no = 50 then round((p.amended_po_qty/(decode(nvl(P.num_units_per_tray,0),0,1,P.num_units_per_tray))),0) end)     amended_po_cases
--                           ----------
           , CASE WHEN BUSINESS_UNIT_NO = 50 THEN  round((p.buyer_edited_po_qty * (zif.reg_rsp * 100 / (100 + P.tax_perc)) * P.static_mass),2)
           ELSE round((p.buyer_edited_po_qty * (P.reg_rsp * 100 / (100 + P.tax_perc)) ),2)  END buyer_edited_po_selling
--                           ----------
           , CASE WHEN BUSINESS_UNIT_NO = 50 THEN round((p.cancel_po_qty * (zif.reg_rsp * 100 / (100 + P.tax_perc)) * P.static_mass),2)   ELSE
           round((p.cancel_po_qty * (P.reg_rsp * 100 / (100 + P.tax_perc)) ),2)  END cancel_po_selling
           , round((p.cancel_po_qty * p.cost_price * P.static_mass),2)                                                                                     cancel_po_cost
           ,  (case when p.business_unit_no = 50 then round((p.cancel_po_qty/(decode(nvl(P.num_units_per_tray,0),0,1,P.num_units_per_tray))),0)  end)     cancel_po_cases
--                           ----------
           , (case when p.business_unit_no = 50 then round((p.rejected_cases*(decode(nvl(P.num_units_per_tray,0),0,1,P.num_units_per_tray))),3) end)     rejection_qty
--                           ----------
           , CASE WHEN BUSINESS_UNIT_NO = 50 THEN round((p.grn_qty * (zif.reg_rsp * 100 / (100 + P.tax_perc)) * P.static_mass),2)
           ELSE round((p.grn_qty * (P.reg_rsp * 100 / (100 + P.tax_perc)) ),2)  END po_grn_selling
           , round((p.grn_qty * p.cost_price * P.static_mass),2)                                                                                          po_grn_cost
           , (case when p.business_unit_no = 50 then round((p.grn_qty/(decode(nvl(P.num_units_per_tray,0),0,1,P.num_units_per_tray))),0) end)            po_grn_cases
           ,CASE WHEN P.business_unit_no not in(50, 70) then
                 CASE WHEN P.po_status_code = 'C' and nvl(P.supply_chain_type,'NSC') not in('HS','VMI','NSC')  then
                     CASE WHEN P.original_po_qty is null then
                        null
                     else
                        CASE WHEN P.buyer_edited_cancel_code = 'B' then
                           CASE WHEN P.original_po_qty = P.buyer_edited_cancel_po_qty then  null  else   buyer_edited_po_qty    end
                        else
                           original_po_qty
                        end
                     end
                  end
               end                                                                                                                                         fillrate_order_qty

             , CASE WHEN  P.business_unit_no = 50 then
                    CASE WHEN (P.amended_po_qty is null) then  P.original_po_qty
                    else
                       case when (p.cancel_code = 'B') then p.original_po_qty - nvl(p.cancel_po_qty,0)
                       else p.amended_po_qty + nvl(p.cancel_po_qty,0)
                       end
                    end
               else
                     case when p.po_status_code = 'C' then   p.grn_qty
                     else
                       case when p.po_status_code = 'A' and (p.bc_shipment_qty is not null) then   p.bc_shipment_qty
                       else   P.amended_po_qty
                       end
                    end
               end                                         latest_po_qty

              ,    CASE WHEN  P.business_unit_no = 50 then
                        CASE WHEN (P.amended_po_qty is null) then
                           round((P.original_po_qty /(case when nvl(p.num_units_per_tray,0) = 0 then 1 else p.num_units_per_tray end)),0)
                        else
                           round(((P.amended_po_qty + nvl(P.cancel_po_qty,0)) /(case when nvl(p.num_units_per_tray,0) = 0 then 1 else p.num_units_per_tray end)),0)
                         END
                  END                                                                                                                                           LATEST_PO_CASES
          from dwh_performance.temp_po_data p
          left join rtl_loc_dept_dy ldd        on  LDD.sk1_location_no   = P.sk1_location_no
                                                         and LDD.sk1_department_no = P.sk1_department_no
                                                         and ldd.post_date = (case P.fd_discipline_type when 'SA' then nvl(p.not_after_date,p.not_before_date)
                                                                                                        when 'SF' then nvl(p.not_after_date,p.not_before_date)
                                                                                                         else p.not_before_date end)
          left join dim_location dl2           on  dl2.location_no = p.distribute_from_location_no
          left join rtl_zone_item_om zif       on  ZIF.sk1_zone_group_zone_no = dl2.sk1_fd_zone_group_zone_no
                                                          and zif.sk1_item_no       = p.sk1_item_no
          left join rtl_zone_item_om zic       on  ZIC.sk1_zone_group_zone_no = dl2.sk1_ch_zone_group_zone_no
                                                          and ZIC.sk1_item_no       = P.sk1_item_no
),
selext2 as ( SELECT
            se1.*
           ,round((se1.rejected_cases * se1.case_selling_excl_vat * se1.static_mass),2)                                                                          REJECTION_SELLING
           ,round((se1.rejected_cases * se1.case_cost * se1.static_mass),2)                                                                                      REJECTION_COST
           ,se1.amended_po_selling - se1.amended_po_cost                                                                                                       AMENDED_PO_MARGIN
           ,CASE WHEN CHAIN_NO = 20 THEN round(se1.po_grn_cost + (se1.po_grn_cost * (se1.debtors_commission_perc/100)),2)  END                                           PO_GRN_FR_COST
           , CASE WHEN se1.business_unit_no not in(50, 70) then
                 CASE WHEN se1.po_status_code = 'C' and nvl(se1.supply_chain_type,'NSC') not in('HS','VMI','NSC')  then
                     CASE WHEN se1.original_po_qty is null then
                        null
                     else
                        CASE WHEN se1.buyer_edited_cancel_code = 'B' then
                           CASE WHEN se1.original_po_qty = se1.buyer_edited_cancel_po_qty then
                              null
                           else
                              buyer_edited_po_selling
                           end
                        else
                           original_po_selling
                        end
                     end
                  end
               end                                                                                                                                         FILLRATE_ORDER_SELLING

              , case when  se1.business_unit_no = 50 then
                   case when (se1.amended_po_qty is null) then   se1.original_po_selling
                    else
                    case when (se1.cancel_code = 'B') then   se1.original_po_selling - nvl(se1.cancel_po_selling,0)
                    else    se1.amended_po_selling + nvl(se1.cancel_po_selling,0)
                    end
                   end
                else
                    CASE WHEN se1.po_status_code = 'C' then  se1.po_grn_selling
                    else
                       case when se1.po_status_code = 'A' and (se1.bc_shipment_qty is not null) then  se1.bc_shipment_selling
                       else    se1.amended_po_selling
                       end
                    end
                end                        LATEST_PO_SELLING

               , CASE WHEN  se1.business_unit_no = 50 then
                    case when (se1.amended_po_qty is null) then  se1.original_po_cost
                     else
                     case when (se1.cancel_code = 'B') then se1.original_po_cost - nvl(se1.cancel_po_cost,0)
                     else se1.amended_po_cost + nvl(se1.cancel_po_cost,0)
                     end
                    end
                 else
                    CASE WHEN se1.po_status_code = 'C' then  se1.po_grn_cost
                    else
                       case when se1.po_status_code = 'A' and (se1.bc_shipment_qty is not null) then  se1.bc_shipment_cost
                       else   se1.amended_po_cost
                       end
                    end
                 end                                                                                                                                            latest_po_cost
                      ,case when business_unit_no = 50 then
--                         case   when  ((se1.cancel_code is null) or se1.cancel_code <> 'B') and nvl(se1.grn_qty,0) < se1.latest_po_qty
                         case   WHEN  nvl(se1.grn_qty,0) < se1.latest_po_qty
                             THEN  se1.latest_po_qty - nvl(se1.grn_qty,0)
                            else
                               0
                            end
                         end                                                                                                                                                   SHORTS_QTY
                      ,case when se1.business_unit_no = 50
--                                 and ( (((se1.cancel_code is null) or se1.cancel_code <> 'B')
                                 AND ((       se1.jv_dept_ind = 0              and se1.packaging_dept_ind = 0
                                           And se1.Gifting_Dept_Ind = 0         And se1.Non_Core_Dept_Ind = 0  And se1.Bucket_Dept_Ind = 0
                                           And se1.Book_Magazine_Dept_Ind = 0   And se1.Uda_Item501 = 1        And se1.Single_Item_Ind = 0 )
                                    Or  se1.New_Item_Type_Ind = '3')
                               then LATEST_PO_qty
                        end      FILLRATE_FD_LATEST_PO_QTY
                      ,case when se1.business_unit_no = 50
--                              and ( (((se1.cancel_code is null) or se1.cancel_code <> 'B')
                                           and ((se1.jv_dept_ind = 0            and se1.packaging_dept_ind = 0
                                           and   se1.gifting_dept_ind = 0       and se1.non_core_dept_ind = 0  and se1.bucket_dept_ind = 0
                                           And   se1.Book_Magazine_Dept_Ind = 0 And se1.Uda_Item501 = 1        And se1.Single_Item_Ind = 0 )
                                    Or  se1.New_Item_Type_Ind = '3')
                               then
                                          se1.grn_qty
                        end       FILLRATE_FD_PO_GRN_QTY
          FROM selext1 se1)
         SELECT DISTINCT
                       SK1_PO_NO
                      ,SK1_SUPPLY_CHAIN_NO
                      ,SK1_LOCATION_NO
                      ,SK1_ITEM_NO
                      ,TRAN_DATE
                      ,SK1_CONTRACT_NO
                      ,SK1_SUPPLIER_NO
                      ,SK2_LOCATION_NO
                      ,SK2_ITEM_NO
                      ,PO_IND
                      ,NOT_BEFORE_DATE
                      ,NOT_AFTER_DATE
                      ,PO_STATUS_CODE
                      ,CANCEL_CODE
                      ,REG_RSP
                      ,COST_PRICE
                      ,CASE_SELLING_EXCL_VAT
                      ,CASE_COST
                      ,SUPP_PACK_SIZE
                      ,ORIGINAL_PO_QTY
                      ,ORIGINAL_PO_SELLING
                      ,ORIGINAL_PO_COST
                      ,ORIGINAL_PO_CASES
                      ,AMENDED_PO_QTY
                      ,AMENDED_PO_SELLING
                      ,AMENDED_PO_COST
                      ,AMENDED_PO_CASES
                      ,AMENDED_PO_MARGIN
                      ,BUYER_EDITED_PO_QTY
                      ,CANCEL_PO_QTY
                      ,CANCEL_PO_SELLING
                      ,CANCEL_PO_COST
                      ,CANCEL_PO_CASES
                      ,REJECTION_QTY
                      ,REJECTION_SELLING
                      ,REJECTION_COST
                      ,REJECTED_CASES
                      ,grn_qty PO_GRN_QTY
                      ,PO_GRN_SELLING
                      ,PO_GRN_COST
                      ,PO_GRN_CASES
                      ,PO_GRN_FR_COST
                      , shorts_qty
                      ,case when business_unit_no = 50 then
--                         case   when  ((se2.cancel_code is null) or se2.cancel_code <> 'B') and nvl(se2.grn_qty,0) < se2.latest_po_qty
                         case   WHEN  nvl(se2.grn_qty,0) < se2.latest_po_qty
                            THEN   se2.latest_po_SELLING - nvl(se2.po_grn_SELLING,0)
                            else
                               0
                            end
                         end                                                                              shorts_selling
                      ,case when business_unit_no = 50 then
--                         case   when  ((se2.cancel_code is null) or se2.cancel_code <> 'B') and nvl(se2.grn_qty,0) < se2.latest_po_qty
                         case   WHEN  nvl(se2.grn_qty,0) < se2.latest_po_qty
                            THEN   se2.latest_po_COST - nvl(se2.po_grn_COST,0)
                            else
                               0
                            end
                         end                                                                               shorts_cost
                      ,case when business_unit_no = 50 then
--                         case   when  ((se2.cancel_code is null) or se2.cancel_code <> 'B') and nvl(se2.grn_qty,0) < se2.latest_po_qty
                         case   WHEN  nvl(se2.grn_qty,0) < se2.latest_po_qty
                           THEN   round((se2.shorts_qty /(case when nvl(se2.num_units_per_tray,0) = 0 then 1 else se2.num_units_per_tray end)),0)
                            else
                               0
                            end
                         end                                                                               SHORTS_CASES
                      ,BC_SHIPMENT_QTY
                      ,BC_SHIPMENT_SELLING
                      ,BC_SHIPMENT_COST
                      , NULL ACTL_GRN_QTY
                      ,null ACTL_GRN_SELLING
                      ,null ACTL_GRN_COST
                      ,null ACTL_GRN_CASES
                      ,null ACTL_GRN_FR_COST
                      ,null ACTL_GRN_MARGIN
                      ,NULL FILLRATE_ACTL_GRN_EXCL_WH_QTY
                      ,null FILLRTE_ACTL_GRN_EXCL_WH_SELL
                      ,LATEST_PO_QTY
                      ,LATEST_PO_SELLING
                      ,LATEST_PO_COST
                      ,LATEST_PO_CASES
                      , NULL LATEST_PO_QTY_ALL_TIME
                      ,null LATEST_PO_SELLING_ALL_TIME
                      ,null LATEST_PO_COST_ALL_TIME
                      ,null AVG_PO_RSP_EXCL_VAT_ALL_TIME
                      ,null AVG_PO_COST_PRICE_ALL_TIME
                      ,null AVG_PO_MARGIN_PERC_ALL_TIME
                      ,null NUM_DU
                      ,null NUM_DAYS_TO_DELIVER_PO
                      ,null NUM_WEIGHTED_DAYS_TO_DELIVER
                      ,FILLRATE_ORDER_QTY
                      ,FILLRATE_ORDER_SELLING
                      ,CASE WHEN se2.business_unit_no not in(50, 70) AND( (se2.supply_chain_type is null) or se2.supply_chain_type <> 'WH') then fillrate_order_qty end      FILLRATE_ORDER_EXCL_WH_QTY
                      ,CASE WHEN se2.business_unit_no not in(50, 70) AND ((se2.supply_chain_type is null) or se2.supply_chain_type <> 'WH') then fillrate_order_selling end  FILLRATE_ORDER_EXCL_WH_SELLING
                      ,G_DATE LAST_UPDATED_DATE
                      ,FILLRATE_FD_PO_GRN_QTY
                      ,CASE WHEN se2.Business_Unit_No = 50 AND se2.uda_item1601 = 1 then  se2.fillrate_fd_po_grn_qty  END                             FILLRATE_FD_PO_GRN_QTY_IMPORT
                      ,CASE WHEN se2.Business_Unit_No = 50 AND se2.uda_item1601 = 2 then se2.fillrate_fd_po_grn_qty  END                                FILLRATE_FD_PO_GRN_QTY_LOCAL
                      ,FILLRATE_FD_LATEST_PO_QTY
                      ,CASE WHEN se2.Business_Unit_No = 50 AND se2.uda_item1601 = 1 then se2.fillrate_fd_LATEST_po_qty END                              FILLRTE_FD_LATEST_PO_QTY_IMPRT
                      ,CASE WHEN se2.Business_Unit_No = 50 AND se2.uda_item1601 = 2 then  se2.fillrate_fd_LATEST_po_qty END                            FILLRTE_FD_LATEST_PO_QTY_local
                      ,null FILLRATE_ACTL_GRN_QTY
                      ,null FILLRATE_ACTL_GRN_SELLING
                      ,FD_FROM_LOC_REG_RSP
                      FROM SELEXT2 SE2;


    g_recs_inserted := 0;
    g_recs_inserted :=  SQL%ROWCOUNT;
    commit;
    l_text := 'Recs inserted into TEMP_PO_CALC='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := 'error in INSERT_TEMP_PO_CALC '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end INSERT_INTO_TEMP_PO_CALC;

--**************************************************************************************************
--
-- Do calculations and load into RTL_PO_SUPCHAIN_LOC_ITEM_DY
--
--**************************************************************************************************
procedure MERGE_INTO_RTL as
begin

MERGE INTO  DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY RTL
 using ( SELECT DISTINCT A.*  FROM     DWH_PERFORMANCE.TEMP_PO_CALC A 
--   WHERE NOT EXISTS (SELECT /*+ materialize parallel(B,4) full(B)  */ TRAN_DATE,
--                                                SK1_SUPPLY_CHAIN_NO,
--                                                SK1_LOCATION_NO,
--                                                SK1_ITEM_NO,
--                                                SK1_PO_NO
--                                        FROM RTL_PO_SUPCHAIN_LOC_ITEM_DY B
--                                        WHERE a.TRAN_DATE = A.TRAN_DATE
--                                         AND       B.SK1_SUPPLY_CHAIN_NO = A.SK1_SUPPLY_CHAIN_NO
--                                         AND       B.SK1_LOCATION_NO = A.SK1_LOCATION_NO
--                                         AND       B.SK1_ITEM_NO = A.SK1_ITEM_NO
--                                         AND       B.SK1_PO_NO = A.SK1_PO_NO)
-- where sk1_po_no <> 28623763  -- emergency fix for dups removal due to issues with vat rate table 04/04/2018 - ADW
-- where sk1_po_no <> 29161691  -- emergency fix for dups removal due to issues UNKNOWN AT THIS TIME 29 JUL 2018 3:47AM - ADW
-- where sk1_po_no <> 29180449 --emergency fix for dups removal due to issues UNKNOWN AT THIS TIME 2 AUG 2018 3:30AM - ADW sk1_item_no 28546364

 ) MER_MART
on (mer_mart.sk1_po_no                   = RTL.sk1_po_no
       and    mer_mart.sk1_supply_chain_no     = RTL.sk1_supply_chain_no
       and    mer_mart.sk1_location_no         = RTL.sk1_location_no
       and    mer_mart.sk1_item_no             = RTL.sk1_item_no
       and    mer_mart.tran_date               = RTL.tran_date
     )
 when matched then
  update
     set      sk1_contract_no		              = MER_MART.sk1_contract_no,
              sk1_supplier_no			            = MER_MART.sk1_supplier_no,
              sk2_location_no			            = MER_MART.sk2_location_no,
              sk2_item_no			                = MER_MART.sk2_item_no,
              po_ind    			                = MER_MART.po_ind,
              not_before_date			            = MER_MART.not_before_date,
              not_after_date			            = MER_MART.not_after_date,
              po_status_code                  = MER_MART.po_status_code,
              cancel_code			                = MER_MART.cancel_code,
              reg_rsp                         = MER_MART.reg_rsp,
              cost_price                      = MER_MART.cost_price,
              case_selling_excl_vat           = MER_MART.case_selling_excl_vat,
              case_cost                       = MER_MART.case_cost,
              supp_pack_size                  = MER_MART.supp_pack_size,
              original_po_qty			            = MER_MART.original_po_qty,
              original_po_selling		          = MER_MART.original_po_selling,
              original_po_cost			          = MER_MART.original_po_cost,
              original_po_cases               = MER_MART.original_po_cases,
              amended_po_qty		              = MER_MART.amended_po_qty,
              amended_po_selling		          = MER_MART.amended_po_selling,
              amended_po_cost		              = MER_MART.amended_po_cost,
              amended_po_cases                = MER_MART.amended_po_cases,
              amended_po_margin               = MER_MART.amended_po_margin,
              buyer_edited_po_qty		          = MER_MART.buyer_edited_po_qty,
              cancel_po_qty			              = MER_MART.cancel_po_qty,
              cancel_po_selling               = MER_MART.cancel_po_selling,
              cancel_po_cost                  = MER_MART.cancel_po_cost,
              cancel_po_cases                 = MER_MART.cancel_po_cases,
              rejection_qty                   = MER_MART.rejection_qty,
              rejection_selling               = MER_MART.rejection_selling,
              rejection_cost                  = MER_MART.rejection_cost,
              rejected_cases                  = MER_MART.rejected_cases,
              po_grn_qty			                = MER_MART.po_grn_qty,
              po_grn_selling			            = MER_MART.po_grn_selling,
              po_grn_cost			                = MER_MART.po_grn_cost,
              po_grn_cases                    = MER_MART.po_grn_cases,
              po_grn_fr_cost                  = MER_MART.po_grn_fr_cost,
              shorts_qty                      = MER_MART.shorts_qty,
              shorts_selling                  = MER_MART.shorts_selling,
              shorts_cost                     = MER_MART.shorts_cost,
              shorts_cases                    = MER_MART.shorts_cases,
              bc_shipment_qty		              = MER_MART.bc_shipment_qty,
              bc_shipment_selling		          = MER_MART.bc_shipment_selling,
              bc_shipment_cost		            = MER_MART.bc_shipment_cost,
              latest_po_qty		                = MER_MART.latest_po_qty,
              latest_po_selling	              = MER_MART.latest_po_selling,
              latest_po_cost		              = MER_MART.latest_po_cost,
              latest_po_cases                 = MER_MART.latest_po_cases,
              latest_po_qty_all_time          = MER_MART.latest_po_qty_all_time,
              latest_po_selling_all_time      = MER_MART.latest_po_selling_all_time,
              latest_po_cost_all_time         = MER_MART.latest_po_cost_all_time,
              avg_po_rsp_excl_vat_all_time    = MER_MART.avg_po_rsp_excl_vat_all_time,
              avg_po_cost_price_all_time      = MER_MART.avg_po_cost_price_all_time,
              avg_po_margin_perc_all_time     = MER_MART.avg_po_margin_perc_all_time,
              fillrate_order_qty              = MER_MART.fillrate_order_qty,
              fillrate_order_selling          = MER_MART.fillrate_order_selling,
              fillrate_order_excl_wh_qty      = MER_MART.fillrate_order_excl_wh_qty,
              fillrate_order_excl_wh_selling  = MER_MART.fillrate_order_excl_wh_selling,
              last_updated_date               = MER_MART.last_updated_date,
              fillrate_fd_po_grn_qty          = MER_MART.fillrate_fd_po_grn_qty,
              fillrate_fd_po_grn_qty_import   = MER_MART.fillrate_fd_po_grn_qty_import,
              fillrate_fd_po_grn_qty_local    = MER_MART.fillrate_fd_po_grn_qty_local,
              fillrate_fd_latest_po_qty       = MER_MART.fillrate_fd_latest_po_qty,
              fillrte_fd_latest_po_qty_imprt  = MER_MART.fillrte_fd_latest_po_qty_imprt,
              fillrte_fd_latest_po_qty_local  = MER_MART.fillrte_fd_latest_po_qty_local,
              fd_from_loc_reg_rsp             = MER_MART.fd_from_loc_reg_rsp

  when not matched then
     insert
      (SK1_PO_NO,
                  SK1_SUPPLY_CHAIN_NO,
                  SK1_LOCATION_NO,
                  SK1_ITEM_NO,
                  TRAN_DATE,
                  SK1_CONTRACT_NO,
                  SK1_SUPPLIER_NO,
                  SK2_LOCATION_NO,
                  SK2_ITEM_NO,
                  PO_IND,
                  NOT_BEFORE_DATE,
                  NOT_AFTER_DATE,
                  PO_STATUS_CODE,
                  CANCEL_CODE,
                  REG_RSP,
                  COST_PRICE,
                  CASE_SELLING_EXCL_VAT,
                  CASE_COST,
                  SUPP_PACK_SIZE,
                  ORIGINAL_PO_QTY,
                  ORIGINAL_PO_SELLING,
                  ORIGINAL_PO_COST,
                  ORIGINAL_PO_CASES,
                  AMENDED_PO_QTY,
                  AMENDED_PO_SELLING,
                  AMENDED_PO_COST,
                  AMENDED_PO_CASES,
                  AMENDED_PO_MARGIN,
                  BUYER_EDITED_PO_QTY,
                  CANCEL_PO_QTY,
                  CANCEL_PO_SELLING,
                  CANCEL_PO_COST,
                  CANCEL_PO_CASES,
                  REJECTION_QTY,
                  REJECTION_SELLING,
                  REJECTION_COST,
                  REJECTED_CASES,
                  PO_GRN_QTY,
                  PO_GRN_SELLING,
                  PO_GRN_COST,
                  PO_GRN_CASES,
                  PO_GRN_FR_COST,
                  SHORTS_QTY,
                  SHORTS_SELLING,
                  SHORTS_COST,
                  SHORTS_CASES,
                  BC_SHIPMENT_QTY,
                  BC_SHIPMENT_SELLING,
                  BC_SHIPMENT_COST,
                  ACTL_GRN_QTY,
                  ACTL_GRN_SELLING,
                  ACTL_GRN_COST,
                  ACTL_GRN_CASES,
                  ACTL_GRN_FR_COST,
                  ACTL_GRN_MARGIN,
                  FILLRATE_ACTL_GRN_EXCL_WH_QTY,
                  FILLRTE_ACTL_GRN_EXCL_WH_SELL,
                  LATEST_PO_QTY,
                  LATEST_PO_SELLING,
                  LATEST_PO_COST,
                  LATEST_PO_CASES,
                  LATEST_PO_QTY_ALL_TIME,
                  LATEST_PO_SELLING_ALL_TIME,
                  LATEST_PO_COST_ALL_TIME,
                  AVG_PO_RSP_EXCL_VAT_ALL_TIME,
                  AVG_PO_COST_PRICE_ALL_TIME,
                  AVG_PO_MARGIN_PERC_ALL_TIME,
                  NUM_DU,
                  NUM_DAYS_TO_DELIVER_PO,
                  NUM_WEIGHTED_DAYS_TO_DELIVER,
                  FILLRATE_ORDER_QTY,
                  FILLRATE_ORDER_SELLING,
                  FILLRATE_ORDER_EXCL_WH_QTY,
                  FILLRATE_ORDER_EXCL_WH_SELLING,
                  LAST_UPDATED_DATE,
                  FILLRATE_FD_PO_GRN_QTY,
                  FILLRATE_FD_PO_GRN_QTY_IMPORT,
                  FILLRATE_FD_PO_GRN_QTY_LOCAL,
                  FILLRATE_FD_LATEST_PO_QTY,
                  FILLRTE_FD_LATEST_PO_QTY_IMPRT,
                  FILLRTE_FD_LATEST_PO_QTY_LOCAL,
                  FILLRATE_ACTL_GRN_QTY,
                  FILLRATE_ACTL_GRN_SELLING,
                  FD_FROM_LOC_REG_RSP  )
   values
      (MER_MART.SK1_PO_NO
                  ,MER_MART.SK1_SUPPLY_CHAIN_NO
                  ,MER_MART.SK1_LOCATION_NO
                  ,MER_MART.SK1_ITEM_NO
                  ,MER_MART.TRAN_DATE
                  ,MER_MART.SK1_CONTRACT_NO
                  ,MER_MART.SK1_SUPPLIER_NO
                  ,MER_MART.SK2_LOCATION_NO
                  ,MER_MART.SK2_ITEM_NO
                  ,MER_MART.PO_IND
                  ,MER_MART.NOT_BEFORE_DATE
                  ,MER_MART.NOT_AFTER_DATE
                  ,MER_MART.PO_STATUS_CODE
                  ,MER_MART.CANCEL_CODE
                  ,MER_MART.REG_RSP
                  ,MER_MART.COST_PRICE
                  ,MER_MART.CASE_SELLING_EXCL_VAT
                  ,MER_MART.CASE_COST
                  ,MER_MART.SUPP_PACK_SIZE
                  ,MER_MART.ORIGINAL_PO_QTY
                  ,MER_MART.ORIGINAL_PO_SELLING
                  ,MER_MART.ORIGINAL_PO_COST
                  ,MER_MART.ORIGINAL_PO_CASES
                  ,MER_MART.AMENDED_PO_QTY
                  ,MER_MART.AMENDED_PO_SELLING
                  ,MER_MART.AMENDED_PO_COST
                  ,MER_MART.AMENDED_PO_CASES
                  ,MER_MART.AMENDED_PO_MARGIN
                  ,MER_MART.BUYER_EDITED_PO_QTY
                  ,MER_MART.CANCEL_PO_QTY
                  ,MER_MART.CANCEL_PO_SELLING
                  ,MER_MART.CANCEL_PO_COST
                  ,MER_MART.CANCEL_PO_CASES
                  ,MER_MART.REJECTION_QTY
                  ,MER_MART.REJECTION_SELLING
                  ,MER_MART.REJECTION_COST
                  ,MER_MART.REJECTED_CASES
                  ,MER_MART.PO_GRN_QTY
                  ,MER_MART.PO_GRN_SELLING
                  ,MER_MART.PO_GRN_COST
                  ,MER_MART.PO_GRN_CASES
                  ,MER_MART.PO_GRN_FR_COST
                  ,MER_MART.SHORTS_QTY
                  ,MER_MART.SHORTS_SELLING
                  ,MER_MART.SHORTS_COST
                  ,MER_MART.SHORTS_CASES
                  ,MER_MART.BC_SHIPMENT_QTY
                  ,MER_MART.BC_SHIPMENT_SELLING
                  ,MER_MART.BC_SHIPMENT_COST
                  ,MER_MART.ACTL_GRN_QTY
                  ,MER_MART.ACTL_GRN_SELLING
                  ,MER_MART.ACTL_GRN_COST
                  ,MER_MART.ACTL_GRN_CASES
                  ,MER_MART.ACTL_GRN_FR_COST
                  ,MER_MART.ACTL_GRN_MARGIN
                  ,MER_MART.FILLRATE_ACTL_GRN_EXCL_WH_QTY
                  ,MER_MART.FILLRTE_ACTL_GRN_EXCL_WH_SELL
                  ,MER_MART.LATEST_PO_QTY
                  ,MER_MART.LATEST_PO_SELLING
                  ,MER_MART.LATEST_PO_COST
                  ,MER_MART.LATEST_PO_CASES
                  ,MER_MART.LATEST_PO_QTY_ALL_TIME
                  ,MER_MART.LATEST_PO_SELLING_ALL_TIME
                  ,MER_MART.LATEST_PO_COST_ALL_TIME
                  ,MER_MART.AVG_PO_RSP_EXCL_VAT_ALL_TIME
                  ,MER_MART.AVG_PO_COST_PRICE_ALL_TIME
                  ,MER_MART.AVG_PO_MARGIN_PERC_ALL_TIME
                  ,MER_MART.NUM_DU
                  ,MER_MART.NUM_DAYS_TO_DELIVER_PO
                  ,MER_MART.NUM_WEIGHTED_DAYS_TO_DELIVER
                  ,MER_MART.FILLRATE_ORDER_QTY
                  ,MER_MART.FILLRATE_ORDER_SELLING
                  ,MER_MART.FILLRATE_ORDER_EXCL_WH_QTY
                  ,MER_MART.FILLRATE_ORDER_EXCL_WH_SELLING
                  ,MER_MART.LAST_UPDATED_DATE
                  ,MER_MART.FILLRATE_FD_PO_GRN_QTY
                  ,MER_MART.FILLRATE_FD_PO_GRN_QTY_IMPORT
                  ,MER_MART.FILLRATE_FD_PO_GRN_QTY_LOCAL
                  ,MER_MART.FILLRATE_FD_LATEST_PO_QTY
                  ,MER_MART.FILLRTE_FD_LATEST_PO_QTY_IMPRT
                  ,MER_MART.FILLRTE_FD_LATEST_PO_QTY_LOCAL
                  ,MER_MART.FILLRATE_ACTL_GRN_QTY
                  ,MER_MART.FILLRATE_ACTL_GRN_SELLING
                  ,MER_MART.FD_FROM_LOC_REG_RSP
                  );

    g_recs_inserted := 0;
    g_recs_inserted :=  SQL%ROWCOUNT;
    commit;
    l_text := 'Recs merged into RTL_PO_SUPCHAIN_LOC_ITEM_DY='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := 'error in MERGE_INTO_RTL '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end MERGE_INTO_RTL;


--**************************************************************************************************
-- Delete records where record has not been updated
--**************************************************************************************************
procedure delete_invalid_recs as
begin

    l_text := 'STARTING DELETE LIST ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    insert into DWH_PERFORMANCE.temp_po_deletes
    select /*+ no_index (r) full(r) parallel (r,8) */
    r.sk1_po_no, r.sk1_supply_chain_no, r.sk1_location_no, r.sk1_item_no, r.tran_date,
	         r.sk1_supplier_no, di.sk1_style_colour_no, dc.fin_year_no, dc.fin_week_no
    from   rtl_po_supchain_loc_item_dy r
    join   DWH_PERFORMANCE.temp_po_list t     on  r.sk1_po_no        = t.sk1_po_no
                              and r.sk1_item_no      = t.sk1_item_no
    join   dim_item di        on  t.sk1_item_no      = di.sk1_item_no
    join   dim_calendar dc    on  r.tran_date        = dc.calendar_date
    where  r.po_ind = 1
    and    r.last_updated_date <> g_date;

    g_recs_dlet_cnt := sql%rowcount;
    commit;
    l_text := 'RECORDS TO BE DELETED - '||g_recs_dlet_cnt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    delete from DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY r
    where  exists(select t.sk1_po_no
                  from   DWH_PERFORMANCE.temp_po_deletes t
                  where  t.sk1_po_no   = r.sk1_po_no
                  and    t.sk1_item_no = r.sk1_item_no
                  and    t.SK1_SUPPLY_CHAIN_NO = r.SK1_SUPPLY_CHAIN_NO
                  and    t.SK1_LOCATION_NO = r.SK1_LOCATION_NO
                  and    t.tran_date = r.TRAN_DATE);
   --    COMMENTED OUT CODE                                                                 --    delete from RTL_PO_SUPCHAIN_LOC_ITEM_DY r
                                                                        --    where  exists(select t.po_no
                                                                        --                  from   DWH_PERFORMANCE.temp_po_list t
                                                                        --                  join   dim_purchase_order dp on dse2.po_no   = t.po_no
                                                                        --                  join   dim_item di           on di.item_no = t.item_no
                                                                        --                  where  dse2.sk1_po_no   = r.sk1_po_no
                                                                        --                  and    di.sk1_item_no = r.sk1_item_no)
                                                                        --    and    po_ind = 1
                                                                        --    and    last_updated_date <> g_date;

    g_recs_deleted  := g_recs_deleted  + sql%rowcount;
    commit;
    l_text := 'DELETE COMPLETED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_invalid_recs;

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
    l_text := 'LOAD OF RTL_PO_SUPCHAIN_LOC_ITEM_DY EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
 --       G_DATE := '29 AUG 2016';
 --   l_text := 'HARDCODE G_DATE='||G_DATE;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

    l_text := 'NON-DAVID JONES PROCESSING';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    execute immediate 'truncate table DWH_PERFORMANCE.TEMP_PO_DATA';
    l_text := 'DWH_PERFORMANCE.TEMP_PO_DATA TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    INSERT_INTO_TEMP_PO_DATA;

    execute immediate 'truncate table DWH_PERFORMANCE.TEMP_PO_CALC';
    l_text := 'DWH_PERFORMANCE.TEMP_PO_CALC TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    INSERT_INTO_TEMP_PO_CALC;

    MERGE_INTO_RTL;

    execute immediate 'truncate table DWH_PERFORMANCE.temp_po_deletes';
    l_text := 'TABLE DWH_PERFORMANCE.temp_po_deletes TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DELETE_INVALID_RECS;

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

end wh_prf_corp_661U;
