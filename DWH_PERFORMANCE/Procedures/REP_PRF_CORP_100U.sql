--------------------------------------------------------
--  DDL for Procedure REP_PRF_CORP_100U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."REP_PRF_CORP_100U" 
   (
    pLocation     IN NUMBER DEFAULT NULL,
    pFrom_date	  date,
    pTo_date      date,
    pReason_code  IN NUMBER DEFAULT NULL,
    prc OUT sys_refcursor 
    )
AS
  --**************************************************************************************************
  --  Date:        October 2018
  --  Author:      A. Ugolini
  --  Purpose:     Stored Procedures being called for the DC Inventory Adjustment Reporting.
  --
  --  Maintenance:
  --  Date:        
  --  Author:      
  --  Change:      
  --
  --**************************************************************************************************

  query_str1    VARCHAR2 (4000);
  query_str2    VARCHAR2 (4000);
  query_str3    VARCHAR2 (4000);
  query_str4    VARCHAR2 (4000);

  proc_sel      VARCHAR2 (4000);
  proc_loc_sel  VARCHAR2 (50);
  proc_filter   VARCHAR2 (50);
  query_string  VARCHAR2 (17000);

  proc_location      NUMBER         :=pLocation;
  proc_from_Date     date           :=pFrom_date;
  proc_to_Date       date           :=pTo_date;
  proc_reason_code   NUMBER         :=pReason_Code;

--- Build dynamic sql where clause

BEGIN

  proc_sel 	:= ' ';
  proc_loc_sel 	:= ' ';
  proc_filter 	:= ' ';

  IF proc_location = 1 THEN
    proc_loc_sel  := ' and  l.location_no in (6010,6020,6030) ' ;
  END IF;
  IF proc_location = 2 THEN
    proc_loc_sel  := ' and  l.location_no in (6060,6070,6080) ' ;
  END IF;
  IF proc_location = 3 THEN
    proc_loc_sel  := ' and  l.location_no = 6110              ' ;
 END IF;
  IF proc_location not in (1,2,3) THEN
    proc_loc_sel  := ' and  l.location_no = ' || proc_location  ;
  END IF;

  proc_sel := ' and f.tran_date between  (''' ||  proc_from_Date || ''') and (''' ||  proc_to_date  || ''') ';

  IF proc_reason_code <> -1 and proc_reason_code <> null THEN
    proc_filter   := ' where reason_code = ' || proc_reason_code  ;
  END IF;

--- Build selection query

query_str1 :=' with sel as 
(
select /*+ full(f) full(l) full(i) full(z) */
       f.reason_code, 
       f.reason_code || '' - '' || f.reason_desc reason_no_and_name,
       f.tran_date,  
       f.post_date,
       i.department_long_desc,
       i.fd_product_no,
       i.item_no,
       f.sk1_location_no, 
       f.sk1_item_no,
       i.item_upper_desc,
       z.num_units_per_tray,
       i.standard_uom_code,
       f.inv_adj_qty/nvl(z.num_units_per_tray,1) inv_adj_cases,
       case when i.random_mass_ind=0 and i.standard_uom_code=''KG'' then nvl(z.num_units_per_tray,1)/1000
       else nvl(z.num_units_per_tray,1) end units_mass_per_tray, 
       f.inv_adj_cost,
       f.ref_id_1 
from 
    DWH_PERFORMANCE.RTL_LOC_ITEM_DY_INV_ADJ   f,
    DWH_PERFORMANCE.DIM_LOCATION 	  l,
    DWH_PERFORMANCE.DIM_ITEM          i,
    DWH_PERFORMANCE.RTL_ZONE_ITEM_OM  z
where f.tran_date between (''' ||  proc_from_Date || ''') and (''' || proc_to_date ||''')
  and f.inv_adj_no                 <> 0 
  and  f.reason_code               NOT IN (12, 13)
  and  f.sk1_location_no           = l.sk1_location_no
  and  f.sk1_item_no               = i.sk1_item_no
  and  f.sk1_item_no               = z.sk1_item_no
  and  l.sk1_fd_zone_group_zone_no = z.sk1_zone_group_zone_no
  and  i.business_unit_no		   = 50 '
  || proc_loc_sel || '
' ;
query_str2 :='  UNION All  
select /*+ full(f) full(l) full(i) full(z) */
       f.reason_code, 
       f.reason_code || '' - '' || f.reason_desc reason_no_and_name,
       f.tran_date,  
       f.post_date,
       i.department_long_desc,
       i.fd_product_no,
       i.item_no,
       f.sk1_location_no, 
       f.sk1_item_no,
       i.item_upper_desc,
       z.num_units_per_tray,
       i.standard_uom_code,
       f.inv_adj_qty/nvl(z.num_units_per_tray,1) inv_adj_cases,
       case when i.random_mass_ind=0 and i.standard_uom_code=''KG'' then nvl(z.num_units_per_tray,1)/1000
       else nvl(z.num_units_per_tray,1) end units_mass_per_tray, 
       f.inv_adj_cost,
       f.ref_id_1 
from 
    DWH_PERFORMANCE.RTL_LOC_ITEM_DY_INV_ADJ    f,
    DWH_PERFORMANCE.DIM_LOCATION 	  l,
    DWH_PERFORMANCE.DIM_ITEM          i,
    DWH_PERFORMANCE.RTL_ZONE_ITEM_OM  z
where f.tran_date between (''' ||  proc_from_Date || ''') and (''' || proc_to_date ||''')
     and  f.liability_code * 10       = l.location_no
     and  f.inv_adj_no                <> 0 
     and  f.reason_code               IN (12, 13)
     and  f.sk1_item_no               = i.sk1_item_no
     and  f.sk1_item_no               = z.sk1_item_no
     and  l.sk1_fd_zone_group_zone_no = z.sk1_zone_group_zone_no
     and  i.business_unit_no		  = 50 '
     || proc_loc_sel || '
' ;
query_str3 :='  UNION All  
select /*+ full(f) full(l) full(i) full(z) */
               123245 reason_code, ' ||
                '''RTV - RTV to Suppliers''' || ' reason_no_and_name,
                f.tran_date,
                f.post_date,
                i.department_long_desc,
                i.fd_product_no,
                i.item_no,
                f.sk1_location_no,
                f. sk1_item_no,
                i.item_upper_desc,
                z.num_units_per_tray,
                i.standard_uom_code,
                f.rtv_qty / NVL (z.num_units_per_tray, 1) inv_adj_cases,
                CASE
                   WHEN i.random_mass_ind = 0 AND i.standard_uom_code = ''KG''
                   THEN
                      NVL (z.num_units_per_tray, 1) / 1000
                   WHEN i.random_mass_ind = 0 AND i.standard_uom_code <> ''KG''
                   THEN
                      NVL (z.num_units_per_tray, 1)
                   ELSE
                      z.case_mass / NVL (z.num_units_per_tray, 1)
                END
                   units_mass_per_tray,
                f.rtv_cost inv_adj_cost,
                f.rtv_ref_id ref_id_1
           FROM 
                DWH_PERFORMANCE.RTL_LOC_ITEM_DY_RTV       f,                       
                DWH_PERFORMANCE.DIM_LOCATION 	  l,
                DWH_PERFORMANCE.DIM_ITEM          i,
                DWH_PERFORMANCE.RTL_ZONE_ITEM_OM  z
where f.tran_date between (''' ||  proc_from_Date || ''') and (''' || proc_to_date ||''')
    and  f.sk1_location_no           = l.sk1_location_no
    and  f.sk1_item_no               = i.sk1_item_no
    and  f.sk1_item_no               = z.sk1_item_no
    and  l.sk1_fd_zone_group_zone_no = z.sk1_zone_group_zone_no
    and  i.business_unit_no		     = 50 '
    || proc_loc_sel || '
)' ;         

query_str4 :=' 
select
        reason_code,
        reason_no_and_name,
        department_long_desc,
        tran_date,
        post_date,
        fd_product_no,
        item_no,
        item_upper_desc,
        num_units_per_tray,
        standard_uom_code,
        units_mass_per_tray,
        floor(sum(inv_adj_cases)) inv_adj_cases,
        sum(inv_adj_cost) inv_adj_cost,
        ref_id_1
from sel ' || proc_filter ||  
 ' group by
       reason_code,
       reason_no_and_name,
       department_long_desc,
       tran_date,
       post_date,
       fd_product_no,
       item_no,
       item_upper_desc,
       num_units_per_tray,
       standard_uom_code,
       units_mass_per_tray,
       ref_id_1'
;
--- 
--- Combine the result sets for one cursor string
query_string:=query_str1 ||  query_str2 || query_str3 || query_str4;
--dbms_output.put_line(query_string);

COMMIT;

--- Build refcursor to pass back the query result set

OPEN prc FOR query_string;
END REP_PRF_CORP_100U;
