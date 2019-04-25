--------------------------------------------------------
--  DDL for Procedure REP_PRF_FPI_016U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."REP_PRF_FPI_016U" 
   (
    pBusinessUnit IN NUMBER DEFAULT NULL,
    pGroupNo      IN NUMBER DEFAULT NULL,
    pDepartment   IN NUMBER DEFAULT NULL,
    pQuarterNo    IN NUMBER DEFAULT NULL,
    pFinYear      IN NUMBER DEFAULT NULL,
    prc OUT sys_refcursor 
   )
AS
  --**************************************************************************************************
  --  Date:        October 2018
  --  Author:      A. Ugolini
  --  Purpose:     FPI REPORT
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
  query_str5    VARCHAR2 (4000);
  query_str6    VARCHAR2 (4000);
  query_str7    VARCHAR2 (4000);

  proc_sel          VARCHAR2 (4000);
  proc_prod_sel     VARCHAR2 (4000);
  proc_time_sel     VARCHAR2 (4000);
  sub_proc_time_sel VARCHAR2 (4000);

  proc_year         VARCHAR2 (50);
  sub_proc_year     VARCHAR2 (50);

  query_string      VARCHAR2 (17000);

  proc_BusinessUnit NUMBER        :=pBusinessUnit;
  proc_GroupNo      NUMBER        :=pGroupNo;
  proc_Department   NUMBER        :=pDepartment;
  proc_QuarterNo    NUMBER        :=pQuarterNo;
  proc_FinYear      NUMBER        :=pFinYear; 

--- Build dynamic sql where clause

BEGIN
  proc_sel   := ' ';
  proc_year  := ' ';
  sub_proc_year  := ' ';

  proc_prod_sel  := ' ';
  proc_time_sel  := ' ';
  sub_proc_time_sel  := ' ';

----  Product Hierachy Selection ??

  IF proc_BusinessUnit is not null THEN
     proc_prod_sel  := '  di.business_unit_no =  ' || proc_BusinessUnit;
  END IF;
  IF proc_GroupNo is not null THEN
    proc_prod_sel  := '  di.group_no = ' || proc_GroupNo;
  END IF;
  IF proc_Department is not null THEN
    proc_prod_sel  := '  di.department_no = ' || proc_Department;
 END IF;

----  Fin Year OR Fin Quarter ??

  IF proc_FinYear is not null THEN
    proc_year    := ' ytd.fin_year_no = ' || proc_FinYear ;
    sub_proc_year    := ' z.fin_year_no = ' || proc_FinYear ;
    proc_time_sel:= ' ytd.fin_year_no = ' || proc_FinYear ;
    sub_proc_time_sel := ' z.fin_year_no = ' || proc_FinYear ;
  END IF;

  IF proc_QuarterNo is not null THEN
    proc_year	 := ' ytd.fin_year_no = '  || round(proc_QuarterNo / 10);
    sub_proc_year	 := ' z.fin_year_no = '  || round(proc_QuarterNo / 10);
    proc_FinYear := round(proc_QuarterNo / 10);
    proc_time_sel:= proc_year || ' and ytd.fin_year_quarter_no = ' || proc_QuarterNo ;
    sub_proc_time_sel:= proc_year || ' and z.fin_year_quarter_no = ' || proc_QuarterNo ;
  END IF;

  proc_sel := proc_prod_sel || ' and ' ||  proc_time_sel;

--- Build selection query
---- Sales  data
query_str1 :=' with sel as 
(
SELECT /*+ full(gbj) full (ytd) full(di) */
    ytd.sk1_item_no,
    di.item_no,
    di.item_short_desc,
    di.business_unit_no,
    di.group_no,
    di.group_long_desc,
    di.department_no,
    di.department_long_desc,
    SUM(ytd.sales) sales,
    0 sales_ytd,
    0 sales_lytd
 FROM
--    dwh_performance.temp_fpi_foods_item_salesytd ytd,
      dwh_performance.rtl_item_wk_sales_ytd ytd,
      dwh_performance.dim_item 	             di,
	  dwh_performance.dim_control     cal
 where
   	ytd.sk1_item_no  = di.sk1_item_no
   and  ytd.fin_week_no <= cal.last_wk_fin_week_no  
   and '  || proc_sel || '
--   and  ytd.sk1_item_no = 28555422
 GROUP BY
    ytd.sk1_item_no,
    di.item_no,
    di.item_short_desc,
    di.business_unit_no,
    di.group_no,
    di.group_long_desc,
    di.department_no,
    di.department_long_desc,
    0,
    0
' ||
---- Sales YTD data
' UNION ALL
SELECT /*+  full(gbj) full (ytd) full(di) */
    ytd.sk1_item_no,
    di.item_no,
    di.item_short_desc,
    di.business_unit_no,
    di.group_no,
    di.group_long_desc,
    di.department_no,
    di.department_long_desc,
    0 sales,
    sales_ytd,
    sales_lytd
 FROM
    	--    	dwh_performance.temp_fpi_foods_item_salesytd ytd,
      dwh_performance.rtl_item_wk_sales_ytd ytd,
    	dwh_performance.dim_item 	             di,
	    dwh_performance.dim_control  		     cal 
 where
   	ytd.sk1_item_no  = di.sk1_item_no
--   and  ytd.fin_week_no  = cal.last_wk_fin_week_no  
  and  ytd.fin_week_no  = cal.last_wk_fin_week_no  -- -1 
   and '  || proc_prod_sel || '
   and '  || proc_year || '
--   and  ytd.sk1_item_no = 28555422
),';

--- Combined Sales data          
query_str2 :=' sls as  
(select
    sk1_item_no,
    item_no,
    item_short_desc,
    business_unit_no,
    group_no,
    group_long_desc,
    department_no,
    department_long_desc,
    sum(sales) sales,
    sum(sales_ytd) sales_ytd,
    sum(sales_lytd) sales_lytd
from sel 
group by
    sk1_item_no,
    item_no,
    item_short_desc,
    business_unit_no,
    group_no,
    group_long_desc,
    department_no,
    department_long_desc
),';

---- GBJ data
query_str3 := 'gbj as 
(select  
    sk1_item_no,
    sk1_supplier_no,
    attribute_code,
    attribute_value,
    gbj_count
from  	

    rtl_item_sup_wk_gbj_brand       ytd,    
    dwh_performance.dim_control     cal
 where
        attribute_code is not null
  --and ytd.fin_year_no  = ' || proc_FinYear || ' 
   and ' || proc_time_sel || '
   and  ytd.fin_week_no <= cal.last_wk_fin_week_no -- - 1
   and ytd.spec_version = (select max(z.spec_version)
                           from rtl_item_sup_wk_gbj_brand z
                           where z.attribute_code is not null
                           --and  ytd.fin_year_no  = z.fin_year_no
                           --and  ytd.fin_year_quarter_no = z.fin_year_quarter_no
                           and ' || sub_proc_time_sel || ' 
                           and  ytd.fin_week_no <= cal.last_wk_fin_week_no -- - 1 
                           and ytd.SK1_ITEM_NO = z.SK1_ITEM_NO
                           
                           )
GROUP BY
    sk1_item_no,
    sk1_supplier_no,
    attribute_code,
    attribute_value,
    gbj_count
),' ;
---- GRN current data
query_str4 := 'sup as 
(select distinct 
    sk1_item_no,
    sk1_supplier_no,
    po_grn_qty_ytd    
from  	
--    temp_fpi_grnytd_item_data       tmp,
    rtl_item_sup_wk_grn_ytd         ytd,
    dwh_performance.dim_control     cal
 where
        ytd.fin_year_no  = ' || proc_FinYear || ' 
   and  ytd.fin_week_no = cal.last_wk_fin_week_no  -- -1 
' ||
---- GRN YTD data
' UNION ALL
select distinct 
    sk1_item_no,
    sk1_supplier_no,
    0 po_grn_qty_ytd    
from  	
--    temp_fpi_grnytd_item_data       tmp,
    rtl_item_sup_wk_grn_ytd       ytd,
    dwh_performance.dim_control   cal
 where
        po_grn_qty_ytd > 0
   and  ytd.fin_year_no  = ' || proc_FinYear || ' 
   and  ytd.fin_week_no <= cal.last_wk_fin_week_no 
),' ;

--- Combined GRN data          
query_str5 :=' grn as  
(select
    sk1_item_no,
    sk1_supplier_no,
    sum(po_grn_qty_ytd) po_grn_qty_ytd
from sup 
group by
    sk1_item_no,
    sk1_supplier_no
),';

--- Data Output - Part 1 (Sales & GBJ)
query_str6 :=' rst as 
(select
    s.sk1_item_no,
    item_no,
    item_short_desc,
    business_unit_no,
    group_no,
    group_long_desc,
    department_no,
    department_long_desc,
    sales,
    sales_ytd,
    sales_lytd,
---
    g.sk1_supplier_no,
    supplier_no,
    supplier_name,
    attribute_code,
    attribute_value,
    gbj_count
from sls s,
     gbj g,
     dwh_performance.dim_supplier dim
where 
      s.sk1_item_no = g.sk1_item_no
  and g.sk1_supplier_no = dim.sk1_supplier_no
  and sales > 10000
)' ;

--- Data Output - Part 2 (include GRN)  
query_str7 :=' 
select
    s.sk1_item_no,
    item_no,
    item_short_desc,
    business_unit_no,
    group_no,
    group_long_desc,
    department_no,
    department_long_desc,
    sales,
    sales_ytd,
    sales_lytd,
    s.sk1_supplier_no,
    supplier_no,
    supplier_name,
    po_grn_qty_ytd,
    attribute_code,
    attribute_value,
    gbj_count
from rst s
 left outer join grn g
    on s.sk1_item_no     = g.sk1_item_no
   and s.sk1_supplier_no = g.sk1_supplier_no
order by s.sk1_item_no, sk1_supplier_no 
';

--- 
--dbms_output.put_line(query_string);
query_string:= query_str1 || query_str2 || query_str3 || 
query_str4 || query_str5  || query_str6 || query_str7;

--  INSERT INTO w7122944.mysqtony
--    (f1) VALUES (query_string);
COMMIT;

--- Build refcursor to pass back the query result set

open prc for query_string;
END REP_PRF_FPI_016U;
