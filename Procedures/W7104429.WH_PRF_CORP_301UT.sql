-- ****** Object: Procedure W7104429.WH_PRF_CORP_301UT Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "WH_PRF_CORP_301UT" 
   (
    pYear       IN NUMBER DEFAULT NULL,
    pRplInd     IN NUMBER DEFAULT NULL,
    pDepartment IN NUMBER DEFAULT NULL,
    pClass      IN NUMBER DEFAULT NULL,
    pSubclass   IN NUMBER DEFAULT NULL,
    pSupplier   IN NUMBER DEFAULT NULL,
    pSeason     IN NUMBER DEFAULT NULL,
    pFromWeekNo IN NUMBER DEFAULT NULL,
    pToWeekNo   IN NUMBER DEFAULT NULL,
    prc OUT sys_refcursor )
AS
  --**************************************************************************************************
  --  Date:        September 2017
  --  Author:      A. Ugolini
  --  Purpose:     Stored Procedures being called for the Procurment Reporting.
  --
  --  Maintenance:
  --  Date:        November 2017
  --  Author:      Lisa Kriel
  --  Change:      Added fields so values can be summed and aggregated in report
  --
  --  Date:        May 2018
  --  Author:      Lisa Kiel
  --  Change:      Combined ''BO'' and ''CON'' records as there is no reason to have them separate
  --**************************************************************************************************
  query_str0    VARCHAR2 (1000);
  query_str1    VARCHAR2 (4000);
  query_str2    VARCHAR2 (4000);
  query_str3    VARCHAR2 (3000);
  query_str4    VARCHAR2 (5000);
  query_string  VARCHAR2 (17000);
  proc_period   VARCHAR2 (17000);
  proc_sel      VARCHAR2 (17000);

  proc_fin_year NUMBER       :=pYear;
  proc_rpl      NUMBER       :=pRplInd;
  proc_dept     NUMBER       :=pDepartment;
  proc_class    NUMBER       :=pClass;
  proc_subclass NUMBER       :=pSubclass;
  proc_supplier NUMBER       :=pSupplier;
  proc_season   NUMBER       :=pSeason;
  proc_wk_from  NUMBER       :=pFromWeekNo;
  proc_wk_to    NUMBER       :=pToWeekNo;

--- Build dynamic sql where clause

BEGIN
  IF proc_fin_year IS NOT NULL THEN
     proc_period       :='cal.fin_year_no             = ' || proc_fin_year;
  ELSE 
     proc_period       := '1=1';  --- Note year is expected
  END IF;
  IF proc_season IS NOT NULL THEN
    proc_period     :=proc_period||' and cal.fin_half_no    = ' || proc_season;
  END IF;
  IF proc_wk_from IS NOT NULL AND proc_wk_to IS NOT NULL THEN
    proc_period      :=proc_period||' and cal.fin_week_no between  ' || proc_wk_from || ' and ' || proc_wk_to;
  END IF;
  proc_sel := proc_period;
  IF proc_rpl IS NOT NULL THEN
    proc_sel  :=proc_sel||' and item.rpl_ind          = ' || proc_rpl;
  END IF;
  IF proc_dept IS NOT NULL THEN
    proc_sel   :=proc_sel||' and item.department_no   = ' || proc_dept;
  END IF;
  IF proc_class IS NOT NULL THEN
    proc_sel    :=proc_sel||' and item.class_no       = ' || proc_class;
  END IF;
  IF proc_subclass IS NOT NULL THEN
    proc_sel       :=proc_sel||' and item.subclass_no = ' || proc_subclass;
  END IF;
  IF proc_supplier IS NOT NULL THEN
    proc_sel       :=proc_sel||' and sup.supplier_no  = ' || proc_supplier;
  END IF;
--- Include all weeks in selected period
query_str0 :=
'with week_list as (
   select   FIN_YEAR_NO ,  
         FIN_HALF_NO ,  
         SEASON_NAME ,
         MONTH_NAME ,  
         THIS_WEEK_START_DATE ,  
         FIN_WEEK_NO
   from dim_calendar_wk cal
  where ' || proc_period ||
  ' order by THIS_WEEK_START_DATE ASC
  ), ';
--dbms_output.put_line(query_str0);
--- Build dynamic sql Purchase Order Query (PO) - t1
query_str1 := ' t1 as  
(select  
item.department_no,
item.department_name,
item.class_no,
item.class_name,
item.subclass_no,  
item.subclass_name,  
case when item.rpl_ind =0 then ''FAST''     
when item.rpl_ind =1 then ''RPL''     
else NULL end Merch_Class,  
item.item_level1_no,  
item.item_level1_desc,  
item.color_id,  
uda.PRODUCT_SAMPLE_STATUS_DESC_304 PRODUCT_SAMPLE_STATUS,  
sup.supplier_no,  
sup.supplier_name,  
po.contract_no contract_no,                                               -- new field
po.po_no,  
''PO'' Source, 
cal.fin_year_no,  
cal.fin_half_no,  
cal.season_name,
cal.month_name,  
cal.this_week_start_date,  
cal.fin_week_no, 
          sum(po.LATEST_PO_QTY_SUMM)      LATEST_PO_QTY,
          sum(po.LATEST_PO_COST_SUMM)     LATEST_PO_COST_SUMM,           --added
          sum(po.LATEST_PO_SELLING_SUMM)  LATEST_PO_SELLING_SUMM,        --added
          case when sum(po.LATEST_PO_QTY_SUMM) > 0 then
          round((nvl(sum(po.LATEST_PO_COST_SUMM)    / sum(po.LATEST_PO_QTY_SUMM),  0)), 2) 
                else 0 end AVG_PO_COST_PRICE,
          case when sum(po.LATEST_PO_QTY_SUMM) > 0 then
          round((nvl(sum(po.LATEST_PO_SELLING_SUMM)  / sum(po.LATEST_PO_QTY_SUMM) , 0)), 2) 
              else 0 end AVG_PO_RSP_EXCL_VAT,
          case when sum(po.LATEST_PO_SELLING_SUMM) > 0 then
          round((1 - nvl( (sum(po.LATEST_PO_COST_SUMM) / sum(po.LATEST_PO_SELLING_SUMM)   ),0)) * 100, 2) 
              else 0 end AVG_PO_MARGIN_PERC,  
null CONTRACT_QTY,  
null AVG_CON_COST_PRICE,  
null AVG_CON_RSP_EXCL_VAT,  
null AVG_CON_MARGIN_PERC,  
null BOC_QTY,  
null BOC_COST_PRICE,                                                     --added
null BOC_RSP_EXCL_VAT,                                                   --added 
null AVG_BOC_COST_PRICE,  
null AVG_BOC_RSP_EXCL_VAT,  
null AVG_BOC_MARGIN_PERC,
null CONTRACT_STATUS_CODE                                                --added
from week_list cal
left join dwh_performance.mart_ch_procurement_po po 
       on po.this_week_start_date = cal.this_week_start_date
left join dim_item item
       on po.sk1_item_no          = item.sk1_item_no      
left join     dim_supplier sup
       on po.sk1_supplier_no      = sup.sk1_supplier_no
left join dim_item_uda uda
       on po.sk1_item_no          = uda.sk1_item_no           
--    dim_calendar_wk     cal    
where  '
  || proc_sel||
  '  
GROUP BY
item.department_no, 
item.department_name, 
item.class_no,  
item.class_name, 
item.subclass_no,  
item.subclass_name,
case when item.rpl_ind =0 then ''FAST''     
when item.rpl_ind =1 then ''RPL''     
else NULL end,  
item.item_level1_no,  
item.item_level1_desc,  
item.color_id,  
uda.PRODUCT_SAMPLE_STATUS_DESC_304,  
sup.supplier_no,  
sup.supplier_name,  
po.contract_no,
po.po_no,  
cal.fin_year_no,  
cal.fin_half_no,  
cal.season_name,
cal.month_name,  
cal.this_week_start_date,  
cal.fin_week_no)
  , '; 
--dbms_output.put_line(query_str1);

--- Build dynamic sql Contract Order Query (CON) - t2
query_str2 :=' t2 AS   
(SELECT 
item.department_no,
item.department_name,
item.class_no,
item.class_name,
item.subclass_no,    
item.subclass_name,  
CASE      
WHEN item.rpl_ind =0  
THEN ''FAST''      
WHEN item.rpl_ind =1      
THEN ''RPL''      
ELSE NULL    
END Merch_Class,    
item.item_level1_no,    
item.item_level1_desc,    
item.color_id,    
uda.PRODUCT_SAMPLE_STATUS_DESC_304 PRODUCT_SAMPLE_STATUS,    
sup.supplier_no,    
sup.supplier_name,    
con.contract_no,    
NULL po_no,    
''BOC'' Source,    
cal.fin_year_no,    
cal.fin_half_no,    
cal.season_name,
cal.month_name,    
cal.this_week_start_date,    
cal.fin_week_no,    
NULL latest_po_qty, 
NULL LATEST_PO_COST_SUMM,                                                --added
NULL LATEST_PO_SELLING_SUMM,                                             --added
NULL avg_po_cost_price,    
NULL avg_po_rsp_excl_vat,    
NULL avg_po_margin_perc,    
sum(con.contract_qty) contract_qty,    
NULL avg_con_cost_price,    
NULL avg_con_rsp_excl_vat,    
NULL avg_con_margin_perc, 
sum(con.BOC_QTY_SUMM)      BOC_QTY,
sum(con.BOC_COST_SUMM)     BOC_COST_PRICE,                               --added
sum(con.BOC_SELLING_SUMM)  BOC_RSP_EXCL_VAT,                             --added          
          case when sum(con.BOC_QTY_SUMM) > 0 then
          round(nvl(sum(con.BOC_COST_SUMM)        / (sum(con.BOC_QTY_SUMM)       ), 0), 2)
              else 0 end AVG_BOC_COST_PRICE,
          case when sum(con.BOC_QTY_SUMM) > 0 then
          round(nvl(sum(con.BOC_SELLING_SUMM)        / (sum(con.BOC_QTY_SUMM)       ), 0), 2)
              else 0 end AVG_BOC_RSP_EXCL_VAT, 
          case when sum(con.BOC_SELLING_SUMM) > 0 then
          round((1 - nvl( (sum(con.BOC_COST_SUMM) / sum(con.BOC_SELLING_SUMM)   ),0)) * 100, 2) 
              else 0 end AVG_BOC_MARGIN_PERC, 
contract.CONTRACT_STATUS_CODE                                             --added
FROM  week_list cal
left join  dwh_performance.mart_ch_procurement_boc con  
       on con.this_week_start_date  = cal.this_week_start_date
left join  dim_item item
       on con.sk1_item_no           = item.sk1_item_no
left join  dim_supplier sup    
       on con.sk1_supplier_no       = sup.sk1_supplier_no
left join  dim_item_uda uda    
       on con.sk1_item_no           = uda.sk1_item_no  
left join  dim_contract contract                                --added
       on con.contract_no           = contract.contract_no                  --added
WHERE  '                             
  || proc_sel||
   '  and contract.contract_status_code in (''A'',''C'')                    --added
   GROUP BY
item.department_no,
item.department_name, 
item.class_no,  
item.class_name, 
item.subclass_no,    
item.subclass_name,  
case when item.rpl_ind =0 then ''FAST''     
when item.rpl_ind =1 then ''RPL''     
else NULL end,      
item.item_level1_no,    
item.item_level1_desc,    
item.color_id,    
uda.PRODUCT_SAMPLE_STATUS_DESC_304,    
sup.supplier_no,    
sup.supplier_name,    
con.contract_no,    
cal.fin_year_no,    
cal.fin_half_no, 
cal.season_name,
cal.month_name,    
cal.this_week_start_date,    
cal.fin_week_no,
contract.CONTRACT_STATUS_CODE
) '; 
--dbms_output.put_line(query_str2); 
-- ** Cater for possible missing weeks ** --
query_str3 :=  ',  
subclass_list as (   
  select distinct  
  DEPARTMENT_NO , 
  DEPARTMENT_NAME,
  CLASS_NO,  
  CLASS_NAME, 
  SUBCLASS_NO ,  
  SUBCLASS_NAME 
 from t1 
union
 select distinct  
  DEPARTMENT_NO , 
  DEPARTMENT_NAME,
  CLASS_NO,  
  CLASS_NAME, 
  SUBCLASS_NO ,  
  SUBCLASS_NAME 
 from t2
 ),
t3 as (
 select * from subclass_list
  cross join week_list 
 where department_no is not null
   and class_no is not null
   and subclass_no is not null
   and fin_week_no is not null
 ) '; 
--dbms_output.put_line(query_str3); 
--- UNION ALL the result sets of all 3 queries  
query_str4 :=' (SELECT   
  DEPARTMENT_NO ,  
  DEPARTMENT_NAME,
  CLASS_NO,  
  CLASS_NAME, 
  SUBCLASS_NO ,  
  SUBCLASS_NAME ,  
  MERCH_CLASS ,  
  ITEM_LEVEL1_NO ,  
  ITEM_LEVEL1_DESC ,  
  COLOR_ID ,  
  PRODUCT_SAMPLE_STATUS,  
  SUPPLIER_NO ,  
  SUPPLIER_NAME ,  
  CONTRACT_NO ,  
  PO_NO ,  
  SOURCE ,  
  FIN_YEAR_NO ,  
  FIN_HALF_NO ,  
  SEASON_NAME ,
  MONTH_NAME ,  
  THIS_WEEK_START_DATE ,  
  FIN_WEEK_NO ,  
  LATEST_PO_QTY ,  
  LATEST_PO_COST_SUMM,                                                     --added
  LATEST_PO_SELLING_SUMM,                                                  --added
  AVG_PO_COST_PRICE ,  
  AVG_PO_RSP_EXCL_VAT ,  
  AVG_PO_MARGIN_PERC ,  
  CONTRACT_QTY , 
  AVG_CON_COST_PRICE ,  
  AVG_CON_RSP_EXCL_VAT ,  
  AVG_CON_MARGIN_PERC ,  
  BOC_QTY ,  
  BOC_COST_PRICE,                                                          --added
  BOC_RSP_EXCL_VAT,                                                        --added
  AVG_BOC_COST_PRICE ,  
  AVG_BOC_RSP_EXCL_VAT ,  
  AVG_BOC_MARGIN_PERC ,
  CONTRACT_STATUS_CODE                                                     --added
FROM t1 
UNION ALL 
SELECT   
  DEPARTMENT_NO , 
  DEPARTMENT_NAME,
  CLASS_NO,  
  CLASS_NAME, 
  SUBCLASS_NO ,  
  SUBCLASS_NAME ,  
  MERCH_CLASS ,  
  ITEM_LEVEL1_NO ,  
  ITEM_LEVEL1_DESC ,  
  COLOR_ID ,  
  PRODUCT_SAMPLE_STATUS ,  
  SUPPLIER_NO ,  
  SUPPLIER_NAME ,  
  CONTRACT_NO ,  
  PO_NO  ,  
  ''BOC'' SOURCE ,  
  FIN_YEAR_NO ,  
  FIN_HALF_NO ,  
  SEASON_NAME ,
  MONTH_NAME ,  
  THIS_WEEK_START_DATE ,  
  FIN_WEEK_NO ,  
  LATEST_PO_QTY , 
  LATEST_PO_COST_SUMM,                                                     --added
  LATEST_PO_SELLING_SUMM,                                                  --added
  AVG_PO_COST_PRICE ,  
  AVG_PO_RSP_EXCL_VAT ,  
  AVG_PO_MARGIN_PERC ,  
  CONTRACT_QTY ,  
  AVG_CON_COST_PRICE ,  
  AVG_CON_RSP_EXCL_VAT ,  
  AVG_CON_MARGIN_PERC ,  
  BOC_QTY ,  
  BOC_COST_PRICE,                                                          --added
  BOC_RSP_EXCL_VAT,                                                        --added
  AVG_BOC_COST_PRICE ,  
  AVG_BOC_RSP_EXCL_VAT ,  
  AVG_BOC_MARGIN_PERC,
  CONTRACT_STATUS_CODE                                                     --added
  FROM t2
UNION ALL 
SELECT   
  DEPARTMENT_NO , 
  DEPARTMENT_NAME,
  CLASS_NO,  
  CLASS_NAME, 
  SUBCLASS_NO ,  
  SUBCLASS_NAME ,  
  null MERCH_CLASS ,  
  null ITEM_LEVEL1_NO ,  
  null ITEM_LEVEL1_DESC ,  
  null COLOR_ID ,  
  null PRODUCT_SAMPLE_STATUS ,  
  null SUPPLIER_NO ,  
  null SUPPLIER_NAME ,  
  null CONTRACT_NO ,  
  null PO_NO  ,  
  ''FIL'' SOURCE ,  
  FIN_YEAR_NO ,  
  FIN_HALF_NO ,  
  SEASON_NAME ,
  MONTH_NAME ,  
  THIS_WEEK_START_DATE ,  
  FIN_WEEK_NO ,  
  0 LATEST_PO_QTY , 
  null LATEST_PO_COST_SUMM,                                                     --added
  null LATEST_PO_SELLING_SUMM,                                                  --added
  null AVG_PO_COST_PRICE ,  
  null AVG_PO_RSP_EXCL_VAT ,  
  null AVG_PO_MARGIN_PERC ,  
  null CONTRACT_QTY ,  
  null AVG_CON_COST_PRICE ,  
  null AVG_CON_RSP_EXCL_VAT ,  
  null AVG_CON_MARGIN_PERC ,  
  null BOC_QTY ,  
  null BOC_COST_PRICE,                                                          --added
  null BOC_RSP_EXCL_VAT,                                                        --added
  null AVG_BOC_COST_PRICE ,  
  null AVG_BOC_RSP_EXCL_VAT ,  
  null AVG_BOC_MARGIN_PERC,
  null CONTRACT_STATUS_CODE                                                     --added
  FROM t3  
)  
ORDER BY 
   DEPARTMENT_NO ,
   CLASS_NO ,
   SUBCLASS_NO ,
   MERCH_CLASS ,
   ITEM_LEVEL1_NO ,
   CONTRACT_NO ,
   PO_NO,
   SOURCE ,
   SUPPLIER_NO ,
   COLOR_ID,
   FIN_YEAR_NO,
   FIN_WEEK_NO  '
;
--- Combine the result sets for one cursor string
query_string:=query_str0 || query_str1 ||  query_str2 || query_str3 || query_str4;
--dbms_output.put_line(query_string);
COMMIT;

--- Build refcursor to pass back the query result set

  OPEN prc FOR query_string;
END "WH_PRF_CORP_301UT";
/