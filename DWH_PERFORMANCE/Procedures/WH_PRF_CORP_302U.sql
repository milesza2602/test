--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_302U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_302U" 
    (
    pYear       IN NUMBER DEFAULT NULL,
    pRplInd     IN VARCHAR2 DEFAULT NULL,
    pDepartment IN NUMBER DEFAULT NULL,
    pClass      IN NUMBER DEFAULT NULL,
    pSubclass   IN NUMBER DEFAULT NULL,
    pSupplier   IN NUMBER DEFAULT NULL,
    pSeason     IN NUMBER DEFAULT NULL,
    pFromWeekNo IN NUMBER DEFAULT NULL,
    pToWeekNo   IN NUMBER DEFAULT NULL
    )
AS
  --**************************************************************************************************
  --  Date:        September 2017
  --  Author:      A. Ugolini
  --  Purpose:     Control procedure to manage the Stored Procedures being called for the Procurment Reporting.
  --
  --  Maintenance:
  --
  --**************************************************************************************************
  -- Inserts to output tables
  --**************************************************************************************************
prc sys_refcursor;
  temp_procure dwh_performance.wrk_procure_template%rowtype;
BEGIN
  --- Main procedure to bring back the User selection results
  
WH_PRF_CORP_301U (pYear, pRplInd, pDepartment, pClass, pSubclass, pSupplier, pSeason, pFromWeekNo, pToWeekNo ,prc);
  
LOOP
  --Fetch cursor 'prc' into dwh_performance.wrk_procure_template table type 'temp_procure'
  FETCH prc INTO temp_procure;
  --exit if no more records
  EXIT
WHEN prc%NOTFOUND;
--print the matched user_id details - only used for testing
 dbms_output.put_line(
temp_procure.MERCH_CLASS||',' ||   
temp_procure.DEPARTMENT_NO||',' || 
temp_procure.DEPARTMENT_NAME||',' || 
temp_procure.CLASS_NO||',' ||     
temp_procure.CLASS_NAME||',' || 
temp_procure.SUBCLASS_NO||',' ||     
temp_procure.SUBCLASS_NAME||',' ||  
temp_procure.ITEM_LEVEL1_NO||',' ||    
temp_procure.ITEM_LEVEL1_DESC||',' || 
temp_procure.COLOR_ID||',' ||  
temp_procure.PRODUCT_SAMPLE_STATUS||',' || 
temp_procure.SUPPLIER_NO||',' ||    
temp_procure.SUPPLIER_NAME||',' ||
temp_procure.CONTRACT_NO||',' ||
temp_procure.PO_NO||',' ||        
temp_procure.SOURCE||',' ||       
temp_procure.FIN_YEAR_NO||',' ||     
temp_procure.FIN_HALF_NO||',' ||     
temp_procure.MONTH_NAME||',' ||  
temp_procure.THIS_WEEK_START_DATE||',' ||          
temp_procure.FIN_WEEK_NO||',' ||     
temp_procure.LATEST_PO_QTY||',' ||        
temp_procure.AVG_PO_COST_PRICE||',' ||        
temp_procure.AVG_PO_RSP_EXCL_VAT||',' ||        
temp_procure.AVG_PO_MARGIN_PERC||',' || 
temp_procure.CONTRACT_QTY||',' || 
temp_procure.AVG_CON_COST_PRICE||',' ||        
temp_procure.AVG_CON_RSP_EXCL_VAT||',' ||        
temp_procure.AVG_CON_MARGIN_PERC||',' ||
temp_procure.BOC_QTY||',' ||
temp_procure.AVG_BOC_COST_PRICE||',' ||        
temp_procure.AVG_BOC_RSP_EXCL_VAT||',' ||        
temp_procure.AVG_BOC_MARGIN_PERC
);
END LOOP;
CLOSE prc;
END WH_PRF_CORP_302U;
