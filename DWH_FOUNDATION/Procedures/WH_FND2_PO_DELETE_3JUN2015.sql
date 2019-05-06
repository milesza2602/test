--------------------------------------------------------
--  DDL for Procedure WH_FND2_PO_DELETE_3JUN2015
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND2_PO_DELETE_3JUN2015" 
(p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Wendy lyttle
--  Purpose:     DELETE OF PO_NO'S as these are going to be re-used by RMS
--               Total no. of po's in dim_purchase_order selected = 815836
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_deleted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dwh_performance.rtl_loc_item_wk_sales_index%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_recs_DELETED_FRSD   NUMBER := 0;
g_recs_DELETED_FRPO   NUMBER := 0;
g_recs_DELETED_FRA    NUMBER := 0;
g_recs_DELETED_FRIM   NUMBER := 0;
g_recs_DELETED_RSD    NUMBER := 0;
g_recs_DELETED_FRS    NUMBER := 0;
g_recs_DELETED_FDAD   NUMBER := 0;
g_recs_DELETED_RPSLID NUMBER := 0;
g_recs_DELETED_RPCSW  NUMBER := 0;
g_tot_recs  NUMBER := 0;
g_gtot_recs  NUMBER := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND2_PO_DELETE_3JUN2015';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'delete po range 1000000 to 2999999';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
-- BACKUPS TAKEN INTO FOLLOING TABLES :
--                        DWH_FOUNDATION.FND_RTL_STOCKOWN_DCR_old                    17818058 rows created.
--                        DWH_FOUNDATION.FND_RTL_PURCHASE_ORDER_old                 5166339 rows created.
--                        DWH_FOUNDATION.FND_RTL_ALLOCATION_old                             25278620 rows created.
--                        DWH_FOUNDATION.FND_RTL_INVOICE_MATCHING_old            1112154 rows created.
--                        DWH_PERFORMANCE.RTL_STOCKOWN_DCR_old                            4993479 rows created.
--                        DWH_FOUNDATION.FND_RTL_SHIPMENT_old                                 39359505 rows created.
--                        DWH_FOUNDATION.FND_DEAL_ACTUAL_DETAIL_old                   164 rows created.
--                        DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DYold    979849 rows created.
--                        DWH_PERFORMANCE.RTL_PO_CHAIN_SC_WK_old                        3951 rows created

--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

  execute immediate 'alter session enable parallel dml';
  
  
L_TEXT := 'gather stats FND_RTL_SHIPMENT start';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION','FND_RTL_SHIPMENT', DEGREE => 8);
L_TEXT := 'gather stats FND_RTL_SHIPMENT end ';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT); 
  
  
 /*
               DELETE /*+  full(a)  parallel(a,6)   */ 
 /*               FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
/*              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1000000 AND 1010000 
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
          
/* 
           DELETE /*+  full(a)  parallel(a,6)   */ 
/* FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
 /*              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1010000 AND 1020000
                          AND PO_STATUS_CODE = 'C'
                          );

              G_RECS_DELETED := SQL%ROWCOUNT;
              commit;

             DELETE /*+  full(a)  parallel(a,6)   */ 
 /*             FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1020000 AND 1030000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
          

            DELETE /*+  full(a)  parallel(a,6)   */ 
 /*            FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1030000 AND 1040000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  execute immediate 'alter session enable parallel dml';
               DELETE /*+  full(a)  parallel(a,6)   */ 
  /*              FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1040000 AND 1050000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
          

            DELETE /*+  full(a)  parallel(a,6)   */ 
  /*           FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1050000 AND 1060000
                          AND PO_STATUS_CODE = 'C'
                          );

              G_RECS_DELETED := SQL%ROWCOUNT;
              commit;

             DELETE /*+  full(a)  parallel(a,6)   */ 
 /*             FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1060000 AND 1070000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
          

            DELETE /*+  full(a)  parallel(a,6)   */ 
/*             FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1070000 AND 1080000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

             DELETE /*+  full(a)  parallel(a,6)   */ 
  /*            FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1080000 AND 1090000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
          

            DELETE /*+  full(a)  parallel(a,6)   */ 
   /*          FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1090000 AND 1100000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
*/
------------------------------------------------------------------------------------------------------------------------------
/*l_text := 'delete range started=1100000 AND 1120000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

         DELETE /*+  full(a)  parallel(a,6) */ 
 /*        FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1100000 AND 1120000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

*/
------------------------------------------------------------------------------------------------------------------------------
l_text := 'delete range started=1120000 AND 1130000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

         DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1120000 AND 1130000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

------------------------------------------------------------------------------------------------------------------------------
l_text := 'delete range started=1130000 AND 1140000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

         DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1130000 AND 1140000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

------------------------------------------------------------------------------------------------------------------------------
l_text := 'delete range started=1140000 AND 1150000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

         DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1140000 AND 1150000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 




l_text := 'delete range started=1150000 AND 1200000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1150000 AND 1200000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 

L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

l_text := 'delete range started=1200000 AND 1250000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1200000 AND 1250000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 

L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
l_text := 'delete range started=1250000 AND 1300000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1250000 AND 1300000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              

L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
l_text := 'delete range started=1300000 AND 1350000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1300000 AND 1350000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 

L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

l_text := 'delete range started=1350000 AND 1400000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1350000 AND 1400000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

l_text := 'delete range started=1400000 AND 1450000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1400000 AND 1450000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

l_text := 'delete range started=1450000 AND 1500000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1450000 AND 1500000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
L_TEXT := 'gather stats FND_RTL_SHIPMENT start';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION','FND_RTL_SHIPMENT', DEGREE => 8);
L_TEXT := 'gather stats FND_RTL_SHIPMENT end ';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT); 



l_text := 'delete range started=1500000 AND 1550000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1500000 AND 1550000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 


l_text := 'delete range started=1550000 AND 1600000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1550000 AND 1600000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 


l_text := 'delete range started=1600000 AND 1650000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1600000 AND 1650000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 


l_text := 'delete range started=1650000 AND 1700000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1650000 AND 1700000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 
 
l_text := 'delete range started=1700000 AND 1750000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1700000 AND 1750000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
l_text := 'delete range started=1750000 AND 1800000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1750000 AND 1800000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 
l_text := 'delete range started=1800000 AND 1900000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1800000 AND 1900000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 
l_text := 'delete range started=1900000 AND 2000000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 1900000 AND 2000000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

L_TEXT := 'gather stats FND_RTL_SHIPMENT start';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION','FND_RTL_SHIPMENT', DEGREE => 8);
L_TEXT := 'gather stats FND_RTL_SHIPMENT end ';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT); 

 
l_text := 'delete range started=2000000 AND 2100000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2000000 AND 2100000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
l_text := 'delete range started=2100000 AND 2200000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2100000 AND 2200000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 


 
l_text := 'delete range started=2200000 AND 2300000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2200000 AND 2300000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
l_text := 'delete range started=2300000 AND 2400000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2300000 AND 2400000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
L_TEXT := 'gather stats FND_RTL_SHIPMENT start';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION','FND_RTL_SHIPMENT', DEGREE => 8);
L_TEXT := 'gather stats FND_RTL_SHIPMENT end ';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT); 

 
l_text := 'delete range started=2400000 AND 2500000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
 

           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2400000 AND 2500000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
l_text := 'delete range started=2500000 AND 2600000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2500000 AND 2600000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
l_text := 'delete range started=2600000 AND 2700000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 

            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2600000 AND 2700000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  
l_text := 'delete range started=2700000 AND 2800000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2700000 AND 2800000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  
l_text := 'delete range started=2800000 AND 2900000' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 
            DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2800000 AND 2900000
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
l_text := 'FND_RTL_SHIPMENT recs deleted = '||g_recs_deleted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  
l_text := 'delete range started=2900000 AND 2999999' ;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

  
           DELETE /*+  full(a)  parallel(a,6) */ FROM dwh_FOUNDATION.FND_RTL_SHIPMENT  a
              where a.PO_NO in (SELECT PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE 
                          PO_NO BETWEEN 2900000 AND 2999999
                          AND PO_STATUS_CODE = 'C'
                          );

              g_recs_deleted := SQL%ROWCOUNT;
              commit;
              
 
L_TEXT := 'FND_RTL_SHIPMENT recs deleted = '||G_RECS_DELETED;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 


L_TEXT := 'gather stats FND_RTL_SHIPMENT start';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION','FND_RTL_SHIPMENT', DEGREE => 8);
L_TEXT := 'gather stats FND_RTL_SHIPMENT end ';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT); 

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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



END WH_FND2_PO_DELETE_3JUN2015;
