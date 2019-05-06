--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_654U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_654U_FIX" (p_forall_limit in integer,p_success out boolean) 
as
--**************************************************************************************************
-- DATA FIX - reprocess all stg_arc recss
--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Load ARC deal_actual detail information in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_deal_actual_detail_arc
--                        temp_stg_rms_dl_actl_dtl_ARC
--               Output - temp_deal_actual_detail
--  Packages:    dwh_constants, dwh_log, dwh_valid
--  
--
--  Maintenance
--  28 jan 2011 qc4141 Key for temp_deal_actual_detail 
--                       and temp_deal_actual_detail_RTV is not unique enough
--  13 Apr 2011 qc414 - Add CREATE_DATE_TIME
--
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_deal_actual_detail_hsp.sys_process_msg%type;
g_rec_out            temp_deal_actual_detail%rowtype;
g_rec_in             stg_rms_deal_actual_detail_arc%rowtype;
g_found              boolean;
g_valid              boolean;

--g_start_deal         number        :=  3167;
--g_end_deal           number        :=  10000;

--g_start_deal         number        :=  10001;
--g_end_deal           number        :=  15709;

--g_start_deal         number        :=  15710;
--g_end_deal           number        :=  16800;

--g_start_deal         number        :=  16801;
--g_end_deal           number        :=  22000;

--19601
--19602

--g_start_deal         number        :=  22001;
--g_end_deal           number        :=  34500;


--g_start_deal         number        :=  34501;
--g_end_deal           number        :=  35300;


--g_start_deal         number        :=  35301;
--g_end_deal           number        :=  60000;

g_start_deal         number        :=  0;
g_end_deal           number        :=  60000;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_654U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE temp_deal_actual_detail DATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_rms_deal_actual_detail_arc%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of temp_deal_actual_detail%rowtype index by binary_integer;
type tbl_array_u is table of temp_deal_actual_detail%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_deal_actual_detail_arc.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_deal_actual_detail_arc.sys_source_sequence_no%type 
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;



CURSOR c_stg_rms_deal_actual_detail
IS
  SELECT   /*+ FULL(B,4) FULL(A,4) */
    a.SYS_SOURCE_BATCH_ID ,
    a.SYS_SOURCE_SEQUENCE_NO ,
    a.SYS_LOAD_DATE ,
    a.SYS_PROCESS_CODE ,
    a.SYS_LOAD_SYSTEM_NAME ,
    a.SYS_MIDDLEWARE_BATCH_ID ,
    a.SYS_PROCESS_MSG ,
    a.SOURCE_DATA_STATUS_CODE ,
    a.DEAL_NO ,
    a.DEAL_DETAIL_NO ,
    a.PO_NO ,
    a.SHIPMENT_NO ,
    a.TRAN_DATE ,
    a.ITEM_NO ,
    a.LOCATION_NO ,
    a.ACCRUAL_PERIOD_MONTH_END_DATE ,
    a.TOTAL_QTY ,
    a.TOTAL_COST ,
    a.PURCH_SALES_ADJ_CODE ,
    to_timestamp(a.create_datetime, 'dd-mm-yyy hh24:mi:ss') ,
    A.Last_Update_Datetime
    -- use the following when running from copy table
  FROM DWH_FOUNDATION.stg_rms_deal_actual_detail_cpy a,
    dim_item di,
    dim_location dl,
    fnd_deal fd
  WHERE a.deal_no   = fd.deal_no
  AND a.item_no     = di.item_no
  AND a.location_no = dl.location_no
    --use the following when running from archive tables
 --               From Dwh_Foundation.Stg_Rms_Deal_Actual_Detail_Arc A,
 --                 dwh_foundation.temp_stg_rms_dl_actl_dtl_ARC  B
 --      ,
    --              Dim_Item Di,
    --              Dim_Location Dl,
 --                 Fnd_Deal Fd
 --               WHERE
    --                  ((B.Deal_No Between 0 And 60000
     --                And A.Deal_No Between 0 And 60000))
     --                 And
    --                    b.deal_no     = fd.deal_no
     --                    And 
     --                    B.Item_No     = Di.Item_No
     --                    And B.Location_No = Dl.Location_No
    --                    AND
    --                     b.maxsource      = a.sys_source_batch_id
     --                    AND a.deal_no        = b.deal_no
     --                    And A.Deal_Detail_No = B.Deal_Detail_No
     --                    AND a.po_no          = b.po_no
     --                    And A.Shipment_No    = B.Shipment_No
      --                   AND a.tran_date      = b.tran_date
      --                  And A.Item_No        = B.Item_No
       --                 And A.Location_No    = B.Location_No
        --                AND to_timestamp(a.create_datetime, 'dd-mm-yyy hh24:mi:ss')    = to_timestamp(b.create_datetime, 'dd-mm-yyy hh24:mi:ss')
  GROUP BY a.SYS_SOURCE_BATCH_ID ,
    a.SYS_SOURCE_SEQUENCE_NO ,
    a.SYS_LOAD_DATE ,
    a.SYS_PROCESS_CODE ,
    a.SYS_LOAD_SYSTEM_NAME ,
    a.SYS_MIDDLEWARE_BATCH_ID ,
    a.SYS_PROCESS_MSG ,
    a.SOURCE_DATA_STATUS_CODE ,
    a.DEAL_NO ,
    a.DEAL_DETAIL_NO ,
    a.PO_NO ,
    a.SHIPMENT_NO ,
    a.TRAN_DATE ,
    a.ITEM_NO ,
    a.LOCATION_NO ,
    a.ACCRUAL_PERIOD_MONTH_END_DATE ,
    a.TOTAL_QTY ,
    a.TOTAL_COST ,
    a.PURCH_SALES_ADJ_CODE ,
    to_timestamp(a.create_datetime, 'dd-mm-yyy hh24:mi:ss') ,
    a.LAST_UPDATE_DATETIME
  ORDER BY a.sys_source_batch_id,
    a.sys_source_sequence_no,
    a.deal_no ,
    a.deal_detail_no ,
    a.po_no ,
    a.shipment_no ,
    a.tran_date ,
    a.item_no ,
    A.Location_No ,
    to_timestamp(a.create_datetime, 'dd-mm-yyy hh24:mi:ss');


            -- order by only where sequencing is essential to the correct loading of data
               
--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
 
   g_rec_out.deal_no                         := g_rec_in.deal_no; 
   g_rec_out.deal_detail_no                  := g_rec_in.deal_detail_no;
   g_rec_out.po_no                           := g_rec_in.po_no;
   g_rec_out.shipment_no                     := g_rec_in.shipment_no;
   g_rec_out.tran_date                       := g_rec_in.tran_date;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.location_no                     := g_rec_in.location_no;
   g_rec_out.accrual_period_month_end_date   := g_rec_in.accrual_period_month_end_date;
   g_rec_out.total_qty                       := g_rec_in.total_qty;
   g_rec_out.total_cost                      := g_rec_in.total_cost;
   g_rec_out.purch_sales_adj_code            := g_rec_in.purch_sales_adj_code;
   g_rec_out.create_datetime                 := to_timestamp(g_rec_in.create_datetime, 'dd-mm-yyy hh24:mi:ss');
   g_rec_out.last_update_datetime            := g_rec_in.last_update_datetime;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date               := g_date;
--dbms_output.put_line('g_rec_in.deal_no='||g_rec_in.deal_no);
--dbms_output.put_line('g_rec_in.create_datetime='||g_rec_in.create_datetime);
--dbms_output.put_line('g_rec_in.create_datetimeb='||g_rec_in.create_datetimeb);
--    l_text := 'g_rec_in.deal_no='||g_rec_in.deal_no;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--    l_text := 'g_rec_in.create_datetime='||to_timestamp(g_rec_in.create_datetime, 'dd-mm-yyy hh24:mi:ss:ff');
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
        
    
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into temp_deal_actual_detail values a_tbl_insert(i);
       
    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
    
   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).deal_no||
                       ' '||a_tbl_insert(g_error_index).deal_detail_no||
                       ' '||a_tbl_insert(g_error_index).po_no||
                       ' '||a_tbl_insert(g_error_index).shipment_no||
                       ' '||a_tbl_insert(g_error_index).tran_date||
                       ' '||a_tbl_insert(g_error_index).item_no||
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).create_datetime;
          dwh_log.record_error(l_module_name,sqlcode,l_message);  
       end loop;   
       raise;        
end local_bulk_insert;

 
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin
   
    forall i in a_tbl_update.first .. a_tbl_update.last 
       save exceptions
       update temp_deal_actual_detail
       set    accrual_period_month_end_date   = a_tbl_update(i).accrual_period_month_end_date,
              total_qty                       = a_tbl_update(i).total_qty,
              total_cost                      = a_tbl_update(i).total_cost,
              purch_sales_adj_code            = a_tbl_update(i).purch_sales_adj_code,
      --        create_datetime                 = a_tbl_update(i).create_datetime,
              last_update_datetime            = a_tbl_update(i).last_update_datetime,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  deal_no                         = a_tbl_update(i).deal_no and
              deal_detail_no                  = a_tbl_update(i).deal_detail_no and
              po_no                           = a_tbl_update(i).po_no and
              shipment_no                     = a_tbl_update(i).shipment_no and 
              tran_date                       = a_tbl_update(i).tran_date and
              item_no                         = a_tbl_update(i).item_no and
              location_no                     = a_tbl_update(i).location_no and
              to_timestamp(create_datetime, 'dd-mm-yyy hh24:mi:ss')                     = to_timestamp(a_tbl_update(i).create_datetime, 'dd-mm-yyy hh24:mi:ss');
       
       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
               
   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).deal_no||
                       ' '||a_tbl_update(g_error_index).deal_detail_no||
                       ' '||a_tbl_update(g_error_index).po_no||
                       ' '||a_tbl_update(g_error_index).shipment_no||
                       ' '||a_tbl_update(g_error_index).tran_date||
                       ' '||a_tbl_update(g_error_index).item_no||
                       ' '||a_tbl_update(g_error_index).location_no||
                       ' '||a_tbl_update(g_error_index).create_datetime;
          dwh_log.record_error(l_module_name,sqlcode,l_message);  
       end loop;   
       raise;        
end local_bulk_update;



--************************************************************************************************** 
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

v_count integer := 0;

begin

  g_found := false;
   
   select count(1)
   into   v_count
   from   temp_deal_actual_detail
   where  deal_no                       = g_rec_out.deal_no and
          deal_detail_no                = g_rec_out.deal_detail_no and
          po_no                         = g_rec_out.po_no and
          shipment_no                   = g_rec_out.shipment_no and
          tran_date                     = g_rec_out.tran_date and
          item_no                       = g_rec_out.item_no and
          location_no                   = g_rec_out.location_no and
             to_timestamp(create_datetime, 'dd-mm-yyy hh24:mi:ss')                      =    to_timestamp(g_rec_out.create_datetime, 'dd-mm-yyy hh24:mi:ss')   ;

   if v_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).deal_no                       = g_rec_out.deal_no and
             a_tbl_insert(i).deal_detail_no                = g_rec_out.deal_detail_no and
             a_tbl_insert(i).po_no                         = g_rec_out.po_no and
             a_tbl_insert(i).shipment_no                   = g_rec_out.shipment_no  and
             a_tbl_insert(i).tran_date                     = g_rec_out.tran_date and
             a_tbl_insert(i).item_no                       = g_rec_out.item_no and
             a_tbl_insert(i).location_no                   = g_rec_out.location_no and
             to_timestamp(a_tbl_insert(i).create_datetime, 'dd-mm-yyy hh24:mi:ss')   = to_timestamp(g_rec_out.create_datetime, 'dd-mm-yyy hh24:mi:ss')
            then  g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk   
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out; 
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;
   
   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
--   if a_count > 1000 then
   if a_count > g_forall_limit then   
      local_bulk_insert;
      local_bulk_update;    
    
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;
      
      commit;
   end if;      
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
end local_write_output;


--************************************************************************************************** 
-- Main process 
--**************************************************************************************************
begin 
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;   
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF temp_deal_actual_detail EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

 -- EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_foundation.temp_deal_actual_detail');
 -- COMMIT;
 -- l_text := 'TRUNCATE COMPLETED OF dwh_foundation.temp_deal_actual_detail';
 -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Started GATHER STATS on dwh_foundation.temp_deal_actual_detail';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','TEMP_DEAL_ACTUAL_DETAIL', DEGREE => 8);
    commit;
    l_text := 'Finished GATHER STATS on dwh_foundation.temp_deal_actual_detail';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Deal Range is:- '||g_start_deal||'  to  '||g_end_deal;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

--*****************************************************************--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_deal_actual_detail;
    fetch c_stg_rms_deal_actual_detail bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;
   
         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
            local_write_output;
      end loop;
    fetch c_stg_rms_deal_actual_detail bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_stg_rms_deal_actual_detail;
--**************************************************************************************************  
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;    

--    ********************************* 
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
end wh_fnd_corp_654u_fix;
