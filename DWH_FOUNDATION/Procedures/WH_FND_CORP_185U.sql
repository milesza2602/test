--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_185U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_185U" 
(p_forall_limit in integer,p_success out boolean) as



--**************************************************************************************************
--  Date:        September 2016
--  Author:      Lwazi Ntloko
--  Purpose:     Create new product item link from apx table in the foundation layer
--               with input ex staging table from apx
--
--  FYI          Eventhough the catalog data from source comes in 1 day ahead of time,
--               all processing is done for post_date = g_date
--               ie. runs one day 'behind' received data
--
--  Tables:      Input  - STG_apx_item_link_cpy
--               Output - FND_apx_item_link_chn_item_dy
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--**************************************************************************************************
--  oct/nov 2016 - wendy - amended key on update
--
--**************************************************************************************************
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
g_read          integer       :=  0;
g_updated       integer       :=  0;
g_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      STG_apx_item_link_hsp.sys_process_msg%type;
g_rec_out            FND_apx_item_link_chn_item_dy%rowtype;
g_found              boolean;
g_valid              boolean;
g_exp_cnt            number := 0;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_185U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRODUCT ITEM LINK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_apx_item_link_chn_item_dy%rowtype index by binary_integer;
type tbl_array_u is table of FND_apx_item_link_chn_item_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_apx_item_link.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_apx_item_link.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_STG_apx_item_link is
   WITH SELBAT AS (SELECT  CHAIN_CODE
                      , PROD_LINK_REF_NO
                      , PROD_LINK_IND
                      , CHAIN_NO
                      , ITEM_NO
                      , trunc(POST_DATE) post_date
                      , MAX(SYS_SOURCE_BATCH_ID) MAXBAT
                  FROM STG_apx_item_link_cpy
                  GROUP BY CHAIN_CODE
                      , PROD_LINK_REF_NO
                      , PROD_LINK_IND
                      , CHAIN_NO
                      , ITEM_NO
                      , trunc(POST_DATE) 
                      ),
        SELSEQ AS (SELECT  C.CHAIN_CODE
                      , C.PROD_LINK_REF_NO
                      , C.PROD_LINK_IND
                      , C.CHAIN_NO
                      , C.ITEM_NO
                      , trunc(C.POST_DATE) post_date
                      , MAXBAT
                      , MAX(SYS_SOURCE_SEQUENCE_NO) MAXSEQ
                  FROM STG_apx_item_link_cpy C, SELBAT SB
                  WHERE SYS_SOURCE_BATCH_ID = MAXBAT
                  AND C.CHAIN_CODE = SB.CHAIN_CODE
                      AND  C.PROD_LINK_REF_NO = SB.PROD_LINK_REF_NO
                      AND C.PROD_LINK_IND = SB.PROD_LINK_IND
                      AND C.CHAIN_NO = SB.CHAIN_NO
                      AND  C.ITEM_NO = SB.ITEM_NO
                      AND  trunc(C.POST_DATE) = SB.post_date
                  GROUP BY C.CHAIN_CODE
                      , C.PROD_LINK_REF_NO
                      , C.PROD_LINK_IND
                      , C.CHAIN_NO
                      , C.ITEM_NO
                      , trunc(C.POST_DATE) 
                      , MAXBAT)
   select A.SYS_SOURCE_BATCH_ID
        , A.SYS_SOURCE_SEQUENCE_NO
        , A.SYS_LOAD_DATE
        , A.SYS_PROCESS_CODE
        , A.SYS_LOAD_SYSTEM_NAME
        , A.SYS_MIDDLEWARE_BATCH_ID
        , A.SYS_PROCESS_MSG
        , A.SOURCE_DATA_STATUS_CODE
        , A.CHAIN_CODE
        , A.PROD_LINK_REF_NO
        , A.PROD_LINK_IND
        , A.CHAIN_NO
        , A.ITEM_NO
        , trunc(A.POST_DATE) post_date
        , trunc(A.CREATE_DATE) CREATE_DATE
        , trunc(A.LINK_START_DATE) link_start_date
        , trunc(A.LINK_EXPIRED_DATE) LINK_EXPIRED_DATE
   from STG_apx_item_link_cpy a , SELSEQ SS
   WHERE SS.MAXBAT = A.SYS_SOURCE_BATCH_ID
   AND SS.MAXSEQ = A.SYS_SOURCE_SEQUENCE_NO
   order by sys_source_batch_id,sys_source_sequence_no
      ;
   
g_rec_in   c_STG_apx_item_link%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_apx_item_link%rowtype;
a_stg_input   stg_array;
-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
--    NB. in PRD
--     Post_date  Last_updated_date  
--     10/OCT/16	09/OCT/16	 
--     11/OCT/16	10/OCT/16	 
--   hence always make sure that LUD = post_date - 1
--   as important for Merge uncatalogued children into catalog table
--**************************************************************************************************
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';

   g_rec_out.CHAIN_CODE                    	:= g_rec_in.CHAIN_CODE;
   g_rec_out.PROD_LINK_REF_NO               := g_rec_in.PROD_LINK_REF_NO;
   g_rec_out.PROD_LINK_IND                 	:= g_rec_in.PROD_LINK_IND;
   g_rec_out.CHAIN_NO                       := g_rec_in.CHAIN_NO;
   g_rec_out.ITEM_NO                   		  := g_rec_in.ITEM_NO;
   g_rec_out.POST_DATE                   	  := g_rec_in.POST_DATE;    
   g_rec_out.CREATE_DATE                   	:= g_rec_in.CREATE_DATE;      
   g_rec_out.LINK_START_DATE                := g_rec_in.LINK_START_DATE;    
   g_rec_out.LINK_EXPIRED_DATE              := g_rec_in.LINK_EXPIRED_DATE;    
   g_rec_out.last_updated_date              := g_date;
 --  g_rec_out.last_updated_date              := g_rec_in.POST_date;
   
   if g_rec_out.CHAIN_CODE not in ('DJ','WW',NULL) then
     g_hospital      := 'Y';
     g_hospital_text := 'INVALID CHAIN CODE';
   end if;
   
   if g_rec_out.PROD_LINK_IND not in (0,1) then
     g_hospital      := 'Y';
     g_hospital_text := 'INVALID PROD_LINK_IND';
   end if;

   if not  dwh_valid.fnd_chain(g_rec_out.chain_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_chain_not_found;
   end if;
   
   if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
   end if;

   if not dwh_valid.fnd_calendar(g_rec_out.post_date) then
     g_hospital      := 'Y';
     g_hospital_text := 'POST_DATE DOES NOT EXIST';
   end if;

   if not dwh_valid.fnd_calendar(TRUNC(g_rec_out.create_date)) then
     g_hospital      := 'Y';
     g_hospital_text := 'CREATE DATE DOES NOT EXIST';
   end if;

   if not dwh_valid.fnd_calendar(g_rec_out.link_start_date) then
     g_hospital      := 'Y';
     g_hospital_text := 'LINK_START_DATE DOES NOT EXIST';
   end if;

   If g_rec_out.link_expired_date is not null
       then 
          if not dwh_valid.fnd_calendar(g_rec_out.link_expired_date) then
             g_hospital      := 'Y';
             g_hospital_text := 'EXPIRED_DATE DOES NOT EXIST';
          end if;
   end if;

  
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into dwh_foundation.STG_apx_item_link_hsp values 
   (g_rec_in.SYS_SOURCE_BATCH_ID
    ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
    ,g_rec_in.SYS_LOAD_DATE
    ,g_rec_in.SYS_PROCESS_CODE
    ,g_rec_in.SYS_LOAD_SYSTEM_NAME
    ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
    ,g_rec_in.SYS_PROCESS_MSG
    ,g_rec_in.SOURCE_DATA_STATUS_CODE
    ,g_rec_in.CHAIN_CODE
    ,g_rec_in.PROD_LINK_REF_NO
    ,g_rec_in.PROD_LINK_IND
    ,g_rec_in.CHAIN_NO
    ,g_rec_in.ITEM_NO
    ,g_rec_in.POST_DATE
    ,g_rec_in.CREATE_DATE
    ,g_rec_in.LINK_START_DATE
    ,g_rec_in.LINK_EXPIRED_DATE);
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into FND_apx_item_link_chn_item_dy values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).CHAIN_CODE||
                       ' '||a_tbl_insert(g_error_index).PROD_LINK_REF_NO||
                       ' '||a_tbl_insert(g_error_index).PROD_LINK_IND||
                       ' '||a_tbl_insert(g_error_index).CHAIN_NO||
                       ' '||a_tbl_insert(g_error_index).ITEM_NO||
                       ' '||a_tbl_insert(g_error_index).POST_DATE;
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
       update FND_apx_item_link_chn_item_dy
       set    CREATE_DATE                  = a_tbl_update(i).CREATE_DATE ,
              LINK_START_DATE              = a_tbl_update(i).LINK_START_DATE,
              LINK_EXPIRED_DATE            = a_tbl_update(i).LINK_EXPIRED_DATE,
              last_updated_date            = a_tbl_update(i).last_updated_date
       where  CHAIN_CODE                   = a_tbl_update(i).CHAIN_CODE and
              PROD_LINK_REF_NO             = a_tbl_update(i).PROD_LINK_REF_NO     and
              PROD_LINK_IND                = a_tbl_update(i).PROD_LINK_IND    and
              CHAIN_NO                     = a_tbl_update(i).CHAIN_NO and
              ITEM_NO                      = a_tbl_update(i).ITEM_NO and 
              post_date                    = a_tbl_update(i).post_date ;

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
                       ' '||a_tbl_UPDATE(g_error_index).CHAIN_CODE||
                       ' '||a_tbl_UPDATE(g_error_index).PROD_LINK_REF_NO||
                       ' '||a_tbl_UPDATE(g_error_index).PROD_LINK_IND||
                       ' '||a_tbl_UPDATE(g_error_index).CHAIN_NO||
                       ' '||a_tbl_UPDATE(g_error_index).ITEM_NO||
                       ' '||a_tbl_UPDATE(g_error_index).POST_DATE;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update STG_apx_item_link_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   FND_apx_item_link_chn_item_dy
   where  CHAIN_CODE       = g_rec_out.CHAIN_CODE and
          PROD_LINK_REF_NO = g_rec_out.PROD_LINK_REF_NO      and
          CHAIN_NO         = g_rec_out.CHAIN_NO  and
          ITEM_NO          = g_rec_out.ITEM_NO and
          PROD_LINK_IND    = g_rec_out.PROD_LINK_IND AND
          POST_DATE        = G_REC_OUT.POST_DATE;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).CHAIN_CODE                     = g_rec_out.CHAIN_CODE and
              a_tbl_insert(i).PROD_LINK_REF_NO             = g_rec_out.PROD_LINK_REF_NO     and
              a_tbl_insert(i).PROD_LINK_IND                = g_rec_out.PROD_LINK_IND    and
              a_tbl_insert(i).CHAIN_NO                     = g_rec_out.CHAIN_NO and
              a_tbl_insert(i).ITEM_NO                      = g_rec_out.ITEM_NO and 
              a_tbl_insert(i).post_date                    = g_rec_out.post_date  THEN
            g_found := TRUE;
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
      local_bulk_staging_update;

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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum  then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_apx_item_link_chn_item_dy  STARTED AT '||
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

 --   g_date := '3 oct 2016';
 --   l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date||' THRU 30 OCT 2016';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_apx_item_link;
    fetch c_STG_apx_item_link bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_STG_apx_item_link bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_apx_item_link;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;

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

END WH_FND_CORP_185U;
