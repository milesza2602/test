--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_014U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_014U_OLD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        15 Aug 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Loading Business Unit data on the foundation layer
--               with input from the Labour Hierachy staging table.

--  DIM Tables:  Input  - STG_S4S_LABOUR_HIERARCHY_CPY
--               Output - fnd_s4s_LABOUR_HIERARCHY
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  22 Oct 2014  restructure checks were commented out due to business processes
--               this is to be resolved at a later date.
--
--  Naming conventions``  
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
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      STG_S4S_LABOUR_HIERARCHY_HSP.sys_process_msg%type;
g_rec_out            fnd_s4s_LABOUR_HIERARCHY%rowtype;

g_found              boolean;
g_insert_rec         boolean;
g_invalid_plan_type_no boolean;
g_date               date          := trunc(sysdate);

g_S4S_BUSINESS_UNIT_NO number(10);
g_WORKGROUP_ID number(10);
g_JOBGROUP_ID number(10);
g_JOB_ID number(10);
g_LABOUR_ROLE_ID number(10);
g_EFFECTIVE_FROM_DATE date;
g_EFFECTIVE_TO_DATE date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_014U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_pln_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD S4S LABOUR HIERACHY DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_s4s_LABOUR_HIERARCHY%rowtype index by binary_integer;
type tbl_array_u is table of fnd_s4s_LABOUR_HIERARCHY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_S4S_LABOUR_HIERARCHY_CPY.sys_source_batch_id%type index by binary_integer;
type staging_array2 is table of STG_S4S_LABOUR_HIERARCHY_CPY.sys_source_sequence_no%type index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_STG_S4S_LABOUR_HIERARCHY is

WITH 
SELWG -- duplicate workgroup_id
AS (SELECT S4S_BUSINESS_UNIT_NO, WORKGROUP_ID, COUNT(*)
    FROM (SELECT DISTINCT 
              S4S_BUSINESS_UNIT_NO,
              WORKGROUP_ID ,
              EFFECTIVE_FROM_DATE,
              EFFECTIVE_TO_DATE
          from stg_s4s_labour_hierarchy_cpy
          )
    GROUP BY S4S_BUSINESS_UNIT_NO, WORKGROUP_ID,EFFECTIVE_FROM_DATE,EFFECTIVE_TO_DATE HAVING COUNT(*) > 1
    ),
SELJG -- duplicate jobgroup_id
AS (SELECT WORKGROUP_ID,JOBGROUP_ID,JOB_ID, COUNT(*)
    FROM (SELECT DISTINCT 
              S4S_BUSINESS_UNIT_NO,
              WORKGROUP_ID ,
              JOBGROUP_ID,
              JOB_ID,
              EFFECTIVE_FROM_DATE,
              EFFECTIVE_TO_DATE
          from stg_s4s_labour_hierarchy_cpy 
          )
    GROUP BY WORKGROUP_ID,JOBGROUP_ID,JOB_ID,EFFECTIVE_FROM_DATE,EFFECTIVE_TO_DATE HAVING COUNT(*) > 1
    ),
SELJ -- duplicate job_id
AS (SELECT JOBGROUP_ID,JOB_ID, COUNT(*)
    FROM (SELECT DISTINCT 
              S4S_BUSINESS_UNIT_NO,
              WORKGROUP_ID ,
              JOBGROUP_ID,
              JOB_ID,
              EFFECTIVE_FROM_DATE,
              EFFECTIVE_TO_DATE
          from stg_s4s_labour_hierarchy_cpy
          )
    GROUP BY JOBGROUP_ID,JOB_ID,EFFECTIVE_FROM_DATE,EFFECTIVE_TO_DATE HAVING COUNT(*) > 1
--    ),
--SELLR
--AS (SELECT LABOUR_ROLE_ID, COUNT(*)
--    FROM (SELECT DISTINCT 
--              lh.JOB_ID ,
--              lh.LABOUR_ROLE_ID,
--              lh.EFFECTIVE_FROM_DATE,
--              lh.EFFECTIVE_TO_DATE
--          from stg_s4s_labour_hierarchy_cpy LH
--          )
--     GROUP BY LABOUR_ROLE_ID,EFFECTIVE_FROM_DATE,EFFECTIVE_TO_DATE HAVING COUNT(*) > 1
     )
SELECT STG.SYS_SOURCE_BATCH_ID    ,     
       STG.SYS_SOURCE_SEQUENCE_NO,
       STG.SYS_LOAD_DATE,
       STG.SYS_PROCESS_CODE,
       STG.SYS_LOAD_SYSTEM_NAME,
       STG.SYS_MIDDLEWARE_BATCH_ID,
       STG.SYS_PROCESS_MSG,
       STG.SOURCE_DATA_STATUS_CODE,
       STG.S4S_BUSINESS_UNIT_NO ,
       STG.BUSINESS_UNIT_NO ,
       STG.S4S_BUSINESS_UNIT_NAME,
       STG.BUSINESS_UNIT_SEQ ,
       STG.WORKGROUP_ID ,
       STG.WORKGROUP_NAME,
       STG.WORKGROUP_SEQ,
       STG.JOBGROUP_ID,
       STG.JOBGROUP_NAME,
       STG.JOBGROUP_SEQ,
       STG.JOB_ID,
       STG.JOB_NAME ,
       STG.LABOUR_ROLE_ID ,
       STG.LABOUR_ROLE_NAME,
       STG.EFFECTIVE_FROM_DATE,
       STG.EFFECTIVE_TO_DATE,
       SWG.WORKGROUP_ID  DUP_WORKGROUP_ID,
       SJG.JOBGROUP_ID DUP_JOBGROUP_ID,
       SJ.JOB_ID DUP_JOB_ID
--       SLR.LABOUR_ROLE_ID DUP_LABOUR_ROLE_ID
FROM stg_s4s_labour_hierarchy_cpy STG,
     SELWG SWG, SELJG SJG, SELJ SJ--, SELLR SLR
WHERE STG.WORKGROUP_ID = SWG.WORKGROUP_ID(+)
  AND STG.JOBGROUP_ID = SJG.JOBGROUP_ID(+)
  and stg.job_id = sj.job_id(+)
                                
--  AND STG.LABOUR_ROLE_ID = SLR.LABOUR_ROLE_ID(+)
--  AND STG.LABOUR_ROLE_ID NOT IN ( 1000390,1000271,1000243,1000414,1000217,1000410,1000420,1000413,1000316,1000192) -- ADDED TO ALLOW THE BYPASSING OF RECORDS CAUSING DUPLICATES 
ORDER BY SYS_SOURCE_BATCH_ID,STG.SYS_SOURCE_SEQUENCE_NO;
          
g_rec_in             c_STG_S4S_LABOUR_HIERARCHY%rowtype;

-- For input bulk collect --
type stg_array is table of c_STG_S4S_LABOUR_HIERARCHY%rowtype;
a_stg_input      stg_array;

g_dup_count       number;

-- order by only where sequencing is essential to the correct loading of data
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
-- dbms_output.put_line('test 11');

      g_hospital                       := 'N';
      G_REC_OUT.S4S_BUSINESS_UNIT_NO   := G_REC_IN.S4S_BUSINESS_UNIT_NO;
      G_REC_OUT.BUSINESS_UNIT_NO       := G_REC_IN.BUSINESS_UNIT_NO;
      G_REC_OUT.S4S_BUSINESS_UNIT_NAME := upper(G_REC_IN.S4S_BUSINESS_UNIT_NAME);
      G_REC_OUT.BUSINESS_UNIT_SEQ      := G_REC_IN.BUSINESS_UNIT_SEQ;
      G_REC_OUT.WORKGROUP_ID           := G_REC_IN.WORKGROUP_ID;
      G_REC_OUT.WORKGROUP_NAME         := upper(G_REC_IN.WORKGROUP_NAME);
      G_REC_OUT.WORKGROUP_SEQ          := G_REC_IN.WORKGROUP_SEQ;
      G_REC_OUT.JOBGROUP_ID            := G_REC_IN.JOBGROUP_ID;
      G_REC_OUT.JOBGROUP_NAME          := upper(G_REC_IN.JOBGROUP_NAME);
      G_REC_OUT.JOBGROUP_SEQ           := G_REC_IN.JOBGROUP_SEQ;
      G_REC_OUT.JOB_ID                 := G_REC_IN.JOB_ID;
      G_REC_OUT.JOB_NAME               := upper(G_REC_IN.JOB_NAME);
      G_REC_OUT.LABOUR_ROLE_ID         := G_REC_IN.LABOUR_ROLE_ID;
      G_REC_OUT.LABOUR_ROLE_NAME       := upper(G_REC_IN.LABOUR_ROLE_NAME);
      G_REC_OUT.EFFECTIVE_FROM_DATE    := G_REC_IN.EFFECTIVE_FROM_DATE;
      G_REC_OUT.EFFECTIVE_TO_DATE      := G_REC_IN.EFFECTIVE_TO_DATE;
      g_rec_out.last_updated_date      := g_date;

begin
      select S4S_BUSINESS_UNIT_NO ,
             WORKGROUP_ID ,
             JOBGROUP_ID ,
             JOB_ID,
             LABOUR_ROLE_ID,
             EFFECTIVE_FROM_DATE,
             EFFECTIVE_TO_DATE
      into  g_S4S_BUSINESS_UNIT_NO ,
            g_WORKGROUP_ID ,
            g_JOBGROUP_ID ,
            g_JOB_ID,
            g_LABOUR_ROLE_ID,
            g_EFFECTIVE_FROM_DATE,
            g_EFFECTIVE_TO_DATE
      from  fnd_s4s_labour_hierarchy
      where EFFECTIVE_FROM_DATE = (select max(EFFECTIVE_FROM_DATE) from fnd_s4s_labour_hierarchy where labour_role_id = G_REC_OUT.LABOUR_ROLE_ID)
      AND   LABOUR_ROLE_ID  = G_REC_OUT.LABOUR_ROLE_ID
      AND   JOB_ID          = G_REC_OUT.JOB_ID ;

      exception
--      when no_data_found then
      when others then
           G_S4S_BUSINESS_UNIT_NO := g_rec_out.S4S_BUSINESS_UNIT_NO;
           G_WORKGROUP_ID         := g_rec_out.WORKGROUP_ID;
           G_JOBGROUP_ID          := g_rec_out.JOBGROUP_ID;
           G_JOB_ID               := g_rec_out.JOB_ID;
           G_LABOUR_ROLE_ID       := g_rec_out.LABOUR_ROLE_ID;
      end;
      
          --dbms_output.put_line('test 1');

  /*** commented out for now due to business rules
  /***  to be resolved at some stage
        if
         G_S4S_BUSINESS_UNIT_NO  <> g_rec_out.S4S_BUSINESS_UNIT_NO or
          G_WORKGROUP_ID         <> g_rec_out.WORKGROUP_ID or 
          G_JOBGROUP_ID          <> g_rec_out.JOBGROUP_ID or
          G_JOB_ID               <> g_rec_out.JOB_ID
          then
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure hierarchy ';
         l_text          := 'Trying to illegally restructure hierarchy '
         ||' '||g_rec_out.S4S_BUSINESS_UNIT_NO 
          ||' '||g_rec_out.WORKGROUP_ID  
          ||' '||g_rec_out.JOBGROUP_ID 
          ||' '||g_rec_out.JOB_ID
          ||' '||g_rec_out.LABOUR_ROLE_ID;   
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
*/
      IF G_REC_IN.DUP_WORKGROUP_ID IS NOT NULL 
             THEN 
         g_hospital      := 'Y';
         g_hospital_text := 'Duplicate WORKGROUP_ID ';
         l_text          := 'Duplicate WORKGROUP_ID '
         ||' '||g_rec_out.S4S_BUSINESS_UNIT_NO 
          ||' '||g_rec_out.WORKGROUP_ID  
          ||' '||g_rec_out.JOBGROUP_ID 
          ||' '||g_rec_out.JOB_ID
          ||' '||g_rec_out.LABOUR_ROLE_ID;   
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

     IF G_REC_IN.DUP_JOBGROUP_ID IS NOT NULL 
         THEN 
         g_hospital      := 'Y';
         g_hospital_text := 'Duplicate JOBGROUP_ID ';
         l_text          := 'Duplicate JOBGROUP_ID '
         ||' '||g_rec_out.S4S_BUSINESS_UNIT_NO 
          ||' '||g_rec_out.WORKGROUP_ID  
          ||' '||g_rec_out.JOBGROUP_ID 
          ||' '||g_rec_out.JOB_ID
          ||' '||g_rec_out.LABOUR_ROLE_ID;   
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      IF  G_REC_IN.DUP_JOB_ID IS NOT NULL 
                THEN 
         g_hospital      := 'Y';
         g_hospital_text := 'Duplicate JOB_ID ';
         l_text          := 'Duplicate JOB_ID '
         ||' '||g_rec_out.S4S_BUSINESS_UNIT_NO 
          ||' '||g_rec_out.WORKGROUP_ID  
          ||' '||g_rec_out.JOBGROUP_ID 
          ||' '||g_rec_out.JOB_ID
          ||' '||g_rec_out.LABOUR_ROLE_ID;          
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      
--      IF G_REC_IN.DUP_LABOUR_ROLE_ID IS NOT NULL 
--         THEN 
--         g_hospital      := 'Y';
--         g_hospital_text := 'Duplicate LABOUR_ROLE_ID ';
--         l_text          := 'Duplicate LABOUR_ROLE_ID '
--         ||' '||g_rec_out.S4S_BUSINESS_UNIT_NO 
--          ||' '||g_rec_out.WORKGROUP_ID  
--          ||' '||g_rec_out.JOBGROUP_ID 
--          ||' '||g_rec_out.JOB_ID
--          ||' '||g_rec_out.LABOUR_ROLE_ID;
--         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      end if;

   
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

                insert into STG_S4S_LABOUR_HIERARCHY_HSP values 
  ( G_REC_IN.SYS_SOURCE_BATCH_ID    ,     
          G_REC_IN.SYS_SOURCE_SEQUENCE_NO,
          G_REC_IN.SYS_LOAD_DATE,
          G_REC_IN.SYS_PROCESS_CODE,
          G_REC_IN.SYS_LOAD_SYSTEM_NAME,
          G_REC_IN.SYS_MIDDLEWARE_BATCH_ID,
          G_REC_IN.SYS_PROCESS_MSG,
          G_REC_IN.SOURCE_DATA_STATUS_CODE,
          G_REC_IN.S4S_BUSINESS_UNIT_NO ,
          G_REC_IN.BUSINESS_UNIT_NO ,
          G_REC_IN.S4S_BUSINESS_UNIT_NAME,
          G_REC_IN.BUSINESS_UNIT_SEQ ,
          G_REC_IN.WORKGROUP_ID ,
          G_REC_IN.WORKGROUP_NAME,
          G_REC_IN.WORKGROUP_SEQ,
          G_REC_IN.JOBGROUP_ID,
          G_REC_IN.JOBGROUP_NAME,
          G_REC_IN.JOBGROUP_SEQ,
          G_REC_IN.JOB_ID,
          G_REC_IN.JOB_NAME ,
          G_REC_IN.LABOUR_ROLE_ID ,
          G_REC_IN.LABOUR_ROLE_NAME,
          G_REC_IN.EFFECTIVE_FROM_DATE,
          G_REC_IN.EFFECTIVE_TO_DATE
          );
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
      insert into fnd_s4s_LABOUR_HIERARCHY values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).S4S_BUSINESS_UNIT_NO||
                       ' '||a_tbl_insert(g_error_index).WORKGROUP_ID||
                       ' '||a_tbl_insert(g_error_index).JOBGROUP_ID||
                       ' '||a_tbl_insert(g_error_index).JOB_ID||
                       ' '||a_tbl_insert(g_error_index).LABOUR_ROLE_ID||
                       ' '||' INS '
                     ;  --fix index--
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
        update fnd_s4s_LABOUR_HIERARCHY
        set  
             -- S4S_BUSINESS_UNIT_NO = a_tbl_update(i).S4S_BUSINESS_UNIT_NO,
              BUSINESS_UNIT_NO = a_tbl_update(i).BUSINESS_UNIT_NO,
              S4S_BUSINESS_UNIT_NAME = a_tbl_update(i).S4S_BUSINESS_UNIT_NAME,
              BUSINESS_UNIT_SEQ = a_tbl_update(i).BUSINESS_UNIT_SEQ,
           --   WORKGROUP_ID = a_tbl_update(i).WORKGROUP_ID,
              WORKGROUP_NAME = a_tbl_update(i).WORKGROUP_NAME,
              WORKGROUP_SEQ = a_tbl_update(i).WORKGROUP_SEQ,
           --   JOBGROUP_ID = a_tbl_update(i).JOBGROUP_ID,
              JOBGROUP_NAME = a_tbl_update(i).JOBGROUP_NAME,
              JOBGROUP_SEQ = a_tbl_update(i).JOBGROUP_SEQ,
           --   JOB_ID = a_tbl_update(i).JOB_ID,
              JOB_NAME = a_tbl_update(i).JOB_NAME,
          --    LABOUR_ROLE_ID = a_tbl_update(i).LABOUR_ROLE_ID,
              LABOUR_ROLE_NAME = a_tbl_update(i).LABOUR_ROLE_NAME,
              EFFECTIVE_FROM_DATE = a_tbl_update(i).EFFECTIVE_FROM_DATE,
              EFFECTIVE_TO_DATE = a_tbl_update(i).EFFECTIVE_TO_DATE,
              LAST_UPDATED_DATE = a_tbl_update(i).LAST_UPDATED_DATE
        where S4S_BUSINESS_UNIT_NO  = a_tbl_update(i).S4S_BUSINESS_UNIT_NO  and 
              WORKGROUP_ID = a_tbl_update(i).WORKGROUP_ID and 
              JOBGROUP_ID  = a_tbl_update(i).JOBGROUP_ID  and 
              JOB_ID  = a_tbl_update(i).JOB_ID  and 
              LABOUR_ROLE_ID = a_tbl_update(i).LABOUR_ROLE_ID and
              EFFECTIVE_FROM_DATE = a_tbl_update(i).EFFECTIVE_FROM_DATE;


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
                       ' '||a_tbl_update(g_error_index).S4S_BUSINESS_UNIT_NO||
                       ' '||a_tbl_update(g_error_index).WORKGROUP_ID||
                       ' '||a_tbl_update(g_error_index).JOBGROUP_ID||
                       ' '||a_tbl_update(g_error_index).JOB_ID||
                       ' '||a_tbl_update(g_error_index).LABOUR_ROLE_ID||
                       ' '||' UPD ';
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
   -- Check to see if S4S_BUSINESS_UNIT_NO is present on table and update/insert accordingly
   select count(1)
     into g_count
     from fnd_s4s_LABOUR_HIERARCHY
     where S4S_BUSINESS_UNIT_NO  = g_rec_out.S4S_BUSINESS_UNIT_NO  and 
              WORKGROUP_ID = g_rec_out.WORKGROUP_ID and 
              JOBGROUP_ID  = g_rec_out.JOBGROUP_ID  and 
              JOB_ID  = g_rec_out.JOB_ID  and 
              LABOUR_ROLE_ID = g_rec_out.LABOUR_ROLE_ID and
              EFFECTIVE_FROM_DATE = g_rec_out.EFFECTIVE_FROM_DATE;

   if g_count = 1 then
      g_found := TRUE;
   end if;
--   l_text := 'g_count='||g_count;
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      l_text := 'S4S_BUSINESS_UNIT_NO'||g_rec_out.S4S_BUSINESS_UNIT_NO;
--        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      l_text := 'WORKGROUP_ID'||g_rec_out.WORKGROUP_ID ;
--            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      l_text := 'JOBGROUP_ID'||g_rec_out.JOBGROUP_ID ;
--                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      l_text := 'JOB_ID'||g_rec_out.JOB_ID  ;
--                    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --     l_text := 'LABOUR_ROLE_ID'||g_rec_out.LABOUR_ROLE_ID;  
   --                      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- Check if insert of labour role already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
     loop
         if a_tbl_insert(i).S4S_BUSINESS_UNIT_NO  = g_rec_out.S4S_BUSINESS_UNIT_NO  and 
              a_tbl_insert(i).WORKGROUP_ID = g_rec_out.WORKGROUP_ID and 
              a_tbl_insert(i).JOBGROUP_ID  = g_rec_out.JOBGROUP_ID  and 
              a_tbl_insert(i).JOB_ID  = g_rec_out.JOB_ID  and 
              a_tbl_insert(i).LABOUR_ROLE_ID = g_rec_out.LABOUR_ROLE_ID and
              a_tbl_insert(i).EFFECTIVE_FROM_DATE = g_rec_out.EFFECTIVE_FROM_DATE
              then
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

    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOADING FND_AST_GRADE DATA STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date); -- looking at today's date
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
  open c_STG_S4S_LABOUR_HIERARCHY;
    fetch c_STG_S4S_LABOUR_HIERARCHY bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100 = 0 then
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
    fetch c_STG_S4S_LABOUR_HIERARCHY bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_S4S_LABOUR_HIERARCHY;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

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

END WH_FND_S4S_014U_OLD;
