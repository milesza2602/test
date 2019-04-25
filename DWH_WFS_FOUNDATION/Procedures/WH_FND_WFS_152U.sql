--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_152U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_152U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Date:        December 2014
-- Author:      Jerome Appollis
-- Purpose:     Create fnd_wfs_om4_sbc table in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - stg_om4_sbc_cpy
--              Output - fnd_wfs_om4_sbc
-- Packages:    constants, dwh_log, dwh_valid
--
-- Maintenance:
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_application_no       stg_om4_sbc_cpy.application_no%type;
g_code                 stg_om4_sbc_cpy.code%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_152U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS OMSBC DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_om4_sbc_cpy
where (application_no,code)
in
(select application_no,code
from stg_om4_sbc_cpy 
group by application_no,code
having count(*) > 1) 
order by application_no,code,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_wfs_om4_omsbc_dly is
select /*+ FULL(stg)  parallel (stg,2) */  
     stg.sys_source_batch_id
    ,stg.sys_source_sequence_no
    ,stg.sys_load_date
    ,stg.sys_process_code
    ,stg.sys_load_system_name
    ,stg.sys_middleware_batch_id
    ,stg.sys_process_msg
    ,stg.application_no
    ,stg.code
    ,stg.value
    ,CASE
      WHEN TRIM(stg.code) = 'AT001'
      THEN 1
      WHEN TRIM(stg.code) = 'AT002'
      THEN 2
      WHEN TRIM(stg.code) = 'AT007'
      THEN 3
      WHEN TRIM(stg.code) = 'AT021'
      THEN 4
      WHEN TRIM(stg.code) = 'AT022'
      THEN 5
      WHEN TRIM(stg.code) = 'AT026'
      THEN 6
      WHEN TRIM(stg.code) = 'AT029'
      THEN 7
      WHEN TRIM(stg.code) = 'AT030'
      THEN 8
      WHEN TRIM(stg.code) = 'AT033'
      THEN 9
      WHEN TRIM(stg.code) = 'AT038'
      THEN 10
      WHEN TRIM(stg.code) = 'AT039'
      THEN 11
      WHEN TRIM(stg.code) = 'AT040'
      THEN 12
      WHEN TRIM(stg.code) = 'AT042'
      THEN 13
      WHEN TRIM(stg.code) = 'AT044'
      THEN 14
      WHEN TRIM(stg.code) = 'AT036'
      THEN 15
      WHEN TRIM(stg.code) = 'AT046'
      THEN 16
      WHEN TRIM(stg.code) = 'AT090'
      THEN 17
      WHEN TRIM(stg.code) = 'AT091'
      THEN 18
      WHEN TRIM(stg.code) = 'AT092'
      THEN 19
      WHEN TRIM(stg.code) = 'AT093'
      THEN 20
      WHEN TRIM(stg.code) = 'AT096'
      THEN 21
      WHEN TRIM(stg.code) = 'AT097'
      THEN 22
      WHEN TRIM(stg.code) = 'AT098'
      THEN 23
      WHEN TRIM(stg.code) = 'AT099'
      THEN 24
      WHEN TRIM(stg.code) = 'AT100'
      THEN 25
      WHEN TRIM(stg.code) = 'AT102'
      THEN 26
      WHEN TRIM(stg.code) = 'AT106'
      THEN 27
      WHEN TRIM(stg.code) = 'AT108'
      THEN 28
      WHEN TRIM(stg.code) = 'AT109'
      THEN 29
      WHEN TRIM(stg.code) = 'AT120'
      THEN 30
      WHEN TRIM(stg.code) = 'AT140'
      THEN 31
      WHEN TRIM(stg.code) = 'AT153'
      THEN 32
      WHEN TRIM(stg.code) = 'AT154'
      THEN 33
      WHEN TRIM(stg.code) = 'AT160'
      THEN 34
      WHEN TRIM(stg.code) = 'AT162'
      THEN 35
      WHEN TRIM(stg.code) = 'AT163'
      THEN 36
      WHEN TRIM(stg.code) = 'AT164'
      THEN 37
      WHEN TRIM(stg.code) = 'AT165'
      THEN 38
      WHEN TRIM(stg.code) = 'AT179'
      THEN 39
      WHEN TRIM(stg.code) = 'AT180'
      THEN 40
      WHEN TRIM(stg.code) = 'G002'
      THEN 41
      WHEN TRIM(stg.code) = 'G003'
      THEN 42
      WHEN TRIM(stg.code) = 'G010'
      THEN 43
      WHEN TRIM(stg.code) = 'G013'
      THEN 44
      WHEN TRIM(stg.code) = 'G016'
      THEN 45
      WHEN TRIM(stg.code) = 'G017'
      THEN 46
      WHEN TRIM(stg.code) = 'G024'
      THEN 47
      WHEN TRIM(stg.code) = 'G039'
      THEN 48
      WHEN TRIM(stg.code) = 'G041'
      THEN 49
      WHEN TRIM(stg.code) = 'G043'
      THEN 50
      WHEN TRIM(stg.code) = 'G044'
      THEN 51
      WHEN TRIM(stg.code) = 'G057'
      THEN 52
      WHEN TRIM(stg.code) = 'IN001'
      THEN 53
      WHEN TRIM(stg.code) = 'IN006'
      THEN 54
      WHEN TRIM(stg.code) = 'IN021'
      THEN 55
      WHEN TRIM(stg.code) = 'IN022'
      THEN 56
      WHEN TRIM(stg.code) = 'IN025'
      THEN 57
      WHEN TRIM(stg.code) = 'IN026'
      THEN 58
      WHEN TRIM(stg.code) = 'IN027'
      THEN 59
      WHEN TRIM(stg.code) = 'IN039'
      THEN 60
      WHEN TRIM(stg.code) = 'IN075'
      THEN 61
      WHEN TRIM(stg.code) = 'IN105'
      THEN 62
      WHEN TRIM(stg.code) = 'IN106'
      THEN 63
      WHEN TRIM(stg.code) = 'IN122'
      THEN 64
      WHEN TRIM(stg.code) = 'IN153'
      THEN 65
      WHEN TRIM(stg.code) = 'IN158'
      THEN 66
      WHEN TRIM(stg.code) = 'IN179'
      THEN 67
      WHEN TRIM(stg.code) = 'IN180'
      THEN 68
      WHEN TRIM(stg.code) = 'ML002'
      THEN 69
      WHEN TRIM(stg.code) = 'ML021'
      THEN 70
      WHEN TRIM(stg.code) = 'ML026'
      THEN 71
      WHEN TRIM(stg.code) = 'ML035'
      THEN 72
      WHEN TRIM(stg.code) = 'ML039'
      THEN 73
      WHEN TRIM(stg.code) = 'ML042'
      THEN 74
      WHEN TRIM(stg.code) = 'ML090'
      THEN 75
      WHEN TRIM(stg.code) = 'ML179'
      THEN 76
      WHEN TRIM(stg.code) = 'OT001'
      THEN 77
      WHEN TRIM(stg.code) = 'OT002'
      THEN 78
      WHEN TRIM(stg.code) = 'OT007'
      THEN 79
      WHEN TRIM(stg.code) = 'OT008'
      THEN 80
      WHEN TRIM(stg.code) = 'OT022'
      THEN 81
      WHEN TRIM(stg.code) = 'OT025'
      THEN 82
      WHEN TRIM(stg.code) = 'OT152'
      THEN 83
      WHEN TRIM(stg.code) = 'OT153'
      THEN 84
      WHEN TRIM(stg.code) = 'OT164'
      THEN 85
      WHEN TRIM(stg.code) = 'OT167'
      THEN 86
      WHEN TRIM(stg.code) = 'OT179'
      THEN 87
      WHEN TRIM(stg.code) = 'OT180'
      THEN 88
      WHEN TRIM(stg.code) = 'RE001'
      THEN 89
      WHEN TRIM(stg.code) = 'RE002'
      THEN 90
      WHEN TRIM(stg.code) = 'RE007'
      THEN 91
      WHEN TRIM(stg.code) = 'RE008'
      THEN 92
      WHEN TRIM(stg.code) = 'RE026'
      THEN 93
      WHEN TRIM(stg.code) = 'RE033'
      THEN 94
      WHEN TRIM(stg.code) = 'RE039'
      THEN 95
      WHEN TRIM(stg.code) = 'RE042'
      THEN 96
      WHEN TRIM(stg.code) = 'RE045'
      THEN 97
      WHEN TRIM(stg.code) = 'RE102'
      THEN 98
      WHEN TRIM(stg.code) = 'RE103'
      THEN 99
      WHEN TRIM(stg.code) = 'RE104'
      THEN 100
      WHEN TRIM(stg.code) = 'RE105'
      THEN 101
      WHEN TRIM(stg.code) = 'RE120'
      THEN 102
      WHEN TRIM(stg.code) = 'RE121'
      THEN 103
      WHEN TRIM(stg.code) = 'RE122'
      THEN 104
      WHEN TRIM(stg.code) = 'RE140'
      THEN 105
      WHEN TRIM(stg.code) = 'RE153'
      THEN 106
      WHEN TRIM(stg.code) = 'RE156'
      THEN 107
      WHEN TRIM(stg.code) = 'RE159'
      THEN 108
      WHEN TRIM(stg.code) = 'RE162'
      THEN 109
      WHEN TRIM(stg.code) = 'RE164'
      THEN 110
      WHEN TRIM(stg.code) = 'RE167'
      THEN 111
      WHEN TRIM(stg.code) = 'RE179'
      THEN 112
      WHEN TRIM(stg.code) = 'RE180'
      THEN 113
      WHEN TRIM(stg.code) = 'IN159'
      THEN 114
      WHEN TRIM(stg.code) = 'ML044'
      THEN 115
      WHEN TRIM(stg.code) = 'AT025'
      THEN 116
      WHEN TRIM(stg.code) = 'AT034'
      THEN 117
      WHEN TRIM(stg.code) = 'AT035'
      THEN 118
      WHEN TRIM(stg.code) = 'AT155'
      THEN 119
      WHEN TRIM(stg.code) = 'AT159'
      THEN 120
      WHEN TRIM(stg.code) = 'G022'
      THEN 121
      WHEN TRIM(stg.code) = 'G056'
      THEN 122
      WHEN TRIM(stg.code) = 'IN121'
      THEN 123
      WHEN TRIM(stg.code) = 'ML025'
      THEN 124
      WHEN TRIM(stg.code) = 'ML034'
      THEN 125
      WHEN TRIM(stg.code) = 'ML036'
      THEN 126
      WHEN TRIM(stg.code) = 'ML037'
      THEN 127
      WHEN TRIM(stg.code) = 'ML038'
      THEN 128
      WHEN TRIM(stg.code) = 'ML040'
      THEN 129
      WHEN TRIM(stg.code) = 'ML091'
      THEN 130
      WHEN TRIM(stg.code) = 'ML121'
      THEN 131
      WHEN TRIM(stg.code) = 'ML153'
      THEN 132
      WHEN TRIM(stg.code) = 'ML162'
      THEN 133
      WHEN TRIM(stg.code) = 'ML163'
      THEN 134
      WHEN TRIM(stg.code) = 'ML181'
      THEN 135
      WHEN TRIM(stg.code) = 'OT122'
      THEN 136
      WHEN TRIM(stg.code) = 'OT154'
      THEN 137
      WHEN TRIM(stg.code) = 'AT043'
      THEN 138
      WHEN TRIM(stg.code) = 'IN107'
      THEN 139
      WHEN TRIM(stg.code) = 'ML041'
      THEN 140
      WHEN TRIM(stg.code) = 'ML092'
      THEN 141
      WHEN TRIM(stg.code) = 'ML097'
      THEN 142
      WHEN TRIM(stg.code) = 'ML111'
      THEN 143
      WHEN TRIM(stg.code) = 'G011'
      THEN 144
      WHEN TRIM(stg.code) = 'G048'
      THEN 145
      WHEN TRIM(stg.code) = 'AT124'
      THEN 146
      WHEN TRIM(stg.code) = 'ML001'
      THEN 147
      WHEN TRIM(stg.code) = 'ML043'
      THEN 148
      WHEN TRIM(stg.code) = 'ML104'
      THEN 149
      WHEN TRIM(stg.code) = 'ML140'
      THEN 150
      WHEN TRIM(stg.code) = 'OT042'
      THEN 151
      WHEN TRIM(stg.code) = 'AT028'
      THEN 152
      WHEN TRIM(stg.code) = 'G021'
      THEN 153
      WHEN TRIM(stg.code) = 'AT152'
      THEN 154
      WHEN TRIM(stg.code) = 'AT161'
      THEN 155
      WHEN TRIM(stg.code) = 'IN162'
      THEN 156
      WHEN TRIM(stg.code) = 'OT026'
      THEN 157
      WHEN TRIM(stg.code) = 'OT106'
      THEN 158
      WHEN TRIM(stg.code) = 'OT121'
      THEN 159
      WHEN TRIM(stg.code) = 'OT162'
      THEN 160
      WHEN TRIM(stg.code) = 'G052'
      THEN 161
      WHEN TRIM(stg.code) = 'G054'
      THEN 162
      WHEN TRIM(stg.code) = 'AT037'
      THEN 163
      WHEN TRIM(stg.code) = 'G014'
      THEN 164
      WHEN TRIM(stg.code) = 'G030'
      THEN 165
      WHEN TRIM(stg.code) = 'ML094'
      THEN 166
      WHEN TRIM(stg.code) = 'AT107'
      THEN 167
      WHEN TRIM(stg.code) = 'AT006'
      THEN 168
      WHEN TRIM(stg.code) = 'AT041'
      THEN 169
      WHEN TRIM(stg.code) = 'AT103'
      THEN 170
      WHEN TRIM(stg.code) = 'AT121'
      THEN 171
      WHEN TRIM(stg.code) = 'AT141'
      THEN 172
      WHEN TRIM(stg.code) = 'AT156'
      THEN 173
      WHEN TRIM(stg.code) = 'AT167'
      THEN 174
      WHEN TRIM(stg.code) = 'G031'
      THEN 175
      WHEN TRIM(stg.code) = 'IN181'
      THEN 176
      WHEN TRIM(stg.code) = 'ML006'
      THEN 177
      WHEN TRIM(stg.code) = 'ML093'
      THEN 178
      WHEN TRIM(stg.code) = 'ML160'
      THEN 179
      WHEN TRIM(stg.code) = 'RE003'
      THEN 180
      WHEN TRIM(stg.code) = 'RE022'
      THEN 181
      WHEN TRIM(stg.code) = 'RE107'
      THEN 182 ELSE 0 END sequence  
      from    stg_om4_sbc_cpy stg,
              fnd_wfs_om4_sbc fnd
      where   stg.application_no        = fnd.application_no   and             
              stg.code                  = fnd.code    and    
              stg.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.application_no,
              stg.code,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

g_application_no   := 0;
g_code             := 0; 


for dupp_record in stg_dup
   loop

    if  dupp_record.application_no       = g_application_no and
        dupp_record.code                 = g_code  then
        update stg_om4_sbc_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_application_no   := dupp_record.application_no;
    g_code             := dupp_record.code; 
    
    end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
 
 
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_om4_sbc fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
               cpy.     application_no  
              ,cpy.     code    
              ,cpy.     value   
              ,CASE
                WHEN TRIM(cpy.code) = 'AT001'
                THEN 1
                WHEN TRIM(cpy.code) = 'AT002'
                THEN 2
                WHEN TRIM(cpy.code) = 'AT007'
                THEN 3
                WHEN TRIM(cpy.code) = 'AT021'
                THEN 4
                WHEN TRIM(cpy.code) = 'AT022'
                THEN 5
                WHEN TRIM(cpy.code) = 'AT026'
                THEN 6
                WHEN TRIM(cpy.code) = 'AT029'
                THEN 7
                WHEN TRIM(cpy.code) = 'AT030'
                THEN 8
                WHEN TRIM(cpy.code) = 'AT033'
                THEN 9
                WHEN TRIM(cpy.code) = 'AT038'
                THEN 10
                WHEN TRIM(cpy.code) = 'AT039'
                THEN 11
                WHEN TRIM(cpy.code) = 'AT040'
                THEN 12
                WHEN TRIM(cpy.code) = 'AT042'
                THEN 13
                WHEN TRIM(cpy.code) = 'AT044'
                THEN 14
                WHEN TRIM(cpy.code) = 'AT036'
                THEN 15
                WHEN TRIM(cpy.code) = 'AT046'
                THEN 16
                WHEN TRIM(cpy.code) = 'AT090'
                THEN 17
                WHEN TRIM(cpy.code) = 'AT091'
                THEN 18
                WHEN TRIM(cpy.code) = 'AT092'
                THEN 19
                WHEN TRIM(cpy.code) = 'AT093'
                THEN 20
                WHEN TRIM(cpy.code) = 'AT096'
                THEN 21
                WHEN TRIM(cpy.code) = 'AT097'
                THEN 22
                WHEN TRIM(cpy.code) = 'AT098'
                THEN 23
                WHEN TRIM(cpy.code) = 'AT099'
                THEN 24
                WHEN TRIM(cpy.code) = 'AT100'
                THEN 25
                WHEN TRIM(cpy.code) = 'AT102'
                THEN 26
                WHEN TRIM(cpy.code) = 'AT106'
                THEN 27
                WHEN TRIM(cpy.code) = 'AT108'
                THEN 28
                WHEN TRIM(cpy.code) = 'AT109'
                THEN 29
                WHEN TRIM(cpy.code) = 'AT120'
                THEN 30
                WHEN TRIM(cpy.code) = 'AT140'
                THEN 31
                WHEN TRIM(cpy.code) = 'AT153'
                THEN 32
                WHEN TRIM(cpy.code) = 'AT154'
                THEN 33
                WHEN TRIM(cpy.code) = 'AT160'
                THEN 34
                WHEN TRIM(cpy.code) = 'AT162'
                THEN 35
                WHEN TRIM(cpy.code) = 'AT163'
                THEN 36
                WHEN TRIM(cpy.code) = 'AT164'
                THEN 37
                WHEN TRIM(cpy.code) = 'AT165'
                THEN 38
                WHEN TRIM(cpy.code) = 'AT179'
                THEN 39
                WHEN TRIM(cpy.code) = 'AT180'
                THEN 40
                WHEN TRIM(cpy.code) = 'G002'
                THEN 41
                WHEN TRIM(cpy.code) = 'G003'
                THEN 42
                WHEN TRIM(cpy.code) = 'G010'
                THEN 43
                WHEN TRIM(cpy.code) = 'G013'
                THEN 44
                WHEN TRIM(cpy.code) = 'G016'
                THEN 45
                WHEN TRIM(cpy.code) = 'G017'
                THEN 46
                WHEN TRIM(cpy.code) = 'G024'
                THEN 47
                WHEN TRIM(cpy.code) = 'G039'
                THEN 48
                WHEN TRIM(cpy.code) = 'G041'
                THEN 49
                WHEN TRIM(cpy.code) = 'G043'
                THEN 50
                WHEN TRIM(cpy.code) = 'G044'
                THEN 51
                WHEN TRIM(cpy.code) = 'G057'
                THEN 52
                WHEN TRIM(cpy.code) = 'IN001'
                THEN 53
                WHEN TRIM(cpy.code) = 'IN006'
                THEN 54
                WHEN TRIM(cpy.code) = 'IN021'
                THEN 55
                WHEN TRIM(cpy.code) = 'IN022'
                THEN 56
                WHEN TRIM(cpy.code) = 'IN025'
                THEN 57
                WHEN TRIM(cpy.code) = 'IN026'
                THEN 58
                WHEN TRIM(cpy.code) = 'IN027'
                THEN 59
                WHEN TRIM(cpy.code) = 'IN039'
                THEN 60
                WHEN TRIM(cpy.code) = 'IN075'
                THEN 61
                WHEN TRIM(cpy.code) = 'IN105'
                THEN 62
                WHEN TRIM(cpy.code) = 'IN106'
                THEN 63
                WHEN TRIM(cpy.code) = 'IN122'
                THEN 64
                WHEN TRIM(cpy.code) = 'IN153'
                THEN 65
                WHEN TRIM(cpy.code) = 'IN158'
                THEN 66
                WHEN TRIM(cpy.code) = 'IN179'
                THEN 67
                WHEN TRIM(cpy.code) = 'IN180'
                THEN 68
                WHEN TRIM(cpy.code) = 'ML002'
                THEN 69
                WHEN TRIM(cpy.code) = 'ML021'
                THEN 70
                WHEN TRIM(cpy.code) = 'ML026'
                THEN 71
                WHEN TRIM(cpy.code) = 'ML035'
                THEN 72
                WHEN TRIM(cpy.code) = 'ML039'
                THEN 73
                WHEN TRIM(cpy.code) = 'ML042'
                THEN 74
                WHEN TRIM(cpy.code) = 'ML090'
                THEN 75
                WHEN TRIM(cpy.code) = 'ML179'
                THEN 76
                WHEN TRIM(cpy.code) = 'OT001'
                THEN 77
                WHEN TRIM(cpy.code) = 'OT002'
                THEN 78
                WHEN TRIM(cpy.code) = 'OT007'
                THEN 79
                WHEN TRIM(cpy.code) = 'OT008'
                THEN 80
                WHEN TRIM(cpy.code) = 'OT022'
                THEN 81
                WHEN TRIM(cpy.code) = 'OT025'
                THEN 82
                WHEN TRIM(cpy.code) = 'OT152'
                THEN 83
                WHEN TRIM(cpy.code) = 'OT153'
                THEN 84
                WHEN TRIM(cpy.code) = 'OT164'
                THEN 85
                WHEN TRIM(cpy.code) = 'OT167'
                THEN 86
                WHEN TRIM(cpy.code) = 'OT179'
                THEN 87
                WHEN TRIM(cpy.code) = 'OT180'
                THEN 88
                WHEN TRIM(cpy.code) = 'RE001'
                THEN 89
                WHEN TRIM(cpy.code) = 'RE002'
                THEN 90
                WHEN TRIM(cpy.code) = 'RE007'
                THEN 91
                WHEN TRIM(cpy.code) = 'RE008'
                THEN 92
                WHEN TRIM(cpy.code) = 'RE026'
                THEN 93
                WHEN TRIM(cpy.code) = 'RE033'
                THEN 94
                WHEN TRIM(cpy.code) = 'RE039'
                THEN 95
                WHEN TRIM(cpy.code) = 'RE042'
                THEN 96
                WHEN TRIM(cpy.code) = 'RE045'
                THEN 97
                WHEN TRIM(cpy.code) = 'RE102'
                THEN 98
                WHEN TRIM(cpy.code) = 'RE103'
                THEN 99
                WHEN TRIM(cpy.code) = 'RE104'
                THEN 100
                WHEN TRIM(cpy.code) = 'RE105'
                THEN 101
                WHEN TRIM(cpy.code) = 'RE120'
                THEN 102
                WHEN TRIM(cpy.code) = 'RE121'
                THEN 103
                WHEN TRIM(cpy.code) = 'RE122'
                THEN 104
                WHEN TRIM(cpy.code) = 'RE140'
                THEN 105
                WHEN TRIM(cpy.code) = 'RE153'
                THEN 106
                WHEN TRIM(cpy.code) = 'RE156'
                THEN 107
                WHEN TRIM(cpy.code) = 'RE159'
                THEN 108
                WHEN TRIM(cpy.code) = 'RE162'
                THEN 109
                WHEN TRIM(cpy.code) = 'RE164'
                THEN 110
                WHEN TRIM(cpy.code) = 'RE167'
                THEN 111
                WHEN TRIM(cpy.code) = 'RE179'
                THEN 112
                WHEN TRIM(cpy.code) = 'RE180'
                THEN 113
                WHEN TRIM(cpy.code) = 'IN159'
                THEN 114
                WHEN TRIM(cpy.code) = 'ML044'
                THEN 115
                WHEN TRIM(cpy.code) = 'AT025'
                THEN 116
                WHEN TRIM(cpy.code) = 'AT034'
                THEN 117
                WHEN TRIM(cpy.code) = 'AT035'
                THEN 118
                WHEN TRIM(cpy.code) = 'AT155'
                THEN 119
                WHEN TRIM(cpy.code) = 'AT159'
                THEN 120
                WHEN TRIM(cpy.code) = 'G022'
                THEN 121
                WHEN TRIM(cpy.code) = 'G056'
                THEN 122
                WHEN TRIM(cpy.code) = 'IN121'
                THEN 123
                WHEN TRIM(cpy.code) = 'ML025'
                THEN 124
                WHEN TRIM(cpy.code) = 'ML034'
                THEN 125
                WHEN TRIM(cpy.code) = 'ML036'
                THEN 126
                WHEN TRIM(cpy.code) = 'ML037'
                THEN 127
                WHEN TRIM(cpy.code) = 'ML038'
                THEN 128
                WHEN TRIM(cpy.code) = 'ML040'
                THEN 129
                WHEN TRIM(cpy.code) = 'ML091'
                THEN 130
                WHEN TRIM(cpy.code) = 'ML121'
                THEN 131
                WHEN TRIM(cpy.code) = 'ML153'
                THEN 132
                WHEN TRIM(cpy.code) = 'ML162'
                THEN 133
                WHEN TRIM(cpy.code) = 'ML163'
                THEN 134
                WHEN TRIM(cpy.code) = 'ML181'
                THEN 135
                WHEN TRIM(cpy.code) = 'OT122'
                THEN 136
                WHEN TRIM(cpy.code) = 'OT154'
                THEN 137
                WHEN TRIM(cpy.code) = 'AT043'
                THEN 138
                WHEN TRIM(cpy.code) = 'IN107'
                THEN 139
                WHEN TRIM(cpy.code) = 'ML041'
                THEN 140
                WHEN TRIM(cpy.code) = 'ML092'
                THEN 141
                WHEN TRIM(cpy.code) = 'ML097'
                THEN 142
                WHEN TRIM(cpy.code) = 'ML111'
                THEN 143
                WHEN TRIM(cpy.code) = 'G011'
                THEN 144
                WHEN TRIM(cpy.code) = 'G048'
                THEN 145
                WHEN TRIM(cpy.code) = 'AT124'
                THEN 146
                WHEN TRIM(cpy.code) = 'ML001'
                THEN 147
                WHEN TRIM(cpy.code) = 'ML043'
                THEN 148
                WHEN TRIM(cpy.code) = 'ML104'
                THEN 149
                WHEN TRIM(cpy.code) = 'ML140'
                THEN 150
                WHEN TRIM(cpy.code) = 'OT042'
                THEN 151
                WHEN TRIM(cpy.code) = 'AT028'
                THEN 152
                WHEN TRIM(cpy.code) = 'G021'
                THEN 153
                WHEN TRIM(cpy.code) = 'AT152'
                THEN 154
                WHEN TRIM(cpy.code) = 'AT161'
                THEN 155
                WHEN TRIM(cpy.code) = 'IN162'
                THEN 156
                WHEN TRIM(cpy.code) = 'OT026'
                THEN 157
                WHEN TRIM(cpy.code) = 'OT106'
                THEN 158
                WHEN TRIM(cpy.code) = 'OT121'
                THEN 159
                WHEN TRIM(cpy.code) = 'OT162'
                THEN 160
                WHEN TRIM(cpy.code) = 'G052'
                THEN 161
                WHEN TRIM(cpy.code) = 'G054'
                THEN 162
                WHEN TRIM(cpy.code) = 'AT037'
                THEN 163
                WHEN TRIM(cpy.code) = 'G014'
                THEN 164
                WHEN TRIM(cpy.code) = 'G030'
                THEN 165
                WHEN TRIM(cpy.code) = 'ML094'
                THEN 166
                WHEN TRIM(cpy.code) = 'AT107'
                THEN 167
                WHEN TRIM(cpy.code) = 'AT006'
                THEN 168
                WHEN TRIM(cpy.code) = 'AT041'
                THEN 169
                WHEN TRIM(cpy.code) = 'AT103'
                THEN 170
                WHEN TRIM(cpy.code) = 'AT121'
                THEN 171
                WHEN TRIM(cpy.code) = 'AT141'
                THEN 172
                WHEN TRIM(cpy.code) = 'AT156'
                THEN 173
                WHEN TRIM(cpy.code) = 'AT167'
                THEN 174
                WHEN TRIM(cpy.code) = 'G031'
                THEN 175
                WHEN TRIM(cpy.code) = 'IN181'
                THEN 176
                WHEN TRIM(cpy.code) = 'ML006'
                THEN 177
                WHEN TRIM(cpy.code) = 'ML093'
                THEN 178
                WHEN TRIM(cpy.code) = 'ML160'
                THEN 179
                WHEN TRIM(cpy.code) = 'RE003'
                THEN 180
                WHEN TRIM(cpy.code) = 'RE022'
                THEN 181
                WHEN TRIM(cpy.code) = 'RE107'
                THEN 182 ELSE 0 END sequence  
              ,g_date as last_updated_date
      from    stg_om4_sbc_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_om4_sbc 
       where  application_no           = cpy.application_no and
              code                     = cpy.code )
-- Any further validation goes in here - like xxx.ind in (0,1) ---  
       and sys_process_code = 'N';
 

      g_recs_inserted := g_recs_inserted + sql%rowcount;
      
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



for upd_rec in c_stg_wfs_om4_omsbc_dly
   loop
     update fnd_wfs_om4_sbc fnd 
     set    fnd.value                             =     upd_rec.value   ,
            fnd.sequence                              = upd_rec.sequence        ,
            fnd.last_updated_date               = g_date
     where  fnd.application_no                  = upd_rec.application_no and
            fnd.code                            =       upd_rec.code and
            ( 
            nvl(fnd.value       ,0) <>  upd_rec.value   or
            nvl(fnd.sequence    ,0) <>  upd_rec.sequence        
            );         
             
      g_recs_updated := g_recs_updated + 1;        
   end loop;


      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;

   
--************************************************************************************************** 
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_om4_sbc_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
              cpy.sys_source_batch_id,
              cpy.sys_source_sequence_no,
              sysdate,'Y','DWH',
              cpy.sys_middleware_batch_id,
              'VALIDATION FAIL - REFERENCIAL ERROR',
              cpy.application_no        ,
              cpy.code  ,
              cpy.value ,
              CASE
                WHEN TRIM(cpy.code) = 'AT001'
                THEN 1
                WHEN TRIM(cpy.code) = 'AT002'
                THEN 2
                WHEN TRIM(cpy.code) = 'AT007'
                THEN 3
                WHEN TRIM(cpy.code) = 'AT021'
                THEN 4
                WHEN TRIM(cpy.code) = 'AT022'
                THEN 5
                WHEN TRIM(cpy.code) = 'AT026'
                THEN 6
                WHEN TRIM(cpy.code) = 'AT029'
                THEN 7
                WHEN TRIM(cpy.code) = 'AT030'
                THEN 8
                WHEN TRIM(cpy.code) = 'AT033'
                THEN 9
                WHEN TRIM(cpy.code) = 'AT038'
                THEN 10
                WHEN TRIM(cpy.code) = 'AT039'
                THEN 11
                WHEN TRIM(cpy.code) = 'AT040'
                THEN 12
                WHEN TRIM(cpy.code) = 'AT042'
                THEN 13
                WHEN TRIM(cpy.code) = 'AT044'
                THEN 14
                WHEN TRIM(cpy.code) = 'AT036'
                THEN 15
                WHEN TRIM(cpy.code) = 'AT046'
                THEN 16
                WHEN TRIM(cpy.code) = 'AT090'
                THEN 17
                WHEN TRIM(cpy.code) = 'AT091'
                THEN 18
                WHEN TRIM(cpy.code) = 'AT092'
                THEN 19
                WHEN TRIM(cpy.code) = 'AT093'
                THEN 20
                WHEN TRIM(cpy.code) = 'AT096'
                THEN 21
                WHEN TRIM(cpy.code) = 'AT097'
                THEN 22
                WHEN TRIM(cpy.code) = 'AT098'
                THEN 23
                WHEN TRIM(cpy.code) = 'AT099'
                THEN 24
                WHEN TRIM(cpy.code) = 'AT100'
                THEN 25
                WHEN TRIM(cpy.code) = 'AT102'
                THEN 26
                WHEN TRIM(cpy.code) = 'AT106'
                THEN 27
                WHEN TRIM(cpy.code) = 'AT108'
                THEN 28
                WHEN TRIM(cpy.code) = 'AT109'
                THEN 29
                WHEN TRIM(cpy.code) = 'AT120'
                THEN 30
                WHEN TRIM(cpy.code) = 'AT140'
                THEN 31
                WHEN TRIM(cpy.code) = 'AT153'
                THEN 32
                WHEN TRIM(cpy.code) = 'AT154'
                THEN 33
                WHEN TRIM(cpy.code) = 'AT160'
                THEN 34
                WHEN TRIM(cpy.code) = 'AT162'
                THEN 35
                WHEN TRIM(cpy.code) = 'AT163'
                THEN 36
                WHEN TRIM(cpy.code) = 'AT164'
                THEN 37
                WHEN TRIM(cpy.code) = 'AT165'
                THEN 38
                WHEN TRIM(cpy.code) = 'AT179'
                THEN 39
                WHEN TRIM(cpy.code) = 'AT180'
                THEN 40
                WHEN TRIM(cpy.code) = 'G002'
                THEN 41
                WHEN TRIM(cpy.code) = 'G003'
                THEN 42
                WHEN TRIM(cpy.code) = 'G010'
                THEN 43
                WHEN TRIM(cpy.code) = 'G013'
                THEN 44
                WHEN TRIM(cpy.code) = 'G016'
                THEN 45
                WHEN TRIM(cpy.code) = 'G017'
                THEN 46
                WHEN TRIM(cpy.code) = 'G024'
                THEN 47
                WHEN TRIM(cpy.code) = 'G039'
                THEN 48
                WHEN TRIM(cpy.code) = 'G041'
                THEN 49
                WHEN TRIM(cpy.code) = 'G043'
                THEN 50
                WHEN TRIM(cpy.code) = 'G044'
                THEN 51
                WHEN TRIM(cpy.code) = 'G057'
                THEN 52
                WHEN TRIM(cpy.code) = 'IN001'
                THEN 53
                WHEN TRIM(cpy.code) = 'IN006'
                THEN 54
                WHEN TRIM(cpy.code) = 'IN021'
                THEN 55
                WHEN TRIM(cpy.code) = 'IN022'
                THEN 56
                WHEN TRIM(cpy.code) = 'IN025'
                THEN 57
                WHEN TRIM(cpy.code) = 'IN026'
                THEN 58
                WHEN TRIM(cpy.code) = 'IN027'
                THEN 59
                WHEN TRIM(cpy.code) = 'IN039'
                THEN 60
                WHEN TRIM(cpy.code) = 'IN075'
                THEN 61
                WHEN TRIM(cpy.code) = 'IN105'
                THEN 62
                WHEN TRIM(cpy.code) = 'IN106'
                THEN 63
                WHEN TRIM(cpy.code) = 'IN122'
                THEN 64
                WHEN TRIM(cpy.code) = 'IN153'
                THEN 65
                WHEN TRIM(cpy.code) = 'IN158'
                THEN 66
                WHEN TRIM(cpy.code) = 'IN179'
                THEN 67
                WHEN TRIM(cpy.code) = 'IN180'
                THEN 68
                WHEN TRIM(cpy.code) = 'ML002'
                THEN 69
                WHEN TRIM(cpy.code) = 'ML021'
                THEN 70
                WHEN TRIM(cpy.code) = 'ML026'
                THEN 71
                WHEN TRIM(cpy.code) = 'ML035'
                THEN 72
                WHEN TRIM(cpy.code) = 'ML039'
                THEN 73
                WHEN TRIM(cpy.code) = 'ML042'
                THEN 74
                WHEN TRIM(cpy.code) = 'ML090'
                THEN 75
                WHEN TRIM(cpy.code) = 'ML179'
                THEN 76
                WHEN TRIM(cpy.code) = 'OT001'
                THEN 77
                WHEN TRIM(cpy.code) = 'OT002'
                THEN 78
                WHEN TRIM(cpy.code) = 'OT007'
                THEN 79
                WHEN TRIM(cpy.code) = 'OT008'
                THEN 80
                WHEN TRIM(cpy.code) = 'OT022'
                THEN 81
                WHEN TRIM(cpy.code) = 'OT025'
                THEN 82
                WHEN TRIM(cpy.code) = 'OT152'
                THEN 83
                WHEN TRIM(cpy.code) = 'OT153'
                THEN 84
                WHEN TRIM(cpy.code) = 'OT164'
                THEN 85
                WHEN TRIM(cpy.code) = 'OT167'
                THEN 86
                WHEN TRIM(cpy.code) = 'OT179'
                THEN 87
                WHEN TRIM(cpy.code) = 'OT180'
                THEN 88
                WHEN TRIM(cpy.code) = 'RE001'
                THEN 89
                WHEN TRIM(cpy.code) = 'RE002'
                THEN 90
                WHEN TRIM(cpy.code) = 'RE007'
                THEN 91
                WHEN TRIM(cpy.code) = 'RE008'
                THEN 92
                WHEN TRIM(cpy.code) = 'RE026'
                THEN 93
                WHEN TRIM(cpy.code) = 'RE033'
                THEN 94
                WHEN TRIM(cpy.code) = 'RE039'
                THEN 95
                WHEN TRIM(cpy.code) = 'RE042'
                THEN 96
                WHEN TRIM(cpy.code) = 'RE045'
                THEN 97
                WHEN TRIM(cpy.code) = 'RE102'
                THEN 98
                WHEN TRIM(cpy.code) = 'RE103'
                THEN 99
                WHEN TRIM(cpy.code) = 'RE104'
                THEN 100
                WHEN TRIM(cpy.code) = 'RE105'
                THEN 101
                WHEN TRIM(cpy.code) = 'RE120'
                THEN 102
                WHEN TRIM(cpy.code) = 'RE121'
                THEN 103
                WHEN TRIM(cpy.code) = 'RE122'
                THEN 104
                WHEN TRIM(cpy.code) = 'RE140'
                THEN 105
                WHEN TRIM(cpy.code) = 'RE153'
                THEN 106
                WHEN TRIM(cpy.code) = 'RE156'
                THEN 107
                WHEN TRIM(cpy.code) = 'RE159'
                THEN 108
                WHEN TRIM(cpy.code) = 'RE162'
                THEN 109
                WHEN TRIM(cpy.code) = 'RE164'
                THEN 110
                WHEN TRIM(cpy.code) = 'RE167'
                THEN 111
                WHEN TRIM(cpy.code) = 'RE179'
                THEN 112
                WHEN TRIM(cpy.code) = 'RE180'
                THEN 113
                WHEN TRIM(cpy.code) = 'IN159'
                THEN 114
                WHEN TRIM(cpy.code) = 'ML044'
                THEN 115
                WHEN TRIM(cpy.code) = 'AT025'
                THEN 116
                WHEN TRIM(cpy.code) = 'AT034'
                THEN 117
                WHEN TRIM(cpy.code) = 'AT035'
                THEN 118
                WHEN TRIM(cpy.code) = 'AT155'
                THEN 119
                WHEN TRIM(cpy.code) = 'AT159'
                THEN 120
                WHEN TRIM(cpy.code) = 'G022'
                THEN 121
                WHEN TRIM(cpy.code) = 'G056'
                THEN 122
                WHEN TRIM(cpy.code) = 'IN121'
                THEN 123
                WHEN TRIM(cpy.code) = 'ML025'
                THEN 124
                WHEN TRIM(cpy.code) = 'ML034'
                THEN 125
                WHEN TRIM(cpy.code) = 'ML036'
                THEN 126
                WHEN TRIM(cpy.code) = 'ML037'
                THEN 127
                WHEN TRIM(cpy.code) = 'ML038'
                THEN 128
                WHEN TRIM(cpy.code) = 'ML040'
                THEN 129
                WHEN TRIM(cpy.code) = 'ML091'
                THEN 130
                WHEN TRIM(cpy.code) = 'ML121'
                THEN 131
                WHEN TRIM(cpy.code) = 'ML153'
                THEN 132
                WHEN TRIM(cpy.code) = 'ML162'
                THEN 133
                WHEN TRIM(cpy.code) = 'ML163'
                THEN 134
                WHEN TRIM(cpy.code) = 'ML181'
                THEN 135
                WHEN TRIM(cpy.code) = 'OT122'
                THEN 136
                WHEN TRIM(cpy.code) = 'OT154'
                THEN 137
                WHEN TRIM(cpy.code) = 'AT043'
                THEN 138
                WHEN TRIM(cpy.code) = 'IN107'
                THEN 139
                WHEN TRIM(cpy.code) = 'ML041'
                THEN 140
                WHEN TRIM(cpy.code) = 'ML092'
                THEN 141
                WHEN TRIM(cpy.code) = 'ML097'
                THEN 142
                WHEN TRIM(cpy.code) = 'ML111'
                THEN 143
                WHEN TRIM(cpy.code) = 'G011'
                THEN 144
                WHEN TRIM(cpy.code) = 'G048'
                THEN 145
                WHEN TRIM(cpy.code) = 'AT124'
                THEN 146
                WHEN TRIM(cpy.code) = 'ML001'
                THEN 147
                WHEN TRIM(cpy.code) = 'ML043'
                THEN 148
                WHEN TRIM(cpy.code) = 'ML104'
                THEN 149
                WHEN TRIM(cpy.code) = 'ML140'
                THEN 150
                WHEN TRIM(cpy.code) = 'OT042'
                THEN 151
                WHEN TRIM(cpy.code) = 'AT028'
                THEN 152
                WHEN TRIM(cpy.code) = 'G021'
                THEN 153
                WHEN TRIM(cpy.code) = 'AT152'
                THEN 154
                WHEN TRIM(cpy.code) = 'AT161'
                THEN 155
                WHEN TRIM(cpy.code) = 'IN162'
                THEN 156
                WHEN TRIM(cpy.code) = 'OT026'
                THEN 157
                WHEN TRIM(cpy.code) = 'OT106'
                THEN 158
                WHEN TRIM(cpy.code) = 'OT121'
                THEN 159
                WHEN TRIM(cpy.code) = 'OT162'
                THEN 160
                WHEN TRIM(cpy.code) = 'G052'
                THEN 161
                WHEN TRIM(cpy.code) = 'G054'
                THEN 162
                WHEN TRIM(cpy.code) = 'AT037'
                THEN 163
                WHEN TRIM(cpy.code) = 'G014'
                THEN 164
                WHEN TRIM(cpy.code) = 'G030'
                THEN 165
                WHEN TRIM(cpy.code) = 'ML094'
                THEN 166
                WHEN TRIM(cpy.code) = 'AT107'
                THEN 167
                WHEN TRIM(cpy.code) = 'AT006'
                THEN 168
                WHEN TRIM(cpy.code) = 'AT041'
                THEN 169
                WHEN TRIM(cpy.code) = 'AT103'
                THEN 170
                WHEN TRIM(cpy.code) = 'AT121'
                THEN 171
                WHEN TRIM(cpy.code) = 'AT141'
                THEN 172
                WHEN TRIM(cpy.code) = 'AT156'
                THEN 173
                WHEN TRIM(cpy.code) = 'AT167'
                THEN 174
                WHEN TRIM(cpy.code) = 'G031'
                THEN 175
                WHEN TRIM(cpy.code) = 'IN181'
                THEN 176
                WHEN TRIM(cpy.code) = 'ML006'
                THEN 177
                WHEN TRIM(cpy.code) = 'ML093'
                THEN 178
                WHEN TRIM(cpy.code) = 'ML160'
                THEN 179
                WHEN TRIM(cpy.code) = 'RE003'
                THEN 180
                WHEN TRIM(cpy.code) = 'RE022'
                THEN 181
                WHEN TRIM(cpy.code) = 'RE107'
                THEN 182 ELSE 0 END seq
      from   stg_om4_sbc_cpy cpy
      where  
--      (    
--      NOT EXISTS 
--        (SELECT * FROM  dim_table dim
--         where  cpy.xxx       = dim.xxx ) or
--      not exists 
--        (select * from  dim_table dim1
--         where  cpy.xxx    = dim1.xxx ) 
--      ) and 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
        sys_process_code = 'N';
         

g_recs_hospital := g_recs_hospital + sql%rowcount;
      
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;

    

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

 
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************

    
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;
    
    select count(*)
    into   g_recs_read
    from   stg_om4_sbc_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;


--********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_crd_acc_dly_cpy
--    set    sys_process_code = 'Y';

 
   


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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;  


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
       RAISE;
end WH_FND_WFS_152U;
