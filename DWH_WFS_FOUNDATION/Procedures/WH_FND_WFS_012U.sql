--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_012U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_012U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create fnd_wfs_customer_absa fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_absa_customer_cpy
--               Output - fnd_wfs_customer_absa
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
--  10 Nov 2016 - A. De Wet - changes for 2 added fica fields.
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
g_count              integer       :=  0;


g_information_date   stg_absa_customer_cpy.information_date%type;  
g_customer_key       stg_absa_customer_cpy.customer_key%type; 
   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_012U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD fnd_wfs_customer_absa EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select  * from stg_absa_customer_cpy
where 
        (information_date,
         customer_key
        )
in
(select information_date,
        customer_key
from    stg_absa_customer_cpy 
group by information_date,
         customer_key
having count(*) > 1) 
order by information_date,
         customer_key ,sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_customer is
select /*+ FULL(stg)  parallel (stg,2) */  
             cpy.	information_date            	,
             cpy.	customer_key              	,
             cpy.	customer_type_code          	,
             cpy.	population_group_code       	,
             cpy.	nationality_code            	,
             cpy.	title                       	,
             cpy.	initials                    	,
             cpy.	first_name                  	,
             cpy.	customer_name               	,
             cpy.	id_number                   	,
             cpy.	id_type                     	,
             cpy.	country_code_passport_issued	,
             cpy.	date_of_birth               	,
             cpy.	age                         	,
             cpy.	sex_code                    	,
             cpy.	marital_status_code         	,
             cpy.	marriage_contract_type      	,
             cpy.	minor_children_count        	,
             cpy.	no_of_joint_participants    	,
             cpy.	correspondence_language_code	,
             cpy.	home_language_code          	,
             cpy.	cell_phone_number           	,
             cpy.	email_address               	,
             cpy.	fax_dail_code_work          	,
             cpy.	fax_number_work             	,
             cpy.	fax_dial_code_home          	,
             cpy.	fax_number_home             	,
             cpy.	business_dial_code          	,
             cpy.	business_phone_number       	,
             cpy.	residence_code              	,
             cpy.	residential_dial_code       	,
             cpy.	residential_phone_number    	,
             cpy.	date_cif_last_changed       	,
             cpy.	date_cif_opened             	,
             cpy.	date_client_identified      	,
             cpy.	date_client_verified        	,
             cpy.	physical_addr_line_1        	,
             cpy.	physical_addr_line_2        	,
             cpy.	physical_suburb             	,
             cpy.	physical_city               	,
             cpy.	physical_post_code          	,
             cpy.	postal_addr_line_1          	,
             cpy.	postal_addr_line_2          	,
             cpy.	postal_suburb               	,
             cpy.	postal_city                 	,
             cpy.	postal_post_code            	,
             cpy.	employer_addr_line_1        	,
             cpy.	employer_addr_line_2        	,
             cpy.	employer_suburb             	,
             cpy.	employer_city               	,
             cpy.	employer_post_code          	,
             cpy.	employment_sector_code      	,
             cpy.	income_group_code           	,
             cpy.	post_matric_ind             	,
             cpy.	qualification_code          	,
             cpy.	occupation_level            	,
             cpy.	occupational_group_code     	,
             cpy.	occupational_status_code    	,
             cpy.	cont_person_designation_code	,
             cpy.	contact_person              	,
             cpy.	corporate_branch_code       	,
             cpy.	corporate_branch_ind        	,
             cpy.	corporate_division          	,
             cpy.	secondary_card_indicator    	,
             cpy.	number_of_accounts          	,
             cpy.	income_tax_number           	,
             cpy.	sector_code                 	,
             cpy.	sbu_segment_code            	,
             cpy.	sbu_sub_segment_code        	,
             cpy.	cust_sub_class_code         	,
             cpy.	change_number               	,
             cpy.	communication_channel       	,
             cpy.	country_inc_code            	,
             cpy.	dwelling_type_code          	,
             cpy.	exempting_employee_number   	,
             cpy.	exemption_date              	,
             cpy.	exemption_status_ind        	,
             cpy.	exemption_type              	,
             cpy.	funeral_policy_status_code  	,
             cpy.	identifying_attorney_code   	,
             cpy.	identifying_employee_number 	,
             cpy.	notification                	,
             cpy.	sic_code                    	,
             cpy.	signing_instructions_ind    	,
             cpy.	site_cif_changed            	,
             cpy.	site_cif_opened             	,
             cpy.	absa_funeral_policy_ind     	,
             cpy.	absa_marketing_consent_ind  	,
             cpy.	absa_rewards_ind            	,
             cpy.	bad_address_ind             	,
             cpy.	banktel_ind                 	,
             cpy.	cellphone_banking_ind       	,
             cpy.	applied_for_debt_counsel_date 	,
             cpy.	applied_for_debt_counsel_ind  	,
             cpy.	debt_counseling_consent_date  	,
             cpy.	debt_counseling_consent_ind   	,
             cpy.	deceased_estate_ind         	,
             cpy.	court_authority_ind         	,
             cpy.	creditworthiness_consent_ind	,
             cpy.	curatorship_ind             	,
             cpy.	external_life_policy_ind    	,
             cpy.	indirect_liability_ind      	,
             cpy.	insolvent_estate_ind        	,
             cpy.	insolvent_ind               	,
             cpy.	internal_life_policy_ind    	,
             cpy.	internal_short_policy_ind   	,
             cpy.	internet_banking_ind        	,
             cpy.	marketing_email_ind         	,
             cpy.	marketing_mail_ind          	,
             cpy.	marketing_sms_ind           	,
             cpy.	marketing_tele_ind          	,
             cpy.	power_of_attorney_ind       	,
             cpy.	prohibited_ind              	,
             cpy.	security_ind                	,
             cpy.	social_grant_ind            	,
             cpy.	source_of_fund_code         	,
             cpy.	spouse_deceased_ind         	,
             cpy.	sub_sector_code             	,
             cpy.	telephone_banking_ind       	,
             cpy.	teller_cif_changed          	,
             cpy.	temp_res_permit             	,
             cpy.	temp_res_permit_exp_date    	,
             cpy.	unclaimed_funds_ind         	,
             cpy.	vat_registration_number     	,
             cpy.	verifiying_employee_number  	,
             cpy.	voluntary_credit_consolidation,
             cpy.	FICA_BLOCKED_DATE,
             cpy.	FICA_BLOCKED_BY

      from    stg_absa_customer_cpy cpy,
              fnd_wfs_customer_absa fnd
      where   cpy.information_date      = fnd.information_date and             
              cpy.customer_key          = fnd.customer_key     and 
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.information_date,
              cpy.customer_key,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_information_date   := '1 Sep 1900'; 
   g_customer_key       := ' ';
   
for dupp_record in stg_dup
   loop

    if  dupp_record.information_date  = g_information_date and
        dupp_record.customer_key      = g_customer_key     then
        update stg_absa_customer_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_information_date   := dupp_record.information_date; 
    g_customer_key       := dupp_record.customer_key;


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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_customer_absa fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.	information_date            	,
             cpy.	customer_key              	,
             cpy.	customer_type_code          	,
             cpy.	population_group_code       	,
             cpy.	nationality_code            	,
             cpy.	title                       	,
             cpy.	initials                    	,
             cpy.	first_name                  	,
             cpy.	customer_name               	,
             cpy.	id_number                   	,
             cpy.	id_type                     	,
             cpy.	country_code_passport_issued	,
             cpy.	date_of_birth               	,
             cpy.	age                         	,
             cpy.	sex_code                    	,
             cpy.	marital_status_code         	,
             cpy.	marriage_contract_type      	,
             cpy.	minor_children_count        	,
             cpy.	no_of_joint_participants    	,
             cpy.	correspondence_language_code	,
             cpy.	home_language_code          	,
             cpy.	cell_phone_number           	,
             cpy.	email_address               	,
             cpy.	fax_dail_code_work          	,
             cpy.	fax_number_work             	,
             cpy.	fax_dial_code_home          	,
             cpy.	fax_number_home             	,
             cpy.	business_dial_code          	,
             cpy.	business_phone_number       	,
             cpy.	residence_code              	,
             cpy.	residential_dial_code       	,
             cpy.	residential_phone_number    	,
             cpy.	date_cif_last_changed       	,
             cpy.	date_cif_opened             	,
             cpy.	date_client_identified      	,
             cpy.	date_client_verified        	,
             cpy.	physical_addr_line_1        	,
             cpy.	physical_addr_line_2        	,
             cpy.	physical_suburb             	,
             cpy.	physical_city               	,
             cpy.	physical_post_code          	,
             cpy.	postal_addr_line_1          	,
             cpy.	postal_addr_line_2          	,
             cpy.	postal_suburb               	,
             cpy.	postal_city                 	,
             cpy.	postal_post_code            	,
             cpy.	employer_addr_line_1        	,
             cpy.	employer_addr_line_2        	,
             cpy.	employer_suburb             	,
             cpy.	employer_city               	,
             cpy.	employer_post_code          	,
             cpy.	employment_sector_code      	,
             cpy.	income_group_code           	,
             cpy.	post_matric_ind             	,
             cpy.	qualification_code          	,
             cpy.	occupation_level            	,
             cpy.	occupational_group_code     	,
             cpy.	occupational_status_code    	,
             cpy.	cont_person_designation_code	,
             cpy.	contact_person              	,
             cpy.	corporate_branch_code       	,
             cpy.	corporate_branch_ind        	,
             cpy.	corporate_division          	,
             cpy.	secondary_card_indicator    	,
             cpy.	number_of_accounts          	,
             cpy.	income_tax_number           	,
             cpy.	sector_code                 	,
             cpy.	sbu_segment_code            	,
             cpy.	sbu_sub_segment_code        	,
             cpy.	cust_sub_class_code         	,
             cpy.	change_number               	,
             cpy.	communication_channel       	,
             cpy.	country_inc_code            	,
             cpy.	dwelling_type_code          	,
             cpy.	exempting_employee_number   	,
             cpy.	exemption_date              	,
             cpy.	exemption_status_ind        	,
             cpy.	exemption_type              	,
             cpy.	funeral_policy_status_code  	,
             cpy.	identifying_attorney_code   	,
             cpy.	identifying_employee_number 	,
             cpy.	notification                	,
             cpy.	sic_code                    	,
             cpy.	signing_instructions_ind    	,
             cpy.	site_cif_changed            	,
             cpy.	site_cif_opened             	,
             cpy.	absa_funeral_policy_ind     	,
             cpy.	absa_marketing_consent_ind  	,
             cpy.	absa_rewards_ind            	,
             cpy.	bad_address_ind             	,
             cpy.	banktel_ind                 	,
             cpy.	cellphone_banking_ind       	,
             cpy.	applied_for_debt_counsel_date 	,
             cpy.	applied_for_debt_counsel_ind  	,
             cpy.	debt_counseling_consent_date  	,
             cpy.	debt_counseling_consent_ind   	,
             cpy.	deceased_estate_ind         	,
             cpy.	court_authority_ind         	,
             cpy.	creditworthiness_consent_ind	,
             cpy.	curatorship_ind             	,
             cpy.	external_life_policy_ind    	,
             cpy.	indirect_liability_ind      	,
             cpy.	insolvent_estate_ind        	,
             cpy.	insolvent_ind               	,
             cpy.	internal_life_policy_ind    	,
             cpy.	internal_short_policy_ind   	,
             cpy.	internet_banking_ind        	,
             cpy.	marketing_email_ind         	,
             cpy.	marketing_mail_ind          	,
             cpy.	marketing_sms_ind           	,
             cpy.	marketing_tele_ind          	,
             cpy.	power_of_attorney_ind       	,
             cpy.	prohibited_ind              	,
             cpy.	security_ind                	,
             cpy.	social_grant_ind            	,
             cpy.	source_of_fund_code         	,
             cpy.	spouse_deceased_ind         	,
             cpy.	sub_sector_code             	,
             cpy.	telephone_banking_ind       	,
             cpy.	teller_cif_changed          	,
             cpy.	temp_res_permit             	,
             cpy.	temp_res_permit_exp_date    	,
             cpy.	unclaimed_funds_ind         	,
             cpy.	vat_registration_number     	,
             cpy.	verifiying_employee_number  	,
             cpy.	voluntary_credit_consolidation 	,
             g_date as last_updated_date,
             cpy.	FICA_BLOCKED_DATE,
             cpy.	FICA_BLOCKED_BY
      from   stg_absa_customer_cpy cpy,
             dim_calendar cal
      where cpy.information_date   = cal.calendar_date         
      and  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_customer_absa 
       where  information_date    = cpy.information_date and
              customer_key        = cpy.customer_key  )
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


for upd_rec in c_stg_absa_customer
   loop
     update fnd_wfs_customer_absa fnd 
     set    
            fnd.	customer_type_code          	=	upd_rec.	customer_type_code          	,
            fnd.	population_group_code       	=	upd_rec.	population_group_code       	,
            fnd.	nationality_code            	=	upd_rec.	nationality_code            	,
            fnd.	title                       	=	upd_rec.	title                       	,
            fnd.	initials                    	=	upd_rec.	initials                    	,
            fnd.	first_name                  	=	upd_rec.	first_name                  	,
            fnd.	customer_name               	=	upd_rec.	customer_name               	,
            fnd.	id_number                   	=	upd_rec.	id_number                   	,
            fnd.	id_type                     	=	upd_rec.	id_type                     	,
            fnd.	country_code_passport_issued	=	upd_rec.	country_code_passport_issued	,
            fnd.	date_of_birth               	=	upd_rec.	date_of_birth               	,
            fnd.	age                         	=	upd_rec.	age                         	,
            fnd.	sex_code                    	=	upd_rec.	sex_code                    	,
            fnd.	marital_status_code         	=	upd_rec.	marital_status_code         	,
            fnd.	marriage_contract_type      	=	upd_rec.	marriage_contract_type      	,
            fnd.	minor_children_count        	=	upd_rec.	minor_children_count        	,
            fnd.	no_of_joint_participants    	=	upd_rec.	no_of_joint_participants    	,
            fnd.	correspondence_language_code	=	upd_rec.	correspondence_language_code	,
            fnd.	home_language_code          	=	upd_rec.	home_language_code          	,
            fnd.	cell_phone_number           	=	upd_rec.	cell_phone_number           	,
            fnd.	email_address               	=	upd_rec.	email_address               	,
            fnd.	fax_dail_code_work          	=	upd_rec.	fax_dail_code_work          	,
            fnd.	fax_number_work             	=	upd_rec.	fax_number_work             	,
            fnd.	fax_dial_code_home          	=	upd_rec.	fax_dial_code_home          	,
            fnd.	fax_number_home             	=	upd_rec.	fax_number_home             	,
            fnd.	business_dial_code          	=	upd_rec.	business_dial_code          	,
            fnd.	business_phone_number       	=	upd_rec.	business_phone_number       	,
            fnd.	residence_code              	=	upd_rec.	residence_code              	,
            fnd.	residential_dial_code       	=	upd_rec.	residential_dial_code       	,
            fnd.	residential_phone_number    	=	upd_rec.	residential_phone_number    	,
            fnd.	date_cif_last_changed       	=	upd_rec.	date_cif_last_changed       	,
            fnd.	date_cif_opened             	=	upd_rec.	date_cif_opened             	,
            fnd.	date_client_identified      	=	upd_rec.	date_client_identified      	,
            fnd.	date_client_verified        	=	upd_rec.	date_client_verified        	,
            fnd.	physical_addr_line_1        	=	upd_rec.	physical_addr_line_1        	,
            fnd.	physical_addr_line_2        	=	upd_rec.	physical_addr_line_2        	,
            fnd.	physical_suburb             	=	upd_rec.	physical_suburb             	,
            fnd.	physical_city               	=	upd_rec.	physical_city               	,
            fnd.	physical_post_code          	=	upd_rec.	physical_post_code          	,
            fnd.	postal_addr_line_1          	=	upd_rec.	postal_addr_line_1          	,
            fnd.	postal_addr_line_2          	=	upd_rec.	postal_addr_line_2          	,
            fnd.	postal_suburb               	=	upd_rec.	postal_suburb               	,
            fnd.	postal_city                 	=	upd_rec.	postal_city                 	,
            fnd.	postal_post_code            	=	upd_rec.	postal_post_code            	,
            fnd.	employer_addr_line_1        	=	upd_rec.	employer_addr_line_1        	,
            fnd.	employer_addr_line_2        	=	upd_rec.	employer_addr_line_2        	,
            fnd.	employer_suburb             	=	upd_rec.	employer_suburb             	,
            fnd.	employer_city               	=	upd_rec.	employer_city               	,
            fnd.	employer_post_code          	=	upd_rec.	employer_post_code          	,
            fnd.	employment_sector_code      	=	upd_rec.	employment_sector_code      	,
            fnd.	income_group_code           	=	upd_rec.	income_group_code           	,
            fnd.	post_matric_ind             	=	upd_rec.	post_matric_ind             	,
            fnd.	qualification_code          	=	upd_rec.	qualification_code          	,
            fnd.	occupation_level            	=	upd_rec.	occupation_level            	,
            fnd.	occupational_group_code     	=	upd_rec.	occupational_group_code     	,
            fnd.	occupational_status_code    	=	upd_rec.	occupational_status_code    	,
            fnd.	cont_person_designation_code	=	upd_rec.	cont_person_designation_code	,
            fnd.	contact_person              	=	upd_rec.	contact_person              	,
            fnd.	corporate_branch_code       	=	upd_rec.	corporate_branch_code       	,
            fnd.	corporate_branch_ind        	=	upd_rec.	corporate_branch_ind        	,
            fnd.	corporate_division          	=	upd_rec.	corporate_division          	,
            fnd.	secondary_card_indicator    	=	upd_rec.	secondary_card_indicator    	,
            fnd.	number_of_accounts          	=	upd_rec.	number_of_accounts          	,
            fnd.	income_tax_number           	=	upd_rec.	income_tax_number           	,
            fnd.	sector_code                 	=	upd_rec.	sector_code                 	,
            fnd.	sbu_segment_code            	=	upd_rec.	sbu_segment_code            	,
            fnd.	sbu_sub_segment_code        	=	upd_rec.	sbu_sub_segment_code        	,
            fnd.	cust_sub_class_code         	=	upd_rec.	cust_sub_class_code         	,
            fnd.	change_number               	=	upd_rec.	change_number               	,
            fnd.	communication_channel       	=	upd_rec.	communication_channel       	,
            fnd.	country_inc_code            	=	upd_rec.	country_inc_code            	,
            fnd.	dwelling_type_code          	=	upd_rec.	dwelling_type_code          	,
            fnd.	exempting_employee_number   	=	upd_rec.	exempting_employee_number   	,
            fnd.	exemption_date              	=	upd_rec.	exemption_date              	,
            fnd.	exemption_status_ind        	=	upd_rec.	exemption_status_ind        	,
            fnd.	exemption_type              	=	upd_rec.	exemption_type              	,
            fnd.	funeral_policy_status_code  	=	upd_rec.	funeral_policy_status_code  	,
            fnd.	identifying_attorney_code   	=	upd_rec.	identifying_attorney_code   	,
            fnd.	identifying_employee_number 	=	upd_rec.	identifying_employee_number 	,
            fnd.	notification                	=	upd_rec.	notification                	,
            fnd.	sic_code                    	=	upd_rec.	sic_code                    	,
            fnd.	signing_instructions_ind    	=	upd_rec.	signing_instructions_ind    	,
            fnd.	site_cif_changed            	=	upd_rec.	site_cif_changed            	,
            fnd.	site_cif_opened             	=	upd_rec.	site_cif_opened             	,
            fnd.	absa_funeral_policy_ind     	=	upd_rec.	absa_funeral_policy_ind     	,
            fnd.	absa_marketing_consent_ind  	=	upd_rec.	absa_marketing_consent_ind  	,
            fnd.	absa_rewards_ind            	=	upd_rec.	absa_rewards_ind            	,
            fnd.	bad_address_ind             	=	upd_rec.	bad_address_ind             	,
            fnd.	banktel_ind                 	=	upd_rec.	banktel_ind                 	,
            fnd.	cellphone_banking_ind       	=	upd_rec.	cellphone_banking_ind       	,
            fnd.	applied_for_debt_counsel_date 	=	upd_rec.	applied_for_debt_counsel_date 	,
            fnd.	applied_for_debt_counsel_ind  	=	upd_rec.	applied_for_debt_counsel_ind  	,
            fnd.	debt_counseling_consent_date  	=	upd_rec.	debt_counseling_consent_date  	,
            fnd.	debt_counseling_consent_ind   	=	upd_rec.	debt_counseling_consent_ind   	,
            fnd.	deceased_estate_ind         	=	upd_rec.	deceased_estate_ind         	,
            fnd.	court_authority_ind         	=	upd_rec.	court_authority_ind         	,
            fnd.	creditworthiness_consent_ind	=	upd_rec.	creditworthiness_consent_ind	,
            fnd.	curatorship_ind             	=	upd_rec.	curatorship_ind             	,
            fnd.	external_life_policy_ind    	=	upd_rec.	external_life_policy_ind    	,
            fnd.	indirect_liability_ind      	=	upd_rec.	indirect_liability_ind      	,
            fnd.	insolvent_estate_ind        	=	upd_rec.	insolvent_estate_ind        	,
            fnd.	insolvent_ind               	=	upd_rec.	insolvent_ind               	,
            fnd.	internal_life_policy_ind    	=	upd_rec.	internal_life_policy_ind    	,
            fnd.	internal_short_policy_ind   	=	upd_rec.	internal_short_policy_ind   	,
            fnd.	internet_banking_ind        	=	upd_rec.	internet_banking_ind        	,
            fnd.	marketing_email_ind         	=	upd_rec.	marketing_email_ind         	,
            fnd.	marketing_mail_ind          	=	upd_rec.	marketing_mail_ind          	,
            fnd.	marketing_sms_ind           	=	upd_rec.	marketing_sms_ind           	,
            fnd.	marketing_tele_ind          	=	upd_rec.	marketing_tele_ind          	,
            fnd.	power_of_attorney_ind       	=	upd_rec.	power_of_attorney_ind       	,
            fnd.	prohibited_ind              	=	upd_rec.	prohibited_ind              	,
            fnd.	security_ind                	=	upd_rec.	security_ind                	,
            fnd.	social_grant_ind            	=	upd_rec.	social_grant_ind            	,
            fnd.	source_of_fund_code         	=	upd_rec.	source_of_fund_code         	,
            fnd.	spouse_deceased_ind         	=	upd_rec.	spouse_deceased_ind         	,
            fnd.	sub_sector_code             	=	upd_rec.	sub_sector_code             	,
            fnd.	telephone_banking_ind       	=	upd_rec.	telephone_banking_ind       	,
            fnd.	teller_cif_changed          	=	upd_rec.	teller_cif_changed          	,
            fnd.	temp_res_permit             	=	upd_rec.	temp_res_permit             	,
            fnd.	temp_res_permit_exp_date    	=	upd_rec.	temp_res_permit_exp_date    	,
            fnd.	unclaimed_funds_ind         	=	upd_rec.	unclaimed_funds_ind         	,
            fnd.	vat_registration_number     	=	upd_rec.	vat_registration_number     	,
            fnd.	verifiying_employee_number  	=	upd_rec.	verifiying_employee_number  	,
            fnd.	voluntary_credit_consolidation 	=	upd_rec.	voluntary_credit_consolidation 	,
            fnd.	FICA_BLOCKED_DATE           	=	upd_rec.	FICA_BLOCKED_DATE	,
            fnd.	FICA_BLOCKED_BY 	            =	upd_rec.	FICA_BLOCKED_BY 	,
            fnd.  last_updated_date = g_date
     where  fnd.	information_date  =	upd_rec.	information_date and
            fnd.	customer_key	    =	upd_rec.	customer_key	and
            ( 
            fnd.	customer_type_code          	<>	upd_rec.	customer_type_code          	or
            fnd.	population_group_code       	<>	upd_rec.	population_group_code       	or
            fnd.	nationality_code            	<>	upd_rec.	nationality_code            	or
            fnd.	title                       	<>	upd_rec.	title                       	or
            fnd.	initials                    	<>	upd_rec.	initials                    	or
            fnd.	first_name                  	<>	upd_rec.	first_name                  	or
            fnd.	customer_name               	<>	upd_rec.	customer_name               	or
            fnd.	id_number                   	<>	upd_rec.	id_number                   	or
            fnd.	id_type                     	<>	upd_rec.	id_type                     	or
            fnd.	country_code_passport_issued	<>	upd_rec.	country_code_passport_issued	or
            fnd.	date_of_birth               	<>	upd_rec.	date_of_birth               	or
            fnd.	age                         	<>	upd_rec.	age                         	or
            fnd.	sex_code                    	<>	upd_rec.	sex_code                    	or
            fnd.	marital_status_code         	<>	upd_rec.	marital_status_code         	or
            fnd.	marriage_contract_type      	<>	upd_rec.	marriage_contract_type      	or
            fnd.	minor_children_count        	<>	upd_rec.	minor_children_count        	or
            fnd.	no_of_joint_participants    	<>	upd_rec.	no_of_joint_participants    	or
            fnd.	correspondence_language_code	<>	upd_rec.	correspondence_language_code	or
            fnd.	home_language_code          	<>	upd_rec.	home_language_code          	or
            fnd.	cell_phone_number           	<>	upd_rec.	cell_phone_number           	or
            fnd.	email_address               	<>	upd_rec.	email_address               	or
            fnd.	fax_dail_code_work          	<>	upd_rec.	fax_dail_code_work          	or
            fnd.	fax_number_work             	<>	upd_rec.	fax_number_work             	or
            fnd.	fax_dial_code_home          	<>	upd_rec.	fax_dial_code_home          	or
            fnd.	fax_number_home             	<>	upd_rec.	fax_number_home             	or
            fnd.	business_dial_code          	<>	upd_rec.	business_dial_code          	or
            fnd.	business_phone_number       	<>	upd_rec.	business_phone_number       	or
            fnd.	residence_code              	<>	upd_rec.	residence_code              	or
            fnd.	residential_dial_code       	<>	upd_rec.	residential_dial_code       	or
            fnd.	residential_phone_number    	<>	upd_rec.	residential_phone_number    	or
            fnd.	date_cif_last_changed       	<>	upd_rec.	date_cif_last_changed       	or
            fnd.	date_cif_opened             	<>	upd_rec.	date_cif_opened             	or
            fnd.	date_client_identified      	<>	upd_rec.	date_client_identified      	or
            fnd.	date_client_verified        	<>	upd_rec.	date_client_verified        	or
            fnd.	physical_addr_line_1        	<>	upd_rec.	physical_addr_line_1        	or
            fnd.	physical_addr_line_2        	<>	upd_rec.	physical_addr_line_2        	or
            fnd.	physical_suburb             	<>	upd_rec.	physical_suburb             	or
            fnd.	physical_city               	<>	upd_rec.	physical_city               	or
            fnd.	physical_post_code          	<>	upd_rec.	physical_post_code          	or
            fnd.	postal_addr_line_1          	<>	upd_rec.	postal_addr_line_1          	or
            fnd.	postal_addr_line_2          	<>	upd_rec.	postal_addr_line_2          	or
            fnd.	postal_suburb               	<>	upd_rec.	postal_suburb               	or
            fnd.	postal_city                 	<>	upd_rec.	postal_city                 	or
            fnd.	postal_post_code            	<>	upd_rec.	postal_post_code            	or
            fnd.	employer_addr_line_1        	<>	upd_rec.	employer_addr_line_1        	or
            fnd.	employer_addr_line_2        	<>	upd_rec.	employer_addr_line_2        	or
            fnd.	employer_suburb             	<>	upd_rec.	employer_suburb             	or
            fnd.	employer_city               	<>	upd_rec.	employer_city               	or
            fnd.	employer_post_code          	<>	upd_rec.	employer_post_code          	or
            fnd.	employment_sector_code      	<>	upd_rec.	employment_sector_code      	or
            fnd.	income_group_code           	<>	upd_rec.	income_group_code           	or
            fnd.	post_matric_ind             	<>	upd_rec.	post_matric_ind             	or
            fnd.	qualification_code          	<>	upd_rec.	qualification_code          	or
            fnd.	occupation_level            	<>	upd_rec.	occupation_level            	or
            fnd.	occupational_group_code     	<>	upd_rec.	occupational_group_code     	or
            fnd.	occupational_status_code    	<>	upd_rec.	occupational_status_code    	or
            fnd.	cont_person_designation_code	<>	upd_rec.	cont_person_designation_code	or
            fnd.	contact_person              	<>	upd_rec.	contact_person              	or
            fnd.	corporate_branch_code       	<>	upd_rec.	corporate_branch_code       	or
            fnd.	corporate_branch_ind        	<>	upd_rec.	corporate_branch_ind        	or
            fnd.	corporate_division          	<>	upd_rec.	corporate_division          	or
            fnd.	secondary_card_indicator    	<>	upd_rec.	secondary_card_indicator    	or
            fnd.	number_of_accounts          	<>	upd_rec.	number_of_accounts          	or
            fnd.	income_tax_number           	<>	upd_rec.	income_tax_number           	or
            fnd.	sector_code                 	<>	upd_rec.	sector_code                 	or
            fnd.	sbu_segment_code            	<>	upd_rec.	sbu_segment_code            	or
            fnd.	sbu_sub_segment_code        	<>	upd_rec.	sbu_sub_segment_code        	or
            fnd.	cust_sub_class_code         	<>	upd_rec.	cust_sub_class_code         	or
            fnd.	change_number               	<>	upd_rec.	change_number               	or
            fnd.	communication_channel       	<>	upd_rec.	communication_channel       	or
            fnd.	country_inc_code            	<>	upd_rec.	country_inc_code            	or
            fnd.	dwelling_type_code          	<>	upd_rec.	dwelling_type_code          	or
            fnd.	exempting_employee_number   	<>	upd_rec.	exempting_employee_number   	or
            fnd.	exemption_date              	<>	upd_rec.	exemption_date              	or
            fnd.	exemption_status_ind        	<>	upd_rec.	exemption_status_ind        	or
            fnd.	exemption_type              	<>	upd_rec.	exemption_type              	or
            fnd.	funeral_policy_status_code  	<>	upd_rec.	funeral_policy_status_code  	or
            fnd.	identifying_attorney_code   	<>	upd_rec.	identifying_attorney_code   	or
            fnd.	identifying_employee_number 	<>	upd_rec.	identifying_employee_number 	or
            fnd.	notification                	<>	upd_rec.	notification                	or
            fnd.	sic_code                    	<>	upd_rec.	sic_code                    	or
            fnd.	signing_instructions_ind    	<>	upd_rec.	signing_instructions_ind    	or
            fnd.	site_cif_changed            	<>	upd_rec.	site_cif_changed            	or
            fnd.	site_cif_opened             	<>	upd_rec.	site_cif_opened             	or
            fnd.	absa_funeral_policy_ind     	<>	upd_rec.	absa_funeral_policy_ind     	or
            fnd.	absa_marketing_consent_ind  	<>	upd_rec.	absa_marketing_consent_ind  	or
            fnd.	absa_rewards_ind            	<>	upd_rec.	absa_rewards_ind            	or
            fnd.	bad_address_ind             	<>	upd_rec.	bad_address_ind             	or
            fnd.	banktel_ind                 	<>	upd_rec.	banktel_ind                 	or
            fnd.	cellphone_banking_ind       	<>	upd_rec.	cellphone_banking_ind       	or
            fnd.	applied_for_debt_counsel_date 	<>	upd_rec.	applied_for_debt_counsel_date 	or
            fnd.	applied_for_debt_counsel_ind  	<>	upd_rec.	applied_for_debt_counsel_ind  	or
            fnd.	debt_counseling_consent_date  	<>	upd_rec.	debt_counseling_consent_date  	or
            fnd.	debt_counseling_consent_ind   	<>	upd_rec.	debt_counseling_consent_ind   	or
            fnd.	deceased_estate_ind         	<>	upd_rec.	deceased_estate_ind         	or
            fnd.	court_authority_ind         	<>	upd_rec.	court_authority_ind         	or
            fnd.	creditworthiness_consent_ind	<>	upd_rec.	creditworthiness_consent_ind	or
            fnd.	curatorship_ind             	<>	upd_rec.	curatorship_ind             	or
            fnd.	external_life_policy_ind    	<>	upd_rec.	external_life_policy_ind    	or
            fnd.	indirect_liability_ind      	<>	upd_rec.	indirect_liability_ind      	or
            fnd.	insolvent_estate_ind        	<>	upd_rec.	insolvent_estate_ind        	or
            fnd.	insolvent_ind               	<>	upd_rec.	insolvent_ind               	or
            fnd.	internal_life_policy_ind    	<>	upd_rec.	internal_life_policy_ind    	or
            fnd.	internal_short_policy_ind   	<>	upd_rec.	internal_short_policy_ind   	or
            fnd.	internet_banking_ind        	<>	upd_rec.	internet_banking_ind        	or
            fnd.	marketing_email_ind         	<>	upd_rec.	marketing_email_ind         	or
            fnd.	marketing_mail_ind          	<>	upd_rec.	marketing_mail_ind          	or
            fnd.	marketing_sms_ind           	<>	upd_rec.	marketing_sms_ind           	or
            fnd.	marketing_tele_ind          	<>	upd_rec.	marketing_tele_ind          	or
            fnd.	power_of_attorney_ind       	<>	upd_rec.	power_of_attorney_ind       	or
            fnd.	prohibited_ind              	<>	upd_rec.	prohibited_ind              	or
            fnd.	security_ind                	<>	upd_rec.	security_ind                	or
            fnd.	social_grant_ind            	<>	upd_rec.	social_grant_ind            	or
            fnd.	source_of_fund_code         	<>	upd_rec.	source_of_fund_code         	or
            fnd.	spouse_deceased_ind         	<>	upd_rec.	spouse_deceased_ind         	or
            fnd.	sub_sector_code             	<>	upd_rec.	sub_sector_code             	or
            fnd.	telephone_banking_ind       	<>	upd_rec.	telephone_banking_ind       	or
            fnd.	teller_cif_changed          	<>	upd_rec.	teller_cif_changed          	or
            fnd.	temp_res_permit             	<>	upd_rec.	temp_res_permit             	or
            fnd.	temp_res_permit_exp_date    	<>	upd_rec.	temp_res_permit_exp_date    	or
            fnd.	unclaimed_funds_ind         	<>	upd_rec.	unclaimed_funds_ind         	or
            fnd.	vat_registration_number     	<>	upd_rec.	vat_registration_number     	or
            fnd.	verifiying_employee_number  	<>	upd_rec.	verifiying_employee_number  	or
            fnd.	voluntary_credit_consolidation 	<>	upd_rec.	voluntary_credit_consolidation or
            fnd.	FICA_BLOCKED_DATE           	<>	upd_rec.	FICA_BLOCKED_DATE	or
            fnd.	FICA_BLOCKED_BY 	            <>	upd_rec.	FICA_BLOCKED_BY  

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_customer_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	information_date            	,
             cpy.	customer_key              	,
             cpy.	customer_type_code          	,
             cpy.	population_group_code       	,
             cpy.	nationality_code            	,
             cpy.	title                       	,
             cpy.	initials                    	,
             cpy.	first_name                  	,
             cpy.	customer_name               	,
             cpy.	id_number                   	,
             cpy.	id_type                     	,
             cpy.	country_code_passport_issued	,
             cpy.	date_of_birth               	,
             cpy.	age                         	,
             cpy.	sex_code                    	,
             cpy.	marital_status_code         	,
             cpy.	marriage_contract_type      	,
             cpy.	minor_children_count        	,
             cpy.	no_of_joint_participants    	,
             cpy.	correspondence_language_code	,
             cpy.	home_language_code          	,
             cpy.	cell_phone_number           	,
             cpy.	email_address               	,
             cpy.	fax_dail_code_work          	,
             cpy.	fax_number_work             	,
             cpy.	fax_dial_code_home          	,
             cpy.	fax_number_home             	,
             cpy.	business_dial_code          	,
             cpy.	business_phone_number       	,
             cpy.	residence_code              	,
             cpy.	residential_dial_code       	,
             cpy.	residential_phone_number    	,
             cpy.	date_cif_last_changed       	,
             cpy.	date_cif_opened             	,
             cpy.	date_client_identified      	,
             cpy.	date_client_verified        	,
             cpy.	physical_addr_line_1        	,
             cpy.	physical_addr_line_2        	,
             cpy.	physical_suburb             	,
             cpy.	physical_city               	,
             cpy.	physical_post_code          	,
             cpy.	postal_addr_line_1          	,
             cpy.	postal_addr_line_2          	,
             cpy.	postal_suburb               	,
             cpy.	postal_city                 	,
             cpy.	postal_post_code            	,
             cpy.	employer_addr_line_1        	,
             cpy.	employer_addr_line_2        	,
             cpy.	employer_suburb             	,
             cpy.	employer_city               	,
             cpy.	employer_post_code          	,
             cpy.	employment_sector_code      	,
             cpy.	income_group_code           	,
             cpy.	post_matric_ind             	,
             cpy.	qualification_code          	,
             cpy.	occupation_level            	,
             cpy.	occupational_group_code     	,
             cpy.	occupational_status_code    	,
             cpy.	cont_person_designation_code	,
             cpy.	contact_person              	,
             cpy.	corporate_branch_code       	,
             cpy.	corporate_branch_ind        	,
             cpy.	corporate_division          	,
             cpy.	secondary_card_indicator    	,
             cpy.	number_of_accounts          	,
             cpy.	income_tax_number           	,
             cpy.	sector_code                 	,
             cpy.	sbu_segment_code            	,
             cpy.	sbu_sub_segment_code        	,
             cpy.	cust_sub_class_code         	,
             cpy.	change_number               	,
             cpy.	communication_channel       	,
             cpy.	country_inc_code            	,
             cpy.	dwelling_type_code          	,
             cpy.	exempting_employee_number   	,
             cpy.	exemption_date              	,
             cpy.	exemption_status_ind        	,
             cpy.	exemption_type              	,
             cpy.	funeral_policy_status_code  	,
             cpy.	identifying_attorney_code   	,
             cpy.	identifying_employee_number 	,
             cpy.	notification                	,
             cpy.	sic_code                    	,
             cpy.	signing_instructions_ind    	,
             cpy.	site_cif_changed            	,
             cpy.	site_cif_opened             	,
             cpy.	absa_funeral_policy_ind     	,
             cpy.	absa_marketing_consent_ind  	,
             cpy.	absa_rewards_ind            	,
             cpy.	bad_address_ind             	,
             cpy.	banktel_ind                 	,
             cpy.	cellphone_banking_ind       	,
             cpy.	applied_for_debt_counsel_date 	,
             cpy.	applied_for_debt_counsel_ind  	,
             cpy.	debt_counseling_consent_date  	,
             cpy.	debt_counseling_consent_ind   	,
             cpy.	deceased_estate_ind         	,
             cpy.	court_authority_ind         	,
             cpy.	creditworthiness_consent_ind	,
             cpy.	curatorship_ind             	,
             cpy.	external_life_policy_ind    	,
             cpy.	indirect_liability_ind      	,
             cpy.	insolvent_estate_ind        	,
             cpy.	insolvent_ind               	,
             cpy.	internal_life_policy_ind    	,
             cpy.	internal_short_policy_ind   	,
             cpy.	internet_banking_ind        	,
             cpy.	marketing_email_ind         	,
             cpy.	marketing_mail_ind          	,
             cpy.	marketing_sms_ind           	,
             cpy.	marketing_tele_ind          	,
             cpy.	power_of_attorney_ind       	,
             cpy.	prohibited_ind              	,
             cpy.	security_ind                	,
             cpy.	social_grant_ind            	,
             cpy.	source_of_fund_code         	,
             cpy.	spouse_deceased_ind         	,
             cpy.	sub_sector_code             	,
             cpy.	telephone_banking_ind       	,
             cpy.	teller_cif_changed          	,
             cpy.	temp_res_permit             	,
             cpy.	temp_res_permit_exp_date    	,
             cpy.	unclaimed_funds_ind         	,
             cpy.	vat_registration_number     	,
             cpy.	verifiying_employee_number  	,
             cpy.	voluntary_credit_consolidation,
             cpy.	FICA_BLOCKED_DATE,
             cpy.	FICA_BLOCKED_BY

      from   stg_absa_customer_cpy cpy
      where  
      (    
      not exists 
        (select * from  dim_calendar cal
         where  cpy.information_date  = cal.calendar_date )  
      ) 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
      and sys_process_code = 'N';
         

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
    
    
    select count(*)
    into   g_recs_read
    from   stg_absa_customer_cpy
    where  sys_process_code = 'N';
    
    if g_recs_read > 300000 then
       l_text := 'TRUNCATE ABSA MASTER STARTED AT '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       execute immediate 'truncate table dwh_wfs_foundation.fnd_wfs_customer_absa';
    end if;
 
    
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;
    
    select count(*)
    into   g_recs_read
    from   stg_absa_customer_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;
    
    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_customer_cpy
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
       raise;
end wh_fnd_wfs_012u;
