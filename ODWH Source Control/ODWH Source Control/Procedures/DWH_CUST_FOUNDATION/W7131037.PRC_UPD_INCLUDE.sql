-- ****** Object: Procedure W7131037.PRC_UPD_INCLUDE Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."PRC_UPD_INCLUDE" (vROWID VARCHAR2, vVALUE VARCHAR2) AS
PRAGMA autonomous_transaction;

  O_error_message  VARCHAR2(1000) := NULL;
  L_function VARCHAR2(255) := '.PRC_UPD_INCLUDE';

BEGIN

EXECUTE IMMEDIATE 'UPDATE W7131037.'||vVALUE||'
                     SET INCLUDE = DECODE(INCLUDE, ''Y'', ''N'', ''Y'')
                     WHERE rowid = '''||vROWID||'''
                     AND LOWER(CREATED_BY) = LOWER('''||v('user')||''') ';

  COMMIT;

  EXCEPTION
  WHEN OTHERS THEN
--  O_error_message := 'Exception while saving INCLUDE for APEX_AP3_PARAM_1 ' ||  TO_CHAR(SQLCODE) || SQLERRM || L_function;
   O_error_message := '.
                      Warning!!!  An error has occured. If you continue your changes may not be saved!
                      If error occurs again please contact IT Support
                      .';
   INSERT INTO W7131037.AP3_LOG VALUES (O_error_message);
   raise_application_error( -20001, O_error_message );


END "PRC_UPD_INCLUDE";
