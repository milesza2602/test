--------------------------------------------------------
--  DDL for Procedure WW_TABLE_DISABLE_CONS_IND
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WW_TABLE_DISABLE_CONS_IND" (p_table_name VARCHAR2
)
AS
   l_msg   VARCHAR2 (255);
BEGIN
   -- Disable constraints
   FOR r
   IN (SELECT    'alter table '
              || owner
              || '.'
              || table_name
              || ' disable constraint '
              || constraint_name
                 todo
       FROM user_constraints
       WHERE constraint_type IN ('P', 'U') AND table_name = p_table_name)
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   -- DO THE NON PARTITIONED INDEXES
   FOR r
   IN (SELECT 'alter index ' || table_owner || '.' || index_name || ' unusable'
                 todo
       FROM user_indexes
       WHERE table_name = p_table_name AND partitioned = 'NO')
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   -- DO THE PARTITIONED INDEXES
   FOR r
   IN (SELECT    'alter index '
              || index_name
              || ' MODIFY PARTITION '
              || partition_name
              || ' UNUSABLE'
                 todo
       FROM user_ind_partitions
       WHERE index_name IN (SELECT index_name
                            FROM user_part_indexes
                            WHERE table_name = p_table_name))
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;
END;
