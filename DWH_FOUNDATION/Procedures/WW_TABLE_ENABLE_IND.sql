--------------------------------------------------------
--  DDL for Procedure WW_TABLE_ENABLE_IND
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WW_TABLE_ENABLE_IND" (p_table_name varchar2
)
AS
   l_msg   VARCHAR2 (255);
BEGIN
   -- BUILD NON PARTITIONED INDEXES
   FOR r
   IN (SELECT    'alter index '
              || owner
              || '.'
              || index_name
              || ' rebuild nologging parallel (degree 6)'
                 todo
       FROM all_indexes
       WHERE table_name = p_table_name AND partitioned = 'NO')
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   -- build NON PARTITIONED INDEXES
   FOR r
   IN (SELECT    'alter index '
              || owner
              || '.'
              || index_name
              || ' logging noparallel '
                 todo
       FROM all_indexes
       WHERE table_name = p_table_name AND partitioned = 'NO')
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   -- DO THE PARTITIONED INDEXES setting nologging
   FOR r
   IN (SELECT    'alter index '
              || index_owner
              || '.'
              || index_name
              || ' modify PARTITION '
              || partition_name
              || ' nologging'
                 todo
       FROM all_ind_partitions
       WHERE index_name IN (SELECT index_name
                            FROM all_part_indexes
                            WHERE table_name = p_table_name))
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   -- DO THE PARTITIONED INDEXES
   FOR r
   IN (SELECT    'alter index '
              || index_owner
              || '.'
              || index_name
              || ' rebuild subPARTITION '
              || subpartition_name
              || ' parallel (degree  2)'
                 todo
       FROM all_ind_subpartitions
       WHERE index_name IN (SELECT index_name
                            FROM all_part_indexes
                            WHERE table_name = p_table_name))
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   -- DO THE PARTITIONED INDEXES setting logging
   FOR r
   IN (SELECT    'alter index '
              || index_owner
              || '.'
              || index_name
              || ' modify PARTITION '
              || partition_name
              || ' logging '
                 todo
       FROM all_ind_partitions
       WHERE index_name IN (SELECT index_name
                            FROM all_part_indexes
                            WHERE table_name = p_table_name))
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;

   FOR r
   IN (SELECT    'alter table '
              || owner
              || '.'
              || table_name
              || ' enable constraint '
              || constraint_name
                 todo
       FROM all_constraints
       WHERE     constraint_type IN ('P', 'U')
             AND table_name = p_table_name
             AND status <> 'ENABLED')
   LOOP
      DBMS_OUTPUT.put_line ('DOING:' || SUBSTR (r.todo, 1, 200));

      EXECUTE IMMEDIATE r.todo;
   END LOOP;
   
END;
