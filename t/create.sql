CREATE DATABASE "test.gdb" user "SYSDBA" password "masterkey";

CREATE TABLE SIMPLE (person_id integer not null, 
person VARCHAR(50),
comment VARCHAR(200),
mtime DATE,
PRIMARY KEY (person_id));
COMMIT;

CREATE GENERATOR gen_person_id;
COMMIT;
quit;
