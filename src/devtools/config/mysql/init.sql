CREATE TABLE IF NOT EXISTS test.test_table
(
    i int primary key ,
    j int
) engine=monograph;
INSERT INTO test.test_table VALUES (11, 22);
INSERT INTO test.test_table VALUES (33, 44);
INSERT INTO test.test_table VALUES (55, 66);