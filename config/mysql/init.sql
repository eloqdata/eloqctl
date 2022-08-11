CREATE TABLE IF NOT EXISTS t1
(
    i int primary key,
    j int
) engine=monograph;
INSERT INTO t1 VALUES (1, 2);
INSERT INTO t1 VALUES (3, 4);
INSERT INTO t1 VALUES (5, 6);