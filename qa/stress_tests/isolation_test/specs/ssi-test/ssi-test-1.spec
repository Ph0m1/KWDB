# select behavior (without FOR UPDATE)
setup
{
 CREATE TABLE IF NOT EXISTS kv(k int PRIMARY key, v int);
 delete from kv;
 INSERT INTO kv VALUES (1,5);
}

teardown
{
 DROP TABLE kv;
}

session s1
step s1   { BEGIN transaction isolation level SERIALIZABLE; }
step s1r1 { SELECT * FROM kv; }
step s1r2 { SELECT * FROM kv; }
step s1w1 { INSERT INTO kv VALUES(3,7); }
step s1r3 { SELECT * FROM kv; }
step s1r4 { SELECT * FROM kv; }
step s1c  { COMMIT; }

session s2
step s2   { BEGIN transaction isolation level SERIALIZABLE; }
step s2w1 { INSERT INTO kv VALUES(2,6); }
step s2c  { COMMIT; }

permutation s1 s2 s1r1 s2w1 s1r2 s1w1 s1r3 s2c s1r4 s1c