# INSERT behavior
setup
{
 CREATE TABLE IF NOT EXISTS kv(k int PRIMARY key, v int);
 delete from kv;
 insert into kv values (1, 1);
}

teardown
{
 DROP TABLE kv;
}

session s1
step s1   { BEGIN transaction isolation level read committed; }
step s1w1 { insert into kv values (1, 1) on conflict (k) do update set v = 100; }
step s1r1 { select * from kv; }
step s1c  { commit; }

session s2
step s2   { BEGIN transaction isolation level SERIALIZABLE; }
step s2u1 { update kv set k = 2 where k = 1; }
step s2c  { COMMIT; }

permutation s1 s2 s2u1 s1w1 s2c s1r1 s1c
