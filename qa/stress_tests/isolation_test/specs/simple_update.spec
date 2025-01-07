
setup
{
 DROP TABLE IF EXISTS t1;
 CREATE TABLE IF NOT EXISTS t1 (i int, name string);
 insert into t1 values(1, '1111');
 insert into t1 values(2, '2222');
 insert into t1 values(3, '3333');
 insert into t1 values(4, '4444');
}

teardown
{
 DROP TABLE t1;
}

session ss1
step s1 	{ begin transaction ; }
step u1	    { update t1 set name = '1122' where i=1 ; }
step c1	    { COMMIT; }


session ss2
step s2		{ BEGIN transaction; }
step u2 	{ update t1 set name = '1133' where i=1 ; }
step c2	    { COMMIT; }

#permutation s1 u1 c1 s2 u2 c2
permutation s1 s2 u1 u2 c1 c2