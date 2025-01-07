setup
{
  CREATE TABLE foo (
     key INT PRIMARY KEY,
     value INT
  );

  INSERT INTO foo VALUES (1, 1);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1			{ BEGIN transaction isolation level read committed; }
step s1l		{ SELECT * FROM foo; }
step s1svp		{ SAVEPOINT f; }
step s1d		{ DELETE FROM foo WHERE TRUE; }
step s1r		{ ROLLBACK TO f; }
step s1c		{ COMMIT; }

session s2
step s2			{ BEGIN transaction isolation level read committed; }
step s2l		{ SELECT * FROM foo FOR UPDATE; }
step s2c		{ COMMIT; }

permutation s1 s2 s1l s1svp s1d s1r s1c s2l s2c
permutation s1 s2 s1l s1svp s1d s1r s2l s1c s2c
permutation s1 s2 s1l s1svp s1d s2l s1r s1c s2c
permutation s1 s2 s2l s1l s2c s1svp s1d s1r s1c
permutation s1 s2 s2l s2c s1l s1svp s1d s1r s1c
