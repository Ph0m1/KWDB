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
step s1r1		{ SELECT * FROM foo FOR KEY SHARE; }
step s1s		{ SAVEPOINT f; }
step s1r2		{ SELECT * FROM foo FOR NO KEY UPDATE; }
step s1rs		{ ROLLBACK TO f; }
step s1c		{ COMMIT; }

session s2
step s2			{ BEGIN transaction isolation level read committed; }
step s2r1		{ SELECT * FROM foo FOR UPDATE; }
step s2r2		{ SELECT * FROM foo FOR NO KEY UPDATE; }
step s2c		{ COMMIT; }

permutation s1 s2 s1r1 s1s s1r2 s1rs s2r1 s1c s2c
permutation s1 s2 s1r1 s1s s1r2 s2r1 s1rs s1c s2c
permutation s1 s2 s1r1 s1s s1r2 s1rs s2r2 s1c s2c
permutation s1 s2 s1r1 s1s s1r2 s2r2 s1rs s1c s2c