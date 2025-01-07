# test dirty write

setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );

  INSERT INTO foo VALUES (1, 1);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level SERIALIZABLE; }
step s1u	{ UPDATE foo SET key = 2; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level SERIALIZABLE; }
step s2u	{ UPDATE foo SET key = 3; }
step s2r  { select * from foo; }
step s2c	{ ROLLBACK; }

permutation s1 s2 s1u s2u s1c s2r s2c s2r
    

