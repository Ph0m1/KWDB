# test lost update
setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );

  INSERT INTO foo VALUES (1000, 1000);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level SERIALIZABLE; }
step s1r	{ SELECT * FROM foo; }
step s1u  { UPDATE foo SET key=key-200; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level SERIALIZABLE; }
step s2r  { SELECT * FROM foo; }
step s2u	{ UPDATE foo SET key=key+100; }
step s2c	{ ROLLBACK; }

permutation s1 s2 s1u s2u s1c s2c s1r s2r


