# test phantom reads

setup
{
  CREATE TABLE IF NOT EXISTS foo (
	key		int PRIMARY KEY,
	value	int
  );

  INSERT INTO foo VALUES (1, 1),(2, 2),(3, 3),(4, 4);
}

teardown
{
  DROP TABLE foo;
}

session s1
step s1		{ BEGIN transaction isolation level read committed; }
step s1r	{ SELECT * FROM foo where key > 2; }
step s1c	{ COMMIT; }

session s2
step s2		{ BEGIN transaction isolation level read committed; }
step s2w	{ INSERT INTO foo VALUES(5, 5); }
step s2d  { DELETE FROM foo where key = 4; }
step s2c	{ COMMIT; }

permutation s1 s2 s1r s2w s2c s1r s1c
permutation s1 s2 s1r s2d s2c s1r s1c
permutation s1 s2 s1r s2w s2d s2c s1r s1c
permutation s1 s2 s1r s2d s2w s2c s1r s1c



