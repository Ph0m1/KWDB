# savepoint behavior
setup
{
 CREATE TABLE accounts (
     id int PRIMARY KEY,
     balance INT NOT NULL
 );
 INSERT INTO accounts (id,balance) VALUES (1,100), (2,200);
}

teardown
{
 DROP TABLE accounts;
}

session s1
step s1   { BEGIN transaction isolation level read committed; }
step s1q1 { SELECT * FROM accounts; }
step s1p1 { SAVEPOINT s1; }
step s1u1 { UPDATE accounts SET balance = balance - 50 WHERE id = 1; }
step s1r1 { ROLLBACK TO SAVEPOINT s1; }
step s1p2 { SAVEPOINT s2; }
step s1u2 { UPDATE accounts SET balance = balance + 100 WHERE id = 2; }
step s1r2 { ROLLBACK TO SAVEPOINT s2; }
step s1c  { COMMIT; }

session s2
step s2   { BEGIN transaction isolation level read committed; }
step s2q1 { SELECT * FROM accounts; }
step s2u1 { UPDATE accounts SET balance = balance + 50 WHERE id = 1; }
step s2u2 { UPDATE accounts SET balance = balance - 100 WHERE id = 2; }
step s2c  { COMMIT; }

permutation s1 s2 s1q1 s1p1 s1u1 s1p2 s2q1 s2u1 s1r1 s1q1 s1c s2c
permutation s1 s2 s1q1 s1p1 s1u1 s1p2 s2q1 s2u1 s1r1 s1q1 s1u2 s1q1 s1r2 s1q1 s1c s2q1 s2u2 s2c