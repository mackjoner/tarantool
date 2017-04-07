#!./tcltestrunner.lua

# 2013 March 13
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library. 
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix tkt-4dd95f6943

do_execsql_test 1.0 {
  CREATE TABLE t1(id primary key, x);
  INSERT INTO t1 VALUES (1, 3), (2, 4), (3, 2), (4, 1), (5, 5), (6, 6);
}

foreach {tn1 idx} {
  1 { CREATE INDEX i1 ON t1(x ASC) }
  2 { CREATE INDEX i1 ON t1(x DESC) }
} {
  do_execsql_test 1.$tn1.1 { DROP INDEX IF EXISTS 'i1'; }
  do_execsql_test 1.$tn1.2 $idx

  do_execsql_test 1.$tn1.3 {
    SELECT x FROM t1 WHERE x IN(2, 4, 5) ORDER BY x ASC;
  } {2 4 5}

  do_execsql_test 1.$tn1.4 {
    SELECT x FROM t1 WHERE x IN(2, 4, 5) ORDER BY x DESC;
  } {5 4 2}
}


do_execsql_test 2.0 {
  CREATE TABLE t2(id primary key, x, y);
  INSERT INTO t2 VALUES (1, 5, 3), (2, 5, 4), (3, 5, 2), (4, 5, 1), (5, 5, 5), (6, 5, 6);
  INSERT INTO t2 VALUES (7, 1, 3), (8, 1, 4), (9, 1, 2), (10, 1, 1), (11, 1, 5), (12, 1, 6);
  INSERT INTO t2 VALUES (13, 3, 3), (14, 3, 4), (15, 3, 2), (16, 3, 1), (17, 3, 5), (18, 3, 6);
  INSERT INTO t2 VALUES (19, 2, 3), (20, 2, 4), (21, 2, 2), (22, 2, 1), (23, 2, 5), (24, 2, 6);
  INSERT INTO t2 VALUES (25, 4, 3), (26, 4, 4), (27, 4, 2), (28, 4, 1), (29, 4, 5), (30, 4, 6);
  INSERT INTO t2 VALUES (31, 6, 3), (32, 6, 4), (33, 6, 2), (34, 6, 1), (35, 6, 5), (36, 6, 6);

  CREATE TABLE t3(a primary key, b);
  INSERT INTO t3 VALUES (2, 2), (4, 4), (5, 5);
  CREATE UNIQUE INDEX t3i1 ON t3(a ASC);
  CREATE UNIQUE INDEX t3i2 ON t3(b DESC);
}

foreach {tn1 idx} {
  1 { CREATE INDEX i1 ON t2(x ASC,  y ASC) }
  2 { CREATE INDEX i1 ON t2(x ASC,  y DESC) }
  3 { CREATE INDEX i1 ON t2(x DESC, y ASC) }
  4 { CREATE INDEX i1 ON t2(x DESC, y DESC) }

  5 { CREATE INDEX i1 ON t2(y ASC,  x ASC) }
  6 { CREATE INDEX i1 ON t2(y ASC,  x DESC) }
  7 { CREATE INDEX i1 ON t2(y DESC, x ASC) }
  8 { CREATE INDEX i1 ON t2(y DESC, x DESC) }
} {
  do_execsql_test 2.$tn1.1 { DROP INDEX IF EXISTS 'i1'; }
  do_execsql_test 2.$tn1.2 $idx

  foreach {tn2 inexpr} {
    3  "(2, 4, 5)"
    4  "(SELECT a FROM t3)"
    5  "(SELECT b FROM t3)"
  } {
    do_execsql_test 2.$tn1.$tn2.1 "
      SELECT x, y FROM t2 WHERE x = 1 AND y IN $inexpr ORDER BY x ASC, y ASC;
    " {1 2  1 4  1 5}

    do_execsql_test 2.$tn1.$tn2.2 "
      SELECT x, y FROM t2 WHERE x = 2 AND y IN $inexpr ORDER BY x ASC, y DESC;
    " {2 5  2 4  2 2}

    do_execsql_test 2.$tn1.$tn2.3 "
      SELECT x, y FROM t2 WHERE x = 3 AND y IN $inexpr ORDER BY x DESC, y ASC;
    " {3 2  3 4  3 5}

    do_execsql_test 2.$tn1.$tn2.4 "
      SELECT x, y FROM t2 WHERE x = 4 AND y IN $inexpr ORDER BY x DESC, y DESC;
    " {4 5  4 4  4 2}
    
    do_execsql_test 2.$tn1.$tn2.5 "
      SELECT a, x, y FROM t2, t3 WHERE a = 4 AND x = 1 AND y IN $inexpr 
      ORDER BY a, x ASC, y ASC;
    " {4 1 2  4 1 4  4 1 5}
    do_execsql_test 2.$tn1.$tn2.6 "
      SELECT a, x, y FROM t2, t3 WHERE a = 2 AND x = 1 AND y IN $inexpr 
      ORDER BY x ASC, y ASC;
    " {2 1 2  2 1 4  2 1 5}

    do_execsql_test 2.$tn1.$tn2.7 "
      SELECT a, x, y FROM t2, t3 WHERE a = 4 AND x = 1 AND y IN $inexpr 
      ORDER BY a, x ASC, y DESC;
    " {4 1 5  4 1 4  4 1 2}
    do_execsql_test 2.$tn1.8 "
      SELECT a, x, y FROM t2, t3 WHERE a = 2 AND x = 1 AND y IN $inexpr 
      ORDER BY x ASC, y DESC;
    " {2 1 5  2 1 4  2 1 2}

    do_execsql_test 2.$tn1.$tn2.9 "
      SELECT a, x, y FROM t2, t3 WHERE a = 4 AND x = 1 AND y IN $inexpr 
      ORDER BY a, x DESC, y ASC;
    " {4 1 2  4 1 4  4 1 5}
    do_execsql_test 2.$tn1.10 "
      SELECT a, x, y FROM t2, t3 WHERE a = 2 AND x = 1 AND y IN $inexpr 
      ORDER BY x DESC, y ASC;
    " {2 1 2  2 1 4  2 1 5}

    do_execsql_test 2.$tn1.$tn2.11 "
      SELECT a, x, y FROM t2, t3 WHERE a = 4 AND x = 1 AND y IN $inexpr 
      ORDER BY a, x DESC, y DESC;
    " {4 1 5  4 1 4  4 1 2}
    do_execsql_test 2.$tn1.$tn2.12 "
      SELECT a, x, y FROM t2, t3 WHERE a = 2 AND x = 1 AND y IN $inexpr 
      ORDER BY x DESC, y DESC;
    " {2 1 5  2 1 4  2 1 2}
  }
}

do_execsql_test 3.0 {
  CREATE TABLE t7(x primary key);
  INSERT INTO t7 VALUES (1), (2), (3);
  CREATE INDEX i7 ON t7(x);

  CREATE TABLE t8(y primary key);
  INSERT INTO t8 VALUES (1), (2), (3);
}

foreach {tn idxdir sortdir sortdata} {
  1 ASC  ASC  {1 2 3}
  2 ASC  DESC {3 2 1}
  3 DESC ASC  {1 2 3}
  4 ASC  DESC {3 2 1}
} {

  do_execsql_test 3.$tn "
    DROP INDEX IF EXISTS 'i8';
    CREATE UNIQUE INDEX i8 ON t8(y $idxdir);
    SELECT x FROM t7 WHERE x IN (SELECT y FROM t8) ORDER BY x $sortdir;
  " $sortdata
}

finish_test
