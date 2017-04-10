#!./tcltestrunner.lua

# 2001 September 15
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this file is testing the CREATE INDEX statement.
#
# $Id: index.test,v 1.43 2008/01/16 18:20:42 danielk1977 Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Create a basic index and verify it is added to sqlite_master
#
do_test index-1.1 {
  execsql {CREATE TABLE test1(id primary key, f1 int, f2 int, f3 int)}
  execsql {CREATE INDEX index1 ON test1(f1)}
  execsql {SELECT name FROM _space WHERE name='test1'}
} {test1}
do_test index-1.1.1 {
  execsql {SELECT name FROM _index WHERE name='index1'}
} {index1}
# do_test index-1.1b {
#   execsql {SELECT name, sql, tbl_name, type FROM sqlite_master 
#            WHERE name='index1'}
# } {index1 {CREATE INDEX index1 ON test1(f1)} test1 index}

#do_test index-1.1c {
#  db close
#  sqlite3 db test.db
#  execsql {SELECT name FROM _index WHERE name='index1'}
#} {index1}
  # execsql {SELECT name, sql, tbl_name, type FROM sqlite_master 
  #          WHERE name='index1'}
#} {index1 {CREATE INDEX index1 ON test1(f1)} test1 index}

#do_test index-1.1d {
#  db close
#  sqlite3 db test.db
#  execsql {SELECT name FROM _space WHERE name='test1'}
  #execsql {SELECT name FROM sqlite_master WHERE type!='meta' ORDER BY name}
#} {test1}

# Verify that the index dies with the table
#
do_test index-1.2 {
  execsql {DROP TABLE test1}
  execsql {SELECT name FROM _space WHERE name='test1'}
  #execsql {SELECT name FROM sqlite_master WHERE type!='meta' ORDER BY name}
} {}

# Try adding an index to a table that does not exist
#
do_test index-2.1 {
  set v [catch {execsql {CREATE INDEX index1 ON test1(f1)}} msg]
  lappend v $msg
} {1 {no such table: main.test1}}

# Try adding an index on a column of a table where the table
# exists but the column does not.
#
do_test index-2.1b {
  execsql {CREATE TABLE test1(id primary key, f1 int, f2 int, f3 int)}
  set v [catch {execsql {CREATE INDEX index1 ON test1(f4)}} msg]
  lappend v $msg
} {1 {no such column: f4}}

# Try an index with some columns that match and others that do now.
#
do_test index-2.2 {
  set v [catch {execsql {CREATE INDEX index1 ON test1(f1, f2, f4, f3)}} msg]
  execsql {DROP TABLE test1}
  lappend v $msg
} {1 {no such column: f4}}

# MUST_WORK_TEST REINDEX and integrity_check

# # Try creating a bunch of indices on the same table
# #
# set r {}
# for {set i 1} {$i<100} {incr i} {
#   lappend r [format index%02d $i]
# }
# do_test index-3.1 {
#   execsql {CREATE TABLE test1(f1 int, f2 int, f3 int, f4 int, f5 int)}
#   for {set i 1} {$i<100} {incr i} {
#     set sql "CREATE INDEX [format index%02d $i] ON test1(f[expr {($i%5)+1}])"
#     execsql $sql
#   }
#   execsql {SELECT name FROM sqlite_master 
#            WHERE type='index' AND tbl_name='test1'
#            ORDER BY name}
# } $r
# integrity_check index-3.2.1
# ifcapable {reindex} {
#   do_test index-3.2.2 {
#     execsql REINDEX
#   } {}
# }
#integrity_check index-3.2.3


# # Verify that all the indices go away when we drop the table.
# #
# do_test index-3.3 {
#   execsql {DROP TABLE test1}
#   execsql {SELECT name FROM sqlite_master 
#            WHERE type='index' AND tbl_name='test1'
#            ORDER BY name}
# } {}

# Create a table and insert values into that table.  Then create
# an index on that table.  Verify that we can select values
# from the table correctly using the index.
#
# Note that the index names "index9" and "indext" are chosen because
# they both have the same hash.
#
do_test index-4.1 {
  execsql {CREATE TABLE test1(id primary key, cnt int, power int)}
  for {set i 1} {$i<20} {incr i} {
    execsql "INSERT INTO test1 VALUES($i, $i,[expr {1<<$i}])"
  }
  execsql {CREATE INDEX index9 ON test1(cnt)}
  execsql {CREATE INDEX indext ON test1(power)}
  execsql {SELECT name FROM _index WHERE name='index9' OR name='indext'; SELECT name FROM _space WHERE name='test1';}
} {index9 indext test1}
do_test index-4.2 {
  execsql {SELECT cnt FROM test1 WHERE power=4}
} {2}
do_test index-4.3 {
  execsql {SELECT cnt FROM test1 WHERE power=1024}
} {10}
do_test index-4.4 {
  execsql {SELECT power FROM test1 WHERE cnt=6}
} {64}
do_test index-4.5 {
  execsql {DROP INDEX 'indext'}
  execsql {SELECT power FROM test1 WHERE cnt=6}
} {64}
do_test index-4.6 {
  execsql {SELECT cnt FROM test1 WHERE power=1024}
} {10}
do_test index-4.7 {
  execsql {CREATE INDEX indext ON test1(cnt)}
  execsql {SELECT power FROM test1 WHERE cnt=6}
} {64}
do_test index-4.8 {
  execsql {SELECT cnt FROM test1 WHERE power=1024}
} {10}
do_test index-4.9 {
  execsql {DROP INDEX 'index9'}
  execsql {SELECT power FROM test1 WHERE cnt=6}
} {64}
do_test index-4.10 {
  execsql {SELECT cnt FROM test1 WHERE power=1024}
} {10}
do_test index-4.11 {
  execsql {DROP INDEX 'indext'}
  execsql {SELECT power FROM test1 WHERE cnt=6}
} {64}
do_test index-4.12 {
  execsql {SELECT cnt FROM test1 WHERE power=1024}
} {10}
do_test index-4.13 {
  execsql {DROP TABLE test1}
  execsql {SELECT name FROM _space WHERE name='test1'}
  #execsql {SELECT name FROM sqlite_master WHERE type!='meta' ORDER BY name}
} {}

# integrity_check index-4.14

# # Do not allow indices to be added to sqlite_master
# #
# do_test index-5.1 {
#   set v [catch {execsql {CREATE INDEX index1 ON sqlite_master(name)}} msg]
#   lappend v $msg
# } {1 {table sqlite_master may not be indexed}}
# do_test index-5.2 {
#   execsql {SELECT name FROM sqlite_master WHERE type!='meta'}
# } {}

# Do not allow indices with duplicate names to be added
#
do_test index-6.1 {
  execsql {CREATE TABLE test1(id primary key, f1 int, f2 int)}
  execsql {CREATE TABLE test2(id primary key, g1 real, g2 real)}
  execsql {CREATE INDEX index1 ON test1(f1)}
} {}
  #set v [catch {execsql {CREATE INDEX index1 ON test2(g1)}} msg]
  #lappend v $msg
#} {1 {index index1 already exists}}


# do_test index-6.1.1 {
#   catchsql {CREATE INDEX [index1] ON test2(g1)}
# } {1 {index index1 already exists}}
do_test index-6.1b {
  execsql {SELECT name FROM _index WHERE name='index1'; SELECT name FROM _space WHERE name='test1' OR name='test2'}
} {index1 test1 test2}
do_test index-6.1c {
  catchsql {CREATE INDEX IF NOT EXISTS index1 ON test1(f1)}
} {0 {}}
do_test index-6.2 {
  set v [catch {execsql {CREATE INDEX test1 ON test2(g1)}} msg]
  lappend v $msg
} {1 {there is already a table named test1}}
do_test index-6.2b {
  execsql {SELECT name FROM _index WHERE name='index1'; SELECT name FROM _space WHERE name='test1' OR name='test2'}
} {index1 test1 test2}
do_test index-6.3 {
  execsql {DROP TABLE test1}
  execsql {DROP TABLE test2}
  execsql {SELECT name FROM _space WHERE name='test1' OR name='test2'}
  #execsql {SELECT name FROM sqlite_master WHERE type!='meta' ORDER BY name}
} {}
do_test index-6.4 {
  execsql {
    CREATE TABLE test1(id primary key, a,b);
    CREATE INDEX index1 ON test1(a);
    CREATE INDEX index2 ON test1(b);
    CREATE INDEX index3 ON test1(a,b);
    DROP TABLE test1;
    SELECT name FROM _space WHERE name='test1';
  }
} {}

# integrity_check index-6.5


# Create a primary key
#
do_test index-7.1 {
  execsql {CREATE TABLE test1(f1 int, f2 int primary key)}
  for {set i 1} {$i<20} {incr i} {
    execsql "INSERT INTO test1 VALUES($i,[expr {1<<$i}])"
  }
  execsql {SELECT count(*) FROM test1}
} {19}
do_test index-7.2 {
  execsql {SELECT f1 FROM test1 WHERE f2=65536}
} {16}
do_test index-7.3 {
  execsql {SELECT name FROM _index WHERE name='sqlite_autoindex_test1_1'}
} {sqlite_autoindex_test1_1}
do_test index-7.4 {
  execsql {DROP table test1}
  execsql {SELECT name FROM _space WHERE name='test1'}
} {}

# integrity_check index-7.5

# Make sure we cannot drop a non-existant index.
#
do_test index-8.1 {
  set v [catch {execsql {DROP INDEX index1}} msg]
  lappend v $msg
} {1 {no such index: index1}}

# Make sure we don't actually create an index when the EXPLAIN keyword
# is used.
#
do_test index-9.1 {
  execsql {CREATE TABLE tab1(id primary key, a int)}
  ifcapable {explain} {
    execsql {EXPLAIN CREATE INDEX idx1 ON tab1(a)}
  }
  execsql {SELECT name FROM _space WHERE name='tab1'}
} {tab1}
do_test index-9.2 {
  execsql {CREATE INDEX idx1 ON tab1(a)}
  execsql {SELECT name FROM _index WHERE name='idx1'; SELECT name FROM _space WHERE name='tab1'}
} {idx1 tab1}

# integrity_check index-9.3

# Allow more than one entry with the same key.
#
do_test index-10.0 {
  execsql {
    CREATE TABLE t1(id primary key, a int, b int);
    CREATE INDEX i1 ON t1(a);
    INSERT INTO t1 VALUES(1, 1,2);
    INSERT INTO t1 VALUES(2, 2,4);
    INSERT INTO t1 VALUES(3, 3,8);
    INSERT INTO t1 VALUES(4, 1,12);
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {2 12}
do_test index-10.1 {
  execsql {
    SELECT b FROM t1 WHERE a=2 ORDER BY b;
  }
} {4}
do_test index-10.2 {
  execsql {
    DELETE FROM t1 WHERE b=12;
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {2}
do_test index-10.3 {
  execsql {
    DELETE FROM t1 WHERE b=2;
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {}
do_test index-10.4 {
  execsql {
    DELETE FROM t1;
    INSERT INTO t1 VALUES (1, 1,1);
    INSERT INTO t1 VALUES (2, 1,2);
    INSERT INTO t1 VALUES (3, 1,3);
    INSERT INTO t1 VALUES (4, 1,4);
    INSERT INTO t1 VALUES (5, 1,5);
    INSERT INTO t1 VALUES (6, 1,6);
    INSERT INTO t1 VALUES (7, 1,7);
    INSERT INTO t1 VALUES (8, 1,8);
    INSERT INTO t1 VALUES (9, 1,9);
    INSERT INTO t1 VALUES (10, 2,0);
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {1 2 3 4 5 6 7 8 9}
do_test index-10.5 {
  ifcapable subquery {
    execsql { DELETE FROM t1 WHERE b IN (2, 4, 6, 8); }
  } else {
    execsql { DELETE FROM t1 WHERE b = 2 OR b = 4 OR b = 6 OR b = 8; }
  }
  execsql {
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {1 3 5 7 9}
do_test index-10.6 {
  execsql {
    DELETE FROM t1 WHERE b>2;
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {1}
do_test index-10.7 {
  execsql {
    DELETE FROM t1 WHERE b=1;
    SELECT b FROM t1 WHERE a=1 ORDER BY b;
  }
} {}
do_test index-10.8 {
  execsql {
    SELECT b FROM t1 ORDER BY b;
  }
} {0}

# integrity_check index-10.9

# Automatically create an index when we specify a primary key.
#
# Tarantool: WITHOUT ROWID is by default, so search count is less
# by one. Expected result changed {0.1 2} -> {0.1 1}
do_test index-11.1 {
  execsql {
    CREATE TABLE t3(
      a text,
      b int,
      c float,
      PRIMARY KEY(b)
    );
  }
  for {set i 1} {$i<=50} {incr i} {
    execsql "INSERT INTO t3 VALUES('x${i}x',$i,0.$i)"
  }
  set sqlite_search_count 0
  concat [execsql {SELECT c FROM t3 WHERE b==10}] $sqlite_search_count
} {0.1 1}

# integrity_check index-11.2


# Numeric strings should compare as if they were numbers.  So even if the
# strings are not character-by-character the same, if they represent the
# same number they should compare equal to one another.  Verify that this
# is true in indices.
#
# Updated for sqlite3 v3: SQLite will now store these values as numbers
# (because the affinity of column a is NUMERIC) so the quirky
# representations are not retained. i.e. '+1.0' becomes '1'.
do_test index-12.1 {
  execsql {
    CREATE TABLE t4(id primary key, a NUM,b);
    INSERT INTO t4 VALUES(1, '0.0',1);
    INSERT INTO t4 VALUES(2, '0.00',2);
    INSERT INTO t4 VALUES(3, 'abc',3);
    INSERT INTO t4 VALUES(4, '-1.0',4);
    INSERT INTO t4 VALUES(5, '+1.0',5);
    INSERT INTO t4 VALUES(6, '0',6);
    INSERT INTO t4 VALUES(7, '00000',7);
    SELECT a FROM t4 ORDER BY b;
  }
} {0 0 abc -1 1 0 0}
do_test index-12.2 {
  execsql {
    SELECT a FROM t4 WHERE a==0 ORDER BY b
  }
} {0 0 0 0}
do_test index-12.3 {
  execsql {
    SELECT a FROM t4 WHERE a<0.5 ORDER BY b
  }
} {0 0 -1 0 0}
do_test index-12.4 {
  execsql {
    SELECT a FROM t4 WHERE a>-0.5 ORDER BY b
  }
} {0 0 abc 1 0 0}
do_test index-12.5 {
  execsql {
    CREATE INDEX t4i1 ON t4(a);
    SELECT a FROM t4 WHERE a==0 ORDER BY b
  }
} {0 0 0 0}
do_test index-12.6 {
  execsql {
    SELECT a FROM t4 WHERE a<0.5 ORDER BY b
  }
} {0 0 -1 0 0}
do_test index-12.7 {
  execsql {
    SELECT a FROM t4 WHERE a>-0.5 ORDER BY b
  }
} {0 0 abc 1 0 0}

# integrity_check index-12.8

# Make sure we cannot drop an automatically created index.
#
do_test index-13.1 {
  execsql {
   CREATE TABLE t5(
      a int UNIQUE,
      b float PRIMARY KEY,
      c varchar(10),
      UNIQUE(a,c)
   );
   INSERT INTO t5 VALUES(1,2,3);
   SELECT * FROM t5;
  }
} {1 2.0 3}
# do_test index-13.2 {
#   set ::idxlist [execsql {
#     SELECT name FROM sqlite_master WHERE type="index" AND tbl_name="t5";
#   }]
#   llength $::idxlist
# } {3}
# for {set i 0} {$i<[llength $::idxlist]} {incr i} {
#   do_test index-13.3.$i {
#     catchsql "
#       DROP INDEX '[lindex $::idxlist $i]';
#     "
#   } {1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}}
# }
# do_test index-13.4 {
#   execsql {
#     INSERT INTO t5 VALUES('a','b','c');
#     SELECT * FROM t5;
#   }
# } {1 2.0 3 a b c}

# integrity_check index-13.5

# Check the sort order of data in an index.
#
do_test index-14.1 {
  execsql {
    CREATE TABLE t6(id primary key, a,b,c);
    CREATE INDEX t6i1 ON t6(a,b);
    INSERT INTO t6 VALUES(1, '','',1);
    INSERT INTO t6 VALUES(2, '',NULL,2);
    INSERT INTO t6 VALUES(3, NULL,'',3);
    INSERT INTO t6 VALUES(4, 'abc',123,4);
    INSERT INTO t6 VALUES(5, 123,'abc',5);
    SELECT c FROM t6 ORDER BY a,b;
  }
} {3 5 2 1 4}
do_test index-14.2 {
  execsql {
    SELECT c FROM t6 WHERE a='';
  }
} {2 1}
do_test index-14.3 {
  execsql {
    SELECT c FROM t6 WHERE b='';
  }
} {1 3}
do_test index-14.4 {
  execsql {
    SELECT c FROM t6 WHERE a>'';
  }
} {4}
do_test index-14.5 {
  execsql {
    SELECT c FROM t6 WHERE a>='';
  }
} {2 1 4}
do_test index-14.6 {
  execsql {
    SELECT c FROM t6 WHERE a>123;
  }
} {2 1 4}
do_test index-14.7 {
  execsql {
    SELECT c FROM t6 WHERE a>=123;
  }
} {5 2 1 4}
do_test index-14.8 {
  execsql {
    SELECT c FROM t6 WHERE a<'abc';
  }
} {5 2 1}
do_test index-14.9 {
  execsql {
    SELECT c FROM t6 WHERE a<='abc';
  }
} {5 2 1 4}
do_test index-14.10 {
  execsql {
    SELECT c FROM t6 WHERE a<='';
  }
} {5 2 1}
do_test index-14.11 {
  execsql {
    SELECT c FROM t6 WHERE a<'';
  }
} {5}

# integrity_check index-14.12

do_test index-15.1 {
  execsql {
    DELETE FROM t1;
    SELECT * FROM t1;
  }
} {}
# do_test index-15.2 {
#   execsql {
#     INSERT INTO t1 VALUES(1, '1.234e5',1);
#     INSERT INTO t1 VALUES(2, '12.33e04',2);
#     INSERT INTO t1 VALUES(3, '12.35E4',3);
#     INSERT INTO t1 VALUES(4, '12.34e',4);
#     INSERT INTO t1 VALUES(5, '12.32e+4',5);
#     INSERT INTO t1 VALUES(6, '12.36E+04',6);
#     INSERT INTO t1 VALUES(7, '12.36E+',7);
#     INSERT INTO t1 VALUES(8, '+123.10000E+0003',8);
#     INSERT INTO t1 VALUES(9, '+',9);
#     INSERT INTO t1 VALUES(10, '+12347.E+02',10);
#     INSERT INTO t1 VALUES(11, '+12347E+02',11);
#     INSERT INTO t1 VALUES(12, '+.125E+04',12);
#     INSERT INTO t1 VALUES(13, '-.125E+04',13);
#     INSERT INTO t1 VALUES(14, '.125E+0',14);
#     INSERT INTO t1 VALUES(15, '.125',15);
#     SELECT b FROM t1 ORDER BY a, b;
#   }
# } {13 14 15 12 8 5 2 1 3 6 10 11 9 4 7}
# # do_test index-15.3 {
#   execsql {
#     SELECT b FROM t1 WHERE typeof(a) IN ('integer','real') ORDER BY b;
#   }
# } {1 2 3 5 6 8 10 11 12 13 14 15}

# integrity_check index-15.4

# The following tests - index-16.* - test that when a table definition
# includes qualifications that specify the same constraint twice only a
# single index is generated to enforce the constraint.
#
# For example: "CREATE TABLE abc( x PRIMARY KEY, UNIQUE(x) );"
#
do_test index-16.1 {
  execsql {
    CREATE TABLE t7(c UNIQUE PRIMARY KEY);
    SELECT count(*) FROM _index JOIN _space WHERE _index.id = _space.id AND _space.name='t7';
  }
} {1}
do_test index-16.2 {
  execsql {
    DROP TABLE t7;
    CREATE TABLE t7(c UNIQUE PRIMARY KEY);
    SELECT count(*) FROM _index JOIN _space WHERE _index.id = _space.id AND _space.name='t7';
  }
} {1}
do_test index-16.3 {
  execsql {
    DROP TABLE t7;
    CREATE TABLE t7(c PRIMARY KEY, UNIQUE(c) );
    SELECT count(*) FROM _index JOIN _space WHERE _index.id = _space.id AND _space.name='t7';
  }
} {1}
do_test index-16.4 {
  execsql {
    DROP TABLE t7;
    CREATE TABLE t7(c, d , UNIQUE(c, d), PRIMARY KEY(c, d) );
    SELECT count(*) FROM _index JOIN _space WHERE _index.id = _space.id AND _space.name='t7';
  }
} {1}
do_test index-16.5 {
  execsql {
    DROP TABLE t7;
    CREATE TABLE t7(c, d , UNIQUE(c), PRIMARY KEY(c, d) );
    SELECT count(*) FROM _index JOIN _space WHERE _index.id = _space.id AND _space.name='t7';
  }
} {2}

# Test that automatically create indices are named correctly. The current
# convention is: "sqlite_autoindex_<table name>_<integer>"
#
# Then check that it is an error to try to drop any automtically created
# indices.
do_test index-17.1 {
  execsql {
    DROP TABLE t7;
    CREATE TABLE t7(c, d UNIQUE, UNIQUE(c), PRIMARY KEY(c, d) );
    SELECT _index.name FROM _index JOIN _space WHERE _index.id = _space.id AND _space.name='t7';
  }
} {sqlite_autoindex_t7_3 sqlite_autoindex_t7_2 sqlite_autoindex_t7_1}
# do_test index-17.2 {
#   catchsql {
#     DROP INDEX sqlite_autoindex_t7_1;
#   }
# } {1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}}
# do_test index-17.3 {
#   catchsql {
#     DROP INDEX IF EXISTS sqlite_autoindex_t7_1;
#   }
# } {1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}}
do_test index-17.4 {
  catchsql {
    DROP INDEX IF EXISTS no_such_index;
  }
} {0 {}}


# The following tests ensure that it is not possible to explicitly name
# a schema object with a name beginning with "sqlite_". Granted that is a
# little outside the focus of this test scripts, but this has got to be
# tested somewhere.
do_test index-18.1 {
  catchsql {
    CREATE TABLE sqlite_t1(a, b, c);
  }
} {1 {object name reserved for internal use: sqlite_t1}}
do_test index-18.2 {
  catchsql {
    CREATE INDEX sqlite_i1 ON t7(c);
  }
} {1 {object name reserved for internal use: sqlite_i1}}
ifcapable view {
do_test index-18.3 {
  catchsql {
    CREATE VIEW sqlite_v1 AS SELECT * FROM t7;
  }
} {1 {object name reserved for internal use: sqlite_v1}}
} ;# ifcapable view

# MUST_WORK_TEST

# ifcapable {trigger} {
#   do_test index-18.4 {
#     catchsql {
#       CREATE TRIGGER sqlite_tr1 BEFORE INSERT ON t7 BEGIN SELECT 1; END;
#     }
#   } {1 {object name reserved for internal use: sqlite_tr1}}
# }
do_test index-18.5 {
  execsql {
    DROP TABLE t7;
  }
} {}

# MUST_WORK_TEST

# # These tests ensure that if multiple table definition constraints are
# # implemented by a single indice, the correct ON CONFLICT policy applies.
# ifcapable conflict {
#   do_test index-19.1 {
#     execsql {
#       CREATE TABLE t7(a UNIQUE PRIMARY KEY);
#       CREATE TABLE t8(a UNIQUE PRIMARY KEY ON CONFLICT ROLLBACK);
#       INSERT INTO t7 VALUES(1);
#       INSERT INTO t8 VALUES(1);
#     }
#   } {}
#   do_test index-19.2 {
#     catchsql {
#       BEGIN;
#       INSERT INTO t7 VALUES(1);
#     }
#   } {1 {UNIQUE constraint failed: t7.a}}
#   do_test index-19.3 {
#     catchsql {
#       BEGIN;
#     }
#   } {1 {cannot start a transaction within a transaction}}
#   do_test index-19.4 {
#     catchsql {
#       INSERT INTO t8 VALUES(1);
#     }
#   } {1 {UNIQUE constraint failed: t8.a}}
#   do_test index-19.5 {
#     catchsql {
#       BEGIN;
#       COMMIT;
#     }
#   } {0 {}}
#   do_test index-19.6 {
#     catchsql {
#       DROP TABLE t7;
#       DROP TABLE t8;
#       CREATE TABLE t7(
#          a PRIMARY KEY ON CONFLICT FAIL, 
#          UNIQUE(a) ON CONFLICT IGNORE
#       );
#     }
#   } {1 {conflicting ON CONFLICT clauses specified}}
# } ; # end of "ifcapable conflict" block

# MUST_WORK_TEST

# ifcapable {reindex} {
#   do_test index-19.7 {
#     execsql REINDEX
#   } {}
# }
# integrity_check index-19.8

# Drop index with a quoted name.  Ticket #695.
#
do_test index-20.1 {
  execsql {
    CREATE INDEX "t6i2" ON t6(c);
    DROP INDEX "t6i2";
  }
} {}
do_test index-20.2 {
  execsql {
    DROP INDEX "t6i1";
  }
} {}

# Try to create a TEMP index on a non-TEMP table. */
#
do_test index-21.1 {
  catchsql {
     CREATE INDEX temp.i21 ON t6(c);
  }
} {1 {cannot create a TEMP index on non-TEMP table "t6"}}

# MUST_WORK_TEST

# do_test index-21.2 {
#   catchsql {
#      CREATE TEMP TABLE t6(x primary key);
#      INSERT INTO temp.t6 values(1),(5),(9);
#      CREATE INDEX temp.i21 ON t6(x);
#      SELECT x FROM t6 ORDER BY x DESC;
#   }
# } {0 {9 5 1}}

   

finish_test
