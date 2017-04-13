#!./tcltestrunner.lua

# 2007 July 17
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
# focus of this file is testing SELECT statements that contain
# aggregate min() and max() functions and which are handled as
# as a special case.  This file makes sure that the min/max
# optimization works right in the presence of descending
# indices.  Ticket #2514.
#
# $Id: minmax2.test,v 1.2 2008/01/05 17:39:30 danielk1977 Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl

do_test minmax2-1.0 {
  execsql {
    PRAGMA legacy_file_format=0;
    BEGIN;
    CREATE TABLE t1(id PRIMARY KEY, x, y);
    INSERT INTO t1 VALUES(1, 1,1);
    INSERT INTO t1 VALUES(2, 2,2);
    INSERT INTO t1 VALUES(3, 3,2);
    INSERT INTO t1 VALUES(4, 4,3);
    INSERT INTO t1 VALUES(5, 5,3);
    INSERT INTO t1 VALUES(6, 6,3);
    INSERT INTO t1 VALUES(7, 7,3);
    INSERT INTO t1 VALUES(8, 8,4);
    INSERT INTO t1 VALUES(9, 9,4);
    INSERT INTO t1 VALUES(10, 10,4);
    INSERT INTO t1 VALUES(11, 11,4);
    INSERT INTO t1 VALUES(12, 12,4);
    INSERT INTO t1 VALUES(13, 13,4);
    INSERT INTO t1 VALUES(14, 14,4);
    INSERT INTO t1 VALUES(15, 15,4);
    INSERT INTO t1 VALUES(16, 16,5);
    INSERT INTO t1 VALUES(17, 17,5);
    INSERT INTO t1 VALUES(18, 18,5);
    INSERT INTO t1 VALUES(19, 19,5);
    INSERT INTO t1 VALUES(20, 20,5);
    COMMIT;
    SELECT DISTINCT y FROM t1 ORDER BY y;
  }
} {1 2 3 4 5}

do_test minmax2-1.1 {
  set sqlite_search_count 0
  execsql {SELECT min(x) FROM t1}
} {1}
do_test minmax2-1.2 {
  set sqlite_search_count
} {19}
do_test minmax2-1.3 {
  set sqlite_search_count 0
  execsql {SELECT max(x) FROM t1}
} {20}
do_test minmax2-1.4 {
  set sqlite_search_count
} {19}
do_test minmax2-1.5 {
  execsql {CREATE INDEX t1i1 ON t1(x DESC)}
  set sqlite_search_count 0
  execsql {SELECT min(x) FROM t1}
} {1}
do_test minmax2-1.6 {
  set sqlite_search_count
} {1}
do_test minmax2-1.7 {
  set sqlite_search_count 0
  execsql {SELECT max(x) FROM t1}
} {20}
do_test minmax2-1.8 {
  set sqlite_search_count
} {0}
do_test minmax2-1.9 {
  set sqlite_search_count 0
  execsql {SELECT max(y) FROM t1}
} {5}
do_test minmax2-1.10 {
  set sqlite_search_count
} {19}

do_test minmax2-2.0 {
  execsql {
    CREATE TABLE t2(a INTEGER PRIMARY KEY, b);
    INSERT INTO t2 SELECT x, y FROM t1;
  }
  set sqlite_search_count 0
  execsql {SELECT min(a) FROM t2}
} {1}
do_test minmax2-2.1 {
  set sqlite_search_count
} {0}
do_test minmax2-2.2 {
  set sqlite_search_count 0
  execsql {SELECT max(a) FROM t2}
} {20}
do_test minmax2-2.3 {
  set sqlite_search_count
} {0}

do_test minmax2-3.0 {
  ifcapable subquery {
    execsql {INSERT INTO t2 VALUES((SELECT max(a) FROM t2)+1,999)}
  } else {
    db function max_a_t2 {execsql {SELECT max(a) FROM t2}}
    execsql {INSERT INTO t2 VALUES(max_a_t2()+1,999)}
  }
  set sqlite_search_count 0
  execsql {SELECT max(a) FROM t2}
} {21}
do_test minmax2-3.1 {
  set sqlite_search_count
} {0}
do_test minmax2-3.2 {
  ifcapable subquery {
    execsql {INSERT INTO t2 VALUES((SELECT max(a) FROM t2)+1,999)}
  } else {
    db function max_a_t2 {execsql {SELECT max(a) FROM t2}}
    execsql {INSERT INTO t2 VALUES(max_a_t2()+1,999)}
  }
  set sqlite_search_count 0
  ifcapable subquery {
    execsql { SELECT b FROM t2 WHERE a=(SELECT max(a) FROM t2) }
  } else {
    execsql { SELECT b FROM t2 WHERE a=max_a_t2() }
  }
} {999}
# Tarantool: see comment in minmax-3.3. Update expected result: 0 -> 1
do_test minmax2-3.3 {
  set sqlite_search_count
} {1}

ifcapable {compound && subquery} {
  do_test minmax2-4.1 {
    execsql {
      SELECT coalesce(min(x+0),-1), coalesce(max(x+0),-1) FROM
        (SELECT x, y FROM t1 UNION SELECT NULL as 'x', NULL as 'y')
    }
  } {1 20}
  do_test minmax2-4.2 {
    execsql {
      SELECT y, coalesce(sum(x),0) FROM
        (SELECT null AS x, y+1 AS y FROM t1 UNION SELECT x, y FROM t1)
      GROUP BY y ORDER BY y;
    }
  } {1 1 2 5 3 22 4 92 5 90 6 0}
  do_test minmax2-4.3 {
    execsql {
      SELECT y, count(x), count(*) FROM
        (SELECT null AS x, y+1 AS y FROM t1 UNION SELECT x, y FROM t1)
      GROUP BY y ORDER BY y;
    }
  } {1 1 1 2 2 3 3 4 5 4 8 9 5 5 6 6 0 1}
} ;# ifcapable compound

# Make sure the min(x) and max(x) optimizations work on empty tables
# including empty tables with indices. Ticket #296.
#
do_test minmax2-5.1 {
  execsql {
    CREATE TABLE t3(x INTEGER PRIMARY KEY NOT NULL);
    SELECT coalesce(min(x),999) FROM t3;
  }
} {999}
# do_test minmax2-5.2 {
#   execsql {
#     SELECT coalesce(min(rowid),999) FROM t3;
#   }
# } {999}
do_test minmax2-5.3 {
  execsql {
    SELECT coalesce(max(x),999) FROM t3;
  }
} {999}
# do_test minmax2-5.4 {
#   execsql {
#     SELECT coalesce(max(rowid),999) FROM t3;
#   }
# } {999}
# do_test minmax2-5.5 {
#   execsql {
#     SELECT coalesce(max(rowid),999) FROM t3 WHERE rowid<25;
#   }
# } {999}

# Make sure the min(x) and max(x) optimizations work when there
# is a LIMIT clause.  Ticket #396.
#
do_test minmax2-6.1 {
  execsql {
    SELECT min(a) FROM t2 LIMIT 1
  }
} {1}
do_test minmax2-6.2 {
  execsql {
    SELECT max(a) FROM t2 LIMIT 3
  }
} {22}
do_test minmax2-6.3 {
  execsql {
    SELECT min(a) FROM t2 LIMIT 0,100
  }
} {1}
do_test minmax2-6.4 {
  execsql {
    SELECT max(a) FROM t2 LIMIT 1,100
  }
} {}
do_test minmax2-6.5 {
  execsql {
    SELECT min(x) FROM t3 LIMIT 1
  }
} {{}}
do_test minmax2-6.6 {
  execsql {
    SELECT max(x) FROM t3 LIMIT 0
  }
} {}
do_test minmax2-6.7 {
  execsql {
    SELECT max(a) FROM t2 LIMIT 0
  }
} {}

# Make sure the max(x) and min(x) optimizations work for nested
# queries.  Ticket #587.
#
do_test minmax2-7.1 {
  execsql {
    SELECT max(x) FROM t1;
  }
} 20
ifcapable subquery {
  do_test minmax2-7.2 {
    execsql {
      SELECT * FROM (SELECT max(x) FROM t1);
    }
  } 20
}
do_test minmax2-7.3 {
  execsql {
    SELECT min(x) FROM t1;
  }
} 1
ifcapable subquery {
  do_test minmax2-7.4 {
    execsql {
      SELECT * FROM (SELECT min(x) FROM t1);
    }
  } 1
}

# Make sure min(x) and max(x) work correctly when the datatype is
# TEXT instead of NUMERIC.  Ticket #623.
#
do_test minmax2-8.1 {
  execsql {
    CREATE TABLE t4(a TEXT PRIMARY KEY);
    INSERT INTO t4 VALUES('1234');
    INSERT INTO t4 VALUES('234');
    INSERT INTO t4 VALUES('34');
    SELECT min(a), max(a) FROM t4;
  }
} {1234 34}
do_test minmax2-8.2 {
  execsql {
    CREATE TABLE t5(a INTEGER PRIMARY KEY);
    INSERT INTO t5 VALUES('1234');
    INSERT INTO t5 VALUES('234');
    INSERT INTO t5 VALUES('34');
    SELECT min(a), max(a) FROM t5;
  }
} {34 1234}

# # Ticket #658:  Test the min()/max() optimization when the FROM clause
# # is a subquery.
# #
# ifcapable {compound && subquery} {
#   do_test minmax2-9.1 {
#     execsql {
#       SELECT max(rowid) FROM (
#         SELECT max(rowid) FROM t4 UNION SELECT max(rowid) FROM t5
#       )
#     }
#   } {{}}
#   do_test minmax2-9.2 {
#     execsql {
#       SELECT max(rowid) FROM (
#         SELECT max(rowid) FROM t4 EXCEPT SELECT max(rowid) FROM t5
#       )
#     }
#   } {{}}
# } ;# ifcapable compound&&subquery

# If there is a NULL in an aggregate max() or min(), ignore it.  An
# aggregate min() or max() will only return NULL if all values are NULL.
#
do_test minmax2-10.1 {
  execsql {
    CREATE TABLE t6(id primary key, x);
    INSERT INTO t6 VALUES(1, 1);
    INSERT INTO t6 VALUES(2, 2);
    INSERT INTO t6 VALUES(3, NULL);
    SELECT coalesce(min(x),-1) FROM t6;
  }
} {1}
do_test minmax2-10.2 {
  execsql {
    SELECT max(x) FROM t6;
  }
} {2}
do_test minmax2-10.3 {
  execsql {
    CREATE INDEX i6 ON t6(x DESC);
    SELECT coalesce(min(x),-1) FROM t6;
  }
} {1}
do_test minmax2-10.4 {
  execsql {
    SELECT max(x) FROM t6;
  }
} {2}
do_test minmax2-10.5 {
  execsql {
    DELETE FROM t6 WHERE x NOT NULL;
    SELECT count(*) FROM t6;
  }
} 1
do_test minmax2-10.6 {
  execsql {
    SELECT count(x) FROM t6;
  }
} 0
ifcapable subquery {
  do_test minmax2-10.7 {
    execsql {
      SELECT (SELECT min(x) FROM t6), (SELECT max(x) FROM t6);
    }
  } {{} {}}
}
do_test minmax2-10.8 {
  execsql {
    SELECT min(x), max(x) FROM t6;
  }
} {{} {}}
do_test minmax2-10.9 {
  execsql {
    INSERT INTO t6 SELECT id+4,x FROM t6;
    INSERT INTO t6 SELECT id+8,x FROM t6;
    INSERT INTO t6 SELECT id+16,x FROM t6;
    INSERT INTO t6 SELECT id+32,x FROM t6;
    INSERT INTO t6 SELECT id+64,x FROM t6;
    INSERT INTO t6 SELECT id+128,x FROM t6;
    INSERT INTO t6 SELECT id+256,x FROM t6;
    INSERT INTO t6 SELECT id+512,x FROM t6;
    INSERT INTO t6 SELECT id+1024,x FROM t6;
    INSERT INTO t6 SELECT id+2048,x FROM t6;
    SELECT count(*) FROM t6;
  }
} 1024
do_test minmax2-10.10 {
  execsql {
    SELECT count(x) FROM t6;
  }
} 0
ifcapable subquery {
  do_test minmax2-10.11 {
    execsql {
      SELECT (SELECT min(x) FROM t6), (SELECT max(x) FROM t6);
    }
  } {{} {}}
}
do_test minmax2-10.12 {
  execsql {
    SELECT min(x), max(x) FROM t6;
  }
} {{} {}}


finish_test
