import unittest
from .conftest import *
from databass import *
from databass.ops import *
from databass.exprutil import predicate_to_cnf


exprs = [
    "SELECT 1",
    "SELECT 1+2",
    "SELECT (1*2)+3",
    "SELECT a FROM data",
    "SELECT a, b FROM data WHERE (a+b) > 3",
    "SELECT (((4 * 3) * 10) <= 120) and 1",
    "SELECT 1 and 1",
    "SELECT a, b FROM data",
    "SELECT a+b FROM data",
    "SELECT ((4 * 3) * 10) == 120", 
    "SELECT ((4 * 3) * 10) >= 120",
    "SELECT (1.0 / 2)",
    "SELECT 'and the name'", 
    "SELECT '1'", 
    "SELECT 1.0 + 2",
    "SELECT 1 and (2 > 3)", 
    "SELECT 1 and (2 < 3)",
    "SELECT 1 + (2 > 3)",
    "SELECT 1 / 2.0", 
    "SELECT ((4 * 3) * 10) = 120", 
    "SELECT ((4 * 3) * 10) <> 120",
    "SELECT ((4 * 3) * 10) > 120"
]

hashjoin_qs = [
  """SELECT d1.a
  FROM data as d1, data as d2, data as d3
  WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c""" ,
  """SELECT d1.a
  FROM data as d1, 
       data as d2, 
       (SELECT * FROM data) as d3
  WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c""",

  """SELECT d1.a
  FROM (SELECT * FROM data) as d1,
       (SELECT * FROM data) as d2,
       (SELECT * FROM data) as d3
  WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c"""
    ]

gb_qs = [
 " SELECT 1 where (1 = 1) and (3 < 4) ",
 "SELECT a, sum(b) FROM data GROUP BY a""",
 "SELECT a FROM data ORDER BY a ASC, a ASC",
 "SELECT a FROM data ORDER BY a",
  """SELECT d1.a
  FROM data as d1, 
       data as d2, 
       (SELECT a, c, sum(b) AS z
        FROM data GROUP BY a, c) as d3
  WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.z""" ,
 "SELECT a+b AS c, 9 * b, a FROM data ORDER BY a",
 "SELECT 1",
 "SELECT 1 FROM data",
 "SELECT a+b, 9 * b FROM data",
 "SELECT count(b) FROM data GROUP BY c",
 "SELECT c*d, sum(b) FROM data GROUP BY c+d",
 "SELECT * FROM data WHERE a between b and c ",
  """SELECT d1.a, d2.a
  FROM data as d1, data as d2
  WHERE d1.a < d2.a""",

  """SELECT d2.x
  FROM (SELECT a AS x, sum(b) AS z 
        FROM data GROUP BY a) AS d2,
       (SELECT d AS y, sum(b) AS z
        FROM data GROUP BY d+1) AS d3 
  WHERE d2.z = d3.y""",

  """SELECT d1.a, sum(d2.b)
  FROM data as d1, data as d2
  WHERE d1.a = d2.b 
  GROUP BY d1.a""",

  """SELECT d1.a + d2.b
  FROM (SELECT * FROM data) as d1,
       (SELECT * FROM data) as d2,
       (SELECT * FROM data) as d3
  WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c
  GROUP BY d1.a, d2.b""",

  """SELECT sum(c+d) FROM data GROUP BY b""",

  """SELECT sum(c+d) FROM data GROUP BY b LIMIT 2""",

]


@pytest.mark.parametrize("q", exprs)
@pytest.mark.usefixtures("context")
def test_expr(context, q):
  compiled_q = PyCompiledQuery(q)
  rows1 = [tup.row for tup in compiled_q(context['db'])]
  rows2 = run_sqlite_query(context, q)
  compare_results(context, rows1, rows2, False)


@pytest.mark.parametrize("q", hashjoin_qs)
@pytest.mark.usefixtures("context")
def test_hashjoin(context, q):
  compiled_q = PyCompiledQuery(q)
  rows1 = [tup.row for tup in compiled_q(context['db'])]
  rows2 = run_sqlite_query(context, q)
  compare_results(context, rows1, rows2, False)


@pytest.mark.parametrize("q", gb_qs)
@pytest.mark.usefixtures("context")
def test_groupby(context, q):
  compiled_q = PyCompiledQuery(q)
  rows1 = [tup.row for tup in compiled_q(context['db'])]
  rows2 = run_sqlite_query(context, q)
  compare_results(context, rows1, rows2, False)

