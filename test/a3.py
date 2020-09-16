import unittest
from .conftest import *
from databass import *
from databass.ops import *
from databass.exprutil import predicate_to_cnf


exprs = [
    "SELECT 1",
    "SELECT 1+2",
    "SELECT (1*2)+3",
    "SELECT a FROM data"
]

hashjoin_qs = [
  """SELECT d1.a
  FROM data as d1, data as d2, data as d3
  WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c"""


    ]

gb_qs = [
 " SELECT 1 where (1 = 1) and (3 < 4) ",
 "SELECT a, sum(b) FROM data GROUP BY a""",
 "SELECT a FROM data ORDER BY a ASC, a ASC",
 "SELECT a FROM data ORDER BY a"

]



@pytest.mark.parametrize("q", hashjoin_qs + gb_qs)
@pytest.mark.usefixtures("context")
def test_expr(context, q):
  compiled_q = PyCompiledQuery(q)
  rows1 = [tup.row for tup in compiled_q(context['db'])]
  rows2 = run_sqlite_query(context, q)
  compare_results(context, rows1, rows2, False)

