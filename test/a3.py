import unittest
from .util import *
from databass import *
from databass.ops import *
from databass.exprutil import predicate_to_cnf


class TestA3(TestBase):

  def test_expr(self):
    qs = [
        "SELECT 1",
        "SELECT 1+2",
        "SELECT (1*2)+3",
        "SELECT a FROM data"
    ]
    for q in qs:
      compiled_q = PyCompiledQuery(q)
      rows1 = [tup.row for tup in compiled_q(self.db)]
      rows2 = self.run_sqlite_query(q)
      self.compare_results(rows1, rows2, False)

  def test_hashjoin(self):
    qs = [
      """SELECT d1.a
      FROM data as d1, data as d2, data as d3
      WHERE d1.a = d2.b and d2.b = d3.a and d1.a = d3.c"""


        ]

    for q in qs:
      compiled_q = PyCompiledQuery(q)
      rows1 = [tup.row for tup in compiled_q(self.db)]
      rows2 = self.run_sqlite_query(q)
      self.compare_results(rows1, rows2, False)


  def test_groupby(self):
    qs = [
     " SELECT 1 where (1 = 1) and (3 < 4) ",
     "SELECT a, sum(b) FROM data GROUP BY a""",
     "SELECT a FROM data ORDER BY a ASC, a ASC",
     "SELECT a FROM data ORDER BY a"

    ]

    for q in qs:
      compiled_q = PyCompiledQuery(q)
      rows1 = [tup.row for tup in compiled_q(self.db)]
      rows2 = self.run_sqlite_query(q)
      self.compare_results(rows1, rows2, False)
