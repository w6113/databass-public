import random
import pandas as pd
import numpy as np
from .util import *
from databass import *
from databass.tables import InMemoryTable


class TestA1(TestBase):

  def test_limit(self):
    qs = [
        "SELECT * FROM tdata LIMIT 2",
        "SELECT * FROM tdata LIMIT 1+1"
    ]
    badqs = [
        "SELECT * FROM tdata LIMIT a",
    ]

    for q in qs:
      self.run_query(q)
    for q in badqs:
      with self.assertRaises(Exception):
        self.run_query(q)

  def test_distinct(self):
    plan = Distinct(Scan("tdata"))
    rows1 = self.run_plan(plan)
    rows2 = self.run_sqlite_query("SELECT distinct * FROM tdata")
    self.compare_results(rows1, rows2, False)

  def test_phase1(self):
    self.test_limit()
    self.test_distinct()

  def test_project_init_schema(self):
    q = "SELECT a+b as x, a FROM (SELECT a, b, c FROM data) as d1"
    opt = self.opt
    plan = opt(parse(q).to_plan())
    schema = Schema([Attr("x", "num"), Attr("a", "num")])
    self.check_schema(schema, plan.schema)

    aliases_res = ['x', 'a']
    self.assertEqual(plan.aliases, aliases_res)
  
  def test_project_expand_stars(self):
    opt = self.opt
    plan = opt(parse("SELECT * FROM data").to_plan())
    schema = Schema([
      Attr("a", "num"),
      Attr("b", "num"),
      Attr("c", "num"),
      Attr("d", "num"),
      Attr("e", "str"),
      Attr("f", "num"),
      Attr("g", "str"),
      ])
    self.check_schema(schema, plan.schema)

  def test_project_iter(self):
    queries = [
      ("SELECT 1", False),
      ("SELECT a+b as a1, 9 * b as b1 FROM data ORDER BY 9*b",  True),
      ("SELECT b FROM data LIMIT 2",  False),
      ("SELECT * FROM data ORDER BY f LIMIT 2",  True),
    ]

    for q,  ordered in queries:
      self.run_query(q, ordered)

  def test_phase_project(self):
    self.test_project_init_schema()
    self.test_project_expand_stars()
    self.test_project_iter()

  def test_subquery(self):
    qs = [
        "SELECT * FROM (SELECT * FROM data)"
    ]
    for q in qs:
      self.run_query(q, False)


  def test_hashjoin(self):
    q_res = self.run_sqlite_query("SELECT * from data d1, data d2 where d1.a = d2.c")
    plan = HashJoin(Scan('data', 'd1'), Scan('data', "d2"),
            list(map(cond_to_func, ["d1.a", "d2.c"])))
    res = self.run_plan(plan)
    self.compare_results(q_res, res, False)





  def test_groupby(self):
    q_res = [(2.0, 200, 10), (3.0, 220, 10)]
    schema = Schema([ Attr("c", "num"), Attr("sum", "num"), Attr("count", "num") ])
    group_term_schema = Schema([Attr("c", "num")])

    opt = self.opt

    q = "SELECT c+2 as c, sum(f) as sum, count(a) as count FROM data GROUP BY c"
    plan = parse(q).to_plan()
    plan = opt(plan)

    self.check_schema(schema, plan.schema)
    self.check_schema(group_term_schema, plan.group_term_schema)

    res = self.run_plan(plan)
    self.compare_results(q_res, res, False)

  def test_full_qs(self):
    qs = [
    """SELECT d2.x
      FROM (SELECT a AS x, sum(b) AS z
            FROM data GROUP BY a) AS d2,
           (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z = d3.y ORDER BY x""",
    """SELECT d2.x
      FROM (SELECT a AS x, count(b) AS z
            FROM data GROUP BY a) AS d2,
           (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z <> d3.y ORDER BY x""",
    """SELECT d2.x+d3.y
      FROM (SELECT a AS x, count(b) AS z
            FROM data GROUP BY a) AS d2,
           (SELECT d AS y, sum(b) AS z
            FROM data GROUP BY d+1) AS d3
      WHERE d2.z = d3.y ORDER BY x, d2.z, d3.z""",


    ]
    for q in qs:
      self.run_query(q)

  def test_phase2(self):
    self.test_subquery()
    self.test_hashjoin()
    self.test_groupby()
