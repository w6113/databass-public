import random
import pandas as pd
import numpy as np
from .conftest import *
from databass import *
from databass.tables import InMemoryTable





limit_qs = [
    "SELECT * FROM tdata LIMIT 2",
    "SELECT * FROM tdata LIMIT 1+1"
    ,
    "SELECT * FROM tdata LIMIT (2*2)-2",
    "SELECT * FROM tdata LIMIT true"
]
limit_badqs = [
    "SELECT * FROM tdata LIMIT a",
    "SELECT * FROM tdata LIMIT b",
    "SELECT * FROM tdata LIMIT 1, a"
    "SELECT * FROM tdata LIMIT 1,1",
    "SELECT * FROM tdata LIMIT 'foo'"
]
distinct_inputs = [
  (Distinct(Scan("tdata")), "SELECT distinct * FROM tdata")
]

project_schema = [
  (parse("SELECT * FROM data").to_plan(), 
   Schema([
      Attr("a", "num"),
      Attr("b", "num"),
      Attr("c", "num"),
      Attr("d", "num"),
      Attr("e", "str"),
      Attr("f", "num"),
      Attr("g", "str"),
  ])),
  (parse("SELECT a+b as x, a FROM (SELECT a, b, c FROM data) as d1").to_plan(),
   Schema([Attr("x", "num"), Attr("a", "num")]))

]
project_qs_ordered = [
  ("SELECT 1", False),
  ("SELECT a+b as a1, 9 * b as b1 FROM data ORDER BY 9*b",  True),
  ("SELECT b FROM data LIMIT 2",  False),
  ("SELECT * FROM data ORDER BY f LIMIT 2",  True),
]

subquery = [
  "SELECT * FROM (SELECT * FROM data)"
  ,
  "SELECT * FROM (SELECT a+b, a FROM data as d1) as d2",
  "SELECT * FROM (SELECT a+b, a FROM data as d1) as d2 WHERE d2.a > 1",
  "SELECT * FROM (SELECT a+b, a FROM data as d1 WHERE d1.a > 1) as d2 WHERE d2.a > 1",
  "SELECT * FROM (SELECT a+b, a FROM data as d1 WHERE d1.a > 2) as d2 WHERE d2.a > 1"
]

gb_bad = [
  "SELECT b FROM data GROUP BY a",
  "SELECT b FROM data GROUP BY 1",
  "SELECT a FROM data GROUP BY a HAVING b > 2"
]

gb_qs = [
  "SELECT 1 FROM data GROUP BY a",
  "SELECT a, sum(b) FROM data GROUP BY a HAVING a = 1",
  "SELECT sum(b) FROM data GROUP BY a HAVING a = 1",
  "SELECT a FROM data GROUP BY a",
  "SELECT sum(b) FROM data GROUP BY a",
  "SELECT sum(a) FROM data GROUP BY a",
  "SELECT sum(b) FROM data GROUP BY a HAVING 1 = 1",
  "SELECT a, sum(b) FROM data GROUP BY a HAVING a = a",
  "SELECT a, sum(b) FROM data GROUP BY a HAVING sum(b) > 2"
]

hashjoins = [
  (HashJoin(Scan('data', 'd1'), Scan('data', "d2"),
          list(map(cond_to_func, ["d1.a", "d2.c"]))),
   "SELECT * from data d1, data d2 where d1.a = d2.c"),
  (HashJoin(Scan('data', 'd1'), Filter(Scan('data', "d2"), cond_to_func("d2.c = 1")),
            list(map(cond_to_func, ["d1.a", "d2.c"]))),
   "SELECT * from data d1, (SELECT * FROM data WHERE c = 1) d2 where d1.a = d2.c")
    
]

full_qs = [
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

phase1 = lambda v: "test_phase1"
phase2 = lambda v: "test_phase2"

@pytest.mark.parametrize("q", limit_qs, ids=phase1)
@pytest.mark.usefixtures('context')
def test_q_limit(context, q):
  run_query(context, q)

@pytest.mark.parametrize("q", gb_qs + subquery + full_qs, ids=phase2)
@pytest.mark.usefixtures('context')
def test_q_phase2(context, q):
  run_query(context, q)



@pytest.mark.parametrize("q", limit_badqs, ids=phase1)
@pytest.mark.usefixtures('context')
def test_badq_limit(context, q):
  with pytest.raises(Exception):
    run_query(context, q)

@pytest.mark.parametrize("q", gb_bad, ids=phase2)
@pytest.mark.usefixtures('context')
def test_badq_phase2(context, q):
  with pytest.raises(Exception):
    run_query(context, q)



@pytest.mark.parametrize("q,ordered", project_qs_ordered, ids=phase2)
@pytest.mark.usefixtures('context')
def test_q_ordered(context, q, ordered):
  run_query(context, q, ordered)



@pytest.mark.parametrize("plan,q", distinct_inputs, ids=phase1)
@pytest.mark.usefixtures('context')
def test_plan_distinct(context, plan, q):
  """
  plan: the plan to evaluate
  q: query to run on sqlite
  """
  rows1 = run_plan(context, plan)
  rows2 = run_sqlite_query(context, q)  
  compare_results(context, rows1, rows2, False)


@pytest.mark.parametrize("plan,q", hashjoins, ids=phase2)
@pytest.mark.usefixtures('context')
def test_plan_hj(context, plan, q):
  """
  plan: the plan to evaluate
  q: query to run on sqlite
  """
  rows1 = run_plan(context, plan)
  rows2 = run_sqlite_query(context, q)  
  compare_results(context, rows1, rows2, False)



@pytest.mark.parametrize("plan,schema", project_schema, ids=phase2)
@pytest.mark.usefixtures('context')
def test_schema(context, plan, schema):
  plan = context['opt'](plan)
  check_schema(context, schema, plan.schema)


@pytest.mark.parametrize("dummy", [''], ids=phase2)
@pytest.mark.usefixtures('context')
def test_groupby(context, dummy):
  q_res = [(2.0, 200, 10), (3.0, 220, 10)]
  schema = Schema([ Attr("c", "num"), Attr("sum", "num"), Attr("count", "num") ])
  group_term_schema = Schema([Attr("c", "num")])

  q = "SELECT c+2 as c, sum(f) as sum, count(a) as count FROM data GROUP BY c"
  plan = parse(q).to_plan()
  plan = context['opt'](plan)

  check_schema(context, schema, plan.schema)
  check_schema(context, group_term_schema, plan.group_term_schema)

  res = run_plan(context, plan)
  compare_results(context, q_res, res, False)


