import unittest
import os.path
from sqlalchemy import *
import pandas as pd
import pytest

from databass import *
from databass.ops import *



# 
#  The following are used for the a1 tests
#
#

@pytest.fixture(scope="session")
def context(request):
  """
  init and attach sqlite
  """
  db = Database.db()
  tablename = "tdata"

  sqlite = create_engine("sqlite://")
  for tname in db.tablenames:
    if tname in db._df_registry:
      db._df_registry[tname].to_sql(tname, sqlite, index=False)

  sqlite.execute("DROP TABLE IF EXISTS tdata")
  df = pd.DataFrame(np.random.randint(0, 100, size=(1000, 4)), columns=list("abcd"))
  db.register_dataframe(tablename, df)
  db._df_registry[tablename].to_sql(tablename, sqlite, index=False)

  return dict(db=db, sqlite=sqlite, opt=Optimizer(db))

def run_sqlite_query(context, qstr):
  res = context['sqlite'].execute(qstr)
  sqlite_rows = list()
  for row in res:
    vals = []
    for v in row:
      if isinstance(v, str):
        vals.append(v)
      else:
        vals.append(float(v))
    sqlite_rows.append(vals)
  return sqlite_rows

def run_databass_query(context, qstr):
  plan = parse(qstr)
  plan = plan.to_plan()
  return run_plan(context, plan)

def run_plan(context, plan):
  databass_rows = list()
  plan = context['opt'](plan)
  for row in plan:
    vals = []
    for v in row:
      if isinstance(v, str):
        vals.append(v)
      else:
        vals.append(float(v))
    databass_rows.append(vals)
  return databass_rows


def run_query(context, qstr, order_matters=False):
  sqlite_rows = run_sqlite_query(context, qstr)
  databass_rows = run_databass_query(context, qstr)
  compare_results(context,
      sqlite_rows, databass_rows, order_matters)


def compare_results(context, rows1, rows2, order_matters):
  if not order_matters:
    rows1.sort()
    rows2.sort()
  try:
    assert(len(rows1) == len(rows2))
    for r1, r2 in zip(rows1, rows2):
      assert(tuple(r1) == tuple(r2))
  except Exception as e:
    print(rows1)
    print()
    print(rows2)
    raise e


def check_schema(context, schema1, schema2):
  assert(len(schema1.attrs) == len(schema2.attrs))
  for i, attr in enumerate(schema1.attrs):
    assert(attr.aname == schema2.attrs[i].aname)
    assert(attr.get_type() == schema2.attrs[i].get_type())


