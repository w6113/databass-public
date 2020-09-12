import unittest
import os.path
from sqlalchemy import *
import pandas as pd
import pytest

from databass import *
from databass.ops import *



@pytest.fixture(scope="class")
def setup(request):
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

  request.cls.db = db
  request.cls.sqlite = sqlite
  request.cls.opt = Optimizer(db)
  return sqlite


@pytest.mark.usefixtures('setup')
class TestBase(unittest.TestCase):

  def run_sqlite_query(self, qstr):
    res = self.sqlite.execute(qstr)
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

  def run_databass_query(self, qstr):
    plan = parse(qstr)
    plan = plan.to_plan()
    return self.run_plan(plan)

  def run_plan(self, plan):
    databass_rows = list()
    plan = self.opt(plan)
    for row in plan:
      vals = []
      for v in row:
        if isinstance(v, str):
          vals.append(v)
        else:
          vals.append(float(v))
      databass_rows.append(vals)
    return databass_rows


  def run_query(self, qstr, order_matters=False):
    sqlite_rows = self.run_sqlite_query(qstr)
    databass_rows = self.run_databass_query(qstr)
    self.compare_results(
        sqlite_rows, databass_rows, order_matters)


  def compare_results(self, rows1, rows2, order_matters):
    if not order_matters:
      rows1.sort()
      rows2.sort()
    try:
      self.assertEqual(len(rows1), len(rows2))
      for r1, r2 in zip(rows1, rows2):
        self.assertEqual(list(r1), list(r2))
    except Exception as e:
      print(rows1)
      print()
      print(rows2)
      raise e


  def check_schema(self, schema1, schema2):
    self.assertEqual(len(schema1.attrs), len(schema2.attrs))
    for i, attr in enumerate(schema1.attrs):
      self.assertEqual(attr.aname, schema2.attrs[i].aname)
      self.assertEqual(attr.get_type(), schema2.attrs[i].get_type())


