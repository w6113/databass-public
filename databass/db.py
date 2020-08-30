from .util import guess_type
from .schema import Schema
from .tables import *
import pandas
import numbers
import os

openfile = open



def infer_schema_from_df(df):
  from .exprs import guess_type, Attr
  attrs = list(df.columns)
  schema = Schema([])
  row = None
  if df.shape[0]:
    row = df.iloc[0].to_dict()

  for attr in attrs:
    typ = "str"
    if row:
      typ = guess_type(row[attr])
    schema.attrs.append(Attr(attr, typ))
  return schema



class Database(object):
  _db = None

  """
  Manages all tables registered in the database
  """
  def __init__(self):
    self.registry = {}
    self.id2table = {}
    self._df_registry = {}
    self.function_registry = {}
    self.table_function_registry = {}
    self.setup()

  @staticmethod
  def db():
    if not Database._db:
      Database._db = Database()
    return Database._db

  def setup(self):
    """
    Walks all CSV files in the current directory and registers
    them in the database
    """
    for root, dirs, files in os.walk("."):
      for fname in files:
        if fname.lower().endswith(".csv"):
          self.register_file_by_path(os.path.join(root, fname))

  def register_file_by_path(self, path):
    root, fname = os.path.split(path)
    tablename, _ = os.path.splitext(fname)
    fpath = os.path.join(root, fname)
    loaded = False
    exception = None
    for sep in [',', '|', '\t']:
      df = None
      try:
        with openfile(fpath) as f:
          df = pandas.read_csv(f, sep=sep)
      except Exception as e:
        exception = e

      if df is not None:
        self.register_dataframe(tablename, df)
        loaded = True
        break

    if not loaded:
      print("Failed to read data file %s" % (fpath))
      print(exception)


  def register_table(self, tablename, schema, table):
    self.registry[tablename] = table
    self.id2table[table.id] = table

  def register_dataframe(self, tablename, df):
    self._df_registry[tablename] = df
    schema = infer_schema_from_df(df)
    rows = list(df.T.to_dict().values())
    rows = [[row[attr.aname] for attr in schema] for row in rows]
    table = InMemoryTable(schema, rows)
    self.register_table(tablename, schema, table)

  @property
  def tablenames(self):
    return list(self.registry.keys())

  def schema(self, tablename):
    return self[tablename].schema

  def table_by_id(self, id):
    return self.id2table.get(id, None)

  def __contains__(self, tablename):
    return tablename in self.registry

  def __getitem__(self, tablename):
    return self.registry.get(tablename, None)

