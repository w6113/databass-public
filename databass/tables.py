import pandas
import numbers
import os
from .stats import Stats
from .tuples import *
from .exprs import Attr

class Table(object):
  """
  A table consists of a schema, and a way to iterate over the rows.
  Specific subclasses can enforce the specific row representations they want 
  e.g., columnar, row-wise, bytearrays, indexes, etc
  """
  id = 0

  def __init__(self, schema):
    self.schema = schema
    self.id = Table.id
    Table.id += 1

    self._stats = None


  @staticmethod
  def from_rows(rows):
    if not rows:
      return InMemoryTable(Schema([]), rows)
    schema = Table.schema_from_rows(list(rows[0].keys()), rows)
    return InMemoryTable(schema, rows)

  @property
  def stats(self):
    if self._stats is None:
      self._stats = Stats(self)
    return self._stats


  def col_values(self, field):
    idx = self.schema.idx(Attr(field.aname))
    return [row[idx] for row in self]

  def __iter__(self):
    yield


class InMemoryTable(Table):
  """
  Row-oriented table that stores its data as an array in memory.
  """
  def __init__(self, schema, rows):
    super(InMemoryTable, self).__init__(schema)
    self.rows = rows
    self.attr_to_idx = { a.aname: i 
        for i,a in enumerate(self.schema)}

  def __iter__(self):
    for row in self.rows:
      yield ListTuple(self.schema, row)

