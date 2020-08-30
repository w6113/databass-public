"""
A schema consists of a list of Attr instances
"""

from .exprs import *

class Schema(object):
  def __init__(self, attrs):
    self.attrs = attrs or []

  def __iter__(self):
    return iter(self.attrs)

  def get_type(self, attr):
    attr = attr.copy()
    attr.tablename = None
    for a in self.attrs:
      if a.matches(attr):
        return a.typ
    return None

  def idx(self, attr):
    """
    @attr Attr instance to look up
    """
    attr = attr.copy()
    for i, a in enumerate(self.attrs):
      if a.matches(attr):
        return i
    raise Exception("Schema.idx: could not find %s in schema: %s" % (attr, self))

  def copy(self):
    """
    Deep copy of this schema instance
    """
    return Schema([a.copy() for a in self.attrs])

  def set_tablename(self, tablename=None):
    for a in self.attrs:
      a.tablename = tablename 
    return self

  def __contains__(self, attr):
    if isinstance(attr, Attr):
      return any(a.matches(attr) for a in self.attrs)
    if isinstance(attr, str):
      return any(a.aname == attr for a in self.attrs)
    return False


  def __getitem__(self, key):
    for a in self.attrs:
      if isinstance(key, str):
        if a.aname == key:
          return a
      else:
        if a.matches(key):
          return a
    return None

  def __str__(self):
    return ", ".join(map(str, self.attrs))

