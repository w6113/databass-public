from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain

class Limit(UnaryOp):
  def __init__(self, c, limit, offset=0):
    """
    @c            child operator
    @limit        number of tuples to return
    """
    super(Limit, self).__init__(c)
    self.limit = limit
    if isinstance(self.limit, numbers.Number):
      self.limit = Literal(self.limit)

    self._limit =  int(self.limit(None))
    if self._limit < 0:
      raise Exception("LIMIT must not be negative: %d" % l)

    self.offset = offset or 0
    if isinstance(self.offset, numbers.Number):
      self.offset = Literal(self.offset)

    self._offset = int(self.offset(None))
    if self._offset < 0:
      raise Exception("OFFSET must not be negative: %d" % o)


  def __iter__(self):
    """
    LIMIT should skip <offset> number of rows, and yield at most <limit>
    number of rows
    """
    # A1: IMPLEMENT THIS
    raise Exception("Not implemented")

  def __str__(self):
    return "LIMIT(%s OFFSET %s)" % (self.limit, self.offset)


