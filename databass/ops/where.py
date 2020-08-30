from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain

class Filter(UnaryOp):
  def __init__(self, c:Op, cond:Expr):
    """
    @c            child operator
    @cond         boolean Expression 
    """
    super(Filter, self).__init__(c)
    self.cond = cond

  def __iter__(self):
    for row in self.c:
      if self.cond(row):
        yield row


