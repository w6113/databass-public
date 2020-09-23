from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain



class Distinct(UnaryOp):
  def __iter__(self):
    """
    It is OK to use hash(row) to check for equivalence between rows
    """
    seen = set()
    for row in self.c:
      key = hash(row)
      if key in seen: 
        continue

      yield row
      seen.add(key)

