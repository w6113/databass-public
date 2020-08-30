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
    # A1: IMPLEMENT THIS
    raise Exception("Not implemented")

