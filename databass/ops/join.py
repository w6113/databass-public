from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain



class From(NaryOp):
  """
  Logical FROM operator. 
  Optimizer will expand it into a join tree
  """

  def __init__(self, cs, predicates):
    super(From, self).__init__(cs)
    self.predicates = predicates

  def to_str(self, ctx):
    name = "From(%s)" % " and ".join(map(str, self.predicates))
    with ctx.indent(name):
      for c in self.cs:
        c.to_str(ctx)

class Join(BinaryOp):
  pass

class ThetaJoin(Join):
  """
  Theta Join is tuple-nested loops join
  """
  def __init__(self, l, r, cond=Literal(True)):
    """
    @l    left (outer) subplan of the join
    @r    right (inner) subplan of the join
    @cond an Expr object whose output will be interpreted
          as a boolean
    """
    super(ThetaJoin, self).__init__(l, r)
    self.cond = cond

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])

    for lrow in self.l:
      for rrow in self.r:
        # populate intermediate tuple with values
        irow.row[:len(lrow.row)] = lrow.row
        irow.row[len(lrow.row):] = rrow.row

        if self.cond(irow):
          yield irow

  def __str__(self):
    return "THETAJOIN(ON %s)" % (str(self.cond))


