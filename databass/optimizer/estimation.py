from ..ops import *
from ..db import Database
from ..util import *
from itertools import *
from collections import *



class Estimator(object):
  """
  A barebones cost estimation module.  
  """
  def __init__(self, db):
    self.db = db
    self.costs = dict()
    self.cards = dict()
    self.DEFAULT_SELECTIVITY = 0.05

  def cost(self, op):
    """
    Recursively estimate cost of query plan
    """
    if op in self.costs:
      return self.costs[op]

    if op.is_type(Scan):
      cost = self.db[op.tablename].stats.card
    elif op.is_type(HashJoin):
      cost = self.cost(op.l) + self.cost(op.r)
      cost += 0.05 * self.card(op)
    elif op.is_type(ThetaJoin):
      cost = self.cost(op.l) + self.card(op.l) * self.cost(op.r)
      cost += 0.05 * self.card(op)
    else:
      cost = sum(map(self.cost, op.children()))

    self.costs[join] = cost
    return cost
    

  def card(self, op):
    if op in self.cards:
      return self.cards[op]

    if op.is_type(Scan):
      card = self.db[op.tablename].stats.card
    elif op.is_type(Join):
      card = self.card(op.l) * self.card(op.r)
      card *= self.selectivity(op)
    elif op.is_type(Filter):
      card = self.card(op.c) * self.selectivity(op)
    else:
      card = self.card(op.c)

    self.cards[op] = card
    return card

  def selectivity(self, op):
    """
    Computes the selectivity of the operator depending on the number of
    tables, the predicate, and the selectivities of the join attributes
    """
    if op.is_type(Scan):
      return 1.0
    if op.is_type(HashJoin):
      # A2: return selectivite for hashjoin.  It is more precise
      #     than for a generic thetajoin
      return 0
    if op.is_type(ThetaJoin):
      return self.selectivity_cond(op.cond)
    if op.is_type(Filter):
      return self.selectivity_cond(op.cond)
    return self.DEFAULT_SELECTIVITY

  def selectivity_cond(self, cond):
    """
    Estimate the selectivity of a predicate condition
    """
    if cond.is_type(Bool):
      return cond(None) * 1.0

    if cond.is_type(Attr):
      return self.selectivity_attr(cond)

    if cond.op == "and":
      # A2: implement selectivity estimation of conjuctive predicates
      #     assuming independence
      pass

    if cond.op == "=":
      # A2: implement selectivity estimation for equality predicate  of the form:
      #        expression = LITERAL
      if cond.r.is_type(Literal):
        # implement me
        pass
      elif cond.l.is_type(Literal):
        # implement me
        pass

    return self.DEFAULT_SELECTIVITY


  def selectivity_attr(self, attr):
    """
    @source the left or right subplan
    @attr  the attribute in the subplan used in the equijoin

    Estimate the selectivity of a join attribute.  
    We make the following assumptions:

    * if the source is not a base table, then the selectivity is 1
    * if the attribute is numeric then we assume the attribute values are
      uniformly distributed between the min and max values.
    * if the attribute is non-numeric, we assume the values are 
      uniformly distributed across the distinct attribute values
    """
    assert(attr.is_type(Attr))

    table = self.db[attr.tablename]
    if not table: return 1.0

    stat = table.stats[attr]
    typ = table.schema.get_type(attr)
    if typ == "num":
      # A2: Write code to estimate the selectivity of the numeric attribute.
      # You can add 1 to the denominator to avoid divide by 0 errors
      sel = 1.0
    elif typ == "str":
      # A2: Write code to estimaote the selectivity of the non-numeric attribute
      sel = 1.0
    else:
      sel = 0.05
    return sel

