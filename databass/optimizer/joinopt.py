"""
This file contains the base join optimization class, and an 
exhaustive implementation.

Join optimization takes a From() operator, along with the relevant 
join predicates, as input, and outputs an optimized physical join
plan that is "good".

See the README in this folder for details
"""
from ..ops import *
from ..db import Database
from ..util import *
from ..exprutil import *
from .estimation import *
from itertools import *
from collections import *

class JoinInfo(object):
  """
  Stores best plans for a set of relations (tables or subqueries)
  """
  def __init__(self, rels, predicates):
    """
    @rels list of Scan or subquery operators that this JoinInfo represents
    @predicates all predicates that involve one or more relations in @rels
    """
    self.rels = rels
    self.predicates = predicates

    # sorted so there is a unique hash for the same set of relations
    self.aliases = list(sorted([rel.alias for rel in self.rels]))


    self.best_plan = None
    self.best_cost = float("inf")

    # XXX: other info such as interesting sort orders would go here


  def merge(self, jis):
    """
    @jis one or more JoinInfo objects

    Combines the relations and predicates from self and @jis to create
    new JoinInfo
    """
    if not jis: return self
    preds = set(self.predicates)
    if not isinstance(jis, list):
      jis = [jis]
    for ji in jis:
      preds.update(ji.predicates)
    rels = list(chain(*[ji.rels for ji in jis + [self]]))
    return JoinInfo(rels, preds)

  def overlaps(self, join_info):
    """
    Does self's relations overlap with @join_info's relations?
    """
    return bool(set(self.aliases).intersection(join_info.aliases))

  def __hash__(self):
    return hash(tuple(self.aliases))

  def __contains__(self, table_alias):
    return table_alias in self.aliases



class JoinOpt(object):
  """
  This is the base class for Join Optimization, and contains many helper methods 
  """
  def __init__(self, db):
    self.db = db
    self.estimator = Estimator(db)
    self.plans_tested = 0

  def cost(self, plan):
    """
    Since we want to count the number of times we cost a plan
    use this rather than calling self.estimator.cost directly.
    """
    self.plans_tested += 1
    return self.estimator.cost(plan)

  def fix_parent_pointers(self, plan):
    for c in plan.children():
      c.p = plan
      self.fix_parent_pointers(c)

  def new_join_info(self, rels):
    if isinstance(rels, Op): rels = [rels]
    ji = JoinInfo(rels, [])
    ji.predicates = self.get_join_preds(ji)
    return ji

  def valid_join_impls(self, lji, rji):
    """
    @lji left JoinInfo object
    @rji right JoinInfo object
    @return list of physical plans to cost

    Given the left and right JoinInfo objects, enumerate
    all valid join operators for the pair.

    Note: When an operator is initialized, the constructor
    modifies the child operators' parent pointers.  We don't want 
    this to actually happen since these are just candidate plans.
    lp, rp preserves the original parents so we can restore them
    after constructing the candidate join operators

    When we have found the optimal plan, fix_parent_pointers updates
    the parents to point to the chosen physical operators
    """
    l = lji.best_plan
    r = rji.best_plan
    preds = list(set(lji.predicates).intersection(rji.predicates))

    lp, rp = l.p, r.p
    ret = []
    if not preds:
      ret.append(ThetaJoin(l, r, Bool(True)))
      l.p, r.p = lp, rp
      return ret

    cond = cnf_to_predicate(preds)
    ret.extend([ThetaJoin(l, r, cond), ThetaJoin(r, l, cond)])


    # reset parent pointers
    l.p, r.p = lp, rp
    return ret

  def build_predicate_index(self, preds):
    """
    @preds list of join predicates to index

    Build index to map a pair of tablenames to their join predicates e.g., 
    
      SELECT * FROM A,B WHERE A.a = B.b 
   
    creates the lookup table:
   
      B   --> "A.a = B.b"
      A   --> "A.a = B.b"
   """
    pred_index = defaultdict(list)
    for pred in preds:
      lname = pred.l.tablename
      rname = pred.r.tablename
      pred_index[rname].append(pred)
      pred_index[lname].append(pred)
    return pred_index


  def get_join_preds(self, lji, rji=None):
    """
    @lji  left join info
    @rji  right join info
    Return all predicates that reference left (and possibly right) sides of join
    """
    preds, rpreds = set(), set()
    for alias in lji.aliases:
      preds.update(self.pred_index.get(alias, []))
    if rji:
      for alias in rji.aliases:
        rpreds.update(self.pred_index.get(alias, []))
      preds.intersection_update(rpreds)
    return list(preds)


class JoinOptExhaustive(JoinOpt):
  """
  This is an example implementation of a exhaustive plan optimizer.
  It is slower than the bottom-up Selinnger approach
  that you will implement because it ends up checking the same candidate
  plans multiple times.  

  This code is provided to give you hints about how to use the class 
  methods and implement the bottom-up approach
  """

  def __call__(self, preds, sources):
    self.sources = sources
    self.preds = preds
    self.pred_index = self.build_predicate_index(preds)
    self.plans_tested = 0

    # Initialize single-relations as JoinInfo objects
    jis = []
    for rel in sources:
      ji = self.new_join_info(rel)
      ji.best_plan = rel
      jis.append(ji)

    ji = self.best_plan_exhaustive(jis)
    self.fix_parent_pointers(ji.best_plan)
    return ji.best_plan


  def best_plan_exhaustive(self, jis):
    """
    @jis list of JoinInfo objects used to build a join plan 
    @return JoinInfo containing all relations in @jis

    Recursive function that iteratively picks a single JoinInfo as
    one side of the join, and the best plan for the remaining JoinInfos
    as the other side.
    """
    if len(jis) == 1: return jis[0]

    # initialize JoinInfo for all of the inputs
    best_ji = jis[0].merge(jis[1:])

    for i, ji1 in enumerate(jis):
      rest = jis[:i] + jis[i+1:]
      ji2 = self.best_plan_exhaustive(rest)

      plans = self.valid_join_impls(ji2, ji1)
      for plan in plans:
        cost = self.cost(plan)

        if cost < best_ji.best_cost:
          best_ji.best_plan, best_ji.best_cost = plan, cost

    return best_ji




