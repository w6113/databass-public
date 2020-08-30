from ..ops import *
from ..parseops import *
from ..db import Database
from ..util import *
from .joinopt import *
from .selinger import *
from itertools import *
from collections import *


class Optimizer(object):
  """
  The optimizer takes as input a query plan, and provides three functionalities:

  1. Initialize plan: call init_schema() on the operators from bottom up
  2. Disambiguate all attribute references in expressions into indexes into
     the child operator's output tuple schema
  3. Optimize the query plan by replacing the N-ary From operator with a join 
     plan.  This involves performing Selinger-style join ordering 
     Note that this step may create new operators, so the new physical plan needs
     to be initialized and disambiguated again.
  """

  def __init__(self, db=None, join_optimizer_klass=JoinOptExhaustive):
    self.db = db or Database.db()
    self.join_optimizer = join_optimizer_klass(self.db)

  def __call__(self, op):
    if not op: return None

    op = self.initialize_and_resolve(op)

    # Apply join Optimization to each From operator, bottom up
    while op.collectone("From"):
      op = self.expand_from_clause(op)

    # Join may have added new nodes, need to update
    # operator schemas and Attr index references
    op = self.initialize_and_resolve(op)
    self.verify_attr_refs(op)
    return op

  def collect_from_clauses(self, op, froms=None):
    """
    Post-order traversal
    """
    froms = [] if froms is None else froms
    for c in op.children():
      self.collect_from_clauses(c, froms)

    if op.is_type(From):
      froms.append(op)
    return froms

  def initialize_and_resolve(self, op):
    """
    Recursively initialize the schemas for plan operators,
    and resolve Attr indexes (see resolve_attr_idxs)

    We don't do this in parseops.py because we need the actual query plan
    to compute the schema for every single operator, whereas the parse tree
    only computes schemas for each query fragment
    """
    for c in op.children():
      if not c.is_type([POp, ExprBase]):
        self.initialize_and_resolve(c)
    op.init_schema()

    if op.is_type([ThetaJoin,Filter]):
      # ThetaJoin internally concats the left and right tuples into an
      # intermediate tuple.  Its schema is also op.schema
      self.resolve_attr_idxs(op.schema, op.cond)
    elif op.is_type(HashJoin):
      self.resolve_attr_idxs(op.l.schema, op.join_attrs[0])
      self.resolve_attr_idxs(op.r.schema, op.join_attrs[1])
    elif op.is_type(OrderBy):
      self.resolve_attr_idxs(op.c.schema, op.order_exprs)
    elif op.is_type(Project):
      self.resolve_attr_idxs(op.c.schema, op.exprs)
    elif op.is_type(GroupBy):
      self.resolve_attr_idxs(op.c.schema, op.group_exprs)
      self.resolve_attr_idxs(op.c.schema, op.group_attrs)
      for e in op.project_exprs:
        if e.is_type(AggFunc):
          self.resolve_attr_idxs(op.c.schema, e)
        else:
          self.resolve_attr_idxs(op.group_term_schema, e)

    return op

  def find_idx(self, schema, a):
    found = []
    for i, a2 in enumerate(schema):
      if a2.matches(a):
        found.append(i)

    if len(found) == 1:
      return found[0]

    if len(found) > 1:
      raise Exception("%s is ambiguous.  Matched %s of %s" % (a, found, schema))
    raise Exception("%s not found in child's schema %s" % (a, schema))

  def resolve_attr_idxs(self, schema, exprs):
    """
    Update referenced Attr objects with its index into
    its child operator's output tuple schema
    """
    if isinstance(exprs, list):
      for e in exprs:
        self.resolve_attr_idxs(schema, e)
    else:
      for a in exprs.referenced_attrs:
        a.idx = self.find_idx(schema, a)

  def verify_attr_refs(self, root):
    """Verify that all attributes are bound"""
    for attr in root.collect(Attr):
      if attr.idx is None:
        raise Exception("Attr %s:%s not within scope" % (attr, attr.id))

  def expand_from_clause(self, op):
    """
    Replace the first From operator under op with a join tree
    The algorithm is as follows

    0. Find first From operator F
    1. Find all binary expressions in any Where clause (Filter operator)
       that is an ancestor of F
    2. Keep the equality join predicates that only reference tables in
       the operator F
    3. Pick a join order 
    """

    # pick the first From clause to replace with join operators
    fromop = self.collect_from_clauses(op)[0]
    sources = fromop.cs
    preds = fromop.predicates
    sourcealiases = [s.alias for s in sources]

    if not sources:
      fromop.replace(DummyScan())
      return op

    join_tree = self.join_optimizer(preds, sources)
    # xxx: better plan for debug logging.  use logger?
    #print("Tried %d plans" % self.join_optimizer.plans_tested)

    if op == fromop:
      return join_tree
    fromop.replace(join_tree)
    return op

