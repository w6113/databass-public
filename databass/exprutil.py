from itertools import chain
from .exprs import *

def predicate_to_cnf(e):
  """
  Split AND tree into a list of conditions
  """
  if not e:
    return []
  if e.is_type("Expr") and e.op.lower() == "and":
    return list(chain(*map(predicate_to_cnf, [e.l, e.r])))
  if e.is_type("Paren"):
    return [e.c]
  return [e]

def cnf_to_predicate(conds):
  """
  turn list of conditions into an AND tree
  """
  if not conds: return None
  cond = conds[0]
  for c in conds[1:]:
    cond = Expr("and", cond, c)
  return cond


