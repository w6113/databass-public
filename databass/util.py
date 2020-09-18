import numbers
from functools import partial

def pickone(l, attr):
  return [(i and getattr(i, attr) or None) for i in l]

def flatten(list_of_lists):
  return [item for sublist in list_of_lists for item in sublist]

def deduplicate(iterable, keyf=None):
  if keyf is None:
    keyf = str
  seen = set()
  for i in iterable:
    key = keyf(i)
    if key in seen: continue
    yield i
    seen.add(key)

def cond_to_func(expr_or_func):
  """
  Helper function to help automatically interpret string expressions 
  when you manually construct a query plan.
  """
  from .parse_expr import parse

  # if it's a function already, then we're good
  if hasattr(expr_or_func, "__call__"):
    return expr_or_func
  # otherwise, parse it as a string
  if isinstance(expr_or_func, str):
    return parse(expr_or_func)
  raise Exception("Can't interpret as expression: %s" % expr_or_func)

def cache(func):
  class Cache(object):
    def __init__(self):
      self.cache = None
  _cache = Cache()
  def wrapper(*args, **kwargs):
    if _cache.cache is None:
      _cache.cache = func(*args, **kwargs)
    return _cache.cache
  return wrapper

def guess_type(v):
  if v is not None and isinstance(v, numbers.Number):
    return "num"
  if isinstance(v, list):
    return "list"
  return "str"

def print_qplan_pointers(q):
  queue = [q]
  while queue:
    op = queue.pop(0)
    print("%d: %s" % (op.id, op))
    for cop in op.children():
      print("\t%d\t->\t%d" % (op.id, cop.id))
    queue.extend(op.children())

class OBTuple(object):
  '''
  Function to order tuples based on asc or desc order
  '''
  def __init__(self, vals, ascdesc):
    self.vals = vals
    self.ascdesc = ascdesc

  def __lt__(self, o):
    return self.__cmp__(o) < 0

  def __eq__(self, o):
    return self.__cmp__(o) == 0

  def __cmp__(self, other):
    for reverse, v1, v2 in zip(self.ascdesc, self.vals, other.vals):
      if v1 < v2:
        return -1 * reverse
      elif v1 > v2:
        return 1 * reverse
      else: # v1 == v2
        continue

    return 0
