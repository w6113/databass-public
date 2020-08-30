"""

  The following are operators for simple Expressions 
  used within relational algebra operators

  e.g.,
     f() 
     1+2
     T.a + 2 / T.b

"""
from .baseops import *
from .util import guess_type


def unary(op, v):
  """
  interpretor for executing unary operator expressions
  """
  if op == "+":
    return v
  raise Exception("unary op not implemented")

def binary(op, l, r):
  """
  interpretor for executing binary operator expressions
  """
  if op == "+": return l + r
  if op == "*": return l * r
  if op == "-": return l - r
  if op == "=": return l == r
  if op == "<>": return l != r
  if op == "!=": return l != r
  if op == "or": return l or r
  if op == "<": return l < r
  if op == ">": return l > r
  raise Exception("binary op not implemented")

class ExprBase(Op):
  id = 0

  @staticmethod
  def next_id():
    ExprBase.id += 1
    return ExprBase.id - 1

  def get_type(self):
    raise Exception("ExprBase.get_type() not implemented")

  def check_type(self):
    """
    @return True if expression type checks, False otherwise
    """
    return True

  def replace(self, newe):
    """
    We override Op.replace because expressions refer to their children
    differently. This is primarily used in parseops.py to deal with
    HAVING clauses, and in the future for plan rewrites.
    """
    if not self.p: return
    p = self.p
    newe.p = p
    if isinstance(p, Expr):
      if p.l == self:
        p.l = newe
      elif p.r == self:
        p.r = newe
    elif isinstance(p, Paren):
      if p.c == self:
        p.c = newe
    else:
      raise Exception("replace() not implemented for %s of type %s" % (self, type(self)))

  @property
  def referenced_attrs(self):
    seen = set()
    for attr in self.collect(Attr):
      if attr.id in seen: 
        continue
      seen.add(attr.id)
      yield attr

  def copy(self):
    return self

  def __str__(self):
    raise Exception("ExprBase.__str__() not implemented")

  def to_str(self, ctx):
    ctx.add_line(str(self))

class Expr(ExprBase):
  boolean_ops = ["and", "or"]
  numeric_ops = ["+", "/", "*", "-", "<", ">", "<=", ">="]

  def __init__(self, op, l, r=None):
    super(Expr, self).__init__()
    self.op = op
    self.l = l
    self.r = r
    self.id = ExprBase.next_id()

    if l:
      l.p = self
    if r:
      r.p = self

  def get_type(self):
    if self.op in Expr.boolean_ops:
      return "num"

    if self.op in Expr.numeric_ops:
      return "num"
    return "str"

  def check_type(self):
    if self.op in Expr.numeric_ops:
      ltyp = self.l.get_type()
      rtyp = self.r.get_type() if self.r else None
      if ltyp != "num":
        return False
      if rtyp is not None and rtyp != "num":
        return False
    return True

  def operand_to_str(self, operand):
    """
    Helper for __str__ that adds parentheses around operands in a smarter way
    """
    s = str(operand)
    if s.startswith('(') and s.endswith(')'):
      return s
    if (isinstance(operand, Literal) or
        isinstance(operand, Attr) or 
        isinstance(operand, Star)):
      return s
    return "(%s)" % s

  def __str__(self):
    op = self.op
    if op == "=": op = "=="
    l = self.operand_to_str(self.l)
    if self.r:
      r = self.operand_to_str(self.r)
      return "%s %s %s" % (l, op, r)
    return "%s%s" % (self.op, l)

  def copy(self):
    r = self.r
    if r: r = r.copy()
    return Expr(self.op, self.l.copy(), r)

  def __call__(self, row, row2=None):
    l = self.l(row)
    if self.r is None:
      return unary(self.op, l)
    r = self.r(row)
    return binary(self.op, l, r)

class Paren(ExprBase):
  def __init__(self, c):
    super(Paren, self).__init__()
    self.c = c
    self.id = ExprBase.next_id()
    if c:
      c.p = self

  def get_type(self):
    return self.c.get_type()

  def __str__(self):
    return "(%s)" % self.c

  def copy(self):
    return Paren(self.c.copy())

  def __call__(self, row, row2=None):
    return self.c(row)


class Between(ExprBase):
  def __init__(self, expr, lower, upper):
    """
    expr BETWEEN lower AND upper
    """
    super(Between, self).__init__()
    self.expr = expr
    self.lower = lower
    self.upper = upper
    self.id = ExprBase.next_id()

    if expr:
      expr.p = self
    if lower:
      lower.p = self
    if upper:
      upper.p = self

  def get_type(self):
    return "num"

  def check_type(self):
    return (self.expr.get_type() == "num" and
        self.lower.get_type() == "num" and
        self.upper.get_type() == "num")

  def __str__(self):
    return "(%s) BETWEEN (%s) AND (%s)" % (
        self.expr, self.lower, self.upper)

  def copy(self):
    return Between(self.expr.copy(), self.lower.copy(), self.upper.copy())

  def __call__(self, row, row2=None):
    e = self.expr(row, row2)
    l = self.lower(row, row2)
    u = self.upper(row, row2)
    return e >= l and e <= u


class AggFunc(ExprBase):
  """
  Expression Wrapper around an AggUDF instance
  """
  def __init__(self, f, args):
    super(AggFunc, self).__init__()
    self.name = f.name
    self.args = args
    self.f = f
    self.id = ExprBase.next_id()

    for a in args:
      a.p = self

  def get_type(self):
    return "num"

  def check_type(self):
    return all(arg.get_type() == "num" for arg in self.args)

  def copy(self):
    args = [a.copy() for a in self.args]
    return AggFunc(self.name, args, self.f)

  @property
  def is_incremental(self):
    return self.f.is_incremental

  def __call__(self, rows, row2=None):
    args = []
    for grow in rows:
      args.append([arg(grow) for arg in self.args])

    # make the arguments columnar:
    #   [ (a,a,a,a), (b,b,b,b) ]
    args = list(zip(*args))
    return self.f(*args)

  def __str__(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)

class IncAggFunc(AggFunc):
  def init(self):
    return self.f.init()
  
  def update(self, state, row):
    args = [arg(row) for arg in self.args]
    return self.f.update(state, *args)
  
  def finalize(self, state):
    return self.f.finalize(state)


class ScalarFunc(ExprBase): 
  def __init__(self, f, args):
    super(ScalarFunc, self).__init__()
    self.name = f.name
    self.args = args
    self.f = f
    self.id = ExprBase.next_id()

    for a in self.args:
      a.p = self

  def get_type(self):
    return "str"

  def copy(self):
    args = [a.copy() for a in self.args]
    return AggFunc(self.name, args, self.f)

  def __call__(self, row, row2=None):
    args = [arg(row) for arg in self.args]
    return f(*args)

  def __str__(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)


class Literal(ExprBase):
  def __init__(self, v):
    super(Literal, self).__init__()
    self.v = v
    self.id = ExprBase.next_id()

  def __call__(self, row, row2=None):
    return self.v

  def get_type(self):
    return guess_type(self.v)

  def __str__(self):
    if isinstance(self.v, str):
      if "'" == self.v[0] and self.v[-1] == "'":
        return self.v
      if '"' == self.v[0] and self.v[-1] == '"':
        return self.v
      if "'" in self.v:
        raise Exception("Databass doesn't support strings that contain \"'\" :(")
      return "'%s'" % self.v
    return str(self.v)

class List(Literal):
  def __init__(self, v):
    super(List, self).__init__(v)

  def get_type(self):
    return "list"

  def __str__(self):
    return "({v})".format(v=", ".join(map(str, self.v)))

class Bool(Literal):
  def __init__(self, v):
    super(Bool, self).__init__(v)

  def get_type(self):
    return "num"

class Date(Literal):
  def __init__(self, v):
    super(Date, self).__init__(v)

  def get_type(self):
    return "num"

  def __str__(self):
    return "date({year}, {month}, {day})".format(
        year=self.v.year,
        month=self.v.month,
        day=self.v.day)


class Attr(ExprBase):
  """
  This class incorporates all uses and representatinos of attribute references in DataBass.
  At its core, it is represented by the tablename, attribute name, and attribute type.

  Attrs is used in two main capacities
  1. Schema definition: defines table name, attribute name, and type
     Here, Attr should have all three field filled after init_schema() 
     is called in optimizer.__call__()
  2. Column Ref in an Expression: reference to an attribute in the operator's
     input tuple that may need to be disambiguated.  Since Attr may only 
     contain the attribute name, the tablename and type need to be inferred 
     during the optimizer's disambiguation process (optimizer.__call__())

  See constructor for additional fields for special cases.
  """

  NUM = "num"
  STR = "str"
  LIST = "list"

  def __init__(self, aname, typ=None, tablename=None, 
      var=None, idx=None):
    super(Attr, self).__init__()
    self.aname = aname
    self.typ = typ or "?"
    self.tablename = tablename

    # Index for accessing attribute value in ListTuple instances.
    # Populated for Attr instances used as expressions.
    #
    # For joins, it is the idx of the left or right tuple that
    # contains the Attribute value.  The join operator will
    # ensure that the Attr gets the appropriate left or right tuple
    #
    # It should be initialized in optimizer.__call__()
    self.idx = idx

    self.id = ExprBase.next_id()

  def get_type(self):
    return self.typ or '?'

  @property
  def is_agg(self):
    return self.barraytyp == True

  @property
  def referenced_attrs(self):
    yield self

  def copy(self):
    attr = Attr(self.aname)
    id = attr.id
    for key, val in self.__dict__.items():
      attr.__dict__[key] = val
    attr.id = id
    return attr

  def matches(self, attr):
    """
    If self can satisfy the @attr argument, where @attr can be less specific 
    (e.g., tablename and typ are unbound)

    Typically, self will be a schema attribute, and @attr will be an attr reference
    from an expression.
    """
    if attr is None: 
      return False
    if attr.tablename and attr.tablename != self.tablename:
      return False
    if attr.typ and attr.typ != "?" and attr.typ != self.typ:
      return False
    return self.aname == attr.aname

  def __call__(self, row, *args):
    return row[self.idx]

  def __str__(self):
    s = ".".join(filter(bool, [self.tablename, self.aname]))
    #return s
    return ":".join(filter(bool, [s, self.typ]))


class Star(ExprBase):
  def __init__(self, tablename=None):
    self.tablename = tablename

  def __call__(self, row):
    return row

  def __str__(self):
    if self.tablename:
      return "%s.*" % self.tablename
    return "*"

