"""
This file contains the abstract classes for Operators in query plans.  
Logical and physical relational operators, as well as expressions are all represented as subclasses of the classes defined here.

The classes provide default constructors, and methods for travering subplans
"""
import csv
import math
import types
import inspect
import pandas
import numbers
import numpy as np
from itertools import chain
from collections import defaultdict

class Op(object):
  """
  Base class

  all operators have a single parent
  an operator may have multiple children
  """
  _id = 0

  def __init__(self):
    self.p = None
    self.id = Op._id
    Op._id += 1

    # For relational operators, this stores the operator's output schema,
    # which should be computed by a called to self.init_schema()
    self.schema = None

  def __hash__(self):
    return hash(self.id)
    #return hash(str(self))

  def __eq__(self, o):
    return o and hash(self) == hash(o)

  def remove(self):
    if isinstance(self, BinaryOp):
      raise Exception("remove() only valid for unary operators")
    elif isinstance(self, NaryOp):
      if len(self.children()) > 1:
        raise Exception("remove() only valid for unary operators")
      if len(self.children()) == 1:
        c = self.children()[0]
      else:
        c = None
    else:
      c = self.c

    p = self.p
    if c:
      c.p = p

    if not p: return self
    if isinstance(p, UnaryOp):
      p.c = c
    elif isinstance(p, BinaryOp):
      if p.l == self:
        p.l = c
      elif p.r == self:
        p.r = c
    elif isinstance(p, NaryOp):
      if self in p.cs:
        p.cs[p.cs.index(self)] = c
    return self

  def replace(self, newop):
    """
    Replace myself with @newop in the tree.
    The key is to reassign the parent and child pointers appropriately.
    """
    if not self.p: return
    p = self.p
    newop.p = p
    if isinstance(p, UnaryOp):
      p.c = newop
    if isinstance(p, BinaryOp):
      if p.l == self:
        p.l = newop
      elif p.r == self:
        p.r = newop
    if isinstance(p, NaryOp):
      if self in p.cs:
        p.cs[p.cs.index(self)] = newop

  def is_ancestor(self, anc_or_func):
    """
    Check if @anc is an ancestor of the current operator
    """
    if isinstance(anc_or_func, types.FunctionType):
      f = anc_or_func
    else:
      f = lambda n: n == anc_or_func

    n = self
    seen = set()
    while n and n not in seen:
      seen.add(n)
      if f(n):
        return True
      n = n.p
    return False

  def children(self):
    """
    return child operators that are relational operations (as opposed to Expressions)
    """
    children = []
    if self.is_type(UnaryOp):
      children = [self.c]
    if self.is_type(BinaryOp):
      children = [self.l, self.r]
    if self.is_type(NaryOp):
      children = list(self.cs)
    return list(filter(bool, children))

  def referenced_op_children(self):
    """
    return all Op subclasses referenced by current operator
    """
    children = []
    for key, attrval in list(self.__dict__.items()):
      if key in ["p"]:   # avoid following cycles
        continue
      if not isinstance(attrval, list):
        attrval = [attrval]
      for v in attrval:
        if v and isinstance(v, Op):
          children.append(v)
    return children

  def traverse(self, f, path=None, until=lambda n: False):
    """
    Visit all referenced Op subclasses and call f()

    @f a function that takes as input the current operator and 
       the path to the operator.  f() can return False to
       stop traversing subplans.
    """
    if path is None:
      path = []
    path = path + [self]
    if f(self, path) == False:
      return
    for child in self.referenced_op_children():
      if until and until(child): continue
      child.traverse(f, path, until)

  def is_type(self, klass_or_names):
    """
    Check whether self is a subclass of argument

    @klass_or_names an individual or list of classes
    """
    if not isinstance(klass_or_names, list):
      klass_or_names = [klass_or_names]
    names = [kn for kn in klass_or_names if isinstance(kn, str)]
    klasses = [kn for kn in klass_or_names if isinstance(kn, type)]
    return (self.__class__.__name__ in names or
           any([isinstance(self, kn) for kn in klasses]))

  def collect(self, klass_or_names, until=lambda n: False):
    """
    Returns all operators in the subplan rooted at the current object
    that has the same class name, or is a subclass, as the arguments
    does not traverse subplans that match the @until function, if provided

    @until function that takes an Op as input, and returns True if it should stop traversing,
           and False otherwise
    """
    ret = []
    if not isinstance(klass_or_names, list):
      klass_or_names = [klass_or_names]
    names = [kn for kn in klass_or_names if isinstance(kn, str)]
    klasses = [kn for kn in klass_or_names if isinstance(kn, type)]

    def f(node, path):
      if node and (
          node.__class__.__name__ in names or
          any([isinstance(node, kn) for kn in klasses])):
        ret.append(node)
    self.traverse(f, until=until)
    return ret

  def collectone(self, klassnames, until=lambda n: False):
    """
    Helper function to return an arbitrary operator that matches any of the
    klass names or klass objects, or None
    """
    l = self.collect(klassnames, until)
    if l:
      return l[0]
    return None

  def init_schema(self):
    """
    Use operator's child schemas to infer this operator's output schema

    NOTE: this is responsible for setting self.schema appropriately
    """
    raise Exception("Op.schema() not implemented for %s" % self)

  def compile_exprs(self, ctx, exprs):
    """
    Helper function for compilation.  Compiles a list
    of Expr objects, and returns the temporary variables
    where the result of each Expr is stored.

    @ctx    Context
    @exprs  list of Expr objects
    """
    v_in, _ = ctx.pop_io_vars()
    vlist = []
    for e in exprs:
      v_e = ctx.new_var("tmp")
      ctx.add_io_vars(v_in, v_e)
      e.compile(ctx)
      vlist.append(v_e)
    return vlist

  def to_str(self, ctx):
    """
    Helper class to recursively traverse and turn operator subplan into text

    @ctx Context object.
    """
    return ""

  def pretty_print(self):
    """
    Pretty print the query plan rooted at self
    """
    from .context import Context
    ctx = Context()
    self.to_str(ctx)
    return ctx.compiler.compile_to_code()

  def __str__(self):
    printable_attrs = [
          "tablename", 
          "alias",
          "aliases",
          "join_attrs",
          "ascdesc", 
          "cond", 
          "limit"]
    name = self.__class__.__name__
    arg = []
    for k, v in self.__dict__.items():
      if (k in printable_attrs or "expr" in k):
        if isinstance(v, list):
          arg.append(", ".join(map(str, v)))
        else:
          arg.append(str(v))
    return "%s(%s)" % (name, ", ".join(arg))

  def produce(self, ctx):
    """
    Implementation of compilation's produce phase
    """
    raise Exception("Op.produce not implemented")

  def consume(self, ctx):
    """
    Implementation of compilation's consume phase
    """
    raise Exception("Op.consume not implemented")

  def consume_parent(self, ctx):
    if self.p:
      self.p.consume(ctx)


class UnaryOp(Op):
  def __init__(self, c=None):
    super(UnaryOp, self).__init__()
    self.c = c
    if c:
      c.p = self

  def init_schema(self):
    """
    By default, a unary operator such as WHERE or ORDERBY shares the same schema as its child
    """
    if self.c:
      self.schema = self.c.schema.copy()
    else:
      from .schema import Schema
      self.schema = Schema([])
    return self.schema

  def __setattr__(self, attr, v):
    super(UnaryOp, self).__setattr__(attr, v)
    if attr == "c" and v:
      self.c.p = self

  def to_str(self, ctx):
    with ctx.indent(str(self)):
      if self.c:
        self.c.to_str(ctx)

 
class BinaryOp(Op):
  def __init__(self, l, r):
    super(BinaryOp, self).__init__()
    self.l = l
    self.r = r
    if l:
      l.p = self
    if r:
      r.p = self

  def init_schema(self):
    """
    By default, binary operators concatenate its child schemas
    """
    from .schema import Schema
    attrs = []
    for attr in chain(self.l.schema, self.r.schema):
      attrs.append(attr.copy())
    self.schema = Schema(attrs)
    return self.schema


  def __setattr__(self, attr, v):
    super(BinaryOp, self).__setattr__(attr, v)
    if attr in ("l", "r") and v:
      v.p = self

  def to_str(self, ctx):
    with ctx.indent(str(self)):
      if self.l:
        self.l.to_str(ctx)
      if self.r:
        self.r.to_str(ctx)

   
class NaryOp(Op):
  def __init__(self, cs):
    super(NaryOp, self).__init__()
    self.cs = cs
    for c in cs:
      if c:
        c.p = self

  def init_schema(self):
    """
    By default, nary operators concatenate its child schemas
    """
    from .schema import Schema
    schema = Schema([])
    for c in self.cs:
      schema.attrs.extend(c.schema.copy().attrs)
    self.schema = schema
    return schema

  def __setattr__(self, attr, v):
    super(NaryOp, self).__setattr__(attr, v)
    if attr == "cs":
      for c in self.cs:
        c.p = self
 
  def to_str(self, ctx):
    with ctx.indent(str(self)):
      for c in self.cs:
        c.to_str(ctx)


