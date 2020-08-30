import re
import math
import numpy as np
from .ops import *
from .udfs import *

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor


grammar = Grammar(
    r"""
    exprstmt = ws expr ws
    expr     = btwnexpr / biexpr / unexpr / value
    btwnexpr = value BETWEEN wsp value AND wsp value
    biexpr   = value ws binaryop ws expr
    unexpr   = unaryop expr
    value    = parenval / 
               number /
               boolean /
               function /
               col_ref /
               string /
               attr
    parenval = "(" ws expr ws ")"
    function = fname "(" ws arg_list? ws ")"
    arg_list = expr (ws "," ws expr)*
    number   = ~"\d*\.?\d+"i
    string   = ~"\'[^\']*\'"i
    col_ref  = (name ".")? name
    attr     = ~"\w[\w\d]*"i
    name     = ~"[a-zA-Z]\w*"i
    fname    = ~"\w[\w\d]*"i
    boolean  = "true" / "false"
    compound_op = "UNION" / "union"
    binaryop = "+" / "-" / "*" / "/" / "==" / "=" / "<>" /
               "<=" / ">=" / "<" / ">" / "and" / "or"
    unaryop  = "+" / "-" / "not"
    ws       = ~"\s*"i
    wsp      = ~"\s+"i

    AND = wsp ("AND" / "and")
    BETWEEN = wsp ("BETWEEN" / "between")
    """)

def flatten(children, sidx, lidx):
  """
  Helper function used in Visitor to flatten and filter 
  lists of lists
  """
  ret = [children[sidx]]
  rest = children[lidx]
  if not isinstance(rest, list): rest = [rest]
  ret.extend(list(filter(bool, rest)))
  return ret


class Visitor(NodeVisitor):
  """
  Each expression of the form

      XXX = ....
  
  in the grammar can be handled with a custom function by writing 
  
      def visit_XXX(self, node, children):

  You can assume the elements in children are the handled 
  versions of the corresponding child nodes
  """
  grammar = grammar

  def visit_name(self, node, children):
    return node.text

  def visit_col_ref(self, node, children):
    tname = children[0]
    if not tname: 
      tname = None
    return Attr(children[1], None, tname)

  def visit_attr(self, node, children):
    return Attr(node.text)

  def visit_binaryop(self, node, children):
    return node.text

  def visit_biexpr(self, node, children):
    return Expr(children[2], children[0], children[-1])

  def visit_unaryop(self, node, children):
    return node.text

  def visit_unexpr(self, node, children):
    return Expr(children[0], children[1])

  def visit_btwnexpr(self, node, children):
    v1, v2, v3 = children[0], children[3], children[-1]
    return Between(v2, v1, v3)

  def visit_expr(self, node, children):
    return children[0]

  def visit_function(self, node, children):
    fname = children[0]
    arglist = children[3]
    f = UDFRegistry.registry()[fname]
    if not f:
      raise Exception("Function %s not found" % fname)
    if f.is_agg: 
      return AggFunc(f, arglist)
    return ScalarFunc(f, arglist)

  def visit_fname(self, node, children):
    return node.text

  def visit_arg_list(self, node, children):
    return flatten(children, 0, 1)
  
  def visit_number(self, node, children):
    return Literal(float(node.text))

  def visit_string(self, node, children):
    return Literal(node.text[1:-1])

  def visit_parenval(self, node, children):
    return Paren(children[2])

  def visit_value(self, node, children):
    return children[0]

  def visit_boolean(self, node, children):
    if node.text == "true":
      return Literal(True)
    return Literal(False)

  def generic_visit(self, node, children):
    f = lambda v: v and (not isinstance(v, str) or v.strip())
    children = list(filter(f, children))
    if len(children) == 1: 
      return children[0]
    return children

def parse(s):
  return Visitor().parse(s)

