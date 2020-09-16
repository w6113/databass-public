"""
This file contains the grammar and code for parsing a SQL query
string into a tree of operators defined in ops.py

The key thing this DOES NOT do is convert the FROM clause
into a tree of Join operators.  This is performed in
optimizer.py because we need additional logic to figure out
whether or not tables have join conditions
"""
import re
import math
import numpy as np
from .ops import *
from .udfs import *
from .parseops import *

from dateutil.parser import parse as parsedate
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor


grammar = Grammar(
    r"""
    query    = ws select_cores orderby? limit? ws
    select_cores   = select_core (compound_op select_core)*
    select_core    = SELECT distinct_clause? wsp select_results from_clause? where_clause? gb_clause? having_clause?
    select_results = select_result (ws "," ws select_result)*
    select_result  = sel_res_all_star / sel_res_tab_star / sel_res_val / sel_res_col 
    sel_res_tab_star = name ".*"
    sel_res_all_star = "*"
    sel_res_val    = exprand (AS wsp name)?
    sel_res_col    = col_ref (AS wsp name)
    distinct_clause = wsp ("DISTINCT" / "distinct")

    from_clause    = FROM join_source
    join_source    = ws single_source (ws "," ws single_source)*
    single_source  = source_func / source_table / source_subq 
    source_table   = table_name (AS wsp name)?
    source_subq    = "(" ws query ws ")" (AS wsp name)?
    source_func    = function (AS wsp name)?

    where_clause   = WHERE wsp exprand

    gb_clause      = GROUP BY group_clause
    group_clause   = grouping_term (ws "," grouping_term)*
    grouping_term  = ws expr
    having_clause  = HAVING wsp exprand

    orderby        = ORDER BY ordering_term (ws "," ordering_term)*
    ordering_term  = ws expr (ASC/DESC)?

    limit          = LIMIT wsp expr (OFFSET wsp expr)?

    col_ref        = (table_name ".")? column_name



    exprand  = expr (AND wsp expr)*
    expr     = btwnexpr / inexpr / biexpr / unexpr / value
    inexpr   = value IN wsp value
    btwnexpr = value BETWEEN wsp value AND wsp value
    biexpr   = value ws binaryop_no_andor ws expr
    unexpr   = unaryop expr
    value    = parenq / 
               parenval / 
               listval /
               number /
               boolean /
               date / 
               function /
               col_ref /
               string /
               attr
    parenval = "(" ws expr ws ")"
    parenq   = "(" query ")" 
    listval  = "(" ws expr (ws "," ws expr)* ws ")"
    function = fname "(" ws arg_list? ws ")"
    arg_list = expr (ws "," ws expr)*
    number   = ~"\d*\.?\d+"i
    string   = ~"([\"\'])(\\\\?.)*?\\1"i
    attr     = ~"\w[\w\d]*"i
    fname    = ~"\w[\w\d]*"i
    boolean  = "true" / "false"
    date     = ("date" / "DATE") wsp dateregex
    compound_op = "UNION" / "union"
    binaryop = "+" / "-" / "*" / "/" / "==" / "=" / "<>" / "!=" / 
               "<=" / ">=" / "<" / ">" / "and" / 
               "AND" / "or" / "OR" / "like" / "LIKE" 
    binaryop_no_andor = "+" / "-" / "*" / "/" / "==" / "=" / "<>" / "!=" / 
               "<=" / ">=" / "<" / ">" / "or " / "OR " / "like " / "LIKE " 
    unaryop  = "+" / "-" / "not" / "NOT"
    ws       = ~"\s*"i
    wsp      = ~"\s+"i

    dateregex  = ~"'^(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))\1|(?:(?:29|30)(\/|-|\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:29(\/|-|\.)(?:0?2|(?:Feb))\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\4(?:(?:1[6-9]|[2-9]\d)?\d{2})$'"
    name       = ~"[a-zA-Z\_][\w\_]*"i /  ~"`[a-zA-Z][\w\.\-\_\:\*]*`"i / ~"\[[a-zA-Z][\w\.\-\_\:\*]*\]"i 
    table_name = name
    column_name = name

    ADD = wsp ("ADD" / "and")
    ALL = wsp ("ALL" / "all")
    ALTER = wsp ("ALTER" / "alter")
    AND = wsp ("AND" / "and")
    AS = wsp ("AS" / "as")
    ASC = wsp ("ASC" / "asc")
    BETWEEN = wsp ("BETWEEN" / "between")
    BY = wsp ("BY" / "by")
    CAST = wsp ("CAST" / "cast")
    COLUMN = wsp ("COLUMN" / "column")
    DESC = wsp ("DESC" / "desc")
    DISTINCT = wsp ("DISTINCT" / "distinct")
    E = "E"
	ESCAPE  = wsp ("ESCAPE" / "escape")
	EXCEPT  = wsp ("EXCEPT" / "except")
	EXISTS  = wsp ("EXISTS" / "exists")
	EXPLAIN  = ws ("EXPLAIN" / "explain")
	EVENT  = ws ("EVENT" / "event")
	FORALL  = wsp ("FORALL" / "forall")
	FROM  = wsp ("FROM" / "from")
	GLOB  = wsp ("GLOB" / "glob")
	GROUP  = wsp ("GROUP" / "group")
	HAVING  = wsp ("HAVING" / "having")
	IN  = wsp ("IN" / "in")
	INNER  = wsp ("INNER" / "inner")
	INSERT  = ws ("INSERT" / "insert")
	INTERSECT  = wsp ("INTERSECT" / "intersect")
	INTO  = wsp ("INTO" / "into")
	IS  = wsp ("IS" / "is")
	ISNULL  = wsp ("ISNULL" / "isnull")
	JOIN  = wsp ("JOIN" / "join")
	KEY  = wsp ("KEY" / "key")
	LEFT  = wsp ("LEFT" / "left")
	LIKE  = wsp ("LIKE" / "like")
	LIMIT  = wsp ("LIMIT" / "limit")
	MATCH  = wsp ("MATCH" / "match")
	NO  = wsp ("NO" / "no")
	NOT  = wsp ("NOT" / "not")
	NOTNULL  = wsp ("NOTNULL" / "notnull")
	NULL  = wsp ("NULL" / "null")
	OF  = wsp ("OF" / "of")
	OFFSET  = wsp ("OFFSET" / "offset")
	ON  = wsp ("ON" / "on")
	OR  = wsp ("OR" / "or")
	ORDER  = wsp ("ORDER" / "order")
	OUTER  = wsp ("OUTER" / "outer")
	PRIMARY  = wsp ("PRIMARY" / "primary")
	QUERY  = wsp ("QUERY" / "query")
	RAISE  = wsp ("RAISE" / "raise")
	REFERENCES  = wsp ("REFERENCES" / "references")
	REGEXP  = wsp ("REGEXP" / "regexp")
	RENAME  = wsp ("RENAME" / "rename")
	REPLACE  = ws ("REPLACE" / "replace")
	RETURN  = wsp ("RETURN" / "return")
	ROW  = wsp ("ROW" / "row")
	SAVEPOINT  = wsp ("SAVEPOINT" / "savepoint")
	SELECT  = ws ("SELECT" / "select")
	SET  = wsp ("SET" / "set")
	TABLE  = wsp ("TABLE" / "table")
	TEMP  = wsp ("TEMP" / "temp")
	TEMPORARY  = wsp ("TEMPORARY" / "temporary")
	THEN  = wsp ("THEN" / "then")
	TO  = wsp ("TO" / "to")
	UNION  = wsp ("UNION" / "union")
	USING  = wsp ("USING" / "using")
	VALUES  = wsp ("VALUES" / "values")
	VIRTUAL  = wsp ("VIRTUAL" / "virtual")
	WITH  = wsp ("WITH" / "with")
	WHERE  = wsp ("WHERE" / "where")
    """
)

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
  Each expression in the grammar above of the form

      XXX = ....
  
  can be handled with a custom function by writing 
  
      def visit_XXX(self, node, children):

  You can assume the elements in children are the handled 
  versions of the corresponding child nodes
  """
  grammar = grammar

  def visit_query(self, node, children):
    _, select, orderby, limit, _ = tuple(children)
    select.sorts = orderby or []
    select.limit = limit
    return select

  #
  #  SELECT CLAUSE
  #

  def visit_select_cores(self, node, children):
    l = list(filter(bool, children[1]))
    if len(l):
      raise Exception("We don't support multiple SELECT cores")
    return children[0]

  def visit_select_core(self, node, children):
    distinctc, _,  selectc, fromc, wherec, gbc, havingc = tuple(children[1:])
    q = PSelectQuery(
        selectc, fromc, wherec, gbc, havingc, distinctc)
    return q

  def visit_select_results(self, node, children):
    allexprs = flatten(children, 0, 1)
    return [PTarget(e, alias) for e, alias in allexprs]

  def visit_sel_res_tab_star(self, node, children):
    return (Star(children[0]), None)

  def visit_sel_res_all_star(self, node, children):
    return (Star(), None)

  def visit_sel_res_val(self, node, children):
    return (children[0], children[1] or None)

  def visit_sel_res_col(self, node, children):
    return (children[0], children[1] or None)

  def visit_distinct_clause(self, node, children):
    return True


  #
  # FROM CLAUSE
  #

  def visit_from_clause(self, node, children):
    return children[1]

  def visit_join_source(self, node, children):
    sources = flatten(children, 1, 2)
    return sources


  def visit_source_table(self, node, children):
    tname = children[0]
    alias = children[1] or tname
    return PRangeVar(tname, alias, PRangeVar.TABLE)

  def visit_source_subq(self, node, children):
    subq = children[2]
    alias = children[5] 
    if not alias: alias = None
    return PRangeVar(subq, alias, PRangeVar.QUERY)

  def visit_source_func(self, node, children):
    subf = children[0]
    alias = children[1]
    return PRangeVar(subf, alias, PRangeVar.FUNC)

  #
  # Other clauses
  #

  def visit_where_clause(self, node, children):
    return children[2]

  def visit_gb_clause(self, node, children):
    return children[2]

  def visit_group_clause(self, node, children):
    groups = flatten(children, 0, 1)
    return groups

  def visit_grouping_term(self, node, children):
    return children[1]

  def visit_having_clause(self, node, children):
    return children[-1]

  def visit_orderby(self, node, children):
    terms = flatten(children, 2, 3)
    ret = []
    for expr, ascdesc in terms:
      ret.append(PSort(expr, ascdesc))
    return ret

  def visit_ordering_term(self, node, children):
    expr = children[1]
    order = children[2]
    return (expr, order)

  def visit_ASC(self, node, children):
    return PSort.ASC

  def visit_DESC(self, node, children):
    return PSort.DESC

  def visit_limit(self, node, children):
    return PLimit(children[2], children[3])


  ############################################
  #
  #  Expressions
  #
  ############################################

  def visit_col_ref(self, node, children):
    tname = children[0]
    if not tname: 
      tname = None
    return Attr(children[1], None, tname)

  def visit_name(self, node, children):
    name = node.text
    if name[0] == name[-1] == "`":
      name = name[1:-1]
    return name

  def visit_attr(self, node, children):
    return Attr(node.text)

  def visit_binaryop(self, node, children):
    return node.text

  def visit_unaryop(self, node, children):
    return node.text

  def visit_binaryop_no_andor(self, node, children):
    return node.text

  def visit_biexpr(self, node, children):
    op, lhs, rhs = children[2], children[0], children[-1]
    return Expr(op, lhs, rhs)

  def visit_unexpr(self, node, children):
    return Expr(children[0], children[1])

  def visit_btwnexpr(self, node, children):
    v1, v2, v3 = children[0], children[3], children[-1]
    return Between(v1, v2, v3)

  def visit_inexpr(self, node, children):
    lhs, rhs = children[0], children[-1]
    # TODO: verify that the rhs is a one-attribute query, or a list
    return Expr("in", lhs, rhs)

  def visit_exprand(self, node, children):
    exprs = flatten(children, 0, -1)
    ret = exprs[0]
    for e in exprs[1:]:
      ret = Expr("and", ret, e)
    return ret

  def visit_andor(self, node, children):
    return node.text

  def visit_expr(self, node, children):
    return children[0]

  def visit_function(self, node, children):
    fname = children[0]
    arglist = children[3]
    f = UDFRegistry.registry()[fname]
    if not f:
      raise Exception("Function %s not found" % fname)
    if f.is_agg: 
      if f.is_incremental:
        return IncAggFunc(f, arglist)
      return AggFunc(f, arglist)
    return ScalarFunc(f, arglist)

  def visit_fname(self, node, children):
    return node.text

  def visit_arg_list(self, node, children):
    return flatten(children, 0, 1)
  
  def visit_number(self, node, children):
    return Literal(float(node.text))

  def visit_string(self, node, children):
    return Literal(node.text)

  def visit_parenval(self, node, children):
    return Paren(children[2])

  def visit_parenq(self, node, children):
    return children[1]

  def visit_value(self, node, children):
    return children[0]

  def visit_parenval(self, node, children):
    return children[2]

  def visit_boolean(self, node, children):
    if node.text == "true":
      return Literal(True)
    return Literal(False)

  def visit_date(self, node, children):
    return Date(children[1])

  def visit_dateregex(self, node, children):
    return parsedate(children[0])
    
  def visit_listval(self, node, children):
    exprs = flatten(children, 2, 3)
    return List(exprs)



  def generic_visit(self, node, children):
    children = list([v for v in children if v and (not isinstance(v, str) or v.strip())])
    if len(children) == 1: 
      return children[0]
    return children

def parse(s):
  query = Visitor().parse(s)
  query.initialize()
  return query


