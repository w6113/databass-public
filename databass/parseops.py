"""
This file contains classes used to construct the parse tree during parsing in parse_sql.py.
Each parse tree node is a query fragment (select, project, from, groupby...), 
and children correspond to subqueries. For instance:

    SELECT a, count(1)
    FROM (SELECT * FROM data) as d1, (SELECT * from data) as d2
    GROUP BY a

Corresponds to the following tree

        outerquery
        /       \
      d1        d2
    query      query

Each class is prefixed with "P" to signify its use during the parsing phase.
The bulk of the work is in PSelectQuery, which performs query validation
and disambiguation, and is responsible for turning the parse tree into a logical plan.
"""
from itertools import chain

from .db import *
from .baseops import *
from .ops import *
from .exprs import *
from .exprutil import predicate_to_cnf
from .util import deduplicate

class POp(Op):
  """
  Base Parse operator
  """
  def children(self):
    return [self.e]

  def to_str(self, ctx):
    ctx.add_line(str(self))

class PTarget(POp):
  """
  A target variable (select clause expression)
  """
  def __init__(self, e, alias):
    super(PTarget, self).__init__()
    self.e = e
    self.alias = alias

  def __str__(self):
    return "{e} as {a}".format(e=self.e, a=self.alias)

class PRangeVar(POp):
  """
  A range variable (from clause expression)
  """
  TABLE = 0
  QUERY = 1
  FUNC  = 2

  def __init__(self, e, alias, typ):
    super(PRangeVar, self).__init__()
    self.e = e
    self.alias = alias
    self.typ = typ

  def __str__(self):
    e = str(self.e)
    if self.typ == PRangeVar.QUERY:
      e = "(%s)" % e
    return "{e} as {a}".format(e=e, a=self.alias)

class PSort(POp):
  """
  Operator for ORDER BY
  """
  ASC = "asc"
  DESC = "desc"

  def __init__(self, e, order):
    """
    @order asc or desc
    """
    super(PSort, self).__init__()
    if not order: 
      order = PSort.ASC
    self.order = order
    self.e = e

  def __str__(self):
    return "{e} {o}".format(e=self.e, o=self.order)

class PLimit(POp):
  def __init__(self, limit, offset):
    super(PLimit, self).__init__()
    self.limit = limit
    self.offset = offset

  def children(self):
    return list(filter(bool, [self.limit, self.offset]))

  def __str__(self):
    if self.offset:
      return "%s %s" % (self.limit, self.offset)
    return str(self.limit)

class PSelectQuery(POp):
  """
  This is the main workhorse for validating parse trees (self.initialize())
  and turning them into a logical query plan (self.to_plan())

  """
  def __init__(self,
      targets, froms, qual, groups, having_qual,
      distinct=False):
    super(PSelectQuery, self).__init__()

    self.distinct = distinct
    self.targets = targets or []
    self.froms = froms or []
    self.qual = qual
    self.groups = groups or []
    self.having_qual = having_qual
    self.sorts = []
    self.unions  = None
    self.intersects = None
    self.limit = None

    self.schema = None

    # the qualifications will be split into conjunctive normal form as a list
    self.quals_cnf = []
    self.having_quals_cnf = []

    self.having_exprs = None
    self.having_aliases = None

    self.target_alias_id = 0
    self.rangevar_alias_id = 0


  def to_plan(self):
    """
    Walk the parse tree and construct a valid logical plan
    @return logical plan
    """
    # TODO: support subquery
    # TODO: predicate pushdown

    # split quals into join clauses and normal filters
    join_quals = []
    normal_quals = []
    for cond in self.quals_cnf:
      tnames = set([a.tablename for a in cond.referenced_attrs])
      if (cond.is_type(Expr) and cond.op == "=" and len(tnames) == 2):
        join_quals.append(cond)
      else:
        normal_quals.append(cond)

    # "leaves" are the range variables (FROM clause)
    froms = []
    for r in self.froms:
      if r.typ == PRangeVar.TABLE:
        fop = Scan(r.e, r.alias)
      elif r.typ == PRangeVar.QUERY:
        fop = SubQuerySource(r.e.to_plan(), r.alias)
      else:
        raise Exception("Range var %s not recognized" % r)
      froms.append(fop)
    plan = From(froms, join_quals)

    # WHERE clause
    for cond in normal_quals:
      plan = Filter(plan, cond)

    # GROUP BY
    if self.is_agg_query:
      project_exprs = []
      aliases = []
      for t in self.targets:
        project_exprs.append(t.e)
        aliases.append(t.alias)
      plan = GroupBy(plan, self.groups, project_exprs, aliases)
      
      # Groupby is a complicated operator because it actually replaces
      # the functionality of the projection operator.  For instance, in:
      # 
      #   SELECT a, b, sum(c) ... GROUP BY a, b
      #
      # Groupby outputs (a, b, sum(c)) for each group, so a separate Project
      # operator is not needed.
      # 
      # The challenge is that the HAVING and ORDER BY clauses run after 
      # GroupBy and may # contain expressions not emitted by GroupBy.  
      # For instance, count(1) and a are not in the target list
      #
      #   SELECT sum(c) .. GROUP BY a HAVING count(1) > 2 ORDER BY a
      #
      # We wish to generate the following logical plan
      #
      #
      #            PROJECT(sum(c))
      #                  |
      #               ORDERBY(a)
      #                  |
      #           HAVING(count(1) > 2)
      #                  |
      #   GROUP BY(a | a, count(1), sum(c))
      #
      # To support them, we want to:
      #
      # * use GroupBy to compute exprs in HAVING/ORDERBY clauses
      # * use Filter to apply the HAVING clause 
      # * run Orderby
      # * then remove exprs not in the query's target list
      #
      if self.having_exprs or self.sorts:
        if self.having_exprs:
          plan.project_exprs.extend(self.having_exprs)
          plan.aliases.extend(self.having_aliases)

        if self.sorts:
          order_aliases = ["_ordby_%d" % i for i in range(len(self.sorts))]
          plan.project_exprs.extend([s.e for s in self.sorts])
          plan.aliases.extend(["_ordby_%d" % i for i in range(len(self.sorts))])

        if self.having_exprs:
          plan = Filter(plan, self.having_qual)

        if self.sorts:
          order_exprs = [Attr(a, idx=i) for i,a in enumerate(order_aliases)]
          order_ascdesc = [s.order for s in self.sorts]
          plan = OrderBy(plan, order_exprs, order_ascdesc)

        orig_aliases = [t.alias for t in self.targets]
        orig_exprs = [Attr(alias) for alias in orig_aliases]
        plan = Project(plan, orig_exprs, orig_aliases)


    # ORDER BY
    if not self.is_agg_query and self.sorts:
      order_exprs = [s.e for s in self.sorts]
      order_ascdesc = [s.order for s in self.sorts]
      plan = OrderBy(plan, order_exprs, order_ascdesc)

    # LIMIT
    if self.limit:
      plan = Limit(plan, self.limit.limit, self.limit.offset)

    # PROJECT 
    if not self.is_agg_query:
      project_exprs = []
      aliases = []
      for t in self.targets:
        project_exprs.append(t.e)
        aliases.append(t.alias)

      plan = Project(plan, project_exprs, aliases)

    # TODO: support distinct
    if self.distinct:
      raise Exception("DISTINCT not implemented")

    return plan

  def initialize(self, db=None):
    """
    Clean up references, perform validation
    """
    db = db or Database.db()

    # make sure that subqueries in the FROM clause are initialized
    for subq in self.all_subqueries:
      subq.initialize(db)

    self.check_aliases()
    self.resolve_schemas_and_references(db)

    # At this point, all attributes are bound to a range 
    # variable, and all subqueries have been assigned schemas

    self.check_groupby()
    self.check_types()

    # transform qualifications into CNF
    self.transform_quals()


  def check_aliases(self):
    """
    Assign all range variables and targets aliases if they
    have not been explicitly specified
    """

    for target in self.targets:
      if not target.alias:
        if target.e.is_type(Attr):
          target.alias = target.e.aname
        else:
          target.alias = self.new_target_alias()

    rangevar_aliases = set()
    for rangevar in self.froms:
      if not rangevar.alias: 
        if rangevar.typ == PRangeVar.TABLE:
          rangevar.alias = rangevar.e
        else:
          rangevar.alias = self.new_rangevar_alias()

      if rangevar.alias in rangevar_aliases:
        raise Exception("range alias is not unique: %s" % rangevar)
      rangevar_aliases.add(rangevar.alias)


    for q in self.all_subqueries:
      q.check_aliases()


  def check_types(self):
    """
    Find all expressions that contain a subquery, and make sure the lhs and
    rhs type check
    """
    for e in self.get_exprs():
      if not e.is_type(Expr): continue
      if e.r and e.r.is_type(PSelectQuery):
        n = len(e.r.schema.attrs)
        if n != 1:
          raise Exception("Subquery has wrong number of columns: %s" % n)

        # TODO: support scalar subqueries
        raise Exception("Scalar subqueries not supported: %s" % e.r)
      else:
        if not e.check_type():
          raise Exception("Type error in expression: %s" % e)

    for q in self.all_subqueries:
      q.check_types()

  def resolve_schemas_and_references(self, db, scope=None):
    """
    Walk the parse tree to resolve attribute references and assign each query's schema
    """
    # scope is a list of dicts, where each dict is 
    # the set of aliases defined at the query nesting level
    #
    # schemas defined at this level are accessible in subqueries
    # in deeper nesting levels
    scope = list(scope) if scope else []
    scope.append({})

    # subqueries in the FROM clause don't get access to the current level
    for q in self.from_subqueries:
      q.resolve_schemas_and_references(db, scope[:-1])

    # resolves attr tablenames and types, populates scope[-1]
    self.resolve_references(db, scope)
    self.resolve_schema(db, scope)

    for q in self.expr_subqueries:
      q.check_references(db, scope)

    scope.pop()

  def resolve_references(self, db, scope):
    """
    1. Check that attributes are unambiguous and reference actual
       attributes in range variables (tables/subqs)
    2. Resolves tablename and types for all attributes in Exprs

    Consider the following query:

        SELECT *
        FROM T1, (SELECT * FROM T2) as T
        WHERE T1.a IN (SELECT * from T3 WHERE T.a = T3.b)

    * The outer scope defines T1 and T
    * The WHERE clause and the T3 subquery can reference tables in the outer scope
    * The T2 subquery does not have access to T1

    @scope   scope[-1] corresponds to the current query's scope
    """
    # add range var alias -> schema to current level
    for r in self.froms:
      if r.typ == PRangeVar.TABLE:
        if r.e not in db:
          raise Exception("Table does not exist: %s" % r)
        scope[-1][r.alias] = db[r.e].schema
      elif r.typ == PRangeVar.QUERY:
        scope[-1][r.alias] = r.e.schema

    attrs = self.get_attrs()
    for a in attrs:
      if a.tablename: 
        self.resolve_attr_with_tablename(a, scope)
      else:
        self.resolve_attr_wout_tablename(a, scope)

  def resolve_attr_with_tablename(self, a, scope):
    matches = 0
    for rangevars in scope:
      if a.tablename in rangevars:
        if a.aname in rangevars[a.tablename]:
          a.typ = rangevars[a.tablename][a.aname].typ
          matches += 1

    if not matches:
      raise Exception("Invalid reference to table %s" % a)
    if matches > 1:
      raise Exception("Ambiguous reference to table %s" % a)

  def resolve_attr_wout_tablename(self, a, scope):
    # find a.aname in all range vars within scope
    matches = []
    for rangevars in scope:
      for alias, schema in rangevars.items():
        if a.aname in schema:
          a.typ = schema[a.aname].typ
          a.tablename = alias
          matches.append(alias)

    if not matches:
      raise Exception("Invalid attribute reference %s" % a)
    if len(matches) > 1:
      raise Exception("Ambiguous attribute reference %s matches tables: %s" % (a, matches))

  def resolve_schema(self, db, scope):
    """
    1. Expand * in target lists, since that affects a query's schema
    2. Infer schema for all PSelectQuery
    """

    input_schema = []  # x product of all range vars in FROM clause
    for alias, schema in scope[-1].items():
      input_schema.extend(schema.copy())

    # expand * or REF.* in the target list
    targets = []
    for t in self.targets:
      if t.e.is_type(Star):
        if not self.froms:
          raise Exception("Cannot use * when Project has no source relations.")

        if t.e.tablename:
          for att in scope[-1][t.e.tablename]:
            targets.append(PTarget(att, att.aname))
        else:
          for att in input_schema:
            targets.append(PTarget(att, att.aname))
      else:
        if t.e.collect(Star):
          raise Exception("Cannot use * as part of expression: " % t)
        targets.append(t)

    # Finally, assign my own schema
    self.schema = Schema([])
    self.targets = targets
    for t in self.targets:
      self.schema.attrs.append(Attr(t.alias, t.e.get_type()))

  def transform_quals(self):
    """
    Turn quals and having quals into CNF form
    """
    self.quals_cnf = predicate_to_cnf(self.qual)
    self.having_quals_cnf = predicate_to_cnf(self.having_qual)

  def check_groupby(self):
    """
    Ensure SELECT and GROUPBY clause match
    """
    nonaggs = []
    aggs = []
    for i, t in enumerate(self.targets):
      if t.e.is_type(AggFunc):
        aggs.append(i)
      else:
        nonaggs.append(i)

    # If there are grouping exprs, then all non-agg
    # expressions in target list better be grouping exprs
    gb_attrs = [list(e.referenced_attrs) for e in self.groups]
    gb_attrs = list(deduplicate(chain(*gb_attrs)))
    #gb_attrs = set(map(str, chain(*gb_attrs)))
    if gb_attrs:
      for i in nonaggs:
        t = self.targets[i]
        if not all(a.in_attr_list(gb_attrs) for a in t.e.referenced_attrs):
          raise Exception("%s uses attrs not in the groupby clause: %s" % (
            str(t.e), ", ".join(gb_attrs)))


    having_exprs = []
    having_aliases = []
    # If there is a having clause, all attrs referenced by it should
    # be in the grouping clause, or part of an aggregation function
    if self.having_qual:
      self.check_having_expr(self.having_qual, gb_attrs)

      # move all AggFunc into aggs, and replace
      # with attribute references
      tmp = self.get_having_exprs(self.having_qual)
      for expr in tmp:
        alias = self.new_target_alias()
        having_exprs.append(expr)
        having_aliases.append(alias)
        expr.replace(Attr(alias, "num"))

    self.having_exprs = having_exprs
    self.having_aliases = having_aliases

    for q in self.all_subqueries:
      q.check_groupby()


  def check_having_expr(self, e, gb_attrs):
    until = lambda op: op.is_type(AggFunc)
    def f(op, path):
      if op.is_type(Attr):
        if not op.in_attr_list(gb_attrs):
          raise Exception("%s should be in groupby clause or in agg function" % op)
    
    self.having_qual.traverse(f, until=until)


  def children(self):
    """
    Used for pretty print
    """
    return chain(
      self.targets,
      self.froms,
      [self.qual], 
      self.groups,
      [self.having_qual],
      self.sorts)

  @property
  def is_agg_query(self):
    return (any(t.e.is_type(AggFunc) for t in self.targets) or
        self.groups)

  @property
  def all_subqueries(self):
    return chain(self.expr_subqueries, self.from_subqueries)

  @property
  def expr_subqueries(self):
    for e in self.get_exprs():
      for q in self.get_immediate_subqueries(e):
        yield q

  @property
  def from_subqueries(self):
    for r in self.froms:
      if r.typ == PRangeVar.QUERY:
        yield r.e

  #
  #
  # Accessor helpers
  #
  #

  def get_exprs(self):
    """
    return list of expressions within this query (not subqueries)
    """
    ret = []
    ret.extend(t.e for t in self.targets)
    ret.append(self.qual)
    ret.extend(self.groups)
    ret.append(self.having_qual)
    ret.extend(self.sorts)
    if self.limit:
      ret.append(self.limit.limit)
      ret.append(self.limit.offset)
    ret = list(filter(bool, ret))
    return ret

  def get_attrs(self):
    """
    Retrieve all attributes within q's scope (not subqueries)
    """
    ret = []
    until = lambda n: n.is_type(PSelectQuery)
    for e in self.get_exprs():
      ret.extend(e.collect(Attr, until))
    return ret

  def get_having_exprs(self, qual):
    """
    Return all AggFunc and Attr expressions in @qual,
    where the Attrs are not a descendant of an AggFunc
    """
    aggfuncs = qual.collect(AggFunc)
    attrs = qual.collect(Attr)
    ret = list(aggfuncs)
    for attr in attrs:
      if any(attr.is_ancestor(aggfunc) for aggfunc in aggfuncs):
        continue
      ret.append(attr)
    return ret

  def get_immediate_subqueries(self, e):
    """
    return subqueries of @e, but don't searhc recursively
    into those subqueries

    @e is an Expr (technically, an Op)
    """
    ret = []
    def f(node, path):
      if len(list(filter(lambda p:p.is_type(PSelectQuery), path))) > 1: 
        return False
      if node and node.is_type(PSelectQuery):
        ret.append(node)

    e.traverse(f)
    return ret



  def new_target_alias(self):
    self.target_alias_id += 1
    return "a%d" % (self.target_alias_id-1)

  def new_rangevar_alias(self):
    self.rangevar_alias_id += 1
    return "t%d" % (self.rangevar_alias_id-1)



  def __str__(self):
    distinct = " DISTINCT" if self.distinct else ""
    listify = lambda l: ", ".join(map(str, l))
    l = [
      "SELECT%s %s" % (distinct, listify(self.targets)),
      ("FROM %s" % listify(self.froms)) if self.froms else None,
      ("WHERE %s" % self.qual) if self.qual else None,
      ("GROUP BY %s" % listify(self.groups)) if self.groups else None,
      ("HAVING %s" % self.having_qual) if self.having_qual else None,
      ("ORDER BY %s" % listify(self.sorts)) if self.sorts else None,
      ("LIMIT %s" % self.limit) if self.limit else None
    ]
    return "\n".join(filter(bool, l))







