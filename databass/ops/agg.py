from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from ..udfs import *
from itertools import chain


########################################################
#
# Aggregation Operators
#
########################################################



class GroupBy(UnaryOp):
  def __init__(self, c, group_exprs, project_exprs=None, aliases=None):
    """
    @c           child operator
    @group_exprs list of Expression objects
    """
    super(GroupBy, self).__init__(c)

    # Grouping expressions defined in the GROUP BY clause
    self.group_exprs = group_exprs

    # Attrs referenced in group_exprs. 
    # They are made accessible to the project_exprs
    # to compute arbitrary expressions over them e.g.,
    #   
    #    SELECT a+b, sum(c)
    #    ..
    #    GROUP BY b / a
    #
    # self.group_attrs would be: a, b
    # 
    # This is nonstandard, since a given group could have
    # >1 distinct group_attrs values.  For instance:
    #
    #    a b
    #    1 2
    #    2 4
    #
    # Both tuples have a GROUP BY value of 2, so in this case, 
    # a+b will be evaluated on the last tuple of the group (2, 4)
    self.group_attrs = []

    # Schema for group_attrs so we can populate a temporary tuple 
    # to pass into the non-agg projection expressions.
    # From GROUP BY b / a above, it would be [a, b] (or [b, a])
    self.group_term_schema = None

    # The actual output expressions of the groupby e.g., a+b, sum(c)
    self.project_exprs = project_exprs or []
    self.aliases = aliases or []


  def init_schema(self):
    """
    * initialize and set self.schema.
    * set self.group_attrs and self.group_term_schema
    """
    self.schema = Schema([])

    for alias, expr in zip(self.aliases, self.project_exprs):
      typ = expr.get_type()
      self.schema.attrs.append(Attr(alias, typ))

    # collect Attrs from group_exprs
    seen = {}
    for attr in chain(*[e.referenced_attrs for e in self.group_exprs]):
      attr = attr.copy()
      seen[(attr.tablename, attr.aname)] = attr

    self.group_attrs = list(seen.values())
    self.group_term_schema = Schema(self.group_attrs)
    return self.schema

  def __iter__(self):
    """
    GroupBy works as follows:
    
    * Contruct and populate hash table with:
      * key is defined by the group_exprs expressions  
      * Track the values of the attributes from the most recent tuple that is
        referenced in the grouping expressions
      * Track the tuples in each bucket
    * Iterate through each bucket, compose and populate a tuple that conforms to 
      this operator's output schema (see self.init_schema)
    """

    # A1: implement me
    raise Exception("Not Implemented")

  def __str__(self):
    args = list(map(str, self.group_exprs))
    args.append("|")
    for e, alias in zip(self.project_exprs, self.aliases):
      args.append("%s as %s" % (e, alias))
    s = "GROUPBY(%s)" % ", ".join(args)
    return s

  def to_str(self, ctx):
    with ctx.indent(str(self)):
      self.c.to_str(ctx)



