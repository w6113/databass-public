from ..translator import *
from ...exprs import *
from ..agg import *
from .translator import *
from .udfs import *


class PyGroupByBottomTranslator(GroupByBottomTranslator, PyTranslator):
  def produce(self, ctx):
    """
    Produce sets up the variables and hash table so that they can be populated by
    calling child's produce (which eventually calls self.consume). 
    
    """
    self.v_ht = ctx.new_var("gb_ht")
    self.v_irow = self.compile_new_tuple(ctx, self.op.schema, "gb_irow")

    
    # A3: implement code that sets v_ht to the hash table
    raise Exception("Not Implemented")


  def consume(self, ctx):
    """
    Build hashtable

    For instance, if the query is:

        SELECT a-b, count(d)
        FROM data
        GROUP BY a+b-c

    Each hashtable entry contains the following information

    1. the hash probe key: e.g., hash of a+b-c
    2. values of attributes referenced in the grouping terms e.g., (a, b, c)
       see databass/ops/agg.py for their semantics
    3. the group of tuples
    """

    # A3: implement me
    raise Exception("Not Implemented")

  
class PyGroupByTopTranslator(GroupByTopTranslator, PyTranslator):

  def produce(self, ctx):
    # row containing attrs referenced in the grouping terms
    # passed to the non-aggregation functions in the target list
    self.v_term_row = self.compile_new_tuple(
        ctx, self.op.group_term_schema, "gb_term_row")

    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.consume(ctx) 

  def consume(self, ctx):
    """
    Loop through populated hash table and construct output tuple per group.  
    After constructing the output tuple, pass it to the parent's consumer.
    Don't forget to tell the parent the variable name containing the tuple.
    """

    # A3: implement me
    raise Exception("Not Implemented")

