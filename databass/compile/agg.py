from ..ops import GroupBy
from .translator import *

class GroupByBottomTranslator(BottomTranslator):
  def __init__(self, *args, **kwargs):
    super(GroupByBottomTranslator, self).__init__(*args, **kwargs)

    # Compiler variables
    self.v_ht = None       # hash table
    self.v_irow = None     # intermediate row


  def produce(self, ctx):
    """
    Produce sets up the variables and hash table so that they can be populated by
    calling child's produce (which eventually calls self.consume). 
    """
    pass

  def consume(self, ctx):
    """
    Build hashtable

    For instance, if the query is:

        SELECT a-b, count(d)
        FROM data
        GROUP BY a+b-c

    Each hashtable entry contains the following information

    1. the hash probe key: e.g., hash of a+b-c
    2. values of attributes referenced in the grouping expressions (see below)
       e.g., (a, b, c)
    3. the group of tuples
    """
    pass



class GroupByTopTranslator(TopTranslator):
  def __init__(self, *args, **kwargs):
    super(GroupByTopTranslator, self).__init__(*args, **kwargs)

    # Tuple wrapper for attributes used in gb terms for nonaggregate exprs
    self.v_term_row = None

