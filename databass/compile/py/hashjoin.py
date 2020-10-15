from ...ops import HashJoin
from ..hashjoin import *
from .translator import *


class PyHashJoinLeftTranslator(HashJoinLeftTranslator, PyTranslator):
  """
  The left translator scans the left child and populates the hash table
  """
  def produce(self, ctx):
    """
    Produce's job is to 
    1. allocate variable names and create hash table
    2. request the child operator's variable name for the current row
    3. ask the child to produce
    """
    self.v_ht = ctx.new_var("hjoin_ht")

    # A3: implement me
    raise Exception("Not Implemented")


  def consume(self, ctx):
    """
    Given variable name for left row, compute left key and add a copy of the current row
    to the hash table
    """
    # A3: implement me
    raise Exception("Not Implemented")


class PyHashJoinRightTranslator(HashJoinRightTranslator, PyRightTranslator):
  """
  The right translator scans the right child, and probes the hash table
  """

  def produce(self, ctx):
    """
    Allocates intermediate join tuple and asks the child to produce tuples (for the probe)
    """

    # A3: implement me
    raise Exception("Not Implemented")

  def consume(self, ctx):
    """
    Given variable name for right row, 
    1. compute right key, 
    2. probe hash table, 
    3. create intermediate row to pass to parent's consume

    Note that because the hash key may not be unique, it's good hygiene
    to check the join condition again when probing.
    """
    # reference to the left translator's hash table variable
    v_ht = self.left.v_ht

    # A3: implement me
    raise Exception("Not Implemented")




