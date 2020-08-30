"""
This file contains abstract classes for Pipeline and Pipelines.
Compilation language-specific logic is needed for them to actually run
"""
from ..ops import *
from .lindex import *
from .lpolicy import *

class Pipeline(list):
  """
  A pipeline consists of Translator objects that wrap
  physical operators.  Each translator object performs the
  relevant codegen
  """

  _id = 0

  def __init__(self):
    self.id = Pipeline._id
    Pipeline._id += 1

  def prepare(self, ctx, lineage_policy=None):
    """
    Make sure each translator has references to its parent and child
    translators, so that the produce/consume will be called correctly

    Note: 
      This creates connetions within a linear pipeline.
      Connections between pipelines already exist based on 
      paired bottom-top and left-right translators.  
      Top (right) translators are initialized with the matching 
      bottom (left) translators in Pipelines.make_pipelines()
    """
    for i, t in enumerate(self):
      c, p = None, None
      if i > 0: c = self[i-1]
      if i+1 < len(self): p = self[i+1]
      t.prepare(c, p, self)

    if not lineage_policy:
      lineage_policy = NoLineagePolicy()
    if not isinstance(lineage_policy, NoLineagePolicy):
      self.prepare_lineage(ctx, lineage_policy)
    ctx.declare("")

  def prepare_lineage(self, ctx, lineage_policy):
    """
    Walk the pipeline and allocate lineage-based variables and lindexes 
    to the appropriate transformers

    For a given transformer:
      l_i: rids of operator's input relation
      l_o: rids of operator's output relation

    Should be overridden by language-specific implementations.
    For instance, see ./py/pipeline.py
    """
    raise Exception("Not implemented")

  def produce(self, ctx):
    """
    Should be overridden by language-specific subclasses
    For instance, see ./py/pipeline.py
    """
    raise Exception("Not implemented")

  def __str__(self):
    return "\n".join(map(str, self))


class Pipelines(object):
  """
  Walk the physical query plan and chop it into a list of pipelines.
  Each pipeline turns into a nested for loop ending with a pipeline breaker.
  The last pipeline is always the "main" one that contains the root of the plan
  """

  def __init__(self, plan, pipeline_klass):
    self.pipeline_klass = pipeline_klass
    self.pipeline = self.pipeline_klass()
    self.pipelines = []
    self.q = plan

    self.make_pipelines(plan, self.pipeline)
    self.pipelines.append(self.pipeline)

  def produce(self, ctx, lineage_policy=None):
    """
    Produce compiled code from pipelined query plan
    """
    for pipeline in self.pipelines:
      pipeline.prepare(ctx, lineage_policy)

    for pipeline in self.pipelines:
      pipeline.produce(ctx)

    self.produce_lineage_output(ctx)


  def produce_lineage_output(self, ctx):
    """
    Generate the code to add the materialized lineage
    to the lineage manager that's passed in to the compiled
    function.  
    """
    for pipeline in self.pipelines:
      for t in pipeline:
        if not t.lindexes: continue
        if not t.l_materialize: continue
        for lindex in t.lindexes:
          self.produce_lindex(ctx, t, lindex)

  def produce_lindex(self, ctx, t, lindex):
    """
    Implement in language-specific subclasses
    """
    raise Exception("Not implemented")


  def make_pipelines(self, node, curpipeline):
    """
    Walk the query plan top-down and decompose into pipelines.  
    Each pipeline is a path in the tree. For example:

      Query Plan          Pipelines
    --------------    -----------------
         GrpBy         [A, Joinleft]
           |           [B, Joinright]
          Join     ->  [Joinright, GrpBy]
          / \
         A   B

    A new pipeline is created at a pipeline breaker (agg, orderby, hashjoin, etc).
    We also wrap operators with their corresponding translators.

    """
    if node.is_type([OrderBy, GroupBy]):
      bot = self.create_bottom(node)
      top = self.create_top(node, bot)

      newpipeline = self.pipeline_klass()
      self.make_pipelines(node.children()[0], newpipeline)
      newpipeline.append(bot)  # cap pipeline with bottom translator
      self.pipelines.append(newpipeline)

      curpipeline.append(top)

    elif node.is_type(HashJoin):
      left = self.create_left(node)
      right = self.create_right(node, left)

      newpipeline = self.pipeline_klass()
      self.make_pipelines(node.children()[0], newpipeline)
      newpipeline.append(left)  # cap pipeline with left translator
      self.pipelines.append(newpipeline)

      self.make_pipelines(node.children()[1], curpipeline)
      curpipeline.append(right)

    elif node.is_type(ThetaJoin):
      left = self.create_left(node)
      right = self.create_right(node, left)

      # theta join doesn't have pipeline breakers, but
      # needs separate translator for each side.
      self.make_pipelines(node.children()[0], curpipeline)
      curpipeline.append(left)
      self.make_pipelines(node.children()[1], curpipeline)
      curpipeline.append(right)

    else:
      if node.children():
        self.make_pipelines(node.children()[0], curpipeline)

      curpipeline.append(self.create_normal(node))


  def create_bottom(self, op, *args):
    raise Exception("Not implemented")

  def create_top(self, op, *args):
    raise Exception("Not implemented")
  def create_left(self, op, *args):
    raise Exception("Not implemented")

  def create_right(self, op, *args):
    raise Exception("Not implemented")

  def create_normal(self, op, *args):
    raise Exception("Not implemented")

  def pretty_print(self):
    return self.pipelines[-1][-1].pretty_print()
