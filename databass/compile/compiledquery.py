from ..optimizer import *
from ..parse_sql import parse
from ..context import *
from .compiler import *
from .root import *
from .pipeline import *


class CompiledQuery(object):
  """
  Helper function to parse, optimize, compile, execute, and run a query
  """
  def __init__(self, qstr_or_plan, lineage_policy=None):
    if isinstance(qstr_or_plan, str):
      self.plan = Collect(parse(qstr_or_plan).to_plan())
    else:
      self.plan = qstr_or_plan

    self.lineage_policy = lineage_policy

    self.opt = Optimizer()
    self.optimized_plan = self.opt(self.plan)

    self.pipelined_plan = self.create_pipelined_plan(self.optimized_plan)
    self.ctx = Context()
    self.pipelined_plan.produce(self.ctx, lineage_policy)

    self.code = self.compile_to_func("compiled_q")
    #print(self.code)
    execSymbTable = {}
    try:
      exec(self.code, globals(), execSymbTable)
    except Exception as e:
      import traceback; traceback.print_exc()
      raise e
    self.f = execSymbTable["compiled_q"]

    self.lineages = []

  @property
  def source_ops(self):
    return self.optimized_plan.collect(Scan)

  def source_op(self, alias):
    for src in self.source_ops:
      if src.alias == alias:
        return src
    return None


  def create_pipelined_plan(self, plan):
    raise Exception("Not implemented")

  def compile_to_func(self, fname="f"):
    raise Exception("Not implemented")

  def print_code(self, funcname="compiled_q"):
    raise Exception("Not implemented")

  def __call__(self, db=None):
    raise Exception("Not implemented")





