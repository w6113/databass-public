from ...ops import GroupBy
from ..limit import *
from .translator import *


class PyLimitTranslator(LimitTranslator, PyTranslator):

  def produce(self, ctx):
    self.v_nyield = ctx.new_var("lim_nyield")
    self.v_niter = ctx.new_var("lim_niter")
    self.v_break = ctx.new_var("lim_break")
    ctx.request_vars(dict(row=None))

    # TODO: to be more efficient, propogate breaker to all sources in subplan
    #       to exit out of deeply nested loops directly
    lines = [
      "%s = 0" % self.v_nyield,
      "%s = -1" % self.v_niter,
      "%s = %s >= %s" % (self.v_break, self.v_nyield, self.op._limit)
    ]
    ctx.add_lines(lines)
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    v_irow = ctx['row']
    ctx.pop_vars()

    lines = [
      "%s += 1" % self.v_niter, 
      "if %s < %s: continue" % (self.v_niter, self.op._offset),
      "if %s: break" % self.v_break,
      "%s += 1" % self.v_nyield,
      "%s = %s >= %s" % (self.v_break, self.v_nyield, self.op._limit)
    ]

    ctx.add_lines(lines)
    ctx['row'] = v_irow
    self.parent_translator.consume(ctx)


