from ...ops import ThetaJoin
from ..thetajoin import *
from .translator import *


class PyThetaJoinLeftTranslator(ThetaJoinLeftTranslator, PyTranslator):
  """
  Inner loop
  """
  def produce(self, ctx):
    if self.l_capture:
      # reset input index before starting next inner loop
      ctx.add_line("{l_i} = -1", l_i=self.l_i)

    ctx.request_vars(dict(row=None, table=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.v_lrow = ctx['row']
    ctx.pop_vars()

    self.parent_translator.consume(ctx)


class PyThetaJoinRightTranslator(ThetaJoinRightTranslator, PyRightTranslator):
  """
  Outer loop
  """

  def produce(self, ctx):
    # intermediate row
    self.v_irow = self.compile_new_tuple(ctx, self.op.schema, "theta_row")

    if self.l_capture:
      self.initialize_lineage_indexes(ctx)

    ctx.request_vars(dict(row=None, table=None))
    self.child_translator.produce(ctx)

    if self.l_capture:
      self.clean_prev_lineage_indexes()

  def consume(self, ctx):
    v_e = ctx.new_var("theta_cond")
    self.v_rrow = ctx['row']
    ctx.pop_vars()

    nlattrs = len(self.op.l.schema.attrs)
    ctx.add_lines([
      "{irow}.row[:{n}] = {left}.row",
      "{irow}.row[{n}:] = {right}.row"
      ],
      irow=self.v_irow, n=nlattrs, 
      left=self.left.v_lrow, right=self.v_rrow)

    ctx.add_line("# ThetaJoin: if %s" % self.op.cond)
    v_e = self.compile_expr(ctx, self.op.cond, self.v_irow)

    with ctx.indent("if not {e}:", e=v_e):
      # TODO: negative provenance capture here
      ctx.add_line("continue")

    # capture lineage
    if self.l_capture:
      ctx.add_line("{l_o} += 1", l_o=self.l_o)
      
      # left side
      if self.left.l_capture:
        for lindex in self.left.lindexes:
          lindex.fw.add_1(self.left.l_i, self.l_o)
          lindex.bw.append_1(self.left.l_i)

      # right side
      for lindex in self.lindexes:
        lindex.fw.add_1(self.l_i, self.l_o)
        lindex.bw.append_1(self.l_i)

    ctx['row'] = self.v_irow
    self.parent_translator.consume(ctx)


