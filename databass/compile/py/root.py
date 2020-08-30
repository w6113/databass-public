from ...ops import GroupBy
from ..translator import *
from ..root import *
from .translator import *

class PySinkTranslator(SinkTranslator, PyTranslator):

  def initialize_lineage_indexes(self, ctx):
    if self.l_capture:
      ctx.add_line("# {op}", op=self.op)
      self.l_o = ctx.new_var("sink_l_o")
      ctx.declare(self.l_o, -1)
      for lindex in self.lindexes:
        lindex.fw.initialize()
        lindex.bw.initialize()


  def populate_lineage_indexes(self, ctx):
    if self.l_capture:
      ctx.add_line("{l_o} += 1", l_o=self.l_o)
      for lindex in self.lindexes:
        lindex.fw.set_1(self.l_i, self.l_o)
        lindex.bw.append_1(self.l_i)


class PyYieldTranslator(YieldTranslator, PySinkTranslator):
  def consume(self, ctx):
    v_in = ctx['row']
    self.populate_lineage_indexes(ctx)
    ctx.add_line("yield %s" % v_in)


class PyCollectTranslator(CollectTranslator, PySinkTranslator):
  def produce(self, ctx):
    self.v_buffer = ctx.new_var("collect_buf")
    ctx.declare(self.v_buffer, "[]")

    self.initialize_lineage_indexes(ctx)
    self.child_translator.produce(ctx)
    self.clean_prev_lineage_indexes()
    ctx.returns("return {buf}", buf=self.v_buffer)

  def consume(self, ctx):
    v_in = ctx['row']
    self.populate_lineage_indexes(ctx)

    v_tmp = self.compile_new_tuple(ctx, self.op.schema, "collect_tmp")
    ctx.add_lines([
      "{tup}.row = list({v_in}.row)", 
      "{buf}.append({tup})"],
      v_in=v_in, buf=self.v_buffer, tup=v_tmp)


class PyPrintTranslator(PrintTranslator, PySinkTranslator):
  def consume(self, ctx):
    v_in = ctx['row']
    self.populate_lineage_indexes(ctx)
    ctx.add_line("print(%s)" % v_in)




