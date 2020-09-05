from ...ops import GroupBy
from ..translator import *
from ..root import *
from .translator import *

class PySinkTranslator(SinkTranslator, PyTranslator):

  def initialize_lineage_indexes(self, ctx):
    if self.l_capture:
      # A5: Fill in this code to initialize the state needod
      #     to track the record id for each output tuple
      #     as well as initialize the lineage indexes in self.lindexes
      # 
      #     Each Lindex is a data structure to manage the forward (lindex.fw)
      #     and backward (lindex.bw) pointers that will be captured
      #
      #     See databass.compile.lindex for the class definitions
      #     See databass.compile.py.lindex for the python implementations
      #    
      pass


  def populate_lineage_indexes(self, ctx):
    """
    This is called in the root operator's consume stage (see below)
    Its task is to keep track of the output record id
    and attach the appropriate relationships between the output rid
    and input rid (or rids)
    """
    if self.l_capture:
      # A5: Fill in this code to populate each of the lindexes
      #     You can assume that self.l_i contains the current input rid
      #     and that SinkTranslators are always 1-to-1
      pass


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




