from ...ops import *
from ..pipeline import *
from .translator import *
from .hashjoin import *
from .thetajoin import *
from .agg import *
from .project import *
from .scan import *
from .orderby import *
from .where import *
from .limit import *
from .root import *
from .lindex import *

class PyPipeline(Pipeline):

  def prepare_lineage(self, ctx, lineage_policy):
    """
    Walk the pipeline and allocate lineage-based variables and lindexes 
    to the appropriate transformers

    For a given transformer:
      l_i: rids of operator's input relation
      l_o: rids of operator's output relation
    """
    ctx.declare("# Lineage structures for pipeline %d" % self.id)
    l_o = None        # variable name for previous translator's output rids
    prev_t = None     # previous translator that generates output rids
    prev_lineage_translator = None # prev translator that builds a lindex
    for i, t in enumerate(self):
      if not lineage_policy.bcapture(t): continue

      t.l_capture = True
      if lineage_policy.bmaterialize(t):
        t.l_materialize = True

      # translators that consume records need the variable name of the output rid
      # generated from a translator earlier
      # in the pipeline
      if t.is_type([SinkTranslator, TopTranslator, BottomTranslator, 
                    LeftTranslator, RightTranslator]):
        assert(t.l_i is None)
        t.l_i = l_o  # its input rids are the output rids of a previous translator
        t.l_prev_translator = prev_t
        #print "set l_i", l_o, t, t.op

      # record the variable name of the output rid for translators 
      # that generate output rids  (beginnings of pipelines)
      if t.is_type([ScanTranslator, TopTranslator, RightTranslator]):
        assert(t.l_o is None)
        ctx.declare("#   %s | %s" % (t, t.op))
        l_o = ctx.new_var("l_o")
        ctx.declare(l_o, -1)
        t.l_o = l_o
        prev_t = t
        #print "set l_o", l_o, t, t.op

      # translators that initialize lindexes and generate capture logic 
      if t.is_type([ScanTranslator, SinkTranslator, TopTranslator, 
                    LeftTranslator, RightTranslator]):
        if t.is_type(ScanTranslator):
          bw = PyIdentityLidx(ctx)
          fw = PyIdentityLidx(ctx)
        elif t.is_type(SinkTranslator):
          # TODO: if there is no filtering operator between sink
          # and previous pipeline breaker,
          # can use identity lidxs instead
          bw = PyBw(ctx, Lindex.ONE)
          fw = PyFw(ctx, Lindex.ONE)
        elif t.is_type(OrderByTopTranslator):
          bw = PyBw(ctx, Lindex.ONE)
          fw = PyFw(ctx, Lindex.ONE)
        elif t.is_type(GroupByTopTranslator):
          bw = PyBw(ctx, Lindex.N)
          fw = PyFw(ctx, Lindex.ONE)
        elif t.is_type([LeftTranslator, RightTranslator]):
          bw = PyBw(ctx, Lindex.ONE)
          fw = PyFw(ctx, Lindex.N)

        lindex = PyLindex(ctx, bw, fw, t)
        t.lindex = lindex
        t.lindexes = [lindex]

        # 
        # The following logic propogates the edges from the previous
        # lindex to the current translator's lindex, so that the previous
        # lindex's resources can be released 
        #
        # We only propogate the previous lindexes if they are not marked
        # to be materialized
        #
        # Left side of theta-join is also a special case: the next translator is always
        # the child of the right side, so it should be treated as a "pipeline breaker"
        # 
        #
        if prev_lineage_translator and \
            not prev_lineage_translator.is_type(PyThetaJoinLeftTranslator) and \
            not lineage_policy.bmaterialize(prev_lineage_translator):
          lindexes = []
          for l in prev_lineage_translator.propagated_lindexes:
            lindexes.append(lindex + l)
            print(prev_lineage_translator.op, "+", t.op, ' = ', lindexes[-1].src_t.op, ' -> ', lindexes[-1].dst_t.op)
          t.lindexes = lindexes

        t.l_prev_translator = prev_lineage_translator
        prev_lineage_translator = t


  def produce(self, ctx):
    ctx.add_line("# --- Pipeline %s ---" % self.id)
    self[-1].produce(ctx)
    ctx.add_line("")


class PyPipelines(Pipelines):
  def __init__(self, ast):
    super(PyPipelines, self).__init__(ast, PyPipeline)

  def produce_lindex(self, ctx, t, lindex):
    with ctx.indent("if lineage:"):
      if lindex.src_t.is_type(RightTranslator):
        src_pair = (lindex.src_t.op.id, 1)
      else:
        src_pair = (lindex.src_t.op.id, 0)

      dst_id = lindex.dst_t.op.id

      ctx.add_lines([
        "# %s" % t.op,
        "lineage.add({src}, {dst}, {bw}, 'bw', {bwtyp})",
        "lineage.add({src}, {dst}, {fw}, 'fw', {fwtyp})"
        ],
        src=src_pair,
        dst=dst_id,
        bw=lindex.bw.idx if hasattr(lindex.bw, "idx") else None,
        fw=lindex.fw.idx if hasattr(lindex.fw, "idx") else None,
        bwtyp=lindex.bw.type,
        fwtyp=lindex.fw.type
      )

  def create_bottom(self, op, *args):
    if op.is_type(GroupBy):
      return PyGroupByBottomTranslator(op, *args)
    if op.is_type(OrderBy):
      return PyOrderByBottomTranslator(op, *args)
    raise Exception("No Bottom Translator for %s" % op)

  def create_top(self, op, *args):
    if op.is_type(GroupBy):
      return PyGroupByTopTranslator(op, *args)
    if op.is_type(OrderBy):
      return PyOrderByTopTranslator(op, *args)
    raise Exception("No Top Translator for %s" % op)

  def create_left(self, op, *args):
    if op.is_type(HashJoin):
      return PyHashJoinLeftTranslator(op, *args)
    if op.is_type(ThetaJoin):
      return PyThetaJoinLeftTranslator(op, *args)
    raise Exception("No Left Translator for %s" % op)


  def create_right(self, op, *args):
    if op.is_type(HashJoin):
      return PyHashJoinRightTranslator(op, *args)
    if op.is_type(ThetaJoin):
      return PyThetaJoinRightTranslator(op, *args)
    raise Exception("No Right Translator for %s" % op)


  def create_normal(self, op, *args):
    translators = [
        (Project, PyProjectTranslator),
        (Limit, PyLimitTranslator),
        (Yield, PyYieldTranslator),
        (Print, PyPrintTranslator),
        (Collect, PyCollectTranslator),
        (SubQuerySource, PySubQueryTranslator),
        (Scan, PyScanTranslator),
        (DummyScan, PyDummyScanTranslator),
        (Filter, PyFilterTranslator)
    ]

    for opklass, tklass in translators:
      if op.is_type(opklass):
        return tklass(op, *args)

    raise Exception("No Translator for %s" % op)
