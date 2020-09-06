from ...ops import GroupBy
from ..translator import *
from ..orderby import *
from .translator import *

class PyOrderByBottomTranslator(OrderByBottomTranslator, PyTranslator):

  def produce(self, ctx):
    self.v_rows = ctx.new_var("ord_rows")
    self.v_keyf = ctx.new_var("ord_keyf")
    v_ordersort = ctx.new_var("ordersort")

    asc_args = ", ".join(["%s" % '1' if (e == "asc") else '-1'
      for (e) in self.op.ascdescs])
    ctx.set(self.v_rows, "[]")
    ctx.set(v_ordersort, "[%s]" % asc_args)

    # Generate key function for sort(key=?)
    with ctx.indent("def %s(arg):" % self.v_keyf):
      v_all = self.compile_exprs(ctx, self.op.order_exprs, "row")
      ctx.add_lines([
        "row, l_i = arg",
        "return OBTuple(({v_all},), {order})"
        ], v_all=", ".join(v_all), order=v_ordersort)


    ctx.request_vars(dict(row=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    v_in = ctx['row']
    ctx.pop_vars()

    tmp = self.compile_new_tuple(ctx, self.op.schema, "ord_row")
    ctx.add_lines([
      "{tmp}.row = list({v_in}.row)", 
      "{rows}.append(({tmp}, {l_i}))"],
      tmp=tmp, v_in=v_in, rows=self.v_rows, l_i=self.l_i)
    

class PyOrderByTopTranslator(OrderByTopTranslator, PyTranslator):

  def produce(self, ctx):
    if self.l_capture:
      ctx.add_line("# {op}", op=self.op)
      size = "len(%s)" % self.bottom.v_rows
      for lindex in self.lindexes:
        lindex.fw.initialize(size)
        lindex.bw.initialize(size)

    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.consume(ctx)
    if self.l_capture:
      self.clean_prev_lineage_indexes()


  def consume(self, ctx):
    v_irow = ctx.new_var("ord_irow")
    l_i = ctx.new_var("ord_l_i")

    ctx.add_line("{rows}.sort(key={keyf})", 
        rows=self.bottom.v_rows, keyf=self.bottom.v_keyf)

    with ctx.indent("for ({irow}, {l_i}) in {rows}:",
        irow=v_irow, l_i=l_i, rows=self.bottom.v_rows):

      # lineage capture
      if self.l_capture:
        ctx.add_line("{l_o} += 1", l_o=self.l_o)
        for lindex in self.lindexes:
          lindex.fw.set_1(l_i, self.l_o)
          lindex.bw.append_1(l_i)

      ctx['row'] = v_irow
      self.parent_translator.consume(ctx)

