from ..translator import *
from ..scan import *
from .translator import *

class PySubQueryTranslator(SubQueryTranslator):
  pass



class PyScanTranslator(ScanTranslator, PyTranslator):
  def produce(self, ctx):
    v_row = ctx.new_var("scan_row")

    ctx.add_line("# scan %s AS %s" % (self.op.tablename, self.op.alias))
    with ctx.indent("for {row} in db['{tname}']:", 
        row=v_row, tname=self.op.tablename):

      # give variable name for the scan row to parent operator
      ctx["row"] = v_row

      if self.l_o is not None:
        ctx.set(self.l_o, "{l_o}+1", l_o=self.l_o)

      if self.child_translator:
        self.child_translator.produce(ctx)
      else:
        self.parent_translator.consume(ctx)


class PyDummyScanTranslator(ScanTranslator, PyTranslator):
  def produce(self, ctx):
    v_row = ctx.new_var("dummy_row")
    ctx["row"] = v_row

    # no lineage work to be done

    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.parent_translator.consume(ctx)
