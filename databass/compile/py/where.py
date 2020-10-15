from ..translator import *
from ..where import *
from .translator import *


class PyFilterTranslator(FilterTranslator, PyTranslator):
  def consume(self, ctx):
    v_in = ctx['row']

    # add a comment in the compiled code
    ctx.add_line("# if %s" % str(self.op.cond))

    # compile the expression into an "if" statement
    v_cond = self.compile_expr(ctx, self.op.cond, v_in)
    with ctx.indent("if {p}:", p=v_cond):
      self.parent_translator.consume(ctx)

