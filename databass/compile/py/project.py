from ..project import *
from ..translator import *
from .translator import *

class PyProjectTranslator(ProjectTranslator, PyTranslator):

  def produce(self, ctx):
    """
    Setup output tuple variable and generate code to initialize it 
    as an empty tuple with the correct schema..  

    There is a special case when if there is no child operator, such as
    
            SELECT 1
            
    where produce should pretend it is an access method that emits a 
    single empty tuple to its own consume method.
    """
    self.v_out = self.compile_new_tuple(ctx, self.op.schema, "proj_row")

    if self.op.c == None:
      ctx.request_vars(dict(row=self.v_out))
      self.consume(ctx)
      return

    ctx.request_vars(dict(row=None))
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.v_in = ctx['row']
    ctx.pop_vars()

    v_exprs = self.compile_exprs(ctx, self.op.exprs, self.v_in) 
    ctx.add_line("{out}.row[:] = [{exprs}]", 
        out=self.v_out, exprs=",".join(v_exprs))
    ctx['row'] = self.v_out
    self.parent_translator.consume(ctx)

