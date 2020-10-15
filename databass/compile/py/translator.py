"""
Implements expression compilation helpers used by the rest of the translators
"""
from ..translator import *

class PyTranslator(Translator):

  def can_inline(self, e):
    return len(e.collect([AggFunc])) == 0

  def compile_expr(self, ctx, expr, *args):
    """
    Compiles code to evaluate expression, and saves it in a new variable
    @return var name containing expression result
    """
    if self.can_inline(expr):
      return self.compile_expr_inline(ctx, expr, *args)

    funcs = [
        (Attr, self.attr),
        (Between, self.between),
        (Paren, self.paren),
        (AggFunc, self.agg_func),
        (ScalarFunc, self.scalar_func),
        (Literal, self.literal),
        (Expr, self.expr)
    ]

    for opklass, f in funcs:
      if isinstance(expr, opklass): 
        return f(ctx, expr, *args)

    raise Exception("No Translator for Expr %s" % expr)

  def compile_expr_inline(self, ctx, expr, *args):
    """
    Expressions that don't contain agg functions can be directly inlined.

    For the following expression:
           +
          / \
        a    1

    compile_expr will typically generate "v0 = a + 1" and return "v0"
    this method directly returns "a+1" so it can be inlined.

    @return compiled expression that can be inlined by the caller
    """
    funcs = [
        (Attr, self.attr_inline),
        (Between, self.between_inline),
        (Paren, self.paren_inline),
        (ScalarFunc, self.scalar_func_inline),
        (Literal, self.literal_inline),
        (Expr, self.expr_inline)
    ]

    for opklass, f in funcs:
      if isinstance(expr, opklass): 
        return f(ctx, expr, *args)
    raise Exception("No Translator for Expr %s" % expr)


  def compile_exprs(self, ctx, exprs, v_in, *args):
    """
    @return [varname,] list of expression results
    """
    v_outs = []
    for e in exprs:
      v_outs.append(self.compile_expr(ctx, e, v_in, *args))
    return v_outs

  def codegen_schema(self, schema):
    """
    @return compiled code that constructs a new Schema object given @schema
    """
    def codegen_attr(attr):
      attrs_to_keep = ["typ", "tablename"]
      args = ["'%s'" % attr.aname]
      for key in attrs_to_keep:
        val = attr.__dict__[key]
        if isinstance(val, str):
          args.append("'%s'" % val)
        else:
          args.append(str(val))
      return "Attr(%s)" % ", ".join(args)

    attrs = ",".join(map(codegen_attr, schema.attrs))
    return "Schema([%s])" % attrs

  def compile_new_tuple(self, ctx, schema, varname=None):
    """
    Helper function to initialize a new tuple with @schema
    @return var name that references the new tuple
    """
    v_out = ctx.new_var(varname or "newtup")
    code = "ListTuple(%s)" % self.codegen_schema(schema)
    ctx.set(v_out, code)
    return v_out
    
  #
  # The following are helper functions for compiling Expr objects
  # 


  def expr(self, ctx, e, v_in):
    """
    @ctx compiler context
    @e expression
    @v_in variable name of input tuple

    Assigns a new variable to the result of @e and returns it
    """
    v_out = ctx.new_var("e_bi_out")

    # A3: implement me
    #     make sure to support unary and binary expressions
    #     a unary expression is when e.r is None
    raise Exception("Not Implemented")
    return v_out

  def expr_inline(self, ctx, e, v_in):
    """
    @return inlined compiled expression
    """

    # A3: implement me
    raise Exception("Not Implemented")

  def paren(self, ctx, e, v_in):
    return self.compile_expr(ctx, e.c, v_in)

  def paren_inline(self, ctx, e, v_in):
    return "(%)" % self.compile_expr_inline(ctx, e, v_in)

  def between(self, ctx, e, v_in):
    v_out = ctx.new_var("e_btwn_out")
    v_e = self.compile_expr(ctx, e.expr, v_in)
    v_l = self.compile_expr(ctx, e.lower, v_in)
    v_u = self.compile_expr(ctx, e.upper, v_in)
    ctx.set(v_out, "({l} <= {e}) and ({e} <= {u})", e=v_e, l=v_l, u=v_u)
    return v_out

  def between_inline(self, ctx, e, v_in):
    v = self.compile_expr_inline(ctx, e.expr, v_in)
    l = self.compile_expr_inline(ctx, e.lower, v_in)
    u = self.compile_expr_inline(ctx, e.upper, v_in)
    return "({l} <= {v}) and ({v} <= {u})".format(v=v, l=l, u=u)

  def agg_func(self, ctx, e, v_in):
    """
    This is "sloppy" compilation because it still relies on 
    the UDFRegistry and calls the UDF in interpreted mode.

    See UDFTranslator to be able to directly generate code for UDFs.
    """
    v_out = ctx.new_var("e_agg_out")
    v_irow = ctx.new_var("irow")
    v_args = ctx.new_var("args")

    ctx.add_line("{args} = [[] for i in range({n})]",
      args=v_args, n=len(e.args))

    # Iterate through the rows in the group to evaluate each argument
    with ctx.indent("for {irow} in {v_in}: ", irow=v_irow, v_in=v_in):
      for idx, arg in enumerate(e.args):
        v_arg = self.compile_expr(ctx, arg, v_irow)
        ctx.add_line("%s[%d].append(%s)" % (v_args, idx, v_arg))

    line = "%s = UDFRegistry.registry()['%s'](*%s)" % (v_out, e.name, v_args)
    ctx.add_line(line)
    return v_out

  def scalar_func(self, ctx, e, v_in):
    v_out = ctx.new_var("e_scalar_out")
    vlist = [self.compile_expr(ctx, arg, v_in) for arg in e.args]
    vlist = ", ".join(vlist)

    line = "%s = UDFRegistry.registry()['%s'](%s)" % (v_out, e.name, vlist)
    ctx.add_line(line)
    return v_out

  def scalar_func_inline(self, ctx, e, v_in):
    vs = ["(%s)" % self.compile_expr_inline(ctx, arg, v_in) for arg in e.args]
    return "UDFRegistry.registry()['%s'](%s)" % (e.name, ", ".join(vs))

  def literal(self, ctx, e, v_in):
    v_out = ctx.new_var("e_lit_out")
    ctx.set(v_out, e)
    return v_out

  def literal_inline(self, ctx, e, v_in):
    return str(e)

  def attr(self, ctx, e, v_in):
    # A3: implement me
    raise Exception("Not Implemented")

  def attr_inline(self, ctx, e, v_in):
    # A3: implement me
    raise Exception("Not Implemented")


class PyRightTranslator(RightTranslator, PyTranslator):

  def initialize_lineage_indexes(self, ctx, left_size=None, right_size=None):
    if self.left.l_capture:
      ctx.add_line("# {op} left", op=self.op)
      for lindex in self.left.lindexes:
        lindex.bw.initialize()
        lindex.fw.initialize(left_size)

    if self.l_capture:
      ctx.add_line("# {op} right", op=self.op)
      for lindex in self.lindexes:
        lindex.bw.initialize()
        lindex.fw.initialize(right_size)



