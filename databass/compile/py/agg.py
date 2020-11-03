from ..translator import *
from ...exprs import *
from ..agg import *
from .translator import *
from .udfs import *


class PyGroupByBottomTranslator(GroupByBottomTranslator, PyTranslator):
  def produce(self, ctx):
    """
    Produce sets up the variables and hash table so that they can be populated by
    calling child's produce (which eventually calls self.consume). 
    
    """
    self.v_ht = ctx.new_var("gb_ht")
    self.v_irow = self.compile_new_tuple(ctx, self.op.schema, "gb_irow")

    
    # initialize variables in compiled program
    initargs = ["None", "None", "[]"]

    if self.l_i is not None:
      initargs.append("[]")

    htinit = "defaultdict(lambda: [%s])" % ", ".join(initargs)
    ctx.declare(self.v_ht, htinit)
    ctx.request_vars(dict(row=None))
    self.child_translator.produce(ctx)


  def consume(self, ctx):
    """
    Build hashtable

    For instance, if the query is:

        SELECT a-b, count(d)
        FROM data
        GROUP BY a+b-c

    Each hashtable entry contains the following information

    1. the hash probe key: e.g., hash of a+b-c
    2. values of attributes referenced in the grouping terms e.g., (a, b, c)
       see databass/ops/agg.py for their semantics
    3. the group of tuples
    """


    v_in = ctx['row']
    ctx.pop_vars()
    v_key = ctx.new_var("gb_key")
    v_attrvals = ctx.new_var("gb_attvals") # holds values of Attrs referenced in # grouping expression.
                                           # See self.group_attrs

    ctx.add_line("# Compiling group attrs & exprs")

    
    exprs = self.op.group_attrs + self.op.group_exprs
    v_all = self.compile_exprs(ctx, exprs, v_in)
    v_gattrs = v_all[:len(self.op.group_attrs)]
    v_gexprs = v_all[len(self.op.group_attrs):]

    bucket = ctx.new_var("gb_bucket")
    lines = [
      "",
      "# Add entry to hash table",
      "{vals} = [{attrs}]", 
      "{key} = hash(({exprs}))",
      "{bucket} = %s[{key}]" % self.v_ht,
      "{bucket}[0] = {key}",
      "{bucket}[1] = {vals}"]

    lines.append("{bucket}[2].append({v_in}.copy())")

    if self.l_i is not None:
      lines.append("{bucket}[-1].append({l_i})")

    formatargs = dict(
      bucket=bucket, v_in = v_in, key=v_key, vals=v_attrvals,
      l_i=self.l_i,
      exprs=", ".join(v_gexprs),
      attrs=", ".join(v_gattrs)
    )
    ctx.add_lines(lines, **formatargs)

  
class PyGroupByTopTranslator(GroupByTopTranslator, PyTranslator):

  def produce(self, ctx):
    # row containing attrs referenced in the grouping terms
    # passed to the non-aggregation functions in the target list
    self.v_term_row = self.compile_new_tuple(
        ctx, self.op.group_term_schema, "gb_term_row")

    if self.child_translator:
      self.child_translator.produce(ctx)
    else:
      self.consume(ctx) 

  def consume(self, ctx):
    """
    Loop through populated hash table and construct output tuple per group.  
    After constructing the output tuple, pass it to the parent's consumer.
    Don't forget to tell the parent the variable name containing the tuple.
    """

    v_ht = self.bottom.v_ht
    v_bucket = ctx.new_var("gb_bucket")

    self.initialize_lineage_indexes(ctx)

    # loop through hash table to emit results and call parent's consume
    with ctx.indent("for {bucket} in {ht}.values():", bucket=v_bucket, ht=v_ht): 
      self.populate_lineage_indexes(ctx, v_bucket)
      self.fill_output_row(ctx, v_bucket)

      ctx["row"] = self.bottom.v_irow
      self.parent_translator.consume(ctx)

    self.clean_prev_lineage_indexes()

  def initialize_lineage_indexes(self, ctx):
    if not self.l_capture: return

    ctx.add_line("# {op}", op=self.op)
    for lindex in self.lindexes:
      lindex.fw.initialize("%s+1" % self.bottom.l_i)
      lindex.bw.initialize("len({ht})".format(ht=self.bottom.v_ht))

  def populate_lineage_indexes(self, ctx, v_bucket):
    if not self.l_capture: return

    l_iids = "%s[-1]" % v_bucket
    ctx.add_line("{l_o} += 1", l_o=self.l_o)
    for lindex in self.lindexes:
      lindex.bw.set_n(self.l_o, l_iids) 
      l_tmp = ctx.new_var("l_fw_iid")
      with lindex.fw._loop(l_iids, l_tmp):
        lindex.fw.set_1(l_tmp, self.l_o)

  
  def fill_output_row(self, ctx, v_bucket):
    v_expr_result = None 
    v_grp = ctx.new_var("gb_grp")
    v_attrvals = ctx.new_var("gb_attvals") # holds values of Attrs referenced in # grouping expression.

    ctx.set(v_grp, "{bucket}[2]", bucket=v_bucket)

    aggidx = 0
    for i, e in enumerate(self.op.project_exprs):
      if e.is_type(AggFunc):
        v_expr_result = self.compile_expr(ctx, e, v_grp)
      else:
        ctx.add_line("{terms}.row = {bucket}[1]", terms=self.v_term_row, bucket=v_bucket)
        v_expr_result = self.compile_expr(ctx, e, self.v_term_row)

      ctx.add_line("{irow}.row[{i}] = {expr}",
          irow=self.bottom.v_irow, i=i, expr=v_expr_result)
