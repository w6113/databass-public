from ...ops import HashJoin
from ..hashjoin import *
from .translator import *


class PyHashJoinLeftTranslator(HashJoinLeftTranslator, PyTranslator):
  """
  The left translator scans the left child and populates the hash table
  """
  def produce(self, ctx):
    """
    Produce's job is to 
    1. allocate variable names and create hash table
    2. request the child operator's variable name for the current row
    3. ask the child to produce
    """
    self.v_ht = ctx.new_var("hjoin_ht")

    if not self.l_capture:
      htinit = "defaultdict(list)"
    else:
      htinit = "defaultdict(lambda: [[], []])"
    ctx.declare(self.v_ht, htinit)

    ctx.request_vars(dict(row=None))
    self.child_translator.produce(ctx)


  def consume(self, ctx):
    """
    Given variable name for left row, compute left key and add a copy of the current row
    to the hash table
    """
    v_lkey = ctx.new_var("hjoin_lkey")
    v_lrow = ctx['row']
    ctx.pop_vars()

    
    ctx.add_line("# left probe key: %s" % self.op.join_attrs[0])
    v_lkey = self.compile_expr(ctx, self.op.join_attrs[0], v_lrow)
    v_bucket = "%s[%s]" % (self.v_ht, v_lkey)
    if self.l_capture:
      ctx.add_line("{bucket}[0].append({lrow}.copy())", bucket=v_bucket, lrow=v_lrow)
      ctx.add_line("{bucket}[1].append({l_i})", bucket=v_bucket, l_i=self.l_i)
    else:
      ctx.add_line("{bucket}.append({lrow}.copy())", bucket=v_bucket, lrow=v_lrow)


class PyHashJoinRightTranslator(HashJoinRightTranslator, PyRightTranslator):
  """
  The right translator scans the right child, and probes the hash table
  """

  def produce(self, ctx):
    """
    Allocates intermediate join tuple and asks the child to produce tuples (for the probe)
    """

    self.v_irow = self.compile_new_tuple(ctx, self.op.schema, "hjoin_row")

    if self.l_capture:
      size = "%s+1"%self.left.l_i if self.left.l_capture else None
      self.initialize_lineage_indexes(ctx, left_size=size)

    ctx.request_vars(dict(row=None))
    self.child_translator.produce(ctx)

    if self.l_capture:
      self.clean_prev_lineage_indexes()

  def consume(self, ctx):
    """
    Given variable name for right row, 
    1. compute right key, 
    2. probe hash table, 
    3. create intermediate row to pass to parent's consume

    Note that because the hash key may not be unique, it's good hygiene
    to check the join condition again when probing.
    """
    # reference to the left translator's hash table variable
    v_ht = self.left.v_ht

    v_lrow = ctx.new_var("hjoin_lrow")
    v_rkey = ctx.new_var("hjoin_rkey")
    v_rrow = ctx['row']
    ctx.pop_vars()

    
    # compute probe key 
    ctx.add_line("# probe ht with: %s" % self.op.join_attrs[1])
    v_rkey = self.compile_expr(ctx, self.op.join_attrs[1], v_rrow)

    # continue probe loop if no match
    with ctx.indent("if {rkey} not in {ht}:", rkey=v_rkey, ht=v_ht):
      ctx.add_line("continue")

    # build intermediate row f
    nlattrs = len(self.op.l.schema.attrs)
    ctx.add_line("{irow}.row[{n}:] = {rrow}.row", 
        irow=self.v_irow, n=nlattrs, rrow=v_rrow)

    v_bucket = "%s[%s]" % (v_ht, v_rkey)
    v_group = "%s[0]" % v_bucket if self.left.l_capture else v_bucket
    l_idx = ctx.new_var("l_idx")
    cond = "for {idx}, {lrow} in enumerate({group}):"
    with ctx.indent(cond, lrow=v_lrow, group=v_group, idx=l_idx):
      ctx.add_line("{irow}.row[:{n}] = {lrow}.row",
          irow=self.v_irow, n=nlattrs, lrow=v_lrow)

      # capture lineage
      if self.l_capture:
        ctx.add_line("{l_o} += 1", l_o=self.l_o)

        # left side
        if self.left.l_capture:
          ctx.add_line("left_idx = {bucket}[1][{idx}]", 
              bucket=v_bucket, idx=l_idx)
          for lindex in self.left.lindexes:
            lindex.fw.add_1("left_idx", self.l_o)
            lindex.bw.append_1("left_idx")

        # right side
        for lindex in self.lindexes:
          lindex.fw.add_1(self.l_i, self.l_o)
          lindex.bw.append_1(self.l_i)
  
      ctx['row'] = self.v_irow
      self.parent_translator.consume(ctx)




